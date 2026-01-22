# RFC 0003: Ingest Coordination

**Status**: Draft

**Authors**:
- [Your Name](https://github.com/username)

## Summary

This RFC proposes a unified write coordination system for OpenData's storage subsystems (timeseries, vector). The design uses epoch-based durability tracking, where every write is assigned a monotonic epoch number and `flush()` guarantees durability up to a specific epoch. A single coordinator manages write batching, flush execution, and snapshot broadcasting, providing consistent semantics across subsystems while allowing domain-specific customization through traits.

## Motivation

OpenData's storage subsystems share common write coordination needs:

1. **Batching** — Buffer writes in memory for efficiency before flushing to SlateDB
2. **Durability** — Provide clear guarantees: when `flush()` returns, writes are safe
3. **Consistency** — Queries should see a consistent snapshot, not partial flushes
4. **Backpressure** — Bound memory usage under load
5. **Failure handling** — Propagate errors without data loss

The existing v1 implementations in `MiniTsdb` and `VectorDb` share a common pattern: a `Mutex<Delta>` for accumulation, a `flush_mutex` for single-flush-at-a-time, and a `RwLock<Arc<Snapshot>>` for reads. However, this approach has several known issues:

**Issue #82: Cache synchronization problems (timeseries)**
- Ingest cache eviction can discard unflushed deltas, causing data loss
- Separate ingest and query caches can hold divergent snapshots, causing stale reads
- Each `MiniTsdb` maintains its own snapshot, leading to inconsistency across buckets

**Issue #95: Race condition in series ID assignment (timeseries)**
- Series IDs are assigned to an in-memory `DashMap` during ingestion
- A flush may persist samples referencing an ID before the dictionary entry for that ID is flushed
- On crash recovery, orphaned samples exist with no series metadata

**General issues across both subsystems:**
- **Ambiguous flush semantics** — A `write()` followed by `flush()` may not actually flush that write if a flush was already in progress
- **Limited backpressure** — Only channel depth is bounded, not total buffered data size

A unified coordinator addresses these issues by serializing all writes through a single channel with epoch-based tracking, ensuring deterministic flush ordering and clear durability guarantees.

## Goals

- Clear epoch-based durability guarantees
- Single source of truth for snapshots via broadcast channel
- Backpressure on both queue depth and total buffered size
- Composable design via traits for subsystem-specific flush logic
- Explicit state machine with predictable transitions

## Non-Goals

- Retry mechanisms for transient failures (deferred to future work)
- Reading from in-memory buffers (queries use flushed snapshots only)
- Cross-bucket or cross-subsystem coordination
- Distributed coordination (single-node only)

## Design

### Epoch-Based Tracking

Every write is assigned a monotonically increasing epoch number. **Epochs are maintained in memory only** — they are not persisted to storage. This provides unambiguous ordering and enables precise flush/durability guarantees:

```
Write(W1) -> epoch 1
Write(W2) -> epoch 2
Write(W3) -> epoch 3
Flush()   -> "wait for epoch 3 to be durable"
Write(W4) -> epoch 4
Flush()   -> "wait for epoch 4 to be durable"
```

The coordinator tracks two distinct watermarks:

| Watermark | Meaning |
|-----------|---------|
| `flushed_epoch` | Highest epoch reflected in the current snapshot (delta applied to SlateDB memtable) |
| `durable_epoch` | Highest epoch persisted to object storage (SlateDB WAL flush complete) |

**Important:** These are separate events. When a flush completes, it updates the snapshot and advances `flushed_epoch`. However, the data is not yet durable — SlateDB buffers writes in its WAL and asynchronously flushes to object storage. True durability occurs when SlateDB confirms the WAL has been persisted.

Flush waiters requesting durability guarantees should be notified based on `durable_epoch`, not `flushed_epoch`. Until SlateDB exposes a "last durable sequence number" API (which is tracked internally and expected to be available soon), we have two options:
1. Treat `flushed_epoch` as a best-effort durability proxy
2. Have `flush()` return an explicit "durability unavailable" indicator for systems requiring strict guarantees

### State Machine

The coordinator follows an explicit state machine:

```
                    ┌─────────────────────────────────┐
                    │                                 │
                    ▼                                 │
    ┌───────────────────────────────┐                │
    │            IDLE               │                │
    │  (pending empty, no flush)    │                │
    └───────────────────────────────┘                │
                    │                                 │
                    │ Write arrives                   │
                    ▼                                 │
    ┌───────────────────────────────┐                │
    │        ACCUMULATING           │ ◄──────────────┤
    │  (pending has data, no flush) │                │
    └───────────────────────────────┘                │
                    │                                 │
                    │ Flush triggered                 │
                    │ (timer or explicit)             │
                    ▼                                 │
    ┌───────────────────────────────┐                │
    │          FLUSHING             │                │
    │  (flush in progress,          │                │
    │   new writes go to fresh      │────────────────┘
    │   pending buffer)             │   Flush completes
    └───────────────────────────────┘
                    │
                    │ Flush fails
                    ▼
    ┌───────────────────────────────┐
    │           FAILED              │
    │  (propagate error to callers) │
    └───────────────────────────────┘
```

Key behaviors:
- **Single pending buffer** — All writes serialize through one mpsc channel to the coordinator task. Since writes are processed sequentially, only one pending delta is needed at any time. Multiple pending deltas would add complexity without benefit.
- **Non-blocking writes during flush** — When a flush starts, the current pending delta is taken and a fresh empty delta begins accumulating new writes. This allows writes to continue unblocked during flush I/O.
- **No separate sync write path** — Because all writes flow through the channel, write ordering is guaranteed. Synchronous durability is simply `write()` + `flush(epoch)`, now exposed as `write_durable()`.
- **Failure propagation** — Failed flushes notify waiters with errors. All failures are treated as permanent for now; retry mechanisms can be added later without protocol changes.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        WriteCoordinator                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │ EpochTracker│  │ DeltaBuffer │  │    FlushExecutor        │ │
│  │             │  │             │  │                         │ │
│  │ next: u64   │  │ pending: D  │  │ in_flight: Option<...>  │
│  │ flushed: u64│  │             │  │ waiters: BTreeMap       │ │
│  │ durable: u64│  │             │  │                         │ │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    SnapshotBroadcaster                      ││
│  │  tx: watch::Sender<Arc<Snapshot>>                           ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
         ▲                                          │
         │ WriteCommand                             │ watch::Receiver
         │                                          ▼
    ┌─────────┐                              ┌─────────────┐
    │ Clients │                              │   Readers   │
    └─────────┘                              └─────────────┘
```

**Components:**

- **EpochTracker** — Assigns epochs and tracks flushed/durable watermarks
- **DeltaBuffer** — Accumulates pending writes with size tracking for backpressure
- **FlushExecutor** — Manages in-flight flush tasks and epoch-indexed waiters
- **SnapshotBroadcaster** — Single `watch::channel` for all readers to subscribe

### Command Protocol

Clients interact via an async channel with four command types:

| Command | Purpose |
|---------|---------|
| `Write { delta, size_hint }` | Submit a delta, receive assigned epoch |
| `Flush { epoch }` | Wait for specific epoch (or all pending) to be durable |
| `Subscribe` | Get a `watch::Receiver` for snapshot updates |
| `Shutdown` | Graceful shutdown after flushing pending writes |

The `write_durable()` convenience method combines `write()` + `flush(epoch)` for synchronous durability.

### Composability via Traits

Subsystems implement two traits to customize behavior:

**Delta** — Defines how writes are merged:
```rust
pub trait Delta: Send + 'static {
    fn is_empty(&self) -> bool;
    fn merge(&mut self, other: Self);
    fn size_estimate(&self) -> usize;
}
```

**FlushHandler** — Defines how deltas are persisted:
```rust
pub trait FlushHandler: Send + Sync + 'static {
    type Delta: Delta;
    type Snapshot: Clone + Send + Sync + 'static;

    async fn execute_flush(
        &self,
        delta: &Self::Delta,
        current_snapshot: &Self::Snapshot,
    ) -> Result<Self::Snapshot, FlushError>;
}
```

This separation keeps the coordinator generic while allowing timeseries and vector subsystems to define their own delta structures and flush logic.

### Backpressure

The coordinator bounds memory through two mechanisms:

1. **Channel capacity** — Limits queued commands (existing behavior)
2. **Pending size threshold** — Forces flush when buffered delta exceeds `max_pending_bytes`

When the size threshold is reached, the coordinator initiates a flush before accepting more writes, preventing unbounded memory growth under sustained load.

### Flush Triggers

Flushes are initiated by:

1. **Explicit request** — Client calls `flush()` or `write_durable()`
2. **Timer** — Periodic flush interval (e.g., 30 seconds)
3. **Size threshold** — Pending delta exceeds configured limit

The main loop uses `tokio::select!` with biased priority: flush completion > commands > timer > size-based flush.

### Waiter Management

Flush waiters are stored in a `BTreeMap<u64, Vec<Sender>>` keyed by epoch. When `durable_epoch` advances, waiters at or below that epoch are notified via `split_off()`, which efficiently partitions the map.

### Snapshot Broadcasting

A single `watch::channel` holds the current snapshot. Benefits:

- **Single source of truth** — No stale copies
- **Lazy cloning** — Readers borrow via `borrow()`, only cloning when needed
- **Automatic updates** — New values automatically visible to all subscribers

Readers call `subscribe()` once and receive updates automatically.

### Future: Consistent Reads from In-Memory Buffer

This RFC scopes reads to flushed snapshots only. However, a future enhancement could allow queries to also consult the in-memory pending delta for fresher data. The key synchronization challenge is: how do we atomically update the snapshot while retiring (purging) the in-flight delta?

Because the coordinator loop is single-threaded, updating `current_snapshot`, advancing `flushed_epoch`, and clearing the in-flight delta all occur within one loop iteration. This is effectively atomic with respect to readers — no reader can observe an intermediate state.

To support consistent reads that span both the snapshot and in-memory deltas:
1. Attach an epoch fence to both the snapshot and any in-flight delta
2. Readers receive (snapshot, optional delta, read_epoch) tuple
3. The delta is only retired after the snapshot epoch advances past the delta's `end_epoch`

This approach allows read-your-writes semantics without complex locking, but is deferred until there's a concrete use case.

### API and Usage

The coordinator exposes a simple async API through `CoordinatorHandle`. The interesting differences between subsystems are in how they implement the `Delta` and `FlushHandler` traits.

#### TimeSeries: Fingerprint-Based Deduplication

TimeSeries identifies series by a fingerprint (hash of sorted labels). The delta merges samples for the same fingerprint, and ID resolution is deferred to flush time.

**Delta merge semantics** — same fingerprint means same series, so samples accumulate:

```rust
impl Delta for TsdbDelta {
    fn merge(&mut self, other: Self) {
        for (fingerprint, other_series) in other.series {
            match self.series.entry(fingerprint) {
                Occupied(entry) => {
                    // Same series: append samples
                    entry.get_mut().samples.extend(other_series.samples);
                }
                Vacant(entry) => {
                    entry.insert(other_series);
                }
            }
        }
    }
}
```

**Flush behavior** — series IDs are resolved lazily from storage, ensuring the dictionary entry and samples are always flushed together (fixing Issue #95). Inverted index entries are batched by label across all series to minimize record count:

```rust
impl FlushHandler for TsdbFlushHandler {
    async fn execute_flush(&self, delta: &TsdbDelta, ...) -> Result<Snapshot, FlushError> {
        // Batch inverted index entries by label across all series
        let mut inverted_index: HashMap<Label, RoaringBitmap> = HashMap::new();

        for (fingerprint, series) in &delta.series {
            // Lazy resolution: lookup or create ID from storage
            let series_id = self.storage
                .get_or_create_series_id(self.bucket, *fingerprint)
                .await?;

            ops.push(insert_forward_index(series_id, &series.spec));
            ops.push(merge_samples(series_id, &series.samples));

            // Accumulate series IDs per label for batched inverted index
            for label in &series.spec.labels {
                inverted_index.entry(label.clone()).or_default().insert(series_id);
            }
        }

        // Write one inverted index record per label (not per series)
        for (label, series_ids) in inverted_index {
            ops.push(merge_inverted_index(&label, series_ids));
        }

        self.storage.apply(ops).await?;
        // ...
    }
}
```

#### Vector: Upsert with Internal ID Allocation

Vector identifies records by an external string ID. The delta overwrites earlier writes for the same ID, and internal IDs are allocated at flush time to handle upserts correctly.

**Delta merge semantics** — same external ID means overwrite (last write wins):

```rust
impl Delta for VectorDbDelta {
    fn merge(&mut self, other: Self) {
        for (external_id, vector) in other.vectors {
            // Later write for same ID replaces earlier
            self.vectors.insert(external_id, vector);
        }
    }
}
```

**Flush behavior** — must handle upserts by looking up existing internal IDs and managing deletions:

```rust
impl FlushHandler for VectorDbFlushHandler {
    async fn execute_flush(&self, delta: &VectorDbDelta, ...) -> Result<Snapshot, FlushError> {
        for (external_id, vector) in &delta.vectors {
            // Check if this is an upsert (external ID already exists)
            let old_internal_id = self.storage
                .lookup_internal_id(external_id)
                .await?;

            // Allocate fresh internal ID
            let new_internal_id = self.id_allocator.allocate_one().await?;

            // Update external -> internal mapping
            ops.push(put_id_dictionary(external_id, new_internal_id));

            // If upsert: mark old vector as deleted
            if let Some(old_id) = old_internal_id {
                ops.push(merge_deleted_bitmap(old_id));
                ops.push(delete_vector_data(old_id));
            }

            // Write new vector data and assign to centroid
            ops.push(put_vector_data(new_internal_id, &vector.values));
            let centroid = find_nearest_centroid(&vector.values, snapshot);
            ops.push(merge_posting_list(centroid, new_internal_id));
        }
        self.storage.apply(ops).await?;
        // ...
    }
}
```

#### Summary of Differences

| Aspect | TimeSeries | Vector |
|--------|------------|--------|
| Delta key | Fingerprint (label hash) | External ID (string) |
| Merge behavior | Append samples | Overwrite entire record |
| ID source | Storage get-or-create | Sequence allocator |
| Upsert handling | Implicit (same fingerprint) | Explicit delete + insert |
| Index structures | Forward + inverted index | Posting lists + deleted bitmap |

## Alternatives

### Per-Write Response Channels

Instead of epoch-based tracking, each write could include a response channel that's notified on flush. This was rejected because:

- Higher overhead (one channel per write vs. shared waiters map)
- Harder to implement "flush all pending" semantics
- Epoch watermarks are simpler to reason about

### Multiple Pending Buffers

The design could maintain a queue of pending deltas instead of one. This was rejected because:

- All writes serialize through one channel anyway
- Single buffer is simpler and sufficient
- Multiple buffers add complexity without benefit

### Transient vs. Permanent Failure Classification

The design could distinguish transient failures (retry) from permanent failures (propagate). This was deferred because:

- Adds complexity before we understand failure modes
- SlateDB failures are currently unclear on transience
- Retry logic can be added later without protocol changes

### In-Memory Read Path

Queries could consult both the snapshot and pending deltas for fresher data. This was deferred because:

- Adds synchronization complexity
- Snapshot-only reads are simpler and consistent
- Can be added later with epoch fencing

## Open Questions

1. **SlateDB durable watermark** — How will we integrate with SlateDB's durability notification? Options:
   - Callback registration
   - Periodic polling
   - Treat `flushed_epoch` as sufficient

2. **Failure recovery** — When a flush fails, should we:
   - Discard the delta (current: yes, with error propagation)
   - Retain for retry (future enhancement)
   - Mark coordinator as failed, requiring restart

3. **Metrics integration** — What metrics should the coordinator expose? Candidates:
   - Write throughput / latency by epoch
   - Flush duration / success rate
   - Pending buffer size
   - Waiter queue depth

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-22 | Initial draft |
