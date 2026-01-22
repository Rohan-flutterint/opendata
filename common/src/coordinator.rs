//! Write coordination system for OpenData storage subsystems.
//!
//! Provides epoch-based durability tracking and unified write coordination.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, watch};

/// Epoch number representing a write's position in the write stream.
pub type Epoch = u64;

/// Error type for flush operations.
#[derive(Debug, Clone)]
pub enum FlushError {
    Storage(String),
    Internal(String),
}

impl std::fmt::Display for FlushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlushError::Storage(s) => write!(f, "storage error: {}", s),
            FlushError::Internal(s) => write!(f, "internal error: {}", s),
        }
    }
}

impl std::error::Error for FlushError {}

/// Trait for delta types that accumulate writes.
pub trait Delta: Send + 'static {
    /// Returns true if the delta has no data.
    fn is_empty(&self) -> bool;

    /// Merge another delta into this one.
    fn merge(&mut self, other: Self);

    /// Estimate the size in bytes of this delta.
    fn size_estimate(&self) -> usize;
}

/// Trait for handling flush operations.
#[async_trait]
pub trait FlushHandler: Send + Sync + 'static {
    type Delta: Delta;
    type Snapshot: Clone + Send + Sync + 'static;

    /// Execute a flush operation, applying the delta to create a new snapshot.
    async fn execute_flush(
        &self,
        delta: Self::Delta,
        current_snapshot: &Self::Snapshot,
    ) -> Result<Self::Snapshot, FlushError>;

    /// Create an empty delta.
    fn empty_delta(&self) -> Self::Delta;
}

/// Configuration for the write coordinator.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Maximum pending bytes before forcing a flush.
    pub max_pending_bytes: usize,
    /// Flush interval for periodic flushes.
    pub flush_interval: Duration,
    /// Channel capacity for commands.
    pub channel_capacity: usize,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            max_pending_bytes: 64 * 1024 * 1024, // 64MB
            flush_interval: Duration::from_secs(30),
            channel_capacity: 1024,
        }
    }
}

/// Commands sent to the coordinator.
enum Command<D: Delta, S> {
    Write {
        delta: D,
        response: oneshot::Sender<Epoch>,
    },
    Flush {
        epoch: Option<Epoch>,
        response: oneshot::Sender<Result<(), FlushError>>,
    },
    Subscribe {
        response: oneshot::Sender<watch::Receiver<Arc<S>>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<(), FlushError>>,
    },
}

/// Handle for clients to interact with the coordinator.
pub struct CoordinatorHandle<D: Delta, S> {
    tx: mpsc::Sender<Command<D, S>>,
    snapshot_rx: watch::Receiver<Arc<S>>,
}

impl<D: Delta, S: Clone + Send + Sync + 'static> Clone for CoordinatorHandle<D, S> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            snapshot_rx: self.snapshot_rx.clone(),
        }
    }
}

impl<D: Delta, S: Clone + Send + Sync + 'static> CoordinatorHandle<D, S> {
    /// Submit a write and return its epoch.
    pub async fn write(&self, delta: D) -> Result<Epoch, FlushError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(Command::Write {
                delta,
                response: response_tx,
            })
            .await
            .map_err(|_| FlushError::Internal("coordinator shut down".into()))?;

        response_rx
            .await
            .map_err(|_| FlushError::Internal("coordinator dropped response".into()))
    }

    /// Flush up to the given epoch (or all pending if None) and wait for durability.
    pub async fn flush(&self, epoch: Option<Epoch>) -> Result<(), FlushError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(Command::Flush {
                epoch,
                response: response_tx,
            })
            .await
            .map_err(|_| FlushError::Internal("coordinator shut down".into()))?;

        response_rx
            .await
            .map_err(|_| FlushError::Internal("coordinator dropped response".into()))?
    }

    /// Get a receiver for snapshot updates.
    pub fn subscribe(&self) -> watch::Receiver<Arc<S>> {
        self.snapshot_rx.clone()
    }

    /// Get the current snapshot.
    pub fn current_snapshot(&self) -> Arc<S> {
        self.snapshot_rx.borrow().clone()
    }

    /// Shutdown the coordinator gracefully.
    pub async fn shutdown(&self) -> Result<(), FlushError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(Command::Shutdown {
                response: response_tx,
            })
            .await
            .map_err(|_| FlushError::Internal("coordinator shut down".into()))?;

        response_rx
            .await
            .map_err(|_| FlushError::Internal("coordinator dropped response".into()))?
    }
}

/// State of the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoordinatorState {
    Idle,
    Accumulating,
    Flushing,
    Failed,
}

/// The write coordinator manages write batching, flush execution, and snapshot broadcasting.
pub struct WriteCoordinator<F: FlushHandler> {
    config: CoordinatorConfig,
    handler: Arc<F>,
    rx: mpsc::Receiver<Command<F::Delta, F::Snapshot>>,
    snapshot_tx: watch::Sender<Arc<F::Snapshot>>,

    // Epoch tracking
    next_epoch: Epoch,
    flushed_epoch: Epoch,

    // Pending delta
    pending: F::Delta,
    pending_size: usize,

    // Flush waiters (keyed by epoch they're waiting for)
    waiters: BTreeMap<Epoch, Vec<oneshot::Sender<Result<(), FlushError>>>>,

    // State
    state: CoordinatorState,
}

impl<F: FlushHandler> WriteCoordinator<F> {
    /// Create a new coordinator and return the handle for clients.
    pub fn new(
        config: CoordinatorConfig,
        handler: Arc<F>,
        initial_snapshot: F::Snapshot,
    ) -> (Self, CoordinatorHandle<F::Delta, F::Snapshot>) {
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let (snapshot_tx, snapshot_rx) = watch::channel(Arc::new(initial_snapshot));

        let empty_delta = handler.empty_delta();

        let coordinator = Self {
            config,
            handler,
            rx,
            snapshot_tx,
            next_epoch: 1,
            flushed_epoch: 0,
            pending: empty_delta,
            pending_size: 0,
            waiters: BTreeMap::new(),
            state: CoordinatorState::Idle,
        };

        let handle = CoordinatorHandle { tx, snapshot_rx };

        (coordinator, handle)
    }

    /// Run the coordinator loop.
    pub async fn run(mut self) {
        let mut flush_interval = tokio::time::interval(self.config.flush_interval);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;

                // Handle incoming commands
                cmd = self.rx.recv() => {
                    match cmd {
                        Some(Command::Write { delta, response }) => {
                            self.handle_write(delta, response);
                        }
                        Some(Command::Flush { epoch, response }) => {
                            self.handle_flush_request(epoch, response).await;
                        }
                        Some(Command::Subscribe { response }) => {
                            let _ = response.send(self.snapshot_tx.subscribe());
                        }
                        Some(Command::Shutdown { response }) => {
                            self.handle_shutdown(response).await;
                            return;
                        }
                        None => {
                            // Channel closed, exit
                            return;
                        }
                    }
                }

                // Periodic flush
                _ = flush_interval.tick() => {
                    if self.state == CoordinatorState::Accumulating {
                        self.execute_flush().await;
                    }
                }
            }

            // Check size-based flush trigger
            if self.pending_size >= self.config.max_pending_bytes
                && self.state == CoordinatorState::Accumulating
            {
                self.execute_flush().await;
            }
        }
    }

    fn handle_write(&mut self, delta: F::Delta, response: oneshot::Sender<Epoch>) {
        let epoch = self.next_epoch;
        self.next_epoch += 1;

        let size = delta.size_estimate();
        self.pending.merge(delta);
        self.pending_size += size;

        if self.state == CoordinatorState::Idle {
            self.state = CoordinatorState::Accumulating;
        }

        let _ = response.send(epoch);
    }

    async fn handle_flush_request(
        &mut self,
        epoch: Option<Epoch>,
        response: oneshot::Sender<Result<(), FlushError>>,
    ) {
        // Determine target epoch
        let target_epoch = epoch.unwrap_or(self.next_epoch.saturating_sub(1));

        // If already flushed, respond immediately
        if target_epoch <= self.flushed_epoch {
            let _ = response.send(Ok(()));
            return;
        }

        // Add to waiters
        self.waiters.entry(target_epoch).or_default().push(response);

        // Trigger flush if we have pending data
        if self.state == CoordinatorState::Accumulating {
            self.execute_flush().await;
        }
    }

    async fn execute_flush(&mut self) {
        if self.pending.is_empty() {
            self.state = CoordinatorState::Idle;
            return;
        }

        self.state = CoordinatorState::Flushing;

        // Take the pending delta
        let delta = std::mem::replace(&mut self.pending, self.handler.empty_delta());
        let flush_epoch = self.next_epoch.saturating_sub(1);
        self.pending_size = 0;

        // Get current snapshot
        let current_snapshot = self.snapshot_tx.borrow().clone();

        // Execute flush
        match self.handler.execute_flush(delta, &current_snapshot).await {
            Ok(new_snapshot) => {
                // Update snapshot
                let _ = self.snapshot_tx.send(Arc::new(new_snapshot));
                self.flushed_epoch = flush_epoch;

                // Notify waiters up to flushed epoch
                self.notify_waiters(flush_epoch, Ok(()));

                // Transition state
                if self.pending.is_empty() {
                    self.state = CoordinatorState::Idle;
                } else {
                    self.state = CoordinatorState::Accumulating;
                }
            }
            Err(e) => {
                self.state = CoordinatorState::Failed;
                // Notify all waiters of failure
                self.notify_all_waiters(Err(e));
            }
        }
    }

    fn notify_waiters(&mut self, up_to_epoch: Epoch, result: Result<(), FlushError>) {
        // Split off waiters at or below the epoch
        let to_notify = self.waiters.split_off(&(up_to_epoch + 1));
        let old_waiters = std::mem::replace(&mut self.waiters, to_notify);

        for (_, senders) in old_waiters {
            for sender in senders {
                let _ = sender.send(result.clone());
            }
        }
    }

    fn notify_all_waiters(&mut self, result: Result<(), FlushError>) {
        let waiters = std::mem::take(&mut self.waiters);
        for (_, senders) in waiters {
            for sender in senders {
                let _ = sender.send(result.clone());
            }
        }
    }

    async fn handle_shutdown(&mut self, response: oneshot::Sender<Result<(), FlushError>>) {
        // Flush any pending data
        if !self.pending.is_empty() {
            self.execute_flush().await;
        }

        // Respond based on state
        let result = if self.state == CoordinatorState::Failed {
            Err(FlushError::Internal("coordinator failed".into()))
        } else {
            Ok(())
        };

        let _ = response.send(result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Simple test delta
    #[derive(Default)]
    struct TestDelta {
        values: Vec<i32>,
    }

    impl Delta for TestDelta {
        fn is_empty(&self) -> bool {
            self.values.is_empty()
        }

        fn merge(&mut self, other: Self) {
            self.values.extend(other.values);
        }

        fn size_estimate(&self) -> usize {
            self.values.len() * 4
        }
    }

    // Simple test snapshot
    #[derive(Clone, Default)]
    struct TestSnapshot {
        total: i32,
    }

    // Test flush handler
    struct TestFlushHandler {
        flush_count: AtomicUsize,
    }

    #[async_trait]
    impl FlushHandler for TestFlushHandler {
        type Delta = TestDelta;
        type Snapshot = TestSnapshot;

        async fn execute_flush(
            &self,
            delta: Self::Delta,
            current_snapshot: &Self::Snapshot,
        ) -> Result<Self::Snapshot, FlushError> {
            self.flush_count.fetch_add(1, Ordering::SeqCst);
            let sum: i32 = delta.values.iter().sum();
            Ok(TestSnapshot {
                total: current_snapshot.total + sum,
            })
        }

        fn empty_delta(&self) -> Self::Delta {
            TestDelta::default()
        }
    }

    #[tokio::test]
    async fn should_write_and_flush_single_delta() {
        // given
        let handler = Arc::new(TestFlushHandler {
            flush_count: AtomicUsize::new(0),
        });
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (coordinator, handle) =
            WriteCoordinator::new(config, handler.clone(), TestSnapshot::default());

        tokio::spawn(coordinator.run());

        // when
        let delta = TestDelta {
            values: vec![1, 2, 3],
        };
        let epoch = handle.write(delta).await.unwrap();
        handle.flush(Some(epoch)).await.unwrap();

        // then
        assert_eq!(epoch, 1);
        let snapshot = handle.current_snapshot();
        assert_eq!(snapshot.total, 6);
        assert_eq!(handler.flush_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn should_batch_multiple_writes_before_flush() {
        // given
        let handler = Arc::new(TestFlushHandler {
            flush_count: AtomicUsize::new(0),
        });
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (coordinator, handle) =
            WriteCoordinator::new(config, handler.clone(), TestSnapshot::default());

        tokio::spawn(coordinator.run());

        // when
        let _e1 = handle.write(TestDelta { values: vec![1] }).await.unwrap();
        let _e2 = handle.write(TestDelta { values: vec![2] }).await.unwrap();
        let e3 = handle.write(TestDelta { values: vec![3] }).await.unwrap();
        handle.flush(Some(e3)).await.unwrap();

        // then
        let snapshot = handle.current_snapshot();
        assert_eq!(snapshot.total, 6);
        // Only one flush should have occurred
        assert_eq!(handler.flush_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn should_return_increasing_epochs() {
        // given
        let handler = Arc::new(TestFlushHandler {
            flush_count: AtomicUsize::new(0),
        });
        let config = CoordinatorConfig::default();
        let (coordinator, handle) = WriteCoordinator::new(config, handler, TestSnapshot::default());

        tokio::spawn(coordinator.run());

        // when
        let e1 = handle.write(TestDelta { values: vec![1] }).await.unwrap();
        let e2 = handle.write(TestDelta { values: vec![2] }).await.unwrap();
        let e3 = handle.write(TestDelta { values: vec![3] }).await.unwrap();

        // then
        assert_eq!(e1, 1);
        assert_eq!(e2, 2);
        assert_eq!(e3, 3);
    }

    #[tokio::test]
    async fn should_notify_earlier_epochs_on_flush() {
        // given
        let handler = Arc::new(TestFlushHandler {
            flush_count: AtomicUsize::new(0),
        });
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (coordinator, handle) = WriteCoordinator::new(config, handler, TestSnapshot::default());

        tokio::spawn(coordinator.run());

        // when
        let e1 = handle.write(TestDelta { values: vec![1] }).await.unwrap();
        let e2 = handle.write(TestDelta { values: vec![2] }).await.unwrap();

        // Flushing e2 should also satisfy waiters for e1
        handle.flush(Some(e2)).await.unwrap();

        // This should return immediately since e1 < e2 which was flushed
        handle.flush(Some(e1)).await.unwrap();

        // then - no panic or hang means success
    }

    // ==========================================================================
    // Tests for Issue #95: Serialize all ingestion to prevent orphaned samples
    // https://github.com/opendata-oss/opendata/issues/95
    //
    // The problem was that concurrent ingestion could cause samples to be flushed
    // without their corresponding dictionary entries, leading to orphaned samples
    // after a crash. The coordinator fixes this by serializing all writes through
    // a single channel, ensuring dictionary entries and samples are always in the
    // same delta and flushed together atomically.
    // ==========================================================================

    /// Delta that tracks both "dictionary" entries and "samples" separately,
    /// mimicking the timeseries pattern where series IDs must be persisted
    /// alongside their samples.
    #[derive(Default, Clone)]
    struct TsdbLikeDelta {
        /// Simulates series dictionary entries (fingerprint -> series_id)
        dict_entries: Vec<(u64, u32)>,
        /// Simulates samples (series_id, value)
        samples: Vec<(u32, f64)>,
    }

    impl Delta for TsdbLikeDelta {
        fn is_empty(&self) -> bool {
            self.dict_entries.is_empty() && self.samples.is_empty()
        }

        fn merge(&mut self, other: Self) {
            self.dict_entries.extend(other.dict_entries);
            self.samples.extend(other.samples);
        }

        fn size_estimate(&self) -> usize {
            (self.dict_entries.len() * 12) + (self.samples.len() * 12)
        }
    }

    /// Snapshot that records what was flushed, allowing us to verify atomicity.
    #[derive(Clone, Default)]
    struct TsdbLikeSnapshot {
        /// All dictionary entries that have been flushed
        flushed_dict: Vec<(u64, u32)>,
        /// All samples that have been flushed
        flushed_samples: Vec<(u32, f64)>,
        /// Record of each flush operation showing dict and samples together
        flush_records: Vec<(Vec<(u64, u32)>, Vec<(u32, f64)>)>,
    }

    struct TsdbLikeFlushHandler;

    #[async_trait]
    impl FlushHandler for TsdbLikeFlushHandler {
        type Delta = TsdbLikeDelta;
        type Snapshot = TsdbLikeSnapshot;

        async fn execute_flush(
            &self,
            delta: Self::Delta,
            current_snapshot: &Self::Snapshot,
        ) -> Result<Self::Snapshot, FlushError> {
            let mut new_snapshot = current_snapshot.clone();

            // Record this flush operation atomically
            new_snapshot
                .flush_records
                .push((delta.dict_entries.clone(), delta.samples.clone()));

            // Add to cumulative state
            new_snapshot.flushed_dict.extend(delta.dict_entries);
            new_snapshot.flushed_samples.extend(delta.samples);

            Ok(new_snapshot)
        }

        fn empty_delta(&self) -> Self::Delta {
            TsdbLikeDelta::default()
        }
    }

    #[tokio::test]
    async fn should_flush_dictionary_entries_and_samples_atomically_issue_95() {
        // given: A coordinator with a handler that tracks what gets flushed together
        let handler = Arc::new(TsdbLikeFlushHandler);
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (coordinator, handle) =
            WriteCoordinator::new(config, handler, TsdbLikeSnapshot::default());

        tokio::spawn(coordinator.run());

        // when: We write a delta with both a new series (dict entry) and its samples
        let delta = TsdbLikeDelta {
            dict_entries: vec![(0xABCD, 1)], // fingerprint 0xABCD -> series_id 1
            samples: vec![(1, 42.0), (1, 43.0)], // samples for series_id 1
        };
        let epoch = handle.write(delta).await.unwrap();
        handle.flush(Some(epoch)).await.unwrap();

        // then: The dictionary entry and samples must be in the same flush record
        let snapshot = handle.current_snapshot();
        assert_eq!(
            snapshot.flush_records.len(),
            1,
            "Expected exactly one flush"
        );

        let (dict, samples) = &snapshot.flush_records[0];
        assert_eq!(dict.len(), 1, "Expected one dictionary entry in flush");
        assert_eq!(samples.len(), 2, "Expected two samples in flush");

        // Verify the dict entry and samples are consistent
        assert_eq!(dict[0], (0xABCD, 1));
        assert!(samples.iter().all(|(sid, _)| *sid == 1));
    }

    #[tokio::test]
    async fn should_never_flush_samples_without_their_dictionary_entry_issue_95() {
        // given: Multiple writes where later writes reference series created earlier
        let handler = Arc::new(TsdbLikeFlushHandler);
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (coordinator, handle) =
            WriteCoordinator::new(config, handler, TsdbLikeSnapshot::default());

        tokio::spawn(coordinator.run());

        // when: Write 1 creates series 1, Write 2 adds more samples for series 1
        let delta1 = TsdbLikeDelta {
            dict_entries: vec![(0x1111, 1)],
            samples: vec![(1, 10.0)],
        };
        let delta2 = TsdbLikeDelta {
            dict_entries: vec![],                // No new series
            samples: vec![(1, 20.0), (1, 30.0)], // More samples for existing series
        };

        let _e1 = handle.write(delta1).await.unwrap();
        let e2 = handle.write(delta2).await.unwrap();
        handle.flush(Some(e2)).await.unwrap();

        // then: All samples should be flushed, and any sample's series_id must have
        // a corresponding dictionary entry (either in the same flush or earlier)
        let snapshot = handle.current_snapshot();

        // Build a set of all series IDs that have dictionary entries
        let dict_series_ids: std::collections::HashSet<u32> =
            snapshot.flushed_dict.iter().map(|(_, sid)| *sid).collect();

        // Verify every sample references a known series
        for (series_id, _value) in &snapshot.flushed_samples {
            assert!(
                dict_series_ids.contains(series_id),
                "Sample references series_id {} which has no dictionary entry (orphaned sample!)",
                series_id
            );
        }

        // Verify all samples were flushed
        assert_eq!(snapshot.flushed_samples.len(), 3);
    }

    #[tokio::test]
    async fn should_serialize_concurrent_writes_through_channel_issue_95() {
        // given: A coordinator where we'll send many writes concurrently
        let handler = Arc::new(TsdbLikeFlushHandler);
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (coordinator, handle) =
            WriteCoordinator::new(config, handler, TsdbLikeSnapshot::default());

        tokio::spawn(coordinator.run());

        // when: Spawn multiple tasks that write concurrently
        let mut join_handles = vec![];
        for i in 0..10 {
            let h = handle.clone();
            join_handles.push(tokio::spawn(async move {
                let delta = TsdbLikeDelta {
                    dict_entries: vec![(i as u64 * 1000, i as u32)],
                    samples: vec![(i as u32, i as f64)],
                };
                h.write(delta).await.unwrap()
            }));
        }

        // Collect all epochs
        let mut epochs = vec![];
        for jh in join_handles {
            epochs.push(jh.await.unwrap());
        }

        // Flush all writes
        let max_epoch = *epochs.iter().max().unwrap();
        handle.flush(Some(max_epoch)).await.unwrap();

        // then: All epochs should be unique (serialized, not duplicated)
        epochs.sort();
        let unique_epochs: std::collections::HashSet<_> = epochs.iter().collect();
        assert_eq!(
            unique_epochs.len(),
            epochs.len(),
            "Epochs should be unique - concurrent writes must be serialized"
        );

        // And all data should be present
        let snapshot = handle.current_snapshot();
        assert_eq!(snapshot.flushed_dict.len(), 10);
        assert_eq!(snapshot.flushed_samples.len(), 10);
    }

    // ==========================================================================
    // Tests for Issue #82: Properly synchronize/coordinate ingestion and querying
    // https://github.com/opendata-oss/opendata/issues/82
    //
    // The problems were:
    // 1. Ingest cache eviction could discard unflushed deltas (data loss)
    // 2. Separate ingest/query caches could have divergent snapshots (stale reads)
    // 3. Ambiguous flush semantics - write followed by flush may not include that write
    //
    // The coordinator fixes these by:
    // 1. Shutdown flushes all pending data before returning
    // 2. Single watch channel ensures all readers see the same snapshot
    // 3. Epoch-based flushing provides clear guarantees
    // ==========================================================================

    #[tokio::test]
    async fn should_flush_pending_data_on_shutdown_issue_82() {
        // given: A coordinator with pending unflushed writes
        let handler = Arc::new(TestFlushHandler {
            flush_count: AtomicUsize::new(0),
        });
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(3600), // Long interval, won't auto-flush
            ..Default::default()
        };
        let (coordinator, handle) =
            WriteCoordinator::new(config, handler.clone(), TestSnapshot::default());

        let coordinator_task = tokio::spawn(coordinator.run());

        // Write data but don't explicitly flush
        handle
            .write(TestDelta {
                values: vec![1, 2, 3],
            })
            .await
            .unwrap();
        handle
            .write(TestDelta { values: vec![4, 5] })
            .await
            .unwrap();

        // when: Shutdown the coordinator (simulates cache eviction)
        handle.shutdown().await.unwrap();
        coordinator_task.await.unwrap();

        // then: The pending data should have been flushed (no data loss!)
        let snapshot = handle.current_snapshot();
        assert_eq!(
            snapshot.total, 15,
            "Shutdown must flush all pending data to prevent data loss"
        );
        assert_eq!(handler.flush_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn should_provide_consistent_snapshot_to_all_subscribers_issue_82() {
        // given: Multiple subscribers to the same coordinator
        let handler = Arc::new(TestFlushHandler {
            flush_count: AtomicUsize::new(0),
        });
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (coordinator, handle) = WriteCoordinator::new(config, handler, TestSnapshot::default());

        tokio::spawn(coordinator.run());

        // Create multiple subscribers (simulating multiple query readers)
        let subscriber1 = handle.subscribe();
        let subscriber2 = handle.subscribe();
        let subscriber3 = handle.subscribe();

        // when: Perform writes and flush
        let epoch = handle.write(TestDelta { values: vec![100] }).await.unwrap();
        handle.flush(Some(epoch)).await.unwrap();

        // then: All subscribers must see the exact same snapshot (no stale reads!)
        let snap1 = subscriber1.borrow().clone();
        let snap2 = subscriber2.borrow().clone();
        let snap3 = subscriber3.borrow().clone();

        assert_eq!(snap1.total, 100);
        assert_eq!(snap2.total, 100);
        assert_eq!(snap3.total, 100);

        // Perform another write and flush
        let epoch2 = handle.write(TestDelta { values: vec![50] }).await.unwrap();
        handle.flush(Some(epoch2)).await.unwrap();

        // All subscribers should see the updated snapshot
        let snap1_updated = subscriber1.borrow().clone();
        let snap2_updated = subscriber2.borrow().clone();
        let snap3_updated = subscriber3.borrow().clone();

        assert_eq!(snap1_updated.total, 150);
        assert_eq!(snap2_updated.total, 150);
        assert_eq!(snap3_updated.total, 150);
    }

    #[tokio::test]
    async fn should_guarantee_write_is_flushed_when_flush_returns_issue_82() {
        // given: A coordinator
        let handler = Arc::new(TestFlushHandler {
            flush_count: AtomicUsize::new(0),
        });
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (coordinator, handle) = WriteCoordinator::new(config, handler, TestSnapshot::default());

        tokio::spawn(coordinator.run());

        // when: Perform write followed by flush for that specific epoch
        let epoch = handle.write(TestDelta { values: vec![42] }).await.unwrap();
        handle.flush(Some(epoch)).await.unwrap();

        // then: The write MUST be reflected in the snapshot (unambiguous semantics!)
        let snapshot = handle.current_snapshot();
        assert_eq!(
            snapshot.total, 42,
            "Flush must guarantee the written epoch is durable"
        );
    }

    #[tokio::test]
    async fn should_include_all_pending_writes_when_flush_none_issue_82() {
        // given: Multiple writes without explicit epoch tracking
        let handler = Arc::new(TestFlushHandler {
            flush_count: AtomicUsize::new(0),
        });
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (coordinator, handle) = WriteCoordinator::new(config, handler, TestSnapshot::default());

        tokio::spawn(coordinator.run());

        // Write multiple deltas
        handle.write(TestDelta { values: vec![1] }).await.unwrap();
        handle.write(TestDelta { values: vec![2] }).await.unwrap();
        handle.write(TestDelta { values: vec![3] }).await.unwrap();

        // when: Flush with None (flush all pending)
        handle.flush(None).await.unwrap();

        // then: All writes must be included
        let snapshot = handle.current_snapshot();
        assert_eq!(
            snapshot.total, 6,
            "Flush(None) must flush all pending writes"
        );
    }

    #[tokio::test]
    async fn should_allow_reads_during_writes_issue_82() {
        // given: A coordinator with some initial data
        let handler = Arc::new(TestFlushHandler {
            flush_count: AtomicUsize::new(0),
        });
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (coordinator, handle) =
            WriteCoordinator::new(config, handler, TestSnapshot { total: 100 });

        tokio::spawn(coordinator.run());

        let reader = handle.subscribe();

        // when: Read the current snapshot while writes are happening
        let initial_snapshot = reader.borrow().clone();

        // Write but don't flush yet
        handle.write(TestDelta { values: vec![50] }).await.unwrap();

        // Reader should still see the old snapshot (consistency!)
        let during_write_snapshot = reader.borrow().clone();

        // Now flush
        handle.flush(None).await.unwrap();

        // Reader should see updated snapshot
        let after_flush_snapshot = reader.borrow().clone();

        // then: Reads are consistent - only see flushed data
        assert_eq!(initial_snapshot.total, 100);
        assert_eq!(
            during_write_snapshot.total, 100,
            "Reads during unflushed writes should see consistent old snapshot"
        );
        assert_eq!(after_flush_snapshot.total, 150);
    }

    #[tokio::test]
    async fn should_handle_flush_already_flushed_epoch_issue_82() {
        // given: Data that has already been flushed
        let handler = Arc::new(TestFlushHandler {
            flush_count: AtomicUsize::new(0),
        });
        let config = CoordinatorConfig {
            flush_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let (coordinator, handle) =
            WriteCoordinator::new(config, handler.clone(), TestSnapshot::default());

        tokio::spawn(coordinator.run());

        let epoch1 = handle.write(TestDelta { values: vec![10] }).await.unwrap();
        handle.flush(Some(epoch1)).await.unwrap();

        // when: Request to flush an already-flushed epoch
        let result = handle.flush(Some(epoch1)).await;

        // then: Should return immediately without error (idempotent)
        assert!(
            result.is_ok(),
            "Flushing an already-flushed epoch should succeed immediately"
        );
        // Should not have triggered another flush
        assert_eq!(handler.flush_count.load(Ordering::SeqCst), 1);
    }
}
