// Tsdb is the coordination layer for OpenTSDB, ensuring the ingestion and query
// layers are synchronized. It maintains a three-level hierarchy:
//
// 1. Mutable Head: this stores incoming data that has not yet been
//    flushed to storage. They are updated via deltas, which are applied to
//    the ehad chunk for the corresponding time bucket.
//
// 2. Frozen Head: on a trigger (currently time-based), the head
//    is frozen and its data is ready to be flushed to storage. Since the time
//    it takes to flush the data is variable, the frozen head chunks are maintained
//    in memory until the flush is complete. At this point the frozen chunk is
//    atomically discarded with the update of the storage snapshot.
//
// 3. Storage Snapshot: a snapshot of the storage layer that does not yet have the
//    any of the head chunks' data. This is used for consistency as it is possible
//    that data has already been flushed to storage before the frozen chunks are
//    discarded (which would result in duplicate reads).
//
// In addition to the three levels, the Tsdb maintains a cache of the entire series
// dictionary for each active time bucket since this is required for ingestion.

use std::sync::{Arc, atomic::AtomicU32};

use dashmap::DashMap;
use opendata_common::{Storage, storage::StorageSnapshot};
use tokio::sync::{Mutex, RwLock};

use crate::delta::TsdbDelta;
use crate::storage::OpenTsdbStorageReadExt;
use crate::util::Result;
use crate::{
    head::TsdbHead,
    model::{SeriesFingerprint, SeriesId, TimeBucket},
};

pub(crate) struct TsdbState {
    head: TsdbHead,
    frozen_head: Option<TsdbHead>,
    storage_snapshot: Arc<dyn StorageSnapshot>,
}

pub(crate) struct Tsdb {
    bucket: TimeBucket,
    series_dict: DashMap<SeriesFingerprint, SeriesId>,
    next_series_id: AtomicU32,
    /// The state of the Tsdb is protected by a read-write
    /// lock to allow us to atomically flush and update the
    /// storage snapshot. Note that only the read lock is
    /// required for queries and ingestion (which modifies the
    /// underlying data structures in a thread-safe way)
    state: Arc<RwLock<TsdbState>>,
    /// Mutex to ensure only one flush operation can run at a time.
    /// This prevents concurrent flushes from interfering with each other.
    flush_mutex: Arc<Mutex<()>>,
}

impl Tsdb {
    pub(crate) async fn load(bucket: TimeBucket, storage: Arc<dyn Storage>) -> Result<Self> {
        let series_dict = DashMap::new();
        let next_series_id = storage
            .load_series_dictionary(&bucket, |fingerprint, series_id| {
                series_dict.insert(fingerprint, series_id);
            })
            .await?;

        Ok(Self {
            bucket: bucket.clone(),
            series_dict,
            next_series_id: AtomicU32::new(next_series_id),
            state: Arc::new(RwLock::new(TsdbState {
                head: TsdbHead::new(bucket.clone()),
                frozen_head: None,
                storage_snapshot: storage.snapshot().await?,
            })),
            flush_mutex: Arc::new(Mutex::new(())),
        })
    }

    pub(crate) async fn ingest(&self, delta: TsdbDelta) -> Result<()> {
        let state = self.state.read().await;
        state.head.merge(&delta)?;
        Ok(())
    }

    pub(crate) async fn flush(&self, storage: Arc<dyn Storage>) -> Result<()> {
        let _flush_guard = self.flush_mutex.lock().await;

        // blocking section: freeze the head and replaces it
        // with a new head block, keeping a reference to the old
        // frozen head
        {
            let mut state = self.state.write().await;
            state.head.freeze();
            let frozen_head =
                std::mem::replace(&mut state.head, TsdbHead::new(self.bucket.clone()));
            state.frozen_head = Some(frozen_head);
        }

        // non-blocking section: flush the frozen head to storage
        // this can take time so its important that it only holds
        // the read lock
        let snapshot = {
            let state = self.state.read().await;
            let frozen_head = state
                .frozen_head
                .as_ref()
                .expect("frozen_head should be set after write lock above");
            // Clone storage to avoid moving it and for clarity
            frozen_head.flush(storage.clone()).await?
        };

        // blocking section: update the storage snapshot and
        // discard the frozen head
        {
            let mut state = self.state.write().await;
            state.storage_snapshot = snapshot;
            state.frozen_head = None;
        }
        Ok(())
    }
}
