//! FlushHandler implementation for timeseries storage.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use common::{FlushError, FlushHandler, Storage, StorageRead};
use dashmap::DashMap;
use roaring::RoaringBitmap;

use crate::delta::TsdbDelta;
use crate::model::{SeriesFingerprint, SeriesId, TimeBucket};
use crate::storage::OpenTsdbStorageExt;

/// Snapshot type for timeseries - just the storage snapshot.
pub(crate) type TsdbSnapshot = Arc<dyn StorageRead>;

/// FlushHandler implementation for timeseries storage.
///
/// At flush time, this handler resolves fingerprints to series IDs.
/// This follows RFC 0003's design where ID resolution is deferred to
/// flush time to ensure dictionary entries and samples are always
/// flushed together atomically (fixing Issue #95).
///
/// The series dictionary is owned entirely by the flush handler -
/// MiniTsdb doesn't need access to it since ID resolution only
/// happens during flush.
pub(crate) struct TsdbFlushHandler {
    storage: Arc<dyn Storage>,
    bucket: TimeBucket,
    /// Series dictionary for resolving fingerprints to IDs.
    series_dict: DashMap<SeriesFingerprint, SeriesId>,
    /// Next series ID counter for creating new series.
    next_series_id: AtomicU32,
}

impl TsdbFlushHandler {
    pub(crate) fn new(
        storage: Arc<dyn Storage>,
        bucket: TimeBucket,
        series_dict: DashMap<SeriesFingerprint, SeriesId>,
        next_series_id: u32,
    ) -> Self {
        Self {
            storage,
            bucket,
            series_dict,
            next_series_id: AtomicU32::new(next_series_id),
        }
    }
}

#[async_trait]
impl FlushHandler for TsdbFlushHandler {
    type Delta = TsdbDelta;
    type Snapshot = TsdbSnapshot;

    async fn execute_flush(
        &self,
        delta: Self::Delta,
        _current_snapshot: &Self::Snapshot,
    ) -> Result<Self::Snapshot, FlushError> {
        let mut ops = Vec::new();

        // Add bucket to bucket list
        ops.push(
            self.storage
                .merge_bucket_list(self.bucket)
                .map_err(|e| FlushError::Storage(e.to_string()))?,
        );

        // Batch inverted index entries by label across all series (per RFC)
        let mut inverted_index: HashMap<crate::model::Label, RoaringBitmap> = HashMap::new();

        // Process each series in the delta
        for (fingerprint, series_data) in delta.series {
            // Resolve fingerprint to series ID.
            // Since we're in the coordinator's flush execution (single-threaded),
            // this is safe from the race condition described in Issue #95.
            let mut is_new = false;
            let series_id = *self
                .series_dict
                .entry(fingerprint)
                .or_insert_with(|| {
                    is_new = true;
                    self.next_series_id.fetch_add(1, Ordering::SeqCst)
                })
                .value();

            // Only write dictionary and index entries for NEW series
            if is_new {
                // Insert series dictionary entry
                ops.push(
                    self.storage
                        .insert_series_id(self.bucket, fingerprint, series_id)
                        .map_err(|e| FlushError::Storage(e.to_string()))?,
                );

                // Insert forward index entry
                ops.push(
                    self.storage
                        .insert_forward_index(self.bucket, series_id, series_data.spec.clone())
                        .map_err(|e| FlushError::Storage(e.to_string()))?,
                );

                // Accumulate series IDs per label for batched inverted index
                for label in &series_data.spec.labels {
                    inverted_index
                        .entry(label.clone())
                        .or_default()
                        .insert(series_id);
                }
            }

            // Merge samples (always, whether new or existing series)
            ops.push(
                self.storage
                    .merge_samples(self.bucket, series_id, series_data.samples)
                    .map_err(|e| FlushError::Storage(e.to_string()))?,
            );
        }

        // Write one inverted index record per label (not per series) - per RFC
        for (label, series_ids) in inverted_index {
            ops.push(
                self.storage
                    .merge_inverted_index(self.bucket, label, series_ids)
                    .map_err(|e| FlushError::Storage(e.to_string()))?,
            );
        }

        // Apply all operations atomically
        self.storage
            .apply(ops)
            .await
            .map_err(|e| FlushError::Storage(e.to_string()))?;

        // Get new snapshot
        let new_snapshot = self
            .storage
            .snapshot()
            .await
            .map_err(|e| FlushError::Storage(e.to_string()))?;

        Ok(new_snapshot)
    }

    fn empty_delta(&self) -> Self::Delta {
        TsdbDelta::new(self.bucket)
    }
}
