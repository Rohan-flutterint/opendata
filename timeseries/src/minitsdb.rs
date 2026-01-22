#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::{CoordinatorConfig, CoordinatorHandle, Storage, StorageRead, WriteCoordinator};
use dashmap::DashMap;

use crate::delta::TsdbDelta;
use crate::error::Error;
use crate::flush_handler::{TsdbFlushHandler, TsdbSnapshot};
use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::model::{Label, Sample, Series, SeriesId, TimeBucket};
use crate::query::BucketQueryReader;
use crate::serde::key::TimeSeriesKey;
use crate::serde::timeseries::TimeSeriesIterator;
use crate::storage::OpenTsdbStorageReadExt;
use crate::util::Result;

pub(crate) struct MiniQueryReader {
    bucket: TimeBucket,
    snapshot: Arc<dyn StorageRead>,
}

#[async_trait]
impl BucketQueryReader for MiniQueryReader {
    async fn forward_index(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let forward_index = self
            .snapshot
            .get_forward_index_series(&self.bucket, series_ids)
            .await?;
        Ok(Box::new(forward_index))
    }

    async fn all_forward_index(
        &self,
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let forward_index = self.snapshot.get_forward_index(self.bucket).await?;
        Ok(Box::new(forward_index))
    }

    async fn inverted_index(
        &self,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let inverted_index = self
            .snapshot
            .get_inverted_index_terms(&self.bucket, terms)
            .await?;
        Ok(Box::new(inverted_index))
    }

    async fn all_inverted_index(
        &self,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let inverted_index = self.snapshot.get_inverted_index(self.bucket).await?;
        Ok(Box::new(inverted_index))
    }

    async fn label_values(&self, label_name: &str) -> Result<Vec<String>> {
        self.snapshot
            .get_label_values(&self.bucket, label_name)
            .await
    }

    async fn samples(
        &self,
        series_id: SeriesId,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        let storage_key = TimeSeriesKey {
            time_bucket: self.bucket.start,
            bucket_size: self.bucket.size,
            series_id,
        };
        let record = self.snapshot.get(storage_key.encode()).await?;

        match record {
            Some(record) => {
                let iter = TimeSeriesIterator::new(record.value.as_ref())
                    .ok_or_else(|| Error::Internal("Invalid timeseries data in storage".into()))?;

                let samples: Vec<Sample> = iter
                    .filter_map(|r| r.ok())
                    // Filter by time range: timestamp > start_ms && timestamp <= end_ms
                    // Following PromQL lookback window semantics with exclusive start
                    .filter(|s| s.timestamp_ms > start_ms && s.timestamp_ms <= end_ms)
                    .collect();

                Ok(samples)
            }
            None => Ok(Vec::new()),
        }
    }
}

pub(crate) struct MiniTsdb {
    bucket: TimeBucket,
    /// Coordinator handle for managing writes and flushes.
    coordinator_handle: CoordinatorHandle<TsdbDelta, TsdbSnapshot>,
}

impl MiniTsdb {
    /// Returns a reference to the time bucket
    pub(crate) fn bucket(&self) -> &TimeBucket {
        &self.bucket
    }

    /// Create a query reader for read operations.
    pub(crate) fn query_reader(&self) -> MiniQueryReader {
        // Get the current snapshot from the coordinator.
        // The coordinator wraps our TsdbSnapshot (which is Arc<dyn StorageRead>) in another Arc,
        // so we need to clone the inner Arc to get the snapshot we need.
        let outer_arc = self.coordinator_handle.current_snapshot();
        let snapshot = (*outer_arc).clone();
        MiniQueryReader {
            bucket: self.bucket,
            snapshot,
        }
    }

    pub(crate) async fn load(
        bucket: TimeBucket,
        storage: Arc<dyn Storage>,
        flush_interval: Duration,
    ) -> Result<Self> {
        // Load existing series dictionary from storage.
        // The dictionary is owned entirely by the flush handler - MiniTsdb
        // doesn't need access since ID resolution only happens at flush time.
        let series_dict = DashMap::new();
        let next_series_id = storage
            .load_series_dictionary(&bucket, |fingerprint, series_id| {
                series_dict.insert(fingerprint, series_id);
            })
            .await?;

        // Get initial snapshot
        let initial_snapshot = storage.snapshot().await?;

        // Create the flush handler which owns the series dictionary.
        // The handler resolves fingerprints to series IDs at flush time,
        // which is key to fixing Issue #95 (no orphaned samples).
        let handler = Arc::new(TsdbFlushHandler::new(
            storage,
            bucket,
            series_dict,
            next_series_id,
        ));

        // Create coordinator config
        let config = CoordinatorConfig {
            max_pending_bytes: 64 * 1024 * 1024, // 64MB
            flush_interval,
            channel_capacity: 1024,
        };

        // Create the coordinator
        let (coordinator, handle) = WriteCoordinator::new(config, handler, initial_snapshot);

        // Spawn the coordinator task
        tokio::spawn(coordinator.run());

        Ok(Self {
            bucket,
            coordinator_handle: handle,
        })
    }

    /// Ingest a batch of series with samples in a single operation.
    /// Note: Ingested data is batched and NOT visible to queries until flush().
    /// Returns an error if any sample timestamp is outside the bucket's time range.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = series_list.iter().map(|s| s.samples.len()).sum::<usize>()
        )
    )]
    pub(crate) async fn ingest_batch(&self, series_list: &[Series]) -> Result<()> {
        let total_samples = series_list.iter().map(|s| s.samples.len()).sum::<usize>();

        tracing::debug!(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = total_samples,
            "Starting MiniTsdb batch ingest"
        );

        // Create delta directly - no builder needed.
        // Following RFC 0003, ID resolution is deferred to flush time.
        let mut delta = TsdbDelta::new(self.bucket);

        // Ingest all series into the delta
        for series in series_list {
            delta.ingest(series)?;
        }

        // Send to coordinator
        self.coordinator_handle
            .write(delta)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;

        tracing::debug!(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = total_samples,
            "Completed MiniTsdb batch ingest"
        );

        Ok(())
    }

    /// Ingest a single series with samples.
    /// Note: Ingested data is batched and NOT visible to queries until flush().
    /// Returns an error if any sample timestamp is outside the bucket's time range.
    ///
    /// For better performance when ingesting multiple series, use ingest_batch() instead.
    pub(crate) async fn ingest(&self, series: &Series) -> Result<()> {
        // Delegate to batch method with a single series
        self.ingest_batch(std::slice::from_ref(series)).await
    }

    /// Flush pending data to storage, making it durable and visible to queries.
    pub(crate) async fn flush(&self) -> Result<()> {
        self.coordinator_handle
            .flush(None)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(())
    }
}
