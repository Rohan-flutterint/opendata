#![allow(dead_code)]

use std::collections::HashMap;

use common::Delta;

use crate::index::SeriesSpec;
use crate::model::{Sample, Series, SeriesFingerprint, TimeBucket};
use crate::{error::Error, util::Fingerprint, util::Result};

/// Data for a single series collected during ingestion.
/// Series IDs are resolved at flush time, not during ingestion.
#[derive(Clone)]
pub(crate) struct SeriesData {
    pub(crate) spec: SeriesSpec,
    pub(crate) samples: Vec<Sample>,
}

/// The delta represents unflushed data, keyed by fingerprint.
/// Series IDs are resolved at flush time by the FlushHandler.
pub(crate) struct TsdbDelta {
    pub(crate) bucket: TimeBucket,
    /// Series data keyed by fingerprint. At flush time, the handler resolves
    /// fingerprints to series IDs and builds the storage operations.
    pub(crate) series: HashMap<SeriesFingerprint, SeriesData>,
}

impl TsdbDelta {
    /// Create an empty delta for a bucket
    pub(crate) fn new(bucket: TimeBucket) -> Self {
        Self {
            bucket,
            series: HashMap::new(),
        }
    }

    /// Ingest a series with its samples.
    /// Returns an error if any sample timestamp is outside the bucket's time range.
    pub(crate) fn ingest(&mut self, series: &Series) -> Result<()> {
        // Validate all sample timestamps first
        let bucket_start_ms = self.bucket.start as i64 * 60 * 1000;
        let bucket_end_ms =
            (self.bucket.start as i64 + self.bucket.size_in_mins() as i64) * 60 * 1000;

        for sample in &series.samples {
            if sample.timestamp_ms < bucket_start_ms || sample.timestamp_ms >= bucket_end_ms {
                return Err(Error::InvalidInput(format!(
                    "Sample timestamp {} is outside bucket range [{}, {})",
                    sample.timestamp_ms, bucket_start_ms, bucket_end_ms
                )));
            }
        }

        // Sort labels for consistent fingerprinting
        let mut sorted_labels = series.labels.clone();
        sorted_labels.sort_by(|a, b| a.name.cmp(&b.name));
        let fingerprint = sorted_labels.fingerprint();

        // Merge into existing series data or create new
        match self.series.get_mut(&fingerprint) {
            Some(existing) => {
                // Same fingerprint = same series, append samples
                existing.samples.extend(series.samples.iter().cloned());
            }
            None => {
                // New series in this delta
                let spec = SeriesSpec {
                    unit: series.unit.clone(),
                    metric_type: series.metric_type,
                    labels: sorted_labels,
                };
                self.series.insert(
                    fingerprint,
                    SeriesData {
                        spec,
                        samples: series.samples.clone(),
                    },
                );
            }
        }

        Ok(())
    }

    /// Check if delta has any data
    pub(crate) fn is_empty(&self) -> bool {
        self.series.is_empty()
    }

    /// Merge another delta into this one.
    /// Same fingerprint = same series, so samples accumulate.
    fn merge_delta(&mut self, other: TsdbDelta) {
        for (fingerprint, other_data) in other.series {
            match self.series.get_mut(&fingerprint) {
                Some(existing) => {
                    // Same series: append samples
                    existing.samples.extend(other_data.samples);
                }
                None => {
                    self.series.insert(fingerprint, other_data);
                }
            }
        }
    }

    /// Estimate the size in bytes of this delta.
    fn size_estimate(&self) -> usize {
        let mut size = 0;
        for data in self.series.values() {
            // Each sample is ~16 bytes (timestamp + value)
            size += data.samples.len() * 16;
            // Spec overhead: labels + unit + metric_type
            size += data.spec.labels.len() * 50; // rough estimate per label
            size += 20; // fingerprint + base overhead
        }
        size
    }
}

impl Delta for TsdbDelta {
    fn is_empty(&self) -> bool {
        TsdbDelta::is_empty(self)
    }

    fn merge(&mut self, other: Self) {
        self.merge_delta(other)
    }

    fn size_estimate(&self) -> usize {
        TsdbDelta::size_estimate(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Label;
    use crate::model::MetricType;
    use crate::model::Temporality;

    fn create_test_bucket() -> TimeBucket {
        TimeBucket::hour(1000)
    }

    fn create_test_labels() -> Vec<Label> {
        vec![
            Label {
                name: "service".to_string(),
                value: "api".to_string(),
            },
            Label {
                name: "env".to_string(),
                value: "prod".to_string(),
            },
        ]
    }

    fn create_test_sample() -> Sample {
        // Timestamp must be within bucket range (1000 min = 60,000,000 ms to 1060 min = 63,600,000 ms)
        Sample {
            timestamp_ms: 60_000_001,
            value: 42.5,
        }
    }

    fn create_test_series(labels: Vec<Label>, samples: Vec<Sample>) -> Series {
        Series {
            labels,
            samples,
            unit: Some("bytes".to_string()),
            metric_type: Some(MetricType::Gauge),
            description: None,
        }
    }

    #[test]
    fn should_create_series_entry_when_ingesting() {
        // given
        let bucket = create_test_bucket();
        let mut delta = TsdbDelta::new(bucket);
        let labels = create_test_labels();
        let sample = create_test_sample();
        let series = create_test_series(labels.clone(), vec![sample.clone()]);

        // when
        delta.ingest(&series).unwrap();

        // then
        assert_eq!(delta.series.len(), 1);
        let (_, data) = delta.series.iter().next().unwrap();
        assert_eq!(data.samples.len(), 1);
        assert_eq!(data.samples[0], sample);
        assert_eq!(data.spec.unit, Some("bytes".to_string()));
    }

    #[test]
    fn should_accumulate_samples_for_same_fingerprint() {
        // given
        let bucket = create_test_bucket();
        let mut delta = TsdbDelta::new(bucket);
        let labels = create_test_labels();
        let sample1 = Sample {
            timestamp_ms: 60_000_001,
            value: 10.0,
        };
        let sample2 = Sample {
            timestamp_ms: 60_000_002,
            value: 20.0,
        };

        // when: ingest two series with same labels (same fingerprint)
        delta
            .ingest(&create_test_series(labels.clone(), vec![sample1.clone()]))
            .unwrap();
        delta
            .ingest(&create_test_series(labels.clone(), vec![sample2.clone()]))
            .unwrap();

        // then: should have one series entry with both samples
        assert_eq!(delta.series.len(), 1);
        let (_, data) = delta.series.iter().next().unwrap();
        assert_eq!(data.samples.len(), 2);
    }

    #[test]
    fn should_create_separate_entries_for_different_fingerprints() {
        // given
        let bucket = create_test_bucket();
        let mut delta = TsdbDelta::new(bucket);
        let labels1 = vec![Label {
            name: "service".to_string(),
            value: "api".to_string(),
        }];
        let labels2 = vec![Label {
            name: "service".to_string(),
            value: "web".to_string(),
        }];

        // when
        delta
            .ingest(&create_test_series(labels1, vec![create_test_sample()]))
            .unwrap();
        delta
            .ingest(&create_test_series(labels2, vec![create_test_sample()]))
            .unwrap();

        // then
        assert_eq!(delta.series.len(), 2);
    }

    #[test]
    fn should_sort_labels_before_fingerprinting() {
        // given
        let bucket = create_test_bucket();
        let mut delta = TsdbDelta::new(bucket);
        // Labels in different orders should produce same fingerprint
        let labels1 = vec![
            Label {
                name: "z_key".to_string(),
                value: "value".to_string(),
            },
            Label {
                name: "a_key".to_string(),
                value: "value".to_string(),
            },
        ];
        let labels2 = vec![
            Label {
                name: "a_key".to_string(),
                value: "value".to_string(),
            },
            Label {
                name: "z_key".to_string(),
                value: "value".to_string(),
            },
        ];

        // when
        delta
            .ingest(&create_test_series(labels1, vec![create_test_sample()]))
            .unwrap();
        delta
            .ingest(&create_test_series(labels2, vec![create_test_sample()]))
            .unwrap();

        // then: should be same series (same fingerprint after sorting)
        assert_eq!(delta.series.len(), 1);
        let (_, data) = delta.series.iter().next().unwrap();
        assert_eq!(data.samples.len(), 2);
    }

    #[test]
    fn should_reject_sample_before_bucket_start() {
        // given
        let bucket = create_test_bucket();
        let mut delta = TsdbDelta::new(bucket);
        let labels = create_test_labels();
        let sample = Sample {
            timestamp_ms: 59_999_999, // Before bucket start
            value: 42.5,
        };

        // when
        let result = delta.ingest(&create_test_series(labels, vec![sample]));

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("outside bucket range")
        );
    }

    #[test]
    fn should_reject_sample_at_or_after_bucket_end() {
        // given
        let bucket = create_test_bucket();
        let mut delta = TsdbDelta::new(bucket);
        let labels = create_test_labels();
        let sample = Sample {
            timestamp_ms: 63_600_000, // At bucket end (exclusive)
            value: 42.5,
        };

        // when
        let result = delta.ingest(&create_test_series(labels, vec![sample]));

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("outside bucket range")
        );
    }

    #[test]
    fn should_merge_deltas_accumulating_samples_for_same_fingerprint() {
        // given
        let bucket = create_test_bucket();
        let labels = create_test_labels();

        let mut delta1 = TsdbDelta::new(bucket);
        delta1
            .ingest(&create_test_series(
                labels.clone(),
                vec![Sample {
                    timestamp_ms: 60_000_001,
                    value: 10.0,
                }],
            ))
            .unwrap();

        let mut delta2 = TsdbDelta::new(bucket);
        delta2
            .ingest(&create_test_series(
                labels,
                vec![Sample {
                    timestamp_ms: 60_000_002,
                    value: 20.0,
                }],
            ))
            .unwrap();

        // when
        delta1.merge_delta(delta2);

        // then
        assert_eq!(delta1.series.len(), 1);
        let (_, data) = delta1.series.iter().next().unwrap();
        assert_eq!(data.samples.len(), 2);
    }

    #[test]
    fn should_store_metric_unit_and_type_in_spec() {
        // given
        let bucket = create_test_bucket();
        let mut delta = TsdbDelta::new(bucket);
        let labels = create_test_labels();
        let series = Series {
            labels,
            samples: vec![create_test_sample()],
            unit: Some("requests_per_second".to_string()),
            metric_type: Some(MetricType::Sum {
                monotonic: true,
                temporality: Temporality::Cumulative,
            }),
            description: None,
        };

        // when
        delta.ingest(&series).unwrap();

        // then
        let (_, data) = delta.series.iter().next().unwrap();
        assert_eq!(data.spec.unit, Some("requests_per_second".to_string()));
        match data.spec.metric_type {
            Some(MetricType::Sum {
                monotonic,
                temporality,
            }) => {
                assert!(monotonic);
                assert_eq!(temporality, Temporality::Cumulative);
            }
            _ => panic!("Expected Sum metric type"),
        }
    }

    #[test]
    fn should_handle_empty_labels() {
        // given
        let bucket = create_test_bucket();
        let mut delta = TsdbDelta::new(bucket);
        let series = create_test_series(vec![], vec![create_test_sample()]);

        // when
        delta.ingest(&series).unwrap();

        // then
        assert_eq!(delta.series.len(), 1);
        let (_, data) = delta.series.iter().next().unwrap();
        assert_eq!(data.spec.labels.len(), 0);
    }
}
