use std::collections::HashMap;

use dashmap::DashMap;
use opentelemetry_proto::tonic::metrics::v1::Metric;

use crate::{
    index::{ForwardIndex, InvertedIndex},
    model::{MetricType, Sample, SeriesFingerprint, SeriesId, SeriesSpec, TimeBucket},
    otel::OtelUtil,
    util::{Fingerprint, Result, normalize_str},
};

/// The delta chunk is the current in-memory segment of OpenTSDB representing
/// the data that has been ingested but not yet flushed to storage.
pub(crate) struct TsdbDeltaBuilder<'a> {
    pub(crate) bucket: TimeBucket,
    pub(crate) forward_index: ForwardIndex,
    pub(crate) inverted_index: InvertedIndex,
    pub(crate) series_dict: &'a DashMap<SeriesFingerprint, SeriesId>,
    pub(crate) series_dict_delta: HashMap<SeriesFingerprint, SeriesId>,
    pub(crate) samples: HashMap<SeriesId, Vec<Sample>>,
    pub(crate) next_series_id: u32,
}

impl<'a> TsdbDeltaBuilder<'a> {
    pub(crate) fn new(
        bucket: TimeBucket,
        series_dict: &'a DashMap<SeriesFingerprint, SeriesId>,
        next_series_id: u32,
    ) -> Self {
        Self {
            bucket,
            forward_index: ForwardIndex::default(),
            inverted_index: InvertedIndex::default(),
            series_dict,
            series_dict_delta: HashMap::new(),
            samples: HashMap::new(),
            next_series_id,
        }
    }

    pub(crate) fn ingest_metric(mut self, metric: &Metric) -> Result<()> {
        let metric_unit = normalize_str(&metric.unit);
        let metric_type = MetricType::try_from(metric)?;
        let samples_with_attrs = OtelUtil::samples(metric);

        for sample_with_attrs in samples_with_attrs {
            self.ingest_sample(
                sample_with_attrs.attributes,
                metric_unit.clone(),
                metric_type,
                sample_with_attrs.sample,
            );
        }

        Ok(())
    }

    pub(crate) fn ingest_sample(
        &mut self,
        mut attributes: Vec<crate::model::Attribute>,
        metric_unit: Option<String>,
        metric_type: MetricType,
        sample: Sample,
    ) {
        // Sort attributes for consistent fingerprinting
        attributes.sort_by(|a, b| a.key.cmp(&b.key));

        let fingerprint = attributes.fingerprint();

        // register the series in the delta if it's not already present
        // in either the series dictionary or the delta series dictionary
        // (the latter is one that we've registered during building this
        // delta)
        let series_id = self
            .series_dict
            .get(&fingerprint)
            .map(|r| *r.value())
            .or_else(|| self.series_dict_delta.get(&fingerprint).copied())
            .unwrap_or_else(|| {
                let series_id = self.next_series_id;
                self.next_series_id = series_id + 1;

                self.series_dict_delta.insert(fingerprint, series_id);

                let series_spec = SeriesSpec {
                    metric_unit: metric_unit.clone(),
                    metric_type,
                    attributes: attributes.clone(),
                };

                self.forward_index.series.insert(series_id, series_spec);

                for attr in &attributes {
                    let mut entry = self
                        .inverted_index
                        .postings
                        .entry(attr.clone())
                        .or_default();
                    entry.value_mut().insert(series_id);
                }

                series_id
            });

        let entry = self.samples.entry(series_id).or_default();
        entry.push(sample);
    }

    pub(crate) fn build(self) -> TsdbDelta {
        TsdbDelta {
            bucket: self.bucket,
            forward_index: self.forward_index,
            inverted_index: self.inverted_index,
            series_dict: self.series_dict_delta,
            samples: self.samples,
        }
    }
}

pub(crate) struct TsdbDelta {
    pub(crate) bucket: TimeBucket,
    pub(crate) forward_index: ForwardIndex,
    pub(crate) inverted_index: InvertedIndex,
    pub(crate) series_dict: HashMap<SeriesFingerprint, SeriesId>,
    pub(crate) samples: HashMap<SeriesId, Vec<Sample>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Attribute, MetricType, Temporality};
    use dashmap::DashMap;

    fn create_test_bucket() -> TimeBucket {
        TimeBucket::hour(1000)
    }

    fn create_test_attributes() -> Vec<Attribute> {
        vec![
            Attribute {
                key: "service".to_string(),
                value: "api".to_string(),
            },
            Attribute {
                key: "env".to_string(),
                value: "prod".to_string(),
            },
        ]
    }

    fn create_test_sample() -> Sample {
        Sample {
            timestamp: 1234567890,
            value: 42.5,
        }
    }

    #[test]
    fn should_create_new_series_when_ingesting_first_sample() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, 0);
        let attributes = create_test_attributes();
        let sample = create_test_sample();
        let metric_unit = Some("bytes".to_string());
        let metric_type = MetricType::Gauge;

        // when
        builder.ingest_sample(
            attributes.clone(),
            metric_unit.clone(),
            metric_type,
            sample.clone(),
        );

        // then
        assert_eq!(builder.next_series_id, 1);
        assert_eq!(builder.series_dict_delta.len(), 1);
        assert_eq!(builder.samples.len(), 1);

        // Verify sample is stored
        let samples = builder.samples.get(&0).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0], sample);

        // Verify forward index
        let series_spec = builder.forward_index.series.get(&0).unwrap();
        assert_eq!(series_spec.metric_unit, metric_unit);
        match (series_spec.metric_type, metric_type) {
            (MetricType::Gauge, MetricType::Gauge) => {}
            _ => panic!("Metric types don't match"),
        }
        // Attributes are sorted by ingest_sample, so sort them for comparison
        let mut sorted_attributes = attributes.clone();
        sorted_attributes.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(series_spec.attributes, sorted_attributes);

        // Verify inverted index
        for attr in &attributes {
            let postings = builder.inverted_index.postings.get(attr).unwrap();
            assert!(postings.value().contains(0));
        }
    }

    #[test]
    fn should_reuse_series_id_for_samples_with_same_attributes() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, 0);
        let attributes = create_test_attributes();
        let sample1 = Sample {
            timestamp: 1000,
            value: 10.0,
        };
        let sample2 = Sample {
            timestamp: 2000,
            value: 20.0,
        };
        let metric_type = MetricType::Gauge;

        // when
        builder.ingest_sample(
            attributes.clone(),
            Some("bytes".to_string()),
            metric_type,
            sample1.clone(),
        );
        builder.ingest_sample(
            attributes.clone(),
            Some("bytes".to_string()),
            metric_type,
            sample2.clone(),
        );

        // then
        assert_eq!(builder.next_series_id, 1); // Only one series created
        assert_eq!(builder.series_dict_delta.len(), 1);
        assert_eq!(builder.samples.len(), 1);

        // Both samples should be under the same series_id
        let samples = builder.samples.get(&0).unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0], sample1);
        assert_eq!(samples[1], sample2);
    }

    #[test]
    fn should_create_different_series_id_for_different_attributes() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, 0);
        let attributes1 = vec![Attribute {
            key: "service".to_string(),
            value: "api".to_string(),
        }];
        let attributes2 = vec![Attribute {
            key: "service".to_string(),
            value: "web".to_string(),
        }];
        let metric_type = MetricType::Gauge;

        // when
        builder.ingest_sample(
            attributes1,
            Some("bytes".to_string()),
            metric_type,
            create_test_sample(),
        );
        builder.ingest_sample(
            attributes2,
            Some("bytes".to_string()),
            metric_type,
            create_test_sample(),
        );

        // then
        assert_eq!(builder.next_series_id, 2); // Two series created
        assert_eq!(builder.series_dict_delta.len(), 2);
        assert_eq!(builder.samples.len(), 2);
        assert!(builder.samples.contains_key(&0));
        assert!(builder.samples.contains_key(&1));
    }

    #[test]
    fn should_reuse_series_id_from_existing_series_dict() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let mut attributes = create_test_attributes();
        // Sort attributes to match what ingest_sample does
        attributes.sort_by(|a, b| a.key.cmp(&b.key));
        let fingerprint = attributes.fingerprint();
        series_dict.insert(fingerprint, 42); // Existing series_id
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, 0);
        let metric_type = MetricType::Gauge;

        // when
        builder.ingest_sample(
            create_test_attributes(), // Will be sorted by ingest_sample
            Some("bytes".to_string()),
            metric_type,
            create_test_sample(),
        );

        // then
        assert_eq!(builder.next_series_id, 0); // No new series_id created
        assert_eq!(builder.series_dict_delta.len(), 0); // Not added to delta
        assert_eq!(builder.samples.len(), 1);
        assert!(builder.samples.contains_key(&42)); // Uses existing series_id
    }

    #[test]
    fn should_reuse_series_id_from_delta_dict_when_ingesting_again() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, 0);
        let attributes = create_test_attributes();
        let metric_type = MetricType::Gauge;

        // when
        builder.ingest_sample(
            attributes.clone(),
            Some("bytes".to_string()),
            metric_type,
            create_test_sample(),
        );
        builder.ingest_sample(
            attributes.clone(),
            Some("bytes".to_string()),
            metric_type,
            create_test_sample(),
        );

        // then
        assert_eq!(builder.next_series_id, 1); // Only one series_id created
        assert_eq!(builder.series_dict_delta.len(), 1);
        assert_eq!(builder.samples.len(), 1);
        assert!(builder.samples.contains_key(&0)); // Reused series_id 0
        assert_eq!(builder.samples.get(&0).unwrap().len(), 2); // Two samples
    }

    #[test]
    fn should_sort_attributes_before_fingerprinting() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, 0);
        let attributes1 = vec![
            Attribute {
                key: "z_key".to_string(),
                value: "value".to_string(),
            },
            Attribute {
                key: "a_key".to_string(),
                value: "value".to_string(),
            },
        ];
        let attributes2 = vec![
            Attribute {
                key: "a_key".to_string(),
                value: "value".to_string(),
            },
            Attribute {
                key: "z_key".to_string(),
                value: "value".to_string(),
            },
        ];
        let metric_type = MetricType::Gauge;

        // when
        builder.ingest_sample(
            attributes1,
            Some("bytes".to_string()),
            metric_type,
            create_test_sample(),
        );
        builder.ingest_sample(
            attributes2,
            Some("bytes".to_string()),
            metric_type,
            create_test_sample(),
        );

        // then
        assert_eq!(builder.next_series_id, 1); // Same series_id reused
        assert_eq!(builder.series_dict_delta.len(), 1);
        assert_eq!(builder.samples.len(), 1);
    }

    #[test]
    fn should_store_metric_unit_and_type_in_forward_index() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, 0);
        let attributes = create_test_attributes();
        let metric_unit = Some("requests_per_second".to_string());
        let metric_type = MetricType::Sum {
            monotonic: true,
            temporality: Temporality::Cumulative,
        };

        // when
        builder.ingest_sample(
            attributes.clone(),
            metric_unit.clone(),
            metric_type,
            create_test_sample(),
        );

        // then
        let series_spec = builder.forward_index.series.get(&0).unwrap();
        assert_eq!(series_spec.metric_unit, metric_unit);
        match series_spec.metric_type {
            MetricType::Sum {
                monotonic,
                temporality,
            } => {
                assert!(monotonic);
                assert_eq!(temporality, Temporality::Cumulative);
            }
            _ => panic!("Expected Sum metric type"),
        }
    }

    #[test]
    fn should_index_all_attributes_in_inverted_index() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, 0);
        let attributes = vec![
            Attribute {
                key: "service".to_string(),
                value: "api".to_string(),
            },
            Attribute {
                key: "env".to_string(),
                value: "prod".to_string(),
            },
            Attribute {
                key: "region".to_string(),
                value: "us-east".to_string(),
            },
        ];
        let metric_type = MetricType::Gauge;

        // when
        builder.ingest_sample(
            attributes.clone(),
            Some("bytes".to_string()),
            metric_type,
            create_test_sample(),
        );

        // then
        assert_eq!(builder.inverted_index.postings.len(), 3);
        for attr in &attributes {
            let postings = builder.inverted_index.postings.get(attr).unwrap();
            assert!(postings.value().contains(0));
            assert_eq!(postings.value().len(), 1);
        }
    }

    #[test]
    fn should_handle_empty_attributes_list() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, 0);
        let attributes = Vec::<Attribute>::new();
        let metric_type = MetricType::Gauge;

        // when
        builder.ingest_sample(
            attributes,
            Some("bytes".to_string()),
            metric_type,
            create_test_sample(),
        );

        // then
        assert_eq!(builder.next_series_id, 1);
        assert_eq!(builder.series_dict_delta.len(), 1);
        assert_eq!(builder.samples.len(), 1);
        assert_eq!(builder.inverted_index.postings.len(), 0); // No attributes to index
        let series_spec = builder.forward_index.series.get(&0).unwrap();
        assert_eq!(series_spec.attributes.len(), 0);
    }

    #[test]
    fn should_handle_none_metric_unit() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, 0);
        let attributes = create_test_attributes();
        let metric_type = MetricType::Gauge;

        // when
        builder.ingest_sample(attributes.clone(), None, metric_type, create_test_sample());

        // then
        let series_spec = builder.forward_index.series.get(&0).unwrap();
        assert_eq!(series_spec.metric_unit, None);
        match (series_spec.metric_type, metric_type) {
            (MetricType::Gauge, MetricType::Gauge) => {}
            _ => panic!("Metric types don't match"),
        }
    }
}
