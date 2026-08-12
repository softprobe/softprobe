use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use tracing::warn;

/// Stable code logged when an exponential histogram datapoint is skipped.
pub const UNSUPPORTED_EXPONENTIAL_HISTOGRAM: &str = "unsupported_feature";

/// One summary quantile value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SummaryQuantile {
    pub quantile: f64,
    pub value: f64,
}

/// Metric data model representing an OTLP metric data point.
///
/// Matches `OtlpMetricsTable` in `src/storage/schema/tables.rs`. Gauge/sum use
/// `value`; classic histogram/summary also populate fidelity columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub metric_name: String,
    pub description: String,
    pub unit: String,
    /// "gauge", "sum", "histogram", or "summary"
    pub metric_type: String,
    pub timestamp: DateTime<Utc>,
    /// Scalar for gauge/sum; for histogram/summary equals `sum` (SQL compat).
    pub value: f64,
    pub attributes: HashMap<String, String>,
    pub resource_attributes: HashMap<String, String>,
    /// Histogram/summary population count.
    pub count: Option<u64>,
    /// Histogram/summary sum (also mirrored in `value`).
    pub sum: Option<f64>,
    pub bucket_counts: Option<Vec<u64>>,
    pub explicit_bounds: Option<Vec<f64>>,
    pub quantiles: Option<Vec<SummaryQuantile>>,
    /// OTLP AggregationTemporality name: "DELTA" | "CUMULATIVE" | "UNSPECIFIED"
    pub aggregation_temporality: Option<String>,
    /// JSON array of exemplar objects (trace_id, span_id, value, time, attrs).
    pub exemplars_json: Option<String>,
}

impl Default for Metric {
    fn default() -> Self {
        Self {
            metric_name: String::new(),
            description: String::new(),
            unit: String::new(),
            metric_type: String::new(),
            timestamp: Utc::now(),
            value: 0.0,
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
            count: None,
            sum: None,
            bucket_counts: None,
            explicit_bounds: None,
            quantiles: None,
            aggregation_temporality: None,
            exemplars_json: None,
        }
    }
}

impl Metric {
    pub fn to_record_batch(
        metrics: &[Metric],
        schema: &arrow::datatypes::Schema,
    ) -> anyhow::Result<arrow::record_batch::RecordBatch> {
        crate::storage::schema::arrow::metrics_to_record_batch(metrics, schema)
    }

    pub fn from_otlp(
        otlp_metric: &opentelemetry_proto::tonic::metrics::v1::Metric,
        resource_attributes: &HashMap<String, String>,
    ) -> Result<Vec<Self>> {
        let mut metrics = Vec::new();

        let metric_name = otlp_metric.name.clone();
        let description = otlp_metric.description.clone();
        let unit = otlp_metric.unit.clone();

        use opentelemetry_proto::tonic::metrics::v1::metric::Data;
        if let Some(data) = &otlp_metric.data {
            match data {
                Data::Gauge(gauge) => {
                    for data_point in &gauge.data_points {
                        if let Some(metric) = Self::from_number_data_point(
                            data_point,
                            &metric_name,
                            &description,
                            &unit,
                            "gauge",
                            resource_attributes,
                            None,
                        ) {
                            metrics.push(metric);
                        }
                    }
                }
                Data::Sum(sum) => {
                    let temporality = temporality_name(sum.aggregation_temporality);
                    for data_point in &sum.data_points {
                        if let Some(metric) = Self::from_number_data_point(
                            data_point,
                            &metric_name,
                            &description,
                            &unit,
                            "sum",
                            resource_attributes,
                            Some(temporality),
                        ) {
                            metrics.push(metric);
                        }
                    }
                }
                Data::Histogram(histogram) => {
                    let temporality = temporality_name(histogram.aggregation_temporality);
                    for data_point in &histogram.data_points {
                        if let Some(metric) = Self::from_histogram_data_point(
                            data_point,
                            &metric_name,
                            &description,
                            &unit,
                            resource_attributes,
                            temporality,
                        ) {
                            metrics.push(metric);
                        }
                    }
                }
                Data::Summary(summary) => {
                    for data_point in &summary.data_points {
                        if let Some(metric) = Self::from_summary_data_point(
                            data_point,
                            &metric_name,
                            &description,
                            &unit,
                            resource_attributes,
                        ) {
                            metrics.push(metric);
                        }
                    }
                }
                Data::ExponentialHistogram(_) => {
                    // Explicit unsupported: skip datapoints with stable code (do not
                    // silently approximate as classic histogram).
                    warn!(
                        code = UNSUPPORTED_EXPONENTIAL_HISTOGRAM,
                        metric = %metric_name,
                        "ExponentialHistogram datapoints skipped (unsupported_feature)"
                    );
                }
            }
        }

        Ok(metrics)
    }

    pub fn extract_resource_attributes(
        resource_metrics: &opentelemetry_proto::tonic::metrics::v1::ResourceMetrics,
    ) -> HashMap<String, String> {
        match &resource_metrics.resource {
            Some(resource) => crate::models::key_values_to_map(&resource.attributes),
            None => HashMap::new(),
        }
    }

    fn from_number_data_point(
        data_point: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
        metric_name: &str,
        description: &str,
        unit: &str,
        metric_type: &str,
        resource_attributes: &HashMap<String, String>,
        aggregation_temporality: Option<&str>,
    ) -> Option<Self> {
        let timestamp = timestamp_from_unix_nano(data_point.time_unix_nano);

        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
        let value = match &data_point.value {
            Some(Value::AsDouble(v)) => *v,
            Some(Value::AsInt(v)) => *v as f64,
            None => return None,
        };

        let attributes = Self::extract_attributes(&data_point.attributes);
        let exemplars_json = encode_number_exemplars(&data_point.exemplars);

        Some(Metric {
            metric_name: metric_name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metric_type: metric_type.to_string(),
            timestamp,
            value,
            attributes,
            resource_attributes: resource_attributes.clone(),
            count: None,
            sum: None,
            bucket_counts: None,
            explicit_bounds: None,
            quantiles: None,
            aggregation_temporality: aggregation_temporality.map(str::to_string),
            exemplars_json,
        })
    }

    fn from_histogram_data_point(
        data_point: &opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint,
        metric_name: &str,
        description: &str,
        unit: &str,
        resource_attributes: &HashMap<String, String>,
        aggregation_temporality: &str,
    ) -> Option<Self> {
        let timestamp = timestamp_from_unix_nano(data_point.time_unix_nano);
        // Keep absent OTLP histogram sum as NULL (valid when observations may be negative).
        // `value` mirrors sum when present, else 0.0 for backward SQL scalar compatibility.
        let sum = data_point.sum;
        let attributes = Self::extract_attributes(&data_point.attributes);
        let exemplars_json = encode_histogram_exemplars(&data_point.exemplars);

        Some(Metric {
            metric_name: metric_name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metric_type: "histogram".to_string(),
            timestamp,
            value: sum.unwrap_or(0.0),
            attributes,
            resource_attributes: resource_attributes.clone(),
            count: Some(data_point.count),
            sum,
            bucket_counts: Some(data_point.bucket_counts.clone()),
            explicit_bounds: Some(data_point.explicit_bounds.clone()),
            quantiles: None,
            aggregation_temporality: Some(aggregation_temporality.to_string()),
            exemplars_json,
        })
    }

    fn from_summary_data_point(
        data_point: &opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint,
        metric_name: &str,
        description: &str,
        unit: &str,
        resource_attributes: &HashMap<String, String>,
    ) -> Option<Self> {
        let timestamp = timestamp_from_unix_nano(data_point.time_unix_nano);
        let sum = data_point.sum;
        let attributes = Self::extract_attributes(&data_point.attributes);
        let quantiles = data_point
            .quantile_values
            .iter()
            .map(|q| SummaryQuantile {
                quantile: q.quantile,
                value: q.value,
            })
            .collect::<Vec<_>>();

        Some(Metric {
            metric_name: metric_name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metric_type: "summary".to_string(),
            timestamp,
            value: sum,
            attributes,
            resource_attributes: resource_attributes.clone(),
            count: Some(data_point.count),
            sum: Some(sum),
            bucket_counts: None,
            explicit_bounds: None,
            quantiles: Some(quantiles),
            aggregation_temporality: None,
            exemplars_json: None,
        })
    }

    fn extract_attributes(
        otlp_attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    ) -> HashMap<String, String> {
        crate::models::key_values_to_map(otlp_attributes)
    }

    pub fn partition_key(&self) -> chrono::NaiveDate {
        self.timestamp.date_naive()
    }

    pub fn grouping_key(&self) -> String {
        self.metric_name.clone()
    }

    pub fn compare_for_sort(&self, other: &Self) -> Ordering {
        self.metric_name
            .cmp(&other.metric_name)
            .then_with(|| self.timestamp.cmp(&other.timestamp))
    }
}

fn timestamp_from_unix_nano(time_unix_nano: u64) -> DateTime<Utc> {
    if time_unix_nano > 0 {
        chrono::DateTime::from_timestamp(
            (time_unix_nano / 1_000_000_000) as i64,
            (time_unix_nano % 1_000_000_000) as u32,
        )
        .unwrap_or_else(Utc::now)
    } else {
        Utc::now()
    }
}

fn temporality_name(v: i32) -> &'static str {
    match v {
        1 => "DELTA",
        2 => "CUMULATIVE",
        _ => "UNSPECIFIED",
    }
}

fn encode_number_exemplars(
    exemplars: &[opentelemetry_proto::tonic::metrics::v1::Exemplar],
) -> Option<String> {
    if exemplars.is_empty() {
        return None;
    }
    let encoded: Vec<_> = exemplars
        .iter()
        .map(|e| {
            let value = match e.value {
                Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(v)) => {
                    serde_json::json!(v)
                }
                Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsInt(v)) => {
                    serde_json::json!(v as f64)
                }
                None => serde_json::Value::Null,
            };
            let filtered_attributes: serde_json::Map<String, serde_json::Value> = e
                .filtered_attributes
                .iter()
                .filter_map(|kv| {
                    let v = kv
                        .value
                        .as_ref()
                        .and_then(crate::models::any_value_to_json)?;
                    Some((kv.key.clone(), v))
                })
                .collect();
            serde_json::json!({
                "time_unix_nano": e.time_unix_nano,
                "value": value,
                "span_id_hex": hex::encode(&e.span_id),
                "trace_id_hex": hex::encode(&e.trace_id),
                "filtered_attributes": filtered_attributes,
            })
        })
        .collect();
    serde_json::to_string(&encoded).ok()
}

fn encode_histogram_exemplars(
    exemplars: &[opentelemetry_proto::tonic::metrics::v1::Exemplar],
) -> Option<String> {
    encode_number_exemplars(exemplars)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone};
    use opentelemetry_proto::tonic::metrics::v1::{
        metric::Data, summary_data_point::ValueAtQuantile, ExponentialHistogram, Histogram,
        HistogramDataPoint, Metric as OtlpMetric, Summary, SummaryDataPoint,
    };

    fn base_metric(name: &str, metric_type: &str, value: f64, ts: DateTime<Utc>) -> Metric {
        Metric {
            metric_name: name.to_string(),
            description: String::new(),
            unit: String::new(),
            metric_type: metric_type.to_string(),
            timestamp: ts,
            value,
            ..Default::default()
        }
    }

    #[test]
    fn test_metric_partition_key() {
        let metric = base_metric(
            "http.server.duration",
            "histogram",
            123.45,
            Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap(),
        );
        let partition = metric.partition_key();
        assert_eq!(partition.year(), 2025);
        assert_eq!(partition.month(), 1);
        assert_eq!(partition.day(), 15);
    }

    #[test]
    fn test_metric_grouping_key() {
        let metric = base_metric("cpu.usage", "gauge", 75.5, Utc::now());
        assert_eq!(metric.grouping_key(), "cpu.usage");
    }

    #[test]
    fn test_metric_sort_order() {
        let metric1 = base_metric(
            "aaa",
            "gauge",
            1.0,
            Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
        );
        let metric2 = base_metric(
            "aaa",
            "gauge",
            2.0,
            Utc.with_ymd_and_hms(2025, 1, 15, 11, 0, 0).unwrap(),
        );
        let metric3 = base_metric(
            "bbb",
            "gauge",
            3.0,
            Utc.with_ymd_and_hms(2025, 1, 15, 9, 0, 0).unwrap(),
        );
        assert!(metric1.compare_for_sort(&metric2) == Ordering::Less);
        assert!(metric3.compare_for_sort(&metric1) == Ordering::Greater);
    }

    #[test]
    fn classic_histogram_preserves_buckets() {
        let otlp = OtlpMetric {
            name: "http.server.duration".into(),
            description: "latency".into(),
            unit: "ms".into(),
            data: Some(Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: 1_640_995_200_000_000_000,
                    count: 10,
                    sum: Some(100.0),
                    bucket_counts: vec![2, 5, 3],
                    explicit_bounds: vec![10.0, 50.0],
                    exemplars: vec![],
                    flags: 0,
                    min: Some(1.0),
                    max: Some(80.0),
                }],
                aggregation_temporality: 2, // CUMULATIVE
            })),
            metadata: vec![],
        };
        let rows = Metric::from_otlp(&otlp, &HashMap::new()).unwrap();
        assert_eq!(rows.len(), 1);
        let m = &rows[0];
        assert_eq!(m.metric_type, "histogram");
        assert_eq!(m.count, Some(10));
        assert_eq!(m.sum, Some(100.0));
        assert_eq!(m.value, 100.0);
        assert_eq!(m.bucket_counts.as_deref(), Some(&[2, 5, 3][..]));
        assert_eq!(m.explicit_bounds.as_deref(), Some(&[10.0, 50.0][..]));
        assert_eq!(m.aggregation_temporality.as_deref(), Some("CUMULATIVE"));
    }

    #[test]
    fn classic_histogram_absent_sum_stays_null() {
        let otlp = OtlpMetric {
            name: "http.server.duration".into(),
            description: "latency".into(),
            unit: "ms".into(),
            data: Some(Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: 1_640_995_200_000_000_000,
                    count: 3,
                    sum: None,
                    bucket_counts: vec![1, 1, 1],
                    explicit_bounds: vec![10.0, 50.0],
                    exemplars: vec![],
                    flags: 0,
                    min: None,
                    max: None,
                }],
                aggregation_temporality: 1, // DELTA
            })),
            metadata: vec![],
        };
        let rows = Metric::from_otlp(&otlp, &HashMap::new()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sum, None);
        assert_eq!(rows[0].value, 0.0); // scalar fallback only
        assert_eq!(rows[0].count, Some(3));
    }

    #[test]
    fn histogram_exemplars_preserve_filtered_attributes() {
        use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
        use opentelemetry_proto::tonic::metrics::v1::{exemplar, Exemplar};

        let otlp = OtlpMetric {
            name: "http.server.duration".into(),
            description: "".into(),
            unit: "ms".into(),
            data: Some(Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: 1_640_995_200_000_000_000,
                    count: 1,
                    sum: Some(12.0),
                    bucket_counts: vec![1],
                    explicit_bounds: vec![],
                    exemplars: vec![Exemplar {
                        filtered_attributes: vec![KeyValue {
                            key: "http.status_code".into(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::IntValue(500)),
                            }),
                        }],
                        time_unix_nano: 1_640_995_200_000_000_000,
                        value: Some(exemplar::Value::AsDouble(12.0)),
                        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                        trace_id: vec![9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9],
                    }],
                    flags: 0,
                    min: None,
                    max: None,
                }],
                aggregation_temporality: 2,
            })),
            metadata: vec![],
        };
        let rows = Metric::from_otlp(&otlp, &HashMap::new()).unwrap();
        let json = rows[0].exemplars_json.as_deref().expect("exemplars");
        let parsed: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(parsed[0]["filtered_attributes"]["http.status_code"], 500);
        assert_eq!(parsed[0]["value"], 12.0);
    }

    #[test]
    fn summary_preserves_quantiles() {
        let otlp = OtlpMetric {
            name: "rpc.latency".into(),
            description: "".into(),
            unit: "ms".into(),
            data: Some(Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: 1_640_995_200_000_000_000,
                    count: 100,
                    sum: 500.0,
                    quantile_values: vec![
                        ValueAtQuantile {
                            quantile: 0.5,
                            value: 4.0,
                        },
                        ValueAtQuantile {
                            quantile: 0.99,
                            value: 20.0,
                        },
                    ],
                    flags: 0,
                }],
            })),
            metadata: vec![],
        };
        let rows = Metric::from_otlp(&otlp, &HashMap::new()).unwrap();
        assert_eq!(rows.len(), 1);
        let q = rows[0].quantiles.as_ref().unwrap();
        assert_eq!(q.len(), 2);
        assert_eq!(q[0].quantile, 0.5);
        assert_eq!(q[1].value, 20.0);
    }

    #[test]
    fn exponential_histogram_yields_no_rows() {
        let otlp = OtlpMetric {
            name: "exp".into(),
            description: "".into(),
            unit: "".into(),
            data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![],
                aggregation_temporality: 2,
            })),
            metadata: vec![],
        };
        let rows = Metric::from_otlp(&otlp, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }
}
