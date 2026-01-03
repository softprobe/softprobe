use crate::storage::buffer::Bufferable;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use anyhow::Result;

/// Metric data model representing an OTLP metric data point
///
/// This struct EXACTLY matches the Iceberg schema defined in src/storage/iceberg/tables.rs
/// Field order matches Iceberg field IDs for consistency
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    // Field 1-4: Metric identity
    /// Name of the metric (e.g., "http.server.duration")
    pub metric_name: String,

    /// Human-readable description of the metric
    pub description: String,

    /// Unit of measurement (e.g., "ms", "bytes", "1")
    pub unit: String,

    /// Type of metric: "gauge", "sum", "histogram", "summary"
    pub metric_type: String,

    // Field 5-6: Timestamp and value
    /// Timestamp when the metric was recorded
    pub timestamp: DateTime<Utc>,

    /// Numeric value of the metric data point
    pub value: f64,

    // Field 7: Attributes MAP<STRING, STRING>
    /// Additional attributes specific to this metric data point
    /// (e.g., http.method, http.status_code, custom tags)
    pub attributes: HashMap<String, String>,

    // Field 8: Resource attributes MAP<STRING, STRING>
    /// Resource attributes identifying the metric source
    /// (e.g., service.name, host.name, k8s.pod.name)
    pub resource_attributes: HashMap<String, String>,

    // Field 13: record_date (partition key - computed, not stored in struct)
    // Derived from timestamp at write time in arrow.rs
}

impl Metric {
    /// Convert a batch of Metrics to Arrow RecordBatch for Iceberg storage
    pub fn to_record_batch(
        metrics: &[Metric],
        iceberg_schema: &iceberg::spec::Schema,
    ) -> anyhow::Result<arrow::record_batch::RecordBatch> {
        crate::storage::iceberg::arrow::metrics_to_record_batch(metrics, iceberg_schema)
    }

    /// Create Metrics from an OTLP Metric and resource attributes
    /// Returns a Vec because a single OTLP metric can contain multiple data points
    pub fn from_otlp(
        otlp_metric: &opentelemetry_proto::tonic::metrics::v1::Metric,
        resource_attributes: &HashMap<String, String>,
    ) -> Result<Vec<Self>> {
        let mut metrics = Vec::new();

        let metric_name = otlp_metric.name.clone();
        let description = otlp_metric.description.clone();
        let unit = otlp_metric.unit.clone();

        // Determine metric type and extract data points
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
                        ) {
                            metrics.push(metric);
                        }
                    }
                }
                Data::Sum(sum) => {
                    for data_point in &sum.data_points {
                        if let Some(metric) = Self::from_number_data_point(
                            data_point,
                            &metric_name,
                            &description,
                            &unit,
                            "sum",
                            resource_attributes,
                        ) {
                            metrics.push(metric);
                        }
                    }
                }
                Data::Histogram(histogram) => {
                    for data_point in &histogram.data_points {
                        if let Some(metric) = Self::from_histogram_data_point(
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
                    // Simplified: skip exponential histograms
                    tracing::warn!("ExponentialHistogram not fully supported, skipping");
                }
            }
        }

        Ok(metrics)
    }

    /// Extract resource attributes from OTLP ResourceMetrics
    pub fn extract_resource_attributes(
        resource_metrics: &opentelemetry_proto::tonic::metrics::v1::ResourceMetrics,
    ) -> HashMap<String, String> {
        let mut attributes = HashMap::new();

        if let Some(resource) = &resource_metrics.resource {
            for attr in &resource.attributes {
                if let Some(value) = &attr.value {
                    let value_str = match value.value.as_ref() {
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                        _ => continue,
                    };
                    attributes.insert(attr.key.clone(), value_str);
                }
            }
        }

        attributes
    }

    /// Convert a NumberDataPoint to Metric
    fn from_number_data_point(
        data_point: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
        metric_name: &str,
        description: &str,
        unit: &str,
        metric_type: &str,
        resource_attributes: &HashMap<String, String>,
    ) -> Option<Self> {
        // Extract timestamp
        let timestamp = if data_point.time_unix_nano > 0 {
            chrono::DateTime::from_timestamp(
                (data_point.time_unix_nano / 1_000_000_000) as i64,
                (data_point.time_unix_nano % 1_000_000_000) as u32,
            ).unwrap_or_else(|| chrono::Utc::now())
        } else {
            chrono::Utc::now()
        };

        // Extract value
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
        let value = match &data_point.value {
            Some(Value::AsDouble(v)) => *v,
            Some(Value::AsInt(v)) => *v as f64,
            None => return None,
        };

        // Extract attributes
        let attributes = Self::extract_attributes(&data_point.attributes);

        Some(Metric {
            metric_name: metric_name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metric_type: metric_type.to_string(),
            timestamp,
            value,
            attributes,
            resource_attributes: resource_attributes.clone(),
        })
    }

    /// Convert a HistogramDataPoint to Metric (using sum as value)
    fn from_histogram_data_point(
        data_point: &opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint,
        metric_name: &str,
        description: &str,
        unit: &str,
        resource_attributes: &HashMap<String, String>,
    ) -> Option<Self> {
        // Extract timestamp
        let timestamp = if data_point.time_unix_nano > 0 {
            chrono::DateTime::from_timestamp(
                (data_point.time_unix_nano / 1_000_000_000) as i64,
                (data_point.time_unix_nano % 1_000_000_000) as u32,
            ).unwrap_or_else(|| chrono::Utc::now())
        } else {
            chrono::Utc::now()
        };

        // Use sum as the value (default to 0.0 if None)
        let value = data_point.sum.unwrap_or(0.0);

        // Extract attributes
        let mut attributes = Self::extract_attributes(&data_point.attributes);

        // Add histogram-specific attributes
        attributes.insert("count".to_string(), data_point.count.to_string());

        Some(Metric {
            metric_name: metric_name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metric_type: "histogram".to_string(),
            timestamp,
            value,
            attributes,
            resource_attributes: resource_attributes.clone(),
        })
    }

    /// Convert a SummaryDataPoint to Metric (using sum as value)
    fn from_summary_data_point(
        data_point: &opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint,
        metric_name: &str,
        description: &str,
        unit: &str,
        resource_attributes: &HashMap<String, String>,
    ) -> Option<Self> {
        // Extract timestamp
        let timestamp = if data_point.time_unix_nano > 0 {
            chrono::DateTime::from_timestamp(
                (data_point.time_unix_nano / 1_000_000_000) as i64,
                (data_point.time_unix_nano % 1_000_000_000) as u32,
            ).unwrap_or_else(|| chrono::Utc::now())
        } else {
            chrono::Utc::now()
        };

        // Use sum as the value
        let value = data_point.sum;

        // Extract attributes
        let mut attributes = Self::extract_attributes(&data_point.attributes);

        // Add summary-specific attributes
        attributes.insert("count".to_string(), data_point.count.to_string());

        Some(Metric {
            metric_name: metric_name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metric_type: "summary".to_string(),
            timestamp,
            value,
            attributes,
            resource_attributes: resource_attributes.clone(),
        })
    }

    /// Extract attributes from OTLP KeyValue pairs
    fn extract_attributes(
        otlp_attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    ) -> HashMap<String, String> {
        let mut attributes = HashMap::new();
        for attr in otlp_attributes {
            if let Some(attr_value) = &attr.value {
                let value_str = match attr_value.value.as_ref() {
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                    _ => continue,
                };
                attributes.insert(attr.key.clone(), value_str);
            }
        }
        attributes
    }
}

impl Bufferable for Metric {
    /// Partition key based on the date from timestamp
    fn partition_key(&self) -> chrono::NaiveDate {
        self.timestamp.date_naive()
    }

    /// Group by metric_name (NOT session_id - metrics are aggregations)
    fn grouping_key(&self) -> String {
        self.metric_name.clone()
    }

    /// Sort by metric_name first, then timestamp
    /// This matches Iceberg sort order (field 1, 5)
    fn compare_for_sort(&self, other: &Self) -> Ordering {
        self.metric_name
            .cmp(&other.metric_name)
            .then_with(|| self.timestamp.cmp(&other.timestamp))
    }

    /// Return the timestamp for time-based operations
    fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Datelike};

    #[test]
    fn test_metric_partition_key() {
        let metric = Metric {
            metric_name: "http.server.duration".to_string(),
            description: "HTTP request duration".to_string(),
            unit: "ms".to_string(),
            metric_type: "histogram".to_string(),
            timestamp: Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap(),
            value: 123.45,
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
        };

        let partition = metric.partition_key();
        assert_eq!(partition.year(), 2025);
        assert_eq!(partition.month(), 1);
        assert_eq!(partition.day(), 15);
    }

    #[test]
    fn test_metric_grouping_key() {
        let metric = Metric {
            metric_name: "cpu.usage".to_string(),
            description: "CPU usage percentage".to_string(),
            unit: "%".to_string(),
            metric_type: "gauge".to_string(),
            timestamp: Utc::now(),
            value: 75.5,
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
        };

        assert_eq!(metric.grouping_key(), "cpu.usage");
    }

    #[test]
    fn test_metric_sort_order() {
        let metric1 = Metric {
            metric_name: "aaa".to_string(),
            description: "".to_string(),
            unit: "".to_string(),
            metric_type: "gauge".to_string(),
            timestamp: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
            value: 1.0,
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
        };

        let metric2 = Metric {
            metric_name: "aaa".to_string(),
            description: "".to_string(),
            unit: "".to_string(),
            metric_type: "gauge".to_string(),
            timestamp: Utc.with_ymd_and_hms(2025, 1, 15, 11, 0, 0).unwrap(),
            value: 2.0,
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
        };

        let metric3 = Metric {
            metric_name: "bbb".to_string(),
            description: "".to_string(),
            unit: "".to_string(),
            metric_type: "gauge".to_string(),
            timestamp: Utc.with_ymd_and_hms(2025, 1, 15, 9, 0, 0).unwrap(),
            value: 3.0,
            attributes: HashMap::new(),
            resource_attributes: HashMap::new(),
        };

        // metric1 < metric2 (same name, earlier timestamp)
        assert_eq!(metric1.compare_for_sort(&metric2), Ordering::Less);

        // metric1 < metric3 (different name, "aaa" < "bbb")
        assert_eq!(metric1.compare_for_sort(&metric3), Ordering::Less);

        // metric3 > metric2 (different name, "bbb" > "aaa")
        assert_eq!(metric3.compare_for_sort(&metric2), Ordering::Greater);
    }
}
