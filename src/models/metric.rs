use crate::storage::buffer::Bufferable;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

/// Metric data model representing an OTLP metric data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    /// Name of the metric (e.g., "http.server.duration")
    pub metric_name: String,

    /// Human-readable description of the metric
    pub description: String,

    /// Unit of measurement (e.g., "ms", "bytes", "1")
    pub unit: String,

    /// Type of metric: "gauge", "sum", "histogram", "summary"
    pub metric_type: String,

    /// Timestamp when the metric was recorded
    pub timestamp: DateTime<Utc>,

    /// Numeric value of the metric data point
    pub value: f64,

    /// Additional attributes specific to this metric data point
    pub attributes: HashMap<String, String>,

    /// Resource attributes (e.g., service.name, host.name)
    pub resource_attributes: HashMap<String, String>,
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
