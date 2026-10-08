//! OTel attribute → Loki stream labels + structured metadata.

use std::collections::{BTreeMap, HashMap};

use super::prometheus::sanitize_label_name;

/// Conservative default stream-label allowlist (low cardinality).
pub const DEFAULT_STREAM_LABEL_ALLOWLIST: &[&str] = &[
    "service.name",
    "service.namespace",
    "deployment.environment",
    "k8s.namespace.name",
    "k8s.cluster.name",
    "cloud.region",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LokiProjection {
    pub stream_labels: BTreeMap<String, String>,
    pub structured_metadata: BTreeMap<String, String>,
}

pub fn project_loki(
    resource: &HashMap<String, String>,
    log_attrs: &HashMap<String, String>,
    allowlist: &[&str],
) -> LokiProjection {
    let mut stream_labels = BTreeMap::new();
    let mut structured_metadata = BTreeMap::new();
    let mut allowlisted = BTreeMap::new();

    let mut merge = |src: &HashMap<String, String>| {
        for (k, v) in src {
            if k.is_empty() {
                continue;
            }
            if allowlist.iter().any(|a| *a == k) {
                allowlisted.insert(k.clone(), v.clone());
            } else {
                structured_metadata.insert(k.clone(), v.clone());
            }
        }
    };

    merge(resource);
    // Log attributes overwrite resource for allowlisted keys (datapoint wins).
    for (k, v) in log_attrs {
        if k.is_empty() {
            continue;
        }
        if allowlist.iter().any(|a| *a == k) {
            allowlisted.insert(k.clone(), v.clone());
        } else {
            structured_metadata.insert(k.clone(), v.clone());
        }
    }

    // Iterate raw keys in order and keep the first value for a sanitized key.
    // This makes collisions such as `service.name`/`service_name` deterministic
    // while preserving exact-key resource/log-attribute precedence above.
    for (raw_key, value) in allowlisted {
        stream_labels
            .entry(sanitize_label_name(&raw_key))
            .or_insert(value);
    }

    LokiProjection {
        stream_labels,
        structured_metadata,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_allowlisted_stream_labels() {
        let mut resource = HashMap::new();
        resource.insert("service.name".into(), "api".into());
        resource.insert("http.route".into(), "/v1".into());
        let log_attrs = HashMap::new();
        let p = project_loki(&resource, &log_attrs, DEFAULT_STREAM_LABEL_ALLOWLIST);
        assert_eq!(
            p.stream_labels.get("service_name").map(String::as_str),
            Some("api")
        );
        assert_eq!(
            p.structured_metadata.get("http.route").map(String::as_str),
            Some("/v1")
        );
    }

    #[test]
    fn sanitizes_promoted_label_names() {
        let resource = [("service.name".into(), "api".into())]
            .into_iter()
            .collect();
        let projection = project_loki(&resource, &HashMap::new(), &["service.name"]);
        assert_eq!(
            projection.stream_labels.get("service_name"),
            Some(&"api".into())
        );
    }

    #[test]
    fn sanitization_collisions_are_deterministic_and_metadata_keeps_raw_keys() {
        let resource = [
            ("service_name".into(), "collision".into()),
            ("service.name".into(), "api".into()),
            ("http.route".into(), "/v1".into()),
        ]
        .into_iter()
        .collect();
        let projection = project_loki(
            &resource,
            &HashMap::new(),
            &["service.name", "service_name"],
        );

        assert_eq!(
            projection.stream_labels.get("service_name"),
            Some(&"api".into())
        );
        assert_eq!(
            projection.structured_metadata.get("http.route"),
            Some(&"/v1".into())
        );
    }
}
