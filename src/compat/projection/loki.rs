//! OTel attribute → Loki stream labels + structured metadata.

use std::collections::{BTreeMap, HashMap};

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

    let mut merge = |src: &HashMap<String, String>| {
        for (k, v) in src {
            if k.is_empty() {
                continue;
            }
            if allowlist.iter().any(|a| *a == k) {
                // resource first; log attrs overwrite for same allowlisted key
                stream_labels.insert(k.clone(), v.clone());
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
            stream_labels.insert(k.clone(), v.clone());
        } else {
            structured_metadata.insert(k.clone(), v.clone());
        }
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
            p.stream_labels.get("service.name").map(String::as_str),
            Some("api")
        );
        assert_eq!(
            p.structured_metadata.get("http.route").map(String::as_str),
            Some("/v1")
        );
    }
}
