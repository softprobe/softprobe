//! OTel attribute → Prometheus label projection.

use std::collections::{BTreeMap, HashMap};

/// Sanitize a key to Prometheus label name rules.
pub fn sanitize_label_name(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 1);
    for (i, ch) in raw.chars().enumerate() {
        let ok = ch.is_ascii_alphanumeric() || ch == '_';
        if ok {
            if i == 0 && ch.is_ascii_digit() {
                out.push('_');
            }
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "_".into()
    } else {
        out
    }
}

/// Merge resource then datapoint attributes; datapoint wins on collision.
/// Applies cardinality cap after reserved aliases.
pub fn project_prometheus_labels(
    metric_name: &str,
    resource: &HashMap<String, String>,
    datapoint: &HashMap<String, String>,
    max_labels: usize,
) -> BTreeMap<String, String> {
    let mut merged: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in resource {
        if k.is_empty() {
            continue;
        }
        merged.insert(sanitize_label_name(k), v.clone());
    }
    for (k, v) in datapoint {
        if k.is_empty() {
            continue;
        }
        merged.insert(sanitize_label_name(k), v.clone());
    }

    if let Some(svc) = resource
        .get("service.name")
        .or_else(|| datapoint.get("service.name"))
    {
        merged.entry("job".into()).or_insert_with(|| svc.clone());
    }
    if let Some(inst) = resource
        .get("service.instance.id")
        .or_else(|| resource.get("host.name"))
        .or_else(|| datapoint.get("service.instance.id"))
        .or_else(|| datapoint.get("host.name"))
    {
        merged
            .entry("instance".into())
            .or_insert_with(|| inst.clone());
    }

    merged.insert("__name__".into(), sanitize_label_name(metric_name));

    if merged.len() <= max_labels {
        return merged;
    }

    // Keep reserved keys, then fill remaining slots in key order.
    let reserved = ["__name__", "job", "instance"];
    let mut out = BTreeMap::new();
    for key in reserved {
        if let Some(v) = merged.remove(key) {
            out.insert(key.to_string(), v);
        }
    }
    for (k, v) in merged {
        if out.len() >= max_labels {
            break;
        }
        out.insert(k, v);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datapoint_wins_on_collision() {
        let mut resource = HashMap::new();
        resource.insert("http.method".into(), "GET".into());
        let mut dp = HashMap::new();
        dp.insert("http.method".into(), "POST".into());
        let labels = project_prometheus_labels("http_requests", &resource, &dp, 40);
        assert_eq!(labels.get("http_method").map(String::as_str), Some("POST"));
        assert_eq!(labels.get("__name__").map(String::as_str), Some("http_requests"));
    }

    #[test]
    fn sanitizes_invalid_chars() {
        assert_eq!(sanitize_label_name("http.method"), "http_method");
        assert_eq!(sanitize_label_name("9bad"), "_9bad");
    }

    #[test]
    fn enforces_cardinality_cap() {
        let resource = HashMap::new();
        let mut dp = HashMap::new();
        for i in 0..20 {
            dp.insert(format!("k{i}"), format!("v{i}"));
        }
        let labels = project_prometheus_labels("m", &resource, &dp, 5);
        assert!(labels.len() <= 5);
        assert!(labels.contains_key("__name__"));
    }
}
