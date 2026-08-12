//! OTel attribute → Tempo search tags.

use std::collections::{BTreeMap, HashMap};

/// Merge resource then span attributes; span wins on collision.
pub fn project_tempo_tags(
    resource: &HashMap<String, String>,
    span_attrs: &HashMap<String, String>,
    max_tags: usize,
) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    for (k, v) in resource {
        if !k.is_empty() {
            tags.insert(k.clone(), v.clone());
        }
    }
    for (k, v) in span_attrs {
        if !k.is_empty() {
            tags.insert(k.clone(), v.clone());
        }
    }
    if tags.len() <= max_tags {
        return tags;
    }
    tags.into_iter().take(max_tags).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_attrs_win() {
        let mut resource = HashMap::new();
        resource.insert("http.method".into(), "GET".into());
        let mut span = HashMap::new();
        span.insert("http.method".into(), "POST".into());
        let tags = project_tempo_tags(&resource, &span, 40);
        assert_eq!(tags.get("http.method").map(String::as_str), Some("POST"));
    }
}
