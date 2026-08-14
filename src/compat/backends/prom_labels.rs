//! Prom label ↔ DuckLake column / VARIANT path bindings for lean scalar scans.
//!
//! Never emit `CAST(attributes AS JSON)` here. Prefer promoted columns, then
//! per-key `CAST(resource_attributes['k'] AS VARCHAR)` / `CAST(attributes['k'] AS VARCHAR)`.

use crate::compat::projection::prometheus::sanitize_label_name;
use crate::promotion::{PromotionColumn, PromotionSource, TelemetryColumnsManifest, TelemetryTable};
use crate::storage::schema::variant_varchar;
use std::collections::{BTreeMap, BTreeSet};

/// SQL column alias prefix for projected scalar labels (avoids clashing with base cols).
pub const LABEL_ALIAS_PREFIX: &str = "lbl_";

/// Fixed Prom ↔ OTel alias keys always considered for series identity.
pub fn reserved_identity_keys() -> &'static [&'static str] {
    &[
        "service.name",
        "job",
        "service.instance.id",
        "host.name",
        "instance",
    ]
}

/// One logical Prom/OTel label projected as a VARCHAR scalar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelBinding {
    /// OTel-ish key used in resource/datapoint maps (e.g. `service.name`).
    pub otel_key: String,
    /// Prometheus-safe label / SQL alias suffix (sanitized).
    pub prom_label: String,
    /// Preferred typed column when an active metrics promotion matches.
    pub promoted_column: Option<String>,
    /// Keys to try on `resource_attributes` (order matters for COALESCE).
    pub resource_keys: Vec<String>,
    /// Keys to try on `attributes`.
    pub attribute_keys: Vec<String>,
}

impl LabelBinding {
    /// `COALESCE(promoted, resource…, attr…) AS lbl_<prom_label>`.
    pub fn sql_expr(&self) -> String {
        let parts = self.coalesce_parts();
        let alias = self.sql_alias();
        if parts.is_empty() {
            format!("NULL::VARCHAR AS {alias}")
        } else if parts.len() == 1 {
            format!("{} AS {alias}", parts[0])
        } else {
            format!("COALESCE({}) AS {alias}", parts.join(", "))
        }
    }

    /// Equality predicate fragment without alias (for WHERE).
    pub fn sql_value_expr(&self) -> String {
        let parts = self.coalesce_parts();
        if parts.is_empty() {
            "NULL::VARCHAR".to_string()
        } else if parts.len() == 1 {
            parts[0].clone()
        } else {
            format!("COALESCE({})", parts.join(", "))
        }
    }

    pub fn sql_alias(&self) -> String {
        format!("{LABEL_ALIAS_PREFIX}{}", self.prom_label)
    }

    fn coalesce_parts(&self) -> Vec<String> {
        let mut parts = Vec::new();
        if let Some(col) = &self.promoted_column {
            if is_safe_sql_ident(col) {
                parts.push(col.clone());
            }
        }
        for k in &self.resource_keys {
            parts.push(variant_varchar("resource_attributes", k));
        }
        for k in &self.attribute_keys {
            parts.push(variant_varchar("attributes", k));
        }
        parts
    }
}

fn is_safe_sql_ident(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .enumerate()
            .all(|(i, c)| c.is_ascii_alphanumeric() || c == '_' || (i > 0 && c == '$'))
        && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
}

/// Map active metrics promotions: source OTel key → SQL column name.
pub fn metrics_promotion_by_source(
    manifests: &[TelemetryColumnsManifest],
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for m in manifests {
        if !m.target.tables.contains(&TelemetryTable::Metrics) {
            continue;
        }
        for col in &m.columns {
            if let Some(key) = promotion_source_key(col) {
                out.entry(key).or_insert_with(|| col.name.clone());
            }
        }
    }
    out
}

/// All promotion source keys targeting metrics (for expanding \(N\)).
pub fn metrics_promotion_source_keys(manifests: &[TelemetryColumnsManifest]) -> BTreeSet<String> {
    metrics_promotion_by_source(manifests).into_keys().collect()
}

fn promotion_source_key(col: &PromotionColumn) -> Option<String> {
    match &col.source {
        PromotionSource::ResourceAttribute { key } | PromotionSource::Attribute { key } => {
            Some(key.clone())
        }
        _ => None,
    }
}

/// Build a binding for one OTel/Prom key, preferring promotions when present.
pub fn binding_for_key(otel_key: &str, promotions: &BTreeMap<String, String>) -> LabelBinding {
    let prom_label = sanitize_label_name(otel_key);
    let mut resource_keys = Vec::new();
    let mut attribute_keys = Vec::new();
    let mut promoted = promotions.get(otel_key).cloned();

    match otel_key {
        "job" => {
            resource_keys.extend(["service.name".into(), "job".into()]);
            attribute_keys.extend(["service.name".into(), "job".into()]);
            if promoted.is_none() {
                promoted = promotions.get("service.name").cloned();
            }
        }
        "instance" => {
            resource_keys.extend([
                "service.instance.id".into(),
                "host.name".into(),
                "instance".into(),
            ]);
            attribute_keys.extend([
                "service.instance.id".into(),
                "host.name".into(),
                "instance".into(),
            ]);
            if promoted.is_none() {
                promoted = promotions
                    .get("service.instance.id")
                    .or_else(|| promotions.get("host.name"))
                    .cloned();
            }
        }
        "service.name" => {
            resource_keys.push("service.name".into());
            attribute_keys.push("service.name".into());
        }
        "service.instance.id" => {
            resource_keys.push("service.instance.id".into());
            attribute_keys.push("service.instance.id".into());
        }
        "host.name" => {
            resource_keys.push("host.name".into());
            attribute_keys.push("host.name".into());
            if promoted.is_none() {
                promoted = promotions.get("host.name").cloned();
            }
        }
        other => {
            resource_keys.push(other.to_string());
            attribute_keys.push(other.to_string());
            let dotted = other.replace('_', ".");
            if dotted != other {
                resource_keys.push(dotted.clone());
                attribute_keys.push(dotted.clone());
                if promoted.is_none() {
                    promoted = promotions.get(&dotted).cloned();
                }
            }
        }
    }

    // Dedup while preserving order.
    resource_keys = dedup_preserve(resource_keys);
    attribute_keys = dedup_preserve(attribute_keys);

    LabelBinding {
        otel_key: otel_key.to_string(),
        prom_label,
        promoted_column: promoted,
        resource_keys,
        attribute_keys,
    }
}

fn dedup_preserve(items: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for i in items {
        if seen.insert(i.clone()) {
            out.push(i);
        }
    }
    out
}

/// Build ordered bindings for a set of keys (dedup by sanitized Prom label).
pub fn bindings_for_keys(
    keys: &BTreeSet<String>,
    promotions: &BTreeMap<String, String>,
) -> Vec<LabelBinding> {
    let mut by_prom: BTreeMap<String, LabelBinding> = BTreeMap::new();
    for key in keys {
        let b = binding_for_key(key, promotions);
        by_prom.entry(b.prom_label.clone()).or_insert(b);
    }
    by_prom.into_values().collect()
}

/// Parse DuckLake `variant_path` like `"service.name"` → `service.name`.
pub fn parse_variant_stats_path(path: &str) -> Option<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "root" || trimmed == "element" {
        return None;
    }
    // Skip array-element / composed paths for MVP identity discovery.
    if trimmed.starts_with("element.") || trimmed.starts_with("element\"") {
        return None;
    }
    let Some(inner) = trimmed.strip_prefix('"').and_then(|s| s.strip_suffix('"')) else {
        return None;
    };
    let unescaped = inner.replace("\"\"", "\"");
    if unescaped.is_empty() || unescaped == "root" || unescaped == "element" {
        return None;
    }
    Some(unescaped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promotion::{
        PromotionColumn, PromotionDataType, PromotionSource, TelemetryColumnsManifest,
        TelemetryColumnsTarget, TelemetryTable,
    };

    fn promo_service_name() -> BTreeMap<String, String> {
        BTreeMap::from([("service.name".into(), "service_name".into())])
    }

    #[test]
    fn coalesce_prefers_promoted_then_resource_then_attr() {
        let b = binding_for_key("service.name", &promo_service_name());
        let expr = b.sql_expr();
        assert!(expr.starts_with("COALESCE(service_name, "));
        assert!(expr.contains("CAST(resource_attributes['service.name'] AS VARCHAR)"));
        assert!(expr.contains("CAST(attributes['service.name'] AS VARCHAR)"));
        assert!(expr.ends_with(" AS lbl_service_name"));
        assert!(!expr.contains("CAST(attributes AS JSON)"));
        assert!(!expr.contains("CAST(resource_attributes AS JSON)"));
    }

    #[test]
    fn job_binding_uses_service_name_promotion() {
        let b = binding_for_key("job", &promo_service_name());
        assert_eq!(b.promoted_column.as_deref(), Some("service_name"));
        let expr = b.sql_value_expr();
        assert!(expr.contains("service_name"));
        assert!(expr.contains("service.name"));
        assert!(!expr.contains("CAST(attributes AS JSON)"));
    }

    #[test]
    fn instance_binding_order() {
        let promos = BTreeMap::from([
            ("service.instance.id".into(), "instance_id".into()),
            ("host.name".into(), "host_name".into()),
        ]);
        let b = binding_for_key("instance", &promos);
        assert_eq!(b.promoted_column.as_deref(), Some("instance_id"));
        let expr = b.sql_value_expr();
        assert!(expr.starts_with("COALESCE(instance_id,"));
        assert!(expr.contains("service.instance.id"));
    }

    #[test]
    fn metrics_promotion_map_filters_tables() {
        let m = TelemetryColumnsManifest {
            target: TelemetryColumnsTarget {
                tables: vec![TelemetryTable::Metrics],
            },
            columns: vec![PromotionColumn {
                name: "http_method".into(),
                data_type: PromotionDataType::String,
                nullable: true,
                source: PromotionSource::Attribute {
                    key: "http.method".into(),
                },
            }],
        };
        let map = metrics_promotion_by_source(&[m]);
        assert_eq!(map.get("http.method").map(String::as_str), Some("http_method"));
    }

    #[test]
    fn parse_variant_path_quoted() {
        assert_eq!(
            parse_variant_stats_path("\"service.name\"").as_deref(),
            Some("service.name")
        );
        assert_eq!(parse_variant_stats_path("root"), None);
        assert_eq!(parse_variant_stats_path("element.\"a\""), None);
    }

    #[test]
    fn bindings_dedup_by_prom_label() {
        let mut keys = BTreeSet::new();
        keys.insert("http.method".into());
        keys.insert("http_method".into());
        let bs = bindings_for_keys(&keys, &BTreeMap::new());
        assert_eq!(bs.len(), 1);
        assert_eq!(bs[0].prom_label, "http_method");
    }
}
