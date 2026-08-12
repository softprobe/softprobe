//! Canonical Phase 0 classic histogram / summary fidelity columns on `metrics`.
//!
//! Single inventory for DuckLake widen DDL, promotion reserved names, and schema
//! consistency checks. Arrow field construction stays in `storage::schema` (different
//! type system) but must include every name here.

/// `(column_name, DuckDB SQL type)` for nullable fidelity columns.
pub const METRICS_FIDELITY_COLUMNS: &[(&str, &str)] = &[
    ("count", "UBIGINT"),
    ("sum", "DOUBLE"),
    ("bucket_counts", "UBIGINT[]"),
    ("explicit_bounds", "DOUBLE[]"),
    ("quantiles", "STRUCT(quantile DOUBLE, value DOUBLE)[]"),
    ("aggregation_temporality", "VARCHAR"),
    ("exemplars_json", "VARCHAR"),
];

pub fn metrics_fidelity_column_names() -> impl Iterator<Item = &'static str> {
    METRICS_FIDELITY_COLUMNS.iter().map(|(name, _)| *name)
}

/// Loose DuckDB DESCRIBE compatibility for fidelity widen type checks.
pub fn fidelity_sql_types_compatible(found: &str, expected: &str) -> bool {
    let f = normalize_duckdb_type(found);
    let e = normalize_duckdb_type(expected);
    if f == e {
        return true;
    }
    match e.as_str() {
        "UBIGINT" => matches!(f.as_str(), "UBIGINT" | "UINT64" | "BIGINT" | "HUGEINT"),
        "DOUBLE" => matches!(f.as_str(), "DOUBLE" | "FLOAT" | "FLOAT8" | "REAL"),
        "VARCHAR" => f == "VARCHAR" || f == "TEXT" || f.starts_with("VARCHAR("),
        other if other.contains("STRUCT") => f.contains("STRUCT"),
        other if other.ends_with("[]") => {
            let inner_expected = &other[..other.len() - 2];
            f.ends_with("[]") && fidelity_sql_types_compatible(&f[..f.len() - 2], inner_expected)
        }
        _ => false,
    }
}

fn normalize_duckdb_type(t: &str) -> String {
    t.chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fidelity_type_aliases_accepted() {
        assert!(fidelity_sql_types_compatible("UBIGINT", "UBIGINT"));
        assert!(fidelity_sql_types_compatible("UINT64", "UBIGINT"));
        assert!(fidelity_sql_types_compatible("DOUBLE", "DOUBLE"));
        assert!(fidelity_sql_types_compatible("VARCHAR", "VARCHAR"));
        assert!(!fidelity_sql_types_compatible("VARCHAR", "UBIGINT"));
        assert!(!fidelity_sql_types_compatible("DOUBLE", "UBIGINT"));
    }
}
