use serde::Deserialize;
use std::collections::HashSet;

pub const PROMOTION_SPEC_VERSION: &str = "softprobe.promotion.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionManifest {
    TelemetryColumns(TelemetryColumnsManifest),
    BusinessTable(BusinessTableManifest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryColumnsManifest {
    pub target: TelemetryColumnsTarget,
    pub columns: Vec<PromotionColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryColumnsTarget {
    pub tables: Vec<TelemetryTable>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelemetryTable {
    Traces,
    Logs,
    Metrics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessTableManifest {
    pub target: BusinessTableTarget,
    pub row_selector: RowSelector,
    pub columns: Vec<PromotionColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessTableTarget {
    pub table: String,
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowSelector {
    pub attribute: AttributeSelector,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeSelector {
    pub key: String,
    pub equals: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionColumn {
    pub name: String,
    pub data_type: PromotionDataType,
    pub nullable: bool,
    pub source: PromotionSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionDataType {
    String,
    Bool,
    Int64,
    Double,
    Decimal,
    Timestamp,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionSource {
    ResourceAttribute { key: String },
    Attribute { key: String },
    EventAttribute { event_name: String, key: String },
    HttpRequestBody { json_path: String },
    HttpResponseBody { json_path: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionValidationError {
    code: &'static str,
    path: String,
    message: String,
}

impl PromotionValidationError {
    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn new(code: &'static str, path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            path: path.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for PromotionValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at {}: {}", self.code, self.path, self.message)
    }
}

impl std::error::Error for PromotionValidationError {}

pub fn parse_promotion_manifest(
    input: &str,
) -> Result<PromotionManifest, PromotionValidationError> {
    let raw: RawPromotionManifest = serde_yaml::from_str(input).map_err(|err| {
        PromotionValidationError::new(
            "invalid_yaml",
            "$",
            format!("promotion manifest YAML is invalid: {err}"),
        )
    })?;
    raw.validate()
}

/// Ensure the hardcoded promotion metadata tables exist inside one tenant schema.
///
/// These are control/diagnostic tables for the promotion system itself. They live in the tenant's
/// DuckLake metadata SQL schema so applied specs and row-level extraction errors cannot leak across
/// tenants that share the same Postgres metadata database.
pub async fn ensure_promotion_metadata_tables(
    client: &tokio_postgres::Client,
    tenant_schema: &str,
) -> Result<(), tokio_postgres::Error> {
    for ddl in promotion_metadata_table_ddls(tenant_schema) {
        client.execute(&ddl, &[]).await?;
    }
    Ok(())
}

pub fn promotion_metadata_table_ddls(tenant_schema: &str) -> Vec<String> {
    let schema = quote_sql_ident(tenant_schema);
    vec![
        format!("CREATE SCHEMA IF NOT EXISTS {schema};"),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.promotion_specs (
  spec_id TEXT PRIMARY KEY,
  spec_version TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_table TEXT,
  target_tables TEXT,
  business_version BIGINT,
  manifest_json TEXT NOT NULL,
  manifest_hash TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  applied_by TEXT,
  status TEXT NOT NULL
);"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}.promotion_errors (
  timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  spec_id TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_table TEXT,
  target_column TEXT NOT NULL,
  session_id TEXT,
  trace_id TEXT,
  span_id TEXT,
  event_name TEXT,
  source_signal TEXT NOT NULL,
  source_path TEXT NOT NULL,
  error_code TEXT NOT NULL,
  error_message TEXT NOT NULL,
  raw_value_preview TEXT
);"#
        ),
    ]
}

fn quote_sql_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPromotionManifest {
    spec_version: String,
    target: RawTarget,
    #[serde(default, rename = "rowSelector")]
    row_selector: Option<RawRowSelector>,
    #[serde(default)]
    columns: Vec<RawPromotionColumn>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RawTarget {
    TelemetryColumns { tables: Vec<String> },
    BusinessTable { table: String, version: u32 },
}

#[derive(Debug, Deserialize)]
struct RawRowSelector {
    attribute: RawAttributeSelector,
}

#[derive(Debug, Deserialize)]
struct RawAttributeSelector {
    key: String,
    equals: String,
}

#[derive(Debug, Deserialize)]
struct RawPromotionColumn {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    nullable: bool,
    source: RawPromotionSource,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case")]
enum RawPromotionSource {
    ResourceAttribute { key: String },
    Attribute { key: String },
    EventAttribute { event_name: String, key: String },
    HttpRequestBody { json_path: String },
    HttpResponseBody { json_path: String },
}

impl RawPromotionManifest {
    fn validate(self) -> Result<PromotionManifest, PromotionValidationError> {
        if self.spec_version != PROMOTION_SPEC_VERSION {
            return Err(PromotionValidationError::new(
                "unsupported_spec_version",
                "specVersion",
                format!(
                    "expected {}, got {}",
                    PROMOTION_SPEC_VERSION, self.spec_version
                ),
            ));
        }
        if self.columns.is_empty() {
            return Err(PromotionValidationError::new(
                "missing_columns",
                "columns",
                "promotion manifest must declare at least one column",
            ));
        }

        match self.target {
            RawTarget::TelemetryColumns { tables } => {
                let target = validate_telemetry_target(tables)?;
                let columns = validate_columns(self.columns, ColumnNullability::Telemetry)?;
                Ok(PromotionManifest::TelemetryColumns(
                    TelemetryColumnsManifest { target, columns },
                ))
            }
            RawTarget::BusinessTable { table, version } => {
                validate_identifier("target.table", &table)?;
                if version == 0 {
                    return Err(PromotionValidationError::new(
                        "invalid_version",
                        "target.version",
                        "business table version must be greater than zero",
                    ));
                }
                let row_selector = self
                    .row_selector
                    .ok_or_else(|| {
                        PromotionValidationError::new(
                            "missing_row_selector",
                            "rowSelector",
                            "business table promotion requires rowSelector",
                        )
                    })?
                    .validate()?;
                let columns = validate_columns(self.columns, ColumnNullability::Business)?;
                Ok(PromotionManifest::BusinessTable(BusinessTableManifest {
                    target: BusinessTableTarget { table, version },
                    row_selector,
                    columns,
                }))
            }
        }
    }
}

impl RawRowSelector {
    fn validate(self) -> Result<RowSelector, PromotionValidationError> {
        validate_non_empty("rowSelector.attribute.key", &self.attribute.key)?;
        validate_non_empty("rowSelector.attribute.equals", &self.attribute.equals)?;
        Ok(RowSelector {
            attribute: AttributeSelector {
                key: self.attribute.key,
                equals: self.attribute.equals,
            },
        })
    }
}

enum ColumnNullability {
    Telemetry,
    Business,
}

fn validate_telemetry_target(
    tables: Vec<String>,
) -> Result<TelemetryColumnsTarget, PromotionValidationError> {
    if tables.is_empty() {
        return Err(PromotionValidationError::new(
            "missing_tables",
            "target.tables",
            "telemetry column promotion requires at least one table",
        ));
    }
    let mut out = Vec::with_capacity(tables.len());
    let mut seen = HashSet::new();
    for (idx, table) in tables.into_iter().enumerate() {
        let parsed = match table.as_str() {
            "traces" => TelemetryTable::Traces,
            "logs" => TelemetryTable::Logs,
            "metrics" => TelemetryTable::Metrics,
            _ => {
                return Err(PromotionValidationError::new(
                    "invalid_telemetry_table",
                    format!("target.tables[{idx}]"),
                    "telemetry tables must be one of traces, logs, metrics",
                ))
            }
        };
        if !seen.insert(table) {
            return Err(PromotionValidationError::new(
                "duplicate_table",
                format!("target.tables[{idx}]"),
                "telemetry table is listed more than once",
            ));
        }
        out.push(parsed);
    }
    Ok(TelemetryColumnsTarget { tables: out })
}

fn validate_columns(
    raw: Vec<RawPromotionColumn>,
    nullability: ColumnNullability,
) -> Result<Vec<PromotionColumn>, PromotionValidationError> {
    let mut out = Vec::with_capacity(raw.len());
    let mut seen = HashSet::new();
    for (idx, col) in raw.into_iter().enumerate() {
        let path = format!("columns[{idx}].name");
        validate_identifier(&path, &col.name)?;
        if !seen.insert(col.name.clone()) {
            return Err(PromotionValidationError::new(
                "duplicate_column",
                path,
                "column name is declared more than once",
            ));
        }
        if matches!(nullability, ColumnNullability::Telemetry) && !col.nullable {
            return Err(PromotionValidationError::new(
                "telemetry_column_not_nullable",
                format!("columns[{idx}].nullable"),
                "telemetry promoted columns must be nullable",
            ));
        }
        out.push(PromotionColumn {
            name: col.name,
            data_type: validate_type(idx, &col.data_type)?,
            nullable: col.nullable,
            source: validate_source(idx, col.source)?,
        });
    }
    Ok(out)
}

fn validate_type(
    idx: usize,
    data_type: &str,
) -> Result<PromotionDataType, PromotionValidationError> {
    match data_type {
        "string" => Ok(PromotionDataType::String),
        "bool" => Ok(PromotionDataType::Bool),
        "int64" => Ok(PromotionDataType::Int64),
        "double" => Ok(PromotionDataType::Double),
        "decimal" => Ok(PromotionDataType::Decimal),
        "timestamp" => Ok(PromotionDataType::Timestamp),
        "json" => Ok(PromotionDataType::Json),
        _ => Err(PromotionValidationError::new(
            "unsupported_type",
            format!("columns[{idx}].type"),
            "supported types are string, bool, int64, double, decimal, timestamp, json",
        )),
    }
}

fn validate_source(
    idx: usize,
    source: RawPromotionSource,
) -> Result<PromotionSource, PromotionValidationError> {
    match source {
        RawPromotionSource::ResourceAttribute { key } => {
            validate_non_empty(format!("columns[{idx}].source.key"), &key)?;
            Ok(PromotionSource::ResourceAttribute { key })
        }
        RawPromotionSource::Attribute { key } => {
            validate_non_empty(format!("columns[{idx}].source.key"), &key)?;
            Ok(PromotionSource::Attribute { key })
        }
        RawPromotionSource::EventAttribute { event_name, key } => {
            validate_non_empty(format!("columns[{idx}].source.event_name"), &event_name)?;
            validate_non_empty(format!("columns[{idx}].source.key"), &key)?;
            Ok(PromotionSource::EventAttribute { event_name, key })
        }
        RawPromotionSource::HttpRequestBody { json_path } => {
            validate_json_path(idx, &json_path)?;
            Ok(PromotionSource::HttpRequestBody { json_path })
        }
        RawPromotionSource::HttpResponseBody { json_path } => {
            validate_json_path(idx, &json_path)?;
            Ok(PromotionSource::HttpResponseBody { json_path })
        }
    }
}

fn validate_json_path(idx: usize, json_path: &str) -> Result<(), PromotionValidationError> {
    validate_non_empty(format!("columns[{idx}].source.json_path"), json_path)?;
    if !json_path.starts_with('$') {
        return Err(PromotionValidationError::new(
            "invalid_json_path",
            format!("columns[{idx}].source.json_path"),
            "json_path must start with $",
        ));
    }
    Ok(())
}

fn validate_non_empty(
    path: impl Into<String>,
    value: &str,
) -> Result<(), PromotionValidationError> {
    if value.trim().is_empty() {
        return Err(PromotionValidationError::new(
            "empty_value",
            path,
            "value must not be empty",
        ));
    }
    Ok(())
}

fn validate_identifier(path: &str, value: &str) -> Result<(), PromotionValidationError> {
    validate_non_empty(path, value)?;
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(PromotionValidationError::new(
            "invalid_identifier",
            path,
            "identifier must not be empty",
        ));
    };
    if !(first.is_ascii_lowercase() || first == '_') {
        return Err(PromotionValidationError::new(
            "invalid_identifier",
            path,
            "identifier must start with a lowercase ASCII letter or underscore",
        ));
    }
    if !chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_') {
        return Err(PromotionValidationError::new(
            "invalid_identifier",
            path,
            "identifier may contain only lowercase ASCII letters, digits, and underscores",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_promotion_manifest, PromotionManifest};

    #[test]
    fn rejects_telemetry_non_nullable_columns() {
        let err = parse_promotion_manifest(
            r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: division_name
    type: string
    nullable: false
    source:
      from: resource_attribute
      key: division.name
"#,
        )
        .expect_err("telemetry columns must be nullable");

        assert_eq!(err.code(), "telemetry_column_not_nullable");
        assert_eq!(err.path(), "columns[0].nullable");
    }

    #[test]
    fn rejects_invalid_sql_identifier_names() {
        let err = parse_promotion_manifest(
            r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout-orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: order id
    type: string
    nullable: false
    source:
      from: http_response_body
      json_path: $.order.id
"#,
        )
        .expect_err("unsafe identifiers must be rejected");

        assert_eq!(err.code(), "invalid_identifier");
        assert_eq!(err.path(), "target.table");
    }

    #[test]
    fn rejects_business_table_without_row_selector() {
        let err = parse_promotion_manifest(
            r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
columns:
  - name: order_id
    type: string
    nullable: false
    source:
      from: http_response_body
      json_path: $.order.id
"#,
        )
        .expect_err("business table rowSelector is required");

        assert_eq!(err.code(), "missing_row_selector");
        assert_eq!(err.path(), "rowSelector");
    }

    #[test]
    fn accepts_minimal_business_table_manifest() {
        let manifest = parse_promotion_manifest(
            r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: order_id
    type: string
    nullable: false
    source:
      from: http_response_body
      json_path: $.order.id
"#,
        )
        .expect("valid manifest");

        assert!(matches!(manifest, PromotionManifest::BusinessTable(_)));
    }

    #[test]
    fn metadata_ddl_creates_specs_and_errors_tables() {
        let ddl = super::promotion_metadata_table_ddls("tenant_alpha");
        assert_eq!(ddl.len(), 3);
        assert!(ddl[0].contains(r#"CREATE SCHEMA IF NOT EXISTS "tenant_alpha""#));
        assert!(ddl[1].contains(r#""tenant_alpha".promotion_specs"#));
        assert!(ddl[1].contains("manifest_hash TEXT NOT NULL"));
        assert!(ddl[2].contains(r#""tenant_alpha".promotion_errors"#));
        assert!(ddl[2].contains("raw_value_preview TEXT"));
    }
}
