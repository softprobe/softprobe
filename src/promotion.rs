use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

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

/// Minimal telemetry row view used by promotion extraction.
///
/// The ingest path adapts spans/logs/metrics into this shape so selector behavior stays in one
/// place and tests do not depend on OTLP protobuf construction.
pub struct TelemetryPromotionRow<'a> {
    pub resource_attributes: &'a HashMap<String, String>,
    pub attributes: &'a HashMap<String, String>,
    pub events: &'a [TelemetryPromotionEvent],
    pub http_request_body: Option<&'a str>,
    pub http_response_body: Option<&'a str>,
    pub metric_value: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryPromotionEvent {
    pub name: String,
    pub attributes: HashMap<String, String>,
}

/// Minimal source row view for business table promotion extraction.
///
/// Ingest adapters will construct this from spans/logs/events so extraction can stay independent
/// from OTLP protobuf structs and keep evidence anchors explicit.
pub struct BusinessPromotionInput<'a> {
    pub session_id: &'a str,
    pub trace_id: &'a str,
    pub span_id: &'a str,
    pub event_name: Option<&'a str>,
    pub event_timestamp: Option<&'a str>,
    pub service_name: Option<&'a str>,
    pub source_signal: &'a str,
    pub source_timestamp: &'a str,
    pub attributes: &'a HashMap<String, String>,
    pub events: &'a [TelemetryPromotionEvent],
    pub http_request_body: Option<&'a str>,
    pub http_response_body: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessPromotedRow {
    pub session_id: String,
    pub trace_id: String,
    pub span_id: String,
    pub event_name: Option<String>,
    pub event_timestamp: Option<String>,
    pub service_name: Option<String>,
    pub source_signal: String,
    pub source_timestamp: String,
    pub promotion_spec_version: String,
    pub values: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessPromotionError {
    pub target_column: String,
    pub source_signal: String,
    pub source_path: String,
    pub error_code: String,
    pub error_message: String,
    pub raw_value_preview: Option<String>,
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

/// Failed to read or parse promotion specs from the configured DuckLake metadata store.
#[derive(Debug)]
pub enum PromotionSpecLoadError {
    Postgres(tokio_postgres::Error),
    Backend(String),
    InvalidRowManifest {
        spec_id: String,
        source: PromotionValidationError,
    },
}

impl std::fmt::Display for PromotionSpecLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres(e) => write!(f, "postgres error loading promotion_specs: {e}"),
            Self::Backend(e) => write!(f, "error loading promotion_specs: {e}"),
            Self::InvalidRowManifest { spec_id, source } => {
                write!(
                    f,
                    "promotion_specs row {spec_id} has invalid manifest: {source}"
                )
            }
        }
    }
}

impl std::error::Error for PromotionSpecLoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Postgres(e) => Some(e),
            Self::Backend(_) => None,
            Self::InvalidRowManifest { source, .. } => Some(source),
        }
    }
}

/// Content-hash of raw promotion YAML used as the durable identity suffix for `spec_id`.
pub fn promotion_manifest_hash(manifest_yaml: &str) -> String {
    let mut hasher = DefaultHasher::new();
    manifest_yaml.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Stable `promotion_specs.spec_id` for a telemetry_columns document.
pub fn telemetry_spec_id(manifest_yaml: &str) -> String {
    format!(
        "telemetry_columns_{}",
        promotion_manifest_hash(manifest_yaml)
    )
}

/// Stable `promotion_specs.spec_id` for a business_table document.
pub fn business_spec_id(table_name: &str, manifest_yaml: &str) -> String {
    format!(
        "business_table_{}_{}",
        table_name,
        promotion_manifest_hash(manifest_yaml)
    )
}

/// Backend-neutral data needed to activate one promotion spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromotionSpecActivation {
    pub spec_id: String,
    pub manifest_hash: String,
    pub target_kind: &'static str,
    pub target_tables: String,
}

pub fn telemetry_spec_activation(
    manifest_yaml: &str,
    target_tables: &[String],
) -> PromotionSpecActivation {
    PromotionSpecActivation {
        spec_id: telemetry_spec_id(manifest_yaml),
        manifest_hash: promotion_manifest_hash(manifest_yaml),
        target_kind: "telemetry_columns",
        target_tables: target_tables.join(","),
    }
}

pub fn business_spec_activation(table_name: &str, manifest_yaml: &str) -> PromotionSpecActivation {
    PromotionSpecActivation {
        spec_id: business_spec_id(table_name, manifest_yaml),
        manifest_hash: promotion_manifest_hash(manifest_yaml),
        target_kind: "business_table",
        target_tables: table_name.to_string(),
    }
}

/// Shared telemetry apply lifecycle. Backend adapters provide only DDL and activation primitives.
pub async fn run_telemetry_apply<ApplyDdl, ApplyDdlFuture, Activate, ActivateFuture>(
    apply_ddl: ApplyDdl,
    activate: Activate,
) -> anyhow::Result<String>
where
    ApplyDdl: FnOnce() -> ApplyDdlFuture,
    ApplyDdlFuture: std::future::Future<Output = anyhow::Result<()>>,
    Activate: FnOnce() -> ActivateFuture,
    ActivateFuture: std::future::Future<Output = anyhow::Result<String>>,
{
    apply_ddl().await?;
    activate().await
}

/// Error from the shared business apply lifecycle.
pub enum BusinessApplyError {
    Incompatible(PromotionValidationError),
    Other(anyhow::Error),
}

/// Shared business apply lifecycle. Both PostgreSQL and SQLite execute this exact sequence.
pub async fn run_business_apply<
    LoadCurrent,
    LoadFuture,
    ApplyDdl,
    ApplyDdlFuture,
    Activate,
    ActivateFuture,
>(
    requested: &BusinessTableManifest,
    load_current: LoadCurrent,
    apply_ddl: ApplyDdl,
    activate: Activate,
) -> Result<String, BusinessApplyError>
where
    LoadCurrent: FnOnce() -> LoadFuture,
    LoadFuture: std::future::Future<Output = anyhow::Result<Option<BusinessTableManifest>>>,
    ApplyDdl: FnOnce() -> ApplyDdlFuture,
    ApplyDdlFuture: std::future::Future<Output = anyhow::Result<()>>,
    Activate: FnOnce() -> ActivateFuture,
    ActivateFuture: std::future::Future<Output = anyhow::Result<String>>,
{
    if let Some(current) = load_current()
        .await
        .map_err(BusinessApplyError::Other)?
        .as_ref()
    {
        validate_business_table_compatible(current, requested)
            .map_err(BusinessApplyError::Incompatible)?;
    }
    apply_ddl().await.map_err(BusinessApplyError::Other)?;
    activate().await.map_err(BusinessApplyError::Other)
}

/// Parse one `(spec_id, manifest_json)` row into a telemetry columns manifest.
pub fn telemetry_manifest_from_row(
    spec_id: &str,
    manifest_json: &str,
) -> Result<Option<TelemetryColumnsManifest>, PromotionSpecLoadError> {
    match parse_promotion_manifest(manifest_json) {
        Ok(PromotionManifest::TelemetryColumns(m)) => Ok(Some(m)),
        Ok(PromotionManifest::BusinessTable(_)) => Ok(None),
        Err(e) => Err(PromotionSpecLoadError::InvalidRowManifest {
            spec_id: spec_id.to_string(),
            source: e,
        }),
    }
}

/// Parse one `(spec_id, manifest_json)` row into a business-table manifest.
pub fn business_manifest_from_row(
    spec_id: &str,
    manifest_json: &str,
) -> Result<Option<BusinessTableManifest>, PromotionSpecLoadError> {
    match parse_promotion_manifest(manifest_json) {
        Ok(PromotionManifest::BusinessTable(m)) => Ok(Some(m)),
        Ok(PromotionManifest::TelemetryColumns(_)) => Ok(None),
        Err(e) => Err(PromotionSpecLoadError::InvalidRowManifest {
            spec_id: spec_id.to_string(),
            source: e,
        }),
    }
}

/// DuckDB-dialect DDL for the local (single-scope) `promotion_specs` control table.
///
/// Qualifies as `{catalog_alias}.promotion_specs` so the table lives in the attached DuckLake
/// catalog (not the ephemeral in-memory DuckDB `main`). No `CREATE SCHEMA` — DuckLake catalogs
/// already expose the alias as the qualification root for local SQLite.
pub fn local_promotion_specs_table_ddl(catalog_alias: &str) -> String {
    let catalog = quote_sql_ident(catalog_alias);
    // DuckLake tables do not support PRIMARY KEY / UNIQUE constraints. Uniqueness of
    // `spec_id` is enforced by the activate path (UPDATE-then-INSERT, no ON CONFLICT).
    format!(
        r#"CREATE TABLE IF NOT EXISTS {catalog}.promotion_specs (
  spec_id TEXT NOT NULL,
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
    )
}

/// Load every **active** telemetry column manifest for one tenant DuckLake metadata schema.
///
/// Rows with `target_kind != telemetry_columns` are skipped. Business-table specs may live in the
/// same table but are ignored here so ingest/query can resolve telemetry promotion per tenant.
///
/// Under normal apply, at most one telemetry_columns row is active per tenant (superseded specs
/// are marked `inactive`). Loaders still return a vector so older multi-active rows remain readable.
pub async fn load_active_telemetry_columns_manifests(
    client: &tokio_postgres::Client,
    tenant_schema: &str,
) -> Result<Vec<TelemetryColumnsManifest>, PromotionSpecLoadError> {
    let schema = quote_sql_ident(tenant_schema);
    let sql = format!(
        "SELECT spec_id, manifest_json FROM {schema}.promotion_specs WHERE status = 'active' AND target_kind = 'telemetry_columns';"
    );
    let rows = client
        .query(&sql, &[])
        .await
        .map_err(PromotionSpecLoadError::Postgres)?;
    let mut out = Vec::new();
    for row in rows {
        let spec_id: String = row.get(0);
        let manifest_json: String = row.get(1);
        if let Some(m) = telemetry_manifest_from_row(&spec_id, &manifest_json)? {
            out.push(m);
        }
    }
    Ok(out)
}

/// Load the active business-table manifest for one logical table name, if any.
///
/// Apply keeps at most one active `business_table` row per `target_tables` value. When multiple
/// active rows exist (legacy data), the newest by `applied_at` wins.
pub async fn load_active_business_table_manifest(
    client: &tokio_postgres::Client,
    tenant_schema: &str,
    table_name: &str,
) -> Result<Option<BusinessTableManifest>, PromotionSpecLoadError> {
    let schema = quote_sql_ident(tenant_schema);
    let sql = format!(
        r#"SELECT spec_id, manifest_json FROM {schema}.promotion_specs
WHERE status = 'active' AND target_kind = 'business_table' AND target_tables = $1
ORDER BY applied_at DESC
LIMIT 1;"#
    );
    let rows = client
        .query(&sql, &[&table_name])
        .await
        .map_err(PromotionSpecLoadError::Postgres)?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let spec_id: String = row.get(0);
    let manifest_json: String = row.get(1);
    business_manifest_from_row(&spec_id, &manifest_json)
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

fn telemetry_table_bare_name(table: &TelemetryTable) -> &'static str {
    match table {
        TelemetryTable::Traces => "traces",
        TelemetryTable::Logs => "logs",
        TelemetryTable::Metrics => "metrics",
    }
}

/// Canonical DuckLake telemetry column names that must not be re-declared via telemetry promotion.
fn reserved_telemetry_column_names(table: &TelemetryTable) -> &'static [&'static str] {
    match table {
        TelemetryTable::Traces => &[
            "session_id",
            "trace_id",
            "span_id",
            "parent_span_id",
            "app_id",
            "organization_id",
            "tenant_id",
            "message_type",
            "span_kind",
            "timestamp",
            "end_timestamp",
            "attributes",
            "events",
            "status_code",
            "status_message",
            "http_request_method",
            "http_request_path",
            "http_request_headers",
            "http_request_body",
            "http_response_status_code",
            "http_response_headers",
            "http_response_body",
            "record_date",
        ],
        TelemetryTable::Logs => &[
            "session_id",
            "timestamp",
            "observed_timestamp",
            "severity_number",
            "severity_text",
            "body",
            "attributes",
            "resource_attributes",
            "trace_id",
            "span_id",
            "record_date",
        ],
        TelemetryTable::Metrics => &[
            "metric_name",
            "description",
            "unit",
            "metric_type",
            "timestamp",
            "value",
            "attributes",
            "resource_attributes",
            "record_date",
        ],
    }
}

fn promotion_data_type_sql(t: &PromotionDataType) -> &'static str {
    match t {
        PromotionDataType::String => "VARCHAR",
        PromotionDataType::Bool => "BOOLEAN",
        PromotionDataType::Int64 => "BIGINT",
        PromotionDataType::Double => "DOUBLE",
        PromotionDataType::Decimal => "DOUBLE",
        PromotionDataType::Timestamp => "TIMESTAMPTZ",
        PromotionDataType::Json => "VARCHAR",
    }
}

fn business_promotion_data_type_sql(t: &PromotionDataType) -> &'static str {
    match t {
        PromotionDataType::String => "VARCHAR",
        PromotionDataType::Bool => "BOOLEAN",
        PromotionDataType::Int64 => "BIGINT",
        PromotionDataType::Double => "DOUBLE",
        PromotionDataType::Decimal => "DECIMAL(38, 9)",
        PromotionDataType::Timestamp => "TIMESTAMPTZ",
        PromotionDataType::Json => "VARCHAR",
    }
}

fn business_anchor_columns() -> &'static [(&'static str, &'static str, bool)] {
    &[
        ("session_id", "VARCHAR", false),
        ("trace_id", "VARCHAR", false),
        ("span_id", "VARCHAR", false),
        ("event_name", "VARCHAR", true),
        ("event_timestamp", "TIMESTAMPTZ", true),
        ("service_name", "VARCHAR", true),
        ("source_signal", "VARCHAR", false),
        ("source_timestamp", "TIMESTAMPTZ", false),
        ("promotion_spec_version", "VARCHAR", false),
    ]
}

pub fn business_physical_table_name(spec: &BusinessTableManifest) -> String {
    format!("{}_v{}", spec.target.table, spec.target.version)
}

pub fn business_current_view_name(spec: &BusinessTableManifest) -> String {
    format!("{}_current", spec.target.table)
}

/// Validate that a requested business table manifest can reuse an existing physical version.
///
/// Compatibility is intentionally schema-only here. Row extraction semantics are handled by later
/// ingest tasks, but DDL must reject drops, type changes, and nullability tightening for an
/// already-created `<table>_v<version>` table.
pub fn validate_business_table_compatible(
    current: &BusinessTableManifest,
    requested: &BusinessTableManifest,
) -> Result<(), PromotionValidationError> {
    if current.target.table != requested.target.table {
        return Err(PromotionValidationError::new(
            "business_table_changed",
            "target.table",
            "requested manifest targets a different business table",
        ));
    }
    if requested.target.version < current.target.version {
        return Err(PromotionValidationError::new(
            "business_version_regressed",
            "target.version",
            "requested version must not be lower than the applied version",
        ));
    }
    if requested.target.version > current.target.version {
        return Ok(());
    }
    let requested_by_name = requested
        .columns
        .iter()
        .map(|col| (col.name.as_str(), col))
        .collect::<HashMap<_, _>>();
    for current_col in &current.columns {
        let Some(requested_col) = requested_by_name.get(current_col.name.as_str()) else {
            return Err(PromotionValidationError::new(
                "business_column_dropped",
                format!("columns.{}", current_col.name),
                "existing business table columns cannot be dropped from the same version",
            ));
        };
        if requested_col.data_type != current_col.data_type {
            return Err(PromotionValidationError::new(
                "business_column_type_changed",
                format!("columns.{}.type", current_col.name),
                "existing business table column type changed; create a new version",
            ));
        }
        if requested_col.nullable != current_col.nullable {
            return Err(PromotionValidationError::new(
                "business_column_nullability_changed",
                format!("columns.{}.nullable", current_col.name),
                "existing business table column nullability changed; create a new version",
            ));
        }
    }
    let current_names = current
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    for requested_col in &requested.columns {
        if !current_names.contains(requested_col.name.as_str()) && !requested_col.nullable {
            return Err(PromotionValidationError::new(
                "business_required_column_added",
                format!("columns.{}.nullable", requested_col.name),
                "required columns cannot be added to an existing table; create a new version",
            ));
        }
    }
    Ok(())
}

/// Generate DuckLake-compatible DDL for a business table manifest.
///
/// The physical table is versioned as `<target.table>_v<target.version>`. Sequence:
/// 1. `CREATE TABLE IF NOT EXISTS` with the full column list (first apply)
/// 2. `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for each column (same-version additive re-apply)
/// 3. `CREATE OR REPLACE VIEW` so `*_current` points at the physical table
pub fn business_table_create_ddls(
    catalog_schema_prefix: &str,
    spec: &BusinessTableManifest,
) -> Result<Vec<String>, PromotionValidationError> {
    validate_business_table_additive(spec)?;
    let table = business_physical_table_name(spec);
    let view = business_current_view_name(spec);
    let qualified_table = format!("{}.{}", catalog_schema_prefix, quote_sql_ident(&table));
    let qualified_view = format!("{}.{}", catalog_schema_prefix, quote_sql_ident(&view));
    let mut create_columns = Vec::new();
    let mut alter_ddls = Vec::new();
    for (name, sql_type, nullable) in business_anchor_columns() {
        let qname = quote_sql_ident(name);
        create_columns.push(format!(
            "{} {}{}",
            qname,
            sql_type,
            if *nullable { "" } else { " NOT NULL" }
        ));
        alter_ddls.push(format!(
            "ALTER TABLE {qualified_table} ADD COLUMN IF NOT EXISTS {qname} {sql_type};"
        ));
    }
    for col in &spec.columns {
        let qname = quote_sql_ident(&col.name);
        let sql_type = business_promotion_data_type_sql(&col.data_type);
        create_columns.push(format!(
            "{} {}{}",
            qname,
            sql_type,
            if col.nullable { "" } else { " NOT NULL" }
        ));
        alter_ddls.push(format!(
            "ALTER TABLE {qualified_table} ADD COLUMN IF NOT EXISTS {qname} {sql_type};"
        ));
    }
    let mut ddls = Vec::with_capacity(2 + alter_ddls.len());
    ddls.push(format!(
        "CREATE TABLE IF NOT EXISTS {qualified_table} (\n  {}\n);",
        create_columns.join(",\n  ")
    ));
    ddls.extend(alter_ddls);
    ddls.push(format!(
        "CREATE OR REPLACE VIEW {qualified_view} AS SELECT * FROM {qualified_table};"
    ));
    Ok(ddls)
}

fn validate_business_table_additive(
    spec: &BusinessTableManifest,
) -> Result<(), PromotionValidationError> {
    let anchors = business_anchor_columns()
        .iter()
        .map(|(name, _, _)| *name)
        .collect::<HashSet<_>>();
    for (idx, col) in spec.columns.iter().enumerate() {
        validate_identifier(&format!("columns[{idx}].name"), &col.name)?;
        if anchors.contains(col.name.as_str()) {
            return Err(PromotionValidationError::new(
                "business_column_reserved",
                format!("columns[{idx}].name"),
                "business table columns must not redeclare evidence anchor columns",
            ));
        }
    }
    Ok(())
}

/// Merge several `telemetry_columns` manifests into the single platform manifest a tenant can
/// have active at once ([`thelake/docs/promotion.md`]).
///
/// All input manifests must target the exact same table set. `telemetry_column_add_ddls` /
/// `validate_telemetry_column_additive` apply **every** column in a manifest to **every** table
/// listed in `target.tables`; merging manifests with different table sets would silently add
/// each other's columns to tables they were never meant for, so mismatched table sets are
/// rejected up front (`merge_target_tables_mismatch`) rather than merged.
///
/// Column order is preserved (first manifest's columns first) so the resulting manifest is
/// deterministic for `telemetry_columns_manifest_to_yaml` / re-apply. A column repeated across
/// manifests with an identical definition (name, type, nullable, source) is deduplicated
/// idempotently; a column repeated with a *different* definition is rejected as
/// `merge_conflicting_duplicate_column`. The merged manifest is validated the same way a
/// single-source manifest is (`validate_telemetry_column_additive`), so collisions with reserved
/// base `traces`/`logs`/`metrics` columns are still rejected.
pub fn merge_telemetry_columns_manifests(
    manifests: &[TelemetryColumnsManifest],
) -> Result<TelemetryColumnsManifest, PromotionValidationError> {
    if manifests.is_empty() {
        return Err(PromotionValidationError::new(
            "missing_manifests",
            "manifests",
            "merge requires at least one telemetry_columns manifest",
        ));
    }
    let mut tables = Vec::new();
    let mut seen_tables = HashSet::new();
    let mut expected_table_set: Option<HashSet<&'static str>> = None;
    for manifest in manifests {
        let this_table_set: HashSet<&'static str> = manifest
            .target
            .tables
            .iter()
            .map(|t| telemetry_table_bare_name(t))
            .collect();
        match &expected_table_set {
            None => expected_table_set = Some(this_table_set),
            Some(expected) if *expected != this_table_set => {
                return Err(PromotionValidationError::new(
                    "merge_target_tables_mismatch",
                    "target.tables",
                    "all merged telemetry_columns manifests must target the exact same tables",
                ));
            }
            Some(_) => {}
        }
        for table in &manifest.target.tables {
            if seen_tables.insert(telemetry_table_bare_name(table)) {
                tables.push(table.clone());
            }
        }
    }

    let mut columns = Vec::new();
    let mut index_by_name: HashMap<String, usize> = HashMap::new();
    for manifest in manifests {
        for column in &manifest.columns {
            match index_by_name.get(&column.name) {
                None => {
                    index_by_name.insert(column.name.clone(), columns.len());
                    columns.push(column.clone());
                }
                Some(&existing_idx) => {
                    let existing: &PromotionColumn = &columns[existing_idx];
                    if existing.data_type != column.data_type
                        || existing.nullable != column.nullable
                        || existing.source != column.source
                    {
                        return Err(PromotionValidationError::new(
                            "merge_conflicting_duplicate_column",
                            format!("columns.{}", column.name),
                            format!(
                                "column {} is declared with conflicting type/nullable/source across merged manifests",
                                column.name
                            ),
                        ));
                    }
                    // Identical redeclaration (e.g. re-merging the same fragment) is idempotent.
                }
            }
        }
    }

    let merged = TelemetryColumnsManifest {
        target: TelemetryColumnsTarget { tables },
        columns,
    };
    validate_telemetry_column_additive(&merged)?;
    Ok(merged)
}

fn promotion_data_type_name(data_type: &PromotionDataType) -> &'static str {
    match data_type {
        PromotionDataType::String => "string",
        PromotionDataType::Bool => "bool",
        PromotionDataType::Int64 => "int64",
        PromotionDataType::Double => "double",
        PromotionDataType::Decimal => "decimal",
        PromotionDataType::Timestamp => "timestamp",
        PromotionDataType::Json => "json",
    }
}

fn raw_source_from_domain(source: &PromotionSource) -> RawPromotionSource {
    match source {
        PromotionSource::ResourceAttribute { key } => {
            RawPromotionSource::ResourceAttribute { key: key.clone() }
        }
        PromotionSource::Attribute { key } => RawPromotionSource::Attribute { key: key.clone() },
        PromotionSource::EventAttribute { event_name, key } => RawPromotionSource::EventAttribute {
            event_name: event_name.clone(),
            key: key.clone(),
        },
        PromotionSource::HttpRequestBody { json_path } => RawPromotionSource::HttpRequestBody {
            json_path: json_path.clone(),
        },
        PromotionSource::HttpResponseBody { json_path } => RawPromotionSource::HttpResponseBody {
            json_path: json_path.clone(),
        },
    }
}

fn raw_column_from_domain(column: &PromotionColumn) -> RawPromotionColumn {
    RawPromotionColumn {
        name: column.name.clone(),
        data_type: promotion_data_type_name(&column.data_type).to_string(),
        nullable: column.nullable,
        source: raw_source_from_domain(&column.source),
    }
}

/// Serialize a `telemetry_columns` manifest back to the canonical YAML the apply API accepts.
///
/// Used to re-apply a manifest produced by [`merge_telemetry_columns_manifests`] without hand
/// authoring merged YAML text. Goes through the same `Raw*` shape [`parse_promotion_manifest`]
/// deserializes from (via `serde_yaml`) so there is one source of truth for the wire shape and no
/// risk of un-escaped scalars from hand-built YAML strings.
pub fn telemetry_columns_manifest_to_yaml(manifest: &TelemetryColumnsManifest) -> String {
    let raw = RawPromotionManifest {
        spec_version: PROMOTION_SPEC_VERSION.to_string(),
        target: RawTarget::TelemetryColumns {
            tables: manifest
                .target
                .tables
                .iter()
                .map(|t| telemetry_table_bare_name(t).to_string())
                .collect(),
        },
        row_selector: None,
        columns: manifest
            .columns
            .iter()
            .map(raw_column_from_domain)
            .collect(),
    };
    serde_yaml::to_string(&raw).expect("telemetry columns manifest always serializes to YAML")
}

/// Reject telemetry promotion columns that collide with canonical table columns or break nullability rules.
pub fn validate_telemetry_column_additive(
    spec: &TelemetryColumnsManifest,
) -> Result<(), PromotionValidationError> {
    for (idx, col) in spec.columns.iter().enumerate() {
        if !col.nullable {
            return Err(PromotionValidationError::new(
                "telemetry_column_not_nullable",
                format!("columns[{idx}].nullable"),
                "telemetry promoted columns must be nullable",
            ));
        }
        let path = format!("columns[{idx}].name");
        validate_identifier(&path, &col.name)?;
        for table in &spec.target.tables {
            if reserved_telemetry_column_names(table).contains(&col.name.as_str()) {
                return Err(PromotionValidationError::new(
                    "column_already_exists",
                    path.clone(),
                    format!("column {} is already defined on {:?}", col.name, table),
                ));
            }
        }
    }
    Ok(())
}

/// Generate idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statements for telemetry promotion.
///
/// `catalog_schema_prefix` must be a fully quoted DuckLake qualification for catalog + metadata schema,
/// for example `"softprobe"."tenant_acme"` (no trailing dot). Re-applying the same (or additive)
/// manifest is safe: existing columns are skipped.
pub fn telemetry_column_add_ddls(
    catalog_schema_prefix: &str,
    spec: &TelemetryColumnsManifest,
) -> Result<Vec<String>, PromotionValidationError> {
    validate_telemetry_column_additive(spec)?;
    let mut ddls = Vec::new();
    for table in &spec.target.tables {
        let bare = telemetry_table_bare_name(table);
        for col in &spec.columns {
            let typ = promotion_data_type_sql(&col.data_type);
            let qcol = quote_sql_ident(&col.name);
            ddls.push(format!(
                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {};",
                catalog_schema_prefix, bare, qcol, typ
            ));
        }
    }
    Ok(ddls)
}

/// Extract and validate one promoted telemetry value.
///
/// Values are returned in storage-string form because the existing Arrow conversion layer already
/// parses strings into the promoted column's physical Arrow type when building typed batches.
pub fn extract_telemetry_promoted_value(
    row: &TelemetryPromotionRow<'_>,
    column: &PromotionColumn,
) -> Result<Option<String>, PromotionValidationError> {
    let raw = match &column.source {
        PromotionSource::ResourceAttribute { key } => row.resource_attributes.get(key).cloned(),
        PromotionSource::Attribute { key } => row.attributes.get(key).cloned(),
        PromotionSource::EventAttribute { event_name, key } => row
            .events
            .iter()
            .find(|event| event.name == *event_name)
            .and_then(|event| event.attributes.get(key))
            .cloned(),
        PromotionSource::HttpRequestBody { json_path } => {
            extract_json_path_string(row.http_request_body, json_path, &column.name)?
        }
        PromotionSource::HttpResponseBody { json_path } => {
            extract_json_path_string(row.http_response_body, json_path, &column.name)?
        }
    };

    let Some(value) = raw else {
        return Ok(None);
    };
    validate_promoted_value_type(&column.name, &column.data_type, &value)?;
    Ok(Some(value))
}

/// Extract one promoted business row, or `Ok(None)` when the row selector does not match.
///
/// Missing nullable values are omitted from `values`. Missing or invalid non-nullable values return
/// structured errors so ingest can write `promotion_errors` without rejecting the source telemetry
/// row.
pub fn extract_business_promoted_row(
    spec: &BusinessTableManifest,
    input: &BusinessPromotionInput<'_>,
) -> Result<Option<BusinessPromotedRow>, Vec<BusinessPromotionError>> {
    if input
        .attributes
        .get(&spec.row_selector.attribute.key)
        .map(String::as_str)
        != Some(spec.row_selector.attribute.equals.as_str())
    {
        return Ok(None);
    }
    let mut values = HashMap::new();
    let mut errors = Vec::new();
    for column in &spec.columns {
        match extract_business_column_value(column, input) {
            Ok(Some(value)) => {
                if let Err(err) =
                    validate_promoted_value_type(&column.name, &column.data_type, &value)
                {
                    errors.push(business_error(
                        column,
                        input,
                        "type_mismatch",
                        err.message(),
                        Some(value),
                    ));
                } else {
                    values.insert(column.name.clone(), value);
                }
            }
            Ok(None) if column.nullable => {}
            Ok(None) => errors.push(business_error(
                column,
                input,
                "missing_required_value",
                "required business promotion value is missing",
                None,
            )),
            Err(err) => errors.push(business_error(
                column,
                input,
                err.code(),
                err.message(),
                None,
            )),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(Some(BusinessPromotedRow {
        session_id: input.session_id.to_string(),
        trace_id: input.trace_id.to_string(),
        span_id: input.span_id.to_string(),
        event_name: input.event_name.map(str::to_string),
        event_timestamp: input.event_timestamp.map(str::to_string),
        service_name: input.service_name.map(str::to_string),
        source_signal: input.source_signal.to_string(),
        source_timestamp: input.source_timestamp.to_string(),
        promotion_spec_version: PROMOTION_SPEC_VERSION.to_string(),
        values,
    }))
}

fn extract_business_column_value(
    column: &PromotionColumn,
    input: &BusinessPromotionInput<'_>,
) -> Result<Option<String>, PromotionValidationError> {
    match &column.source {
        PromotionSource::ResourceAttribute { key } | PromotionSource::Attribute { key } => {
            Ok(input.attributes.get(key).cloned())
        }
        PromotionSource::EventAttribute { event_name, key } => Ok(input
            .events
            .iter()
            .find(|event| event.name == *event_name)
            .and_then(|event| event.attributes.get(key))
            .cloned()),
        PromotionSource::HttpRequestBody { json_path } => {
            extract_json_path_string(input.http_request_body, json_path, &column.name)
        }
        PromotionSource::HttpResponseBody { json_path } => {
            extract_json_path_string(input.http_response_body, json_path, &column.name)
        }
    }
}

fn business_error(
    column: &PromotionColumn,
    input: &BusinessPromotionInput<'_>,
    code: impl Into<String>,
    message: impl Into<String>,
    raw_value_preview: Option<String>,
) -> BusinessPromotionError {
    BusinessPromotionError {
        target_column: column.name.clone(),
        source_signal: input.source_signal.to_string(),
        source_path: promotion_source_path(&column.source),
        error_code: code.into(),
        error_message: message.into(),
        raw_value_preview: raw_value_preview.map(|s| s.chars().take(128).collect()),
    }
}

fn promotion_source_path(source: &PromotionSource) -> String {
    match source {
        PromotionSource::ResourceAttribute { key } => format!("resource_attribute:{key}"),
        PromotionSource::Attribute { key } => format!("attribute:{key}"),
        PromotionSource::EventAttribute { event_name, key } => {
            format!("event_attribute:{event_name}:{key}")
        }
        PromotionSource::HttpRequestBody { json_path } => format!("http_request_body:{json_path}"),
        PromotionSource::HttpResponseBody { json_path } => {
            format!("http_response_body:{json_path}")
        }
    }
}

fn extract_json_path_string(
    body: Option<&str>,
    json_path: &str,
    column_name: &str,
) -> Result<Option<String>, PromotionValidationError> {
    let Some(body) = body else {
        return Ok(None);
    };
    let doc: serde_json::Value = serde_json::from_str(body).map_err(|err| {
        PromotionValidationError::new(
            "promotion_value_invalid_json",
            format!("columns.{column_name}"),
            format!("HTTP body is not valid JSON: {err}"),
        )
    })?;
    let Some(value) = select_simple_json_path(&doc, json_path) else {
        return Ok(None);
    };
    Ok(Some(json_value_to_storage_string(value)))
}

fn select_simple_json_path<'a>(
    value: &'a serde_json::Value,
    json_path: &str,
) -> Option<&'a serde_json::Value> {
    if json_path == "$" {
        return Some(value);
    }
    let mut current = value;
    for segment in json_path.strip_prefix("$.")?.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn json_value_to_storage_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => value.to_string(),
    }
}

fn validate_promoted_value_type(
    column_name: &str,
    data_type: &PromotionDataType,
    value: &str,
) -> Result<(), PromotionValidationError> {
    let ok = match data_type {
        PromotionDataType::String | PromotionDataType::Json => true,
        PromotionDataType::Bool => value.parse::<bool>().is_ok(),
        PromotionDataType::Int64 => value.parse::<i64>().is_ok(),
        PromotionDataType::Double | PromotionDataType::Decimal => value.parse::<f64>().is_ok(),
        PromotionDataType::Timestamp => chrono::DateTime::parse_from_rfc3339(value).is_ok(),
    };
    if ok {
        Ok(())
    } else {
        Err(PromotionValidationError::new(
            "promotion_value_type_mismatch",
            format!("columns.{column_name}"),
            format!("value cannot be converted to {:?}", data_type),
        ))
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RawPromotionManifest {
    spec_version: String,
    target: RawTarget,
    #[serde(
        default,
        rename = "rowSelector",
        skip_serializing_if = "Option::is_none"
    )]
    row_selector: Option<RawRowSelector>,
    #[serde(default)]
    columns: Vec<RawPromotionColumn>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RawTarget {
    TelemetryColumns { tables: Vec<String> },
    BusinessTable { table: String, version: u32 },
}

#[derive(Debug, Deserialize, Serialize)]
struct RawRowSelector {
    attribute: RawAttributeSelector,
}

#[derive(Debug, Deserialize, Serialize)]
struct RawAttributeSelector {
    key: String,
    equals: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct RawPromotionColumn {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    nullable: bool,
    source: RawPromotionSource,
}

#[derive(Debug, Deserialize, Serialize)]
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
    use std::collections::HashMap;

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
    fn business_table_ddls_create_versioned_table_and_current_view() {
        let manifest = parse_promotion_manifest(
            r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 2
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
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
  - name: payment_status
    type: string
    nullable: true
    source:
      from: attribute
      key: payment.status
"#,
        )
        .expect("valid manifest");
        let PromotionManifest::BusinessTable(spec) = manifest else {
            panic!("expected business table manifest");
        };

        let ddls =
            super::business_table_create_ddls(r#""softprobe"."tenant_alpha""#, &spec).expect("ddl");

        assert!(ddls[0].starts_with(
            r#"CREATE TABLE IF NOT EXISTS "softprobe"."tenant_alpha"."checkout_orders_v2" ("#
        ));
        assert!(ddls[0].contains(r#""session_id" VARCHAR NOT NULL"#));
        assert!(ddls[0].contains(r#""event_name" VARCHAR"#));
        assert!(ddls[0].contains(r#""promotion_spec_version" VARCHAR NOT NULL"#));
        assert!(ddls[0].contains(r#""order_id" VARCHAR NOT NULL"#));
        assert!(ddls[0].contains(r#""total_cents" BIGINT"#));
        assert!(ddls.iter().any(|ddl| {
            ddl == r#"ALTER TABLE "softprobe"."tenant_alpha"."checkout_orders_v2" ADD COLUMN IF NOT EXISTS "order_id" VARCHAR;"#
        }));
        assert!(ddls.iter().any(|ddl| {
            ddl == r#"ALTER TABLE "softprobe"."tenant_alpha"."checkout_orders_v2" ADD COLUMN IF NOT EXISTS "total_cents" BIGINT;"#
        }));
        assert_eq!(
            ddls.last().expect("view ddl"),
            r#"CREATE OR REPLACE VIEW "softprobe"."tenant_alpha"."checkout_orders_current" AS SELECT * FROM "softprobe"."tenant_alpha"."checkout_orders_v2";"#
        );
    }

    #[test]
    fn business_table_compatibility_rejects_same_version_type_changes() {
        let current = parse_promotion_manifest(
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
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
"#,
        )
        .expect("valid current manifest");
        let requested = parse_promotion_manifest(
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
  - name: total_cents
    type: decimal
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
"#,
        )
        .expect("valid requested manifest");
        let PromotionManifest::BusinessTable(current) = current else {
            panic!("expected business table manifest");
        };
        let PromotionManifest::BusinessTable(requested) = requested else {
            panic!("expected business table manifest");
        };

        let err = super::validate_business_table_compatible(&current, &requested)
            .expect_err("type change must require new version");

        assert_eq!(err.code(), "business_column_type_changed");
        assert_eq!(err.path(), "columns.total_cents.type");
    }

    #[test]
    fn business_table_compatibility_rejects_additive_required_columns() {
        let current = parse_promotion_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: business_table, table: checkout_orders, version: 1 }
rowSelector:
  attribute: { key: sp.workflow, equals: checkout }
columns:
  - name: total_cents
    type: int64
    nullable: true
    source: { from: http_response_body, json_path: $.total }
"#,
        )
        .expect("current");
        let requested = parse_promotion_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: business_table, table: checkout_orders, version: 1 }
rowSelector:
  attribute: { key: sp.workflow, equals: checkout }
columns:
  - name: total_cents
    type: int64
    nullable: true
    source: { from: http_response_body, json_path: $.total }
  - name: order_id
    type: string
    nullable: false
    source: { from: attribute, key: order.id }
"#,
        )
        .expect("requested");
        let (
            PromotionManifest::BusinessTable(current),
            PromotionManifest::BusinessTable(requested),
        ) = (current, requested)
        else {
            panic!("business manifests");
        };

        let error = super::validate_business_table_compatible(&current, &requested)
            .expect_err("required additive column needs a new table version");
        assert_eq!(error.code(), "business_required_column_added");
        assert_eq!(error.path(), "columns.order_id.nullable");
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

    #[test]
    fn telemetry_and_business_spec_ids_are_stable_content_hashes() {
        let yaml = "specVersion: softprobe.promotion.v1\n";
        assert_eq!(
            super::telemetry_spec_id(yaml),
            format!("telemetry_columns_{}", super::promotion_manifest_hash(yaml))
        );
        assert_eq!(
            super::business_spec_id("checkout_orders", yaml),
            format!(
                "business_table_checkout_orders_{}",
                super::promotion_manifest_hash(yaml)
            )
        );
        assert_ne!(
            super::promotion_manifest_hash(yaml),
            super::promotion_manifest_hash("different")
        );
    }

    #[test]
    fn local_promotion_specs_ddl_is_catalog_qualified() {
        let ddl = super::local_promotion_specs_table_ddl("softprobe");
        assert!(ddl.starts_with(r#"CREATE TABLE IF NOT EXISTS "softprobe".promotion_specs ("#));
        assert!(ddl.contains(r#"spec_id TEXT NOT NULL"#));
        assert!(!ddl.contains("PRIMARY KEY"));
        assert!(ddl.contains("manifest_hash TEXT NOT NULL"));
        assert!(ddl.contains("status TEXT NOT NULL"));
        assert!(!ddl.contains("CREATE SCHEMA"));
    }

    #[test]
    fn telemetry_manifest_from_row_parses_and_skips_business() {
        let telemetry = r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: service_name
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: service.name
"#;
        let parsed = super::telemetry_manifest_from_row("spec-1", telemetry)
            .expect("parse")
            .expect("telemetry");
        assert_eq!(parsed.columns[0].name, "service_name");

        let business = r#"
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
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
"#;
        assert!(super::telemetry_manifest_from_row("biz-1", business)
            .expect("parse")
            .is_none());
        let biz = super::business_manifest_from_row("biz-1", business)
            .expect("parse")
            .expect("business");
        assert_eq!(biz.target.table, "checkout_orders");
    }

    #[test]
    fn telemetry_column_ddls_are_additive_and_tenant_scoped() {
        let manifest = parse_promotion_manifest(
            r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces, logs]
columns:
  - name: division_name
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: division.name
  - name: checkout_latency_ms
    type: double
    nullable: true
    source:
      from: attribute
      key: checkout.latency_ms
"#,
        )
        .expect("valid manifest");
        let PromotionManifest::TelemetryColumns(spec) = manifest else {
            panic!("expected telemetry manifest");
        };

        let ddls =
            super::telemetry_column_add_ddls(r#""softprobe"."tenant_alpha""#, &spec).expect("ddl");

        assert_eq!(ddls.len(), 4);
        assert_eq!(
            ddls[0],
            r#"ALTER TABLE "softprobe"."tenant_alpha".traces ADD COLUMN IF NOT EXISTS "division_name" VARCHAR;"#
        );
        assert_eq!(
            ddls[3],
            r#"ALTER TABLE "softprobe"."tenant_alpha".logs ADD COLUMN IF NOT EXISTS "checkout_latency_ms" DOUBLE;"#
        );
    }

    #[test]
    fn telemetry_column_compatibility_rejects_existing_columns() {
        let manifest = parse_promotion_manifest(
            r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: session_id
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: session.id
"#,
        )
        .expect("valid manifest");
        let PromotionManifest::TelemetryColumns(spec) = manifest else {
            panic!("expected telemetry manifest");
        };

        let err = super::validate_telemetry_column_additive(&spec).expect_err("reserved column");

        assert_eq!(err.code(), "column_already_exists");
        assert_eq!(err.path(), "columns[0].name");
    }

    #[test]
    fn extracts_telemetry_promoted_values_from_supported_sources() {
        let mut resource_attributes = std::collections::HashMap::new();
        resource_attributes.insert("service.name".to_string(), "checkout-api".to_string());
        let mut attributes = std::collections::HashMap::new();
        attributes.insert("division.name".to_string(), "payments".to_string());
        let mut event_attributes = std::collections::HashMap::new();
        event_attributes.insert("order.total_cents".to_string(), "4200".to_string());
        let events = vec![super::TelemetryPromotionEvent {
            name: "checkout.completed".to_string(),
            attributes: event_attributes,
        }];
        let row = super::TelemetryPromotionRow {
            resource_attributes: &resource_attributes,
            attributes: &attributes,
            events: &events,
            http_request_body: None,
            http_response_body: Some(r#"{"payment":{"status":"paid"}}"#),
            metric_value: None,
        };

        assert_eq!(
            super::extract_telemetry_promoted_value(
                &row,
                &super::PromotionColumn {
                    name: "service_name".to_string(),
                    data_type: super::PromotionDataType::String,
                    nullable: true,
                    source: super::PromotionSource::ResourceAttribute {
                        key: "service.name".to_string(),
                    },
                },
            )
            .expect("resource attr"),
            Some("checkout-api".to_string())
        );
        assert_eq!(
            super::extract_telemetry_promoted_value(
                &row,
                &super::PromotionColumn {
                    name: "division_name".to_string(),
                    data_type: super::PromotionDataType::String,
                    nullable: true,
                    source: super::PromotionSource::Attribute {
                        key: "division.name".to_string(),
                    },
                },
            )
            .expect("attribute"),
            Some("payments".to_string())
        );
        assert_eq!(
            super::extract_telemetry_promoted_value(
                &row,
                &super::PromotionColumn {
                    name: "order_total_cents".to_string(),
                    data_type: super::PromotionDataType::Int64,
                    nullable: true,
                    source: super::PromotionSource::EventAttribute {
                        event_name: "checkout.completed".to_string(),
                        key: "order.total_cents".to_string(),
                    },
                },
            )
            .expect("event attribute"),
            Some("4200".to_string())
        );
        assert_eq!(
            super::extract_telemetry_promoted_value(
                &row,
                &super::PromotionColumn {
                    name: "payment_status".to_string(),
                    data_type: super::PromotionDataType::String,
                    nullable: true,
                    source: super::PromotionSource::HttpResponseBody {
                        json_path: "$.payment.status".to_string(),
                    },
                },
            )
            .expect("http response json path"),
            Some("paid".to_string())
        );
    }

    #[test]
    fn rejects_telemetry_promoted_value_type_mismatch() {
        let mut attributes = std::collections::HashMap::new();
        attributes.insert("checkout.latency_ms".to_string(), "slow".to_string());
        let empty_resource_attributes = std::collections::HashMap::new();
        let row = super::TelemetryPromotionRow {
            resource_attributes: &empty_resource_attributes,
            attributes: &attributes,
            events: &[],
            http_request_body: None,
            http_response_body: None,
            metric_value: None,
        };

        let err = super::extract_telemetry_promoted_value(
            &row,
            &super::PromotionColumn {
                name: "checkout_latency_ms".to_string(),
                data_type: super::PromotionDataType::Double,
                nullable: true,
                source: super::PromotionSource::Attribute {
                    key: "checkout.latency_ms".to_string(),
                },
            },
        )
        .expect_err("double parse should fail");

        assert_eq!(err.code(), "promotion_value_type_mismatch");
        assert_eq!(err.path(), "columns.checkout_latency_ms");
    }

    #[test]
    fn extracts_business_row_with_evidence_anchors_from_http_body_and_attributes() {
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
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
  - name: payment_status
    type: string
    nullable: true
    source:
      from: attribute
      key: payment.status
"#,
        )
        .expect("valid manifest");
        let PromotionManifest::BusinessTable(spec) = manifest else {
            panic!("expected business table manifest");
        };
        let attributes = HashMap::from([
            ("sp.workflow".to_string(), "checkout".to_string()),
            ("payment.status".to_string(), "paid".to_string()),
        ]);
        let input = super::BusinessPromotionInput {
            session_id: "session-1",
            trace_id: "trace-1",
            span_id: "span-1",
            event_name: None,
            event_timestamp: None,
            service_name: Some("checkout-api"),
            source_signal: "trace",
            source_timestamp: "2026-05-06T17:00:00Z",
            attributes: &attributes,
            events: &[],
            http_request_body: None,
            http_response_body: Some(r#"{"order":{"id":"ord_123","total_cents":4200}}"#),
        };

        let row = super::extract_business_promoted_row(&spec, &input)
            .expect("extract row")
            .expect("selector matches");

        assert_eq!(row.session_id, "session-1");
        assert_eq!(row.trace_id, "trace-1");
        assert_eq!(row.span_id, "span-1");
        assert_eq!(row.service_name.as_deref(), Some("checkout-api"));
        assert_eq!(row.source_signal, "trace");
        assert_eq!(row.source_timestamp, "2026-05-06T17:00:00Z");
        assert_eq!(row.values["order_id"], "ord_123");
        assert_eq!(row.values["total_cents"], "4200");
        assert_eq!(row.values["payment_status"], "paid");
    }

    #[test]
    fn business_row_extraction_returns_none_when_selector_does_not_match() {
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
        let PromotionManifest::BusinessTable(spec) = manifest else {
            panic!("expected business table manifest");
        };
        let attributes = HashMap::from([("sp.workflow".to_string(), "refund".to_string())]);
        let input = super::BusinessPromotionInput {
            session_id: "session-1",
            trace_id: "trace-1",
            span_id: "span-1",
            event_name: None,
            event_timestamp: None,
            service_name: None,
            source_signal: "trace",
            source_timestamp: "2026-05-06T17:00:00Z",
            attributes: &attributes,
            events: &[],
            http_request_body: None,
            http_response_body: Some(r#"{"order":{"id":"ord_123"}}"#),
        };

        let row = super::extract_business_promoted_row(&spec, &input).expect("extract row");

        assert!(row.is_none());
    }

    #[test]
    fn business_row_extraction_reports_missing_required_and_reads_event_sources() {
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
  - name: risk_score
    type: double
    nullable: true
    source:
      from: event_attribute
      event_name: risk.checked
      key: risk.score
"#,
        )
        .expect("valid manifest");
        let PromotionManifest::BusinessTable(spec) = manifest else {
            panic!("expected business table manifest");
        };
        let attributes = HashMap::from([("sp.workflow".to_string(), "checkout".to_string())]);
        let events = vec![super::TelemetryPromotionEvent {
            name: "risk.checked".to_string(),
            attributes: HashMap::from([("risk.score".to_string(), "0.87".to_string())]),
        }];
        let input = super::BusinessPromotionInput {
            session_id: "session-1",
            trace_id: "trace-1",
            span_id: "span-1",
            event_name: Some("risk.checked"),
            event_timestamp: Some("2026-05-06T17:00:01Z"),
            service_name: None,
            source_signal: "event",
            source_timestamp: "2026-05-06T17:00:01Z",
            attributes: &attributes,
            events: &events,
            http_request_body: None,
            http_response_body: Some(r#"{"order":{}}"#),
        };

        let errors = super::extract_business_promoted_row(&spec, &input)
            .expect_err("required order id is missing");

        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].target_column, "order_id");
        assert_eq!(errors[0].error_code, "missing_required_value");
    }

    fn telemetry_manifest(yaml: &str) -> super::TelemetryColumnsManifest {
        match parse_promotion_manifest(yaml).expect("valid manifest") {
            PromotionManifest::TelemetryColumns(m) => m,
            PromotionManifest::BusinessTable(_) => panic!("expected telemetry manifest"),
        }
    }

    #[test]
    fn merge_combines_disjoint_columns_from_multiple_manifests() {
        let fragment_a = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: operation_name
    type: string
    nullable: true
    source:
      from: attribute
      key: gen_ai.operation.name
  - name: environment
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: deployment.environment.name
"#,
        );
        let fragment_b = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: http_route
    type: string
    nullable: true
    source:
      from: attribute
      key: http.route
  - name: http_status_code
    type: int64
    nullable: true
    source:
      from: attribute
      key: http.response.status_code
"#,
        );

        let merged =
            super::merge_telemetry_columns_manifests(&[fragment_a, fragment_b]).expect("merge");

        assert_eq!(merged.target.tables, vec![super::TelemetryTable::Traces]);
        let names: Vec<&str> = merged.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "operation_name",
                "environment",
                "http_route",
                "http_status_code"
            ]
        );
    }

    #[test]
    fn merge_is_idempotent_for_identical_duplicate_columns() {
        let a = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [traces] }
columns:
  - name: service_name
    type: string
    nullable: true
    source: { from: resource_attribute, key: service.name }
"#,
        );
        let b = a.clone();

        let merged = super::merge_telemetry_columns_manifests(&[a, b]).expect("merge");

        assert_eq!(merged.columns.len(), 1);
    }

    #[test]
    fn merge_rejects_conflicting_duplicate_column_definitions() {
        let a = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [traces] }
columns:
  - name: environment
    type: string
    nullable: true
    source: { from: resource_attribute, key: deployment.environment.name }
"#,
        );
        let b = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [traces] }
columns:
  - name: environment
    type: int64
    nullable: true
    source: { from: attribute, key: sp_record_environment }
"#,
        );

        let err = super::merge_telemetry_columns_manifests(&[a, b])
            .expect_err("conflicting duplicate column must be rejected");

        assert_eq!(err.code(), "merge_conflicting_duplicate_column");
        assert_eq!(err.path(), "columns.environment");
    }

    #[test]
    fn merge_rejects_reserved_base_column_names() {
        let a = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [traces] }
columns:
  - name: record_operation
    type: string
    nullable: true
    source: { from: attribute, key: sp_operation_name }
"#,
        );
        let b = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [traces] }
columns:
  - name: session_id
    type: string
    nullable: true
    source: { from: attribute, key: sp_session_id }
"#,
        );

        let err = super::merge_telemetry_columns_manifests(&[a, b])
            .expect_err("reserved base column name must be rejected");

        assert_eq!(err.code(), "column_already_exists");
    }

    #[test]
    fn merged_manifest_round_trips_through_yaml_serialization() {
        let a = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [traces] }
columns:
  - name: service_name
    type: string
    nullable: true
    source: { from: resource_attribute, key: service.name }
"#,
        );
        let b = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [traces] }
columns:
  - name: record_operation
    type: string
    nullable: true
    source: { from: attribute, key: sp_operation_name }
  - name: record_deleted
    type: bool
    nullable: true
    source: { from: attribute, key: sp_record_deleted }
  - name: expiration_time
    type: timestamp
    nullable: true
    source: { from: attribute, key: sp_expiration_time }
"#,
        );
        let merged = super::merge_telemetry_columns_manifests(&[a, b]).expect("merge");

        let yaml = super::telemetry_columns_manifest_to_yaml(&merged);
        let reparsed = telemetry_manifest(&yaml);

        assert_eq!(reparsed, merged);
    }

    #[test]
    fn telemetry_columns_manifest_to_yaml_escapes_special_characters_in_keys() {
        // Attribute keys / json_paths containing YAML-significant characters (colons, quotes)
        // must still round-trip; a hand-rolled writer would misparse `foo: bar` as a nested map.
        let manifest = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [traces] }
columns:
  - name: weird_key
    type: string
    nullable: true
    source: { from: attribute, key: "foo: bar \"baz\"" }
  - name: weird_path
    type: string
    nullable: true
    source: { from: http_response_body, json_path: "$.a[\"b: c\"]" }
"#,
        );

        let yaml = super::telemetry_columns_manifest_to_yaml(&manifest);
        let reparsed = telemetry_manifest(&yaml);

        assert_eq!(reparsed, manifest);
    }

    #[test]
    fn merge_rejects_mismatched_target_tables() {
        let traces_only = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [traces] }
columns:
  - name: service_name
    type: string
    nullable: true
    source: { from: resource_attribute, key: service.name }
"#,
        );
        let logs_only = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [logs] }
columns:
  - name: log_source
    type: string
    nullable: true
    source: { from: attribute, key: log.source }
"#,
        );

        let err = super::merge_telemetry_columns_manifests(&[traces_only, logs_only])
            .expect_err("mismatched target tables must be rejected");

        assert_eq!(err.code(), "merge_target_tables_mismatch");
    }

    #[test]
    fn merge_allows_manifests_with_identical_multi_table_targets() {
        let a = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [traces, logs] }
columns:
  - name: service_name
    type: string
    nullable: true
    source: { from: resource_attribute, key: service.name }
"#,
        );
        let b = telemetry_manifest(
            r#"
specVersion: softprobe.promotion.v1
target: { kind: telemetry_columns, tables: [logs, traces] }
columns:
  - name: record_category
    type: string
    nullable: true
    source: { from: attribute, key: sp_category_type }
"#,
        );

        let merged = super::merge_telemetry_columns_manifests(&[a, b]).expect("merge");

        assert_eq!(
            merged.target.tables,
            vec![super::TelemetryTable::Traces, super::TelemetryTable::Logs]
        );
        assert_eq!(merged.columns.len(), 2);
    }

    #[test]
    fn merge_requires_at_least_one_manifest() {
        let err = super::merge_telemetry_columns_manifests(&[])
            .expect_err("empty merge input must be rejected");
        assert_eq!(err.code(), "missing_manifests");
    }
}
