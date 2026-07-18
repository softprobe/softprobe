// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

use crate::config::{Config, DuckLakeConfig};
use crate::models::{Log, Metric, Span};
use crate::promotion::{
    business_table_create_ddls, extract_telemetry_promoted_value, telemetry_column_add_ddls,
    BusinessTableManifest, PromotionColumn, TelemetryColumnsManifest, TelemetryPromotionEvent,
    TelemetryPromotionRow, TelemetryTable,
};
use crate::runtime_engine::{DuckLakeScope, DuckLakeScopeResolver};
use crate::storage::schema::arrow;
use crate::storage::schema::tables::{OtlpLogsTable, OtlpMetricsTable, TraceTable};
use ::arrow::datatypes::Schema;
use ::arrow::record_batch::RecordBatch;
use anyhow::{anyhow, Result};
use duckdb::{Connection, ToSql};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

/// Configure DuckDB httpfs for the configured object store + `data_path`.
///
/// Credentials come from the environment (never YAML):
/// - `gs://` → `GCS_HMAC_*` / `GCP_HMAC_*`
/// - `s3://` → `AWS_*` (or EC2 instance metadata)
///
/// See <https://duckdb.org/docs/current/guides/network_cloud_storage/gcs_import.html>.
pub fn configure_object_store(conn: &Connection, config: &Config, data_path: &str) -> Result<()> {
    if data_path.starts_with("gs://") {
        let creds = config.resolve_object_store_credentials(data_path);
        let (Some(key_id), Some(secret)) = (creds.access_key_id, creds.secret_access_key) else {
            warn!(
                "DuckLake data_path is {} but GCS_HMAC_ACCESS_KEY_ID/GCS_HMAC_SECRET are unset; gs:// I/O may return HTTP 403",
                data_path
            );
            return Ok(());
        };
        let kid = key_id.replace('\'', "''");
        let sec = secret.replace('\'', "''");
        let sql = format!(
            "CREATE OR REPLACE SECRET gcs_hmac (TYPE GCS, KEY_ID '{kid}', SECRET '{sec}');"
        );
        conn.execute_batch(&sql)?;
        return Ok(());
    }

    if let Some(endpoint) = config.object_store.endpoint.as_ref() {
        let trimmed = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        conn.execute("SET s3_endpoint = ?;", [&trimmed as &dyn ToSql])?;
        conn.execute("SET s3_url_style = 'path';", [])?;
        if endpoint.starts_with("http://") {
            conn.execute("SET s3_use_ssl = false;", [])?;
        } else if endpoint.starts_with("https://") {
            conn.execute("SET s3_use_ssl = true;", [])?;
        }
    }

    let creds = config.resolve_object_store_credentials(data_path);
    if let Some(access_key) = creds.access_key_id.as_ref() {
        conn.execute("SET s3_access_key_id = ?;", [access_key as &dyn ToSql])?;
    }
    if let Some(secret) = creds.secret_access_key.as_ref() {
        conn.execute("SET s3_secret_access_key = ?;", [secret as &dyn ToSql])?;
    }
    if let Some(token) = creds.session_token.as_ref() {
        conn.execute("SET s3_session_token = ?;", [token as &dyn ToSql])?;
    }
    conn.execute(
        "SET s3_region = ?;",
        [&config.object_store.region as &dyn ToSql],
    )?;
    Ok(())
}

/// Backward-compatible alias used by older call sites / docs.
pub fn configure_httpfs_gcs_for_data_path(conn: &Connection, data_path: &str) -> Result<()> {
    let config = Config::default();
    configure_object_store(conn, &config, data_path)
}

/// Per-scope pool of already-ATTACH'd DuckDB connections for concurrent DuckLake commits.
struct WriterPool {
    conns: Vec<Mutex<Connection>>,
    next: AtomicUsize,
}

impl WriterPool {
    fn with_conn<R>(&self, f: impl FnOnce(&Connection) -> Result<R>) -> Result<R> {
        let n = self.conns.len();
        if n == 0 {
            return Err(anyhow!("DuckLake writer pool is empty"));
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
        for i in 0..n {
            let idx = (start + i) % n;
            if let Ok(guard) = self.conns[idx].try_lock() {
                return f(&guard);
            }
        }
        let guard = self.conns[start]
            .lock()
            .map_err(|_| anyhow!("DuckLake writer connection lock poisoned"))?;
        f(&guard)
    }
}

pub struct DuckLakeWriter {
    config: Config,
    ducklake: DuckLakeConfig,
    dropdown_catalog: Option<std::sync::Arc<crate::catalog::DropdownCatalog>>,
    /// When set with `catalog_type = postgres`, commits route to per-tenant metadata schemas.
    tenant_ducklake: Option<DuckLakeScopeResolver>,
    /// When true, this writer is permanently bound to `ducklake.metadata_schema` / `data_path`
    /// (built via [`IngestPipeline::build_tenant_storage`]). When false, postgres writers with a
    /// registry resolver route each batch by `span.tenant_id` even if config carries a non-main
    /// registry schema name.
    scope_bound: bool,
    /// Per catalog-scope pools of reused ATTACH'd DuckDB connections.
    writer_pools: Mutex<HashMap<String, Arc<WriterPool>>>,
}

impl DuckLakeWriter {
    pub async fn new(
        config: &Config,
        dropdown_catalog: Option<std::sync::Arc<crate::catalog::DropdownCatalog>>,
        tenant_ducklake: Option<DuckLakeScopeResolver>,
    ) -> Result<Self> {
        Self::new_inner(config, dropdown_catalog, tenant_ducklake, false).await
    }

    /// Writer permanently bound to one DuckLake scope (per-tenant runtime engine).
    pub async fn new_scope_bound(
        config: &Config,
        dropdown_catalog: Option<std::sync::Arc<crate::catalog::DropdownCatalog>>,
        tenant_ducklake: Option<DuckLakeScopeResolver>,
    ) -> Result<Self> {
        Self::new_inner(config, dropdown_catalog, tenant_ducklake, true).await
    }

    async fn new_inner(
        config: &Config,
        dropdown_catalog: Option<std::sync::Arc<crate::catalog::DropdownCatalog>>,
        tenant_ducklake: Option<DuckLakeScopeResolver>,
        scope_bound: bool,
    ) -> Result<Self> {
        config.validate_ducklake_catalog()?;
        let ducklake = config.ducklake.clone();
        let writer = Self {
            config: config.clone(),
            ducklake,
            dropdown_catalog,
            tenant_ducklake,
            scope_bound,
            writer_pools: Mutex::new(HashMap::new()),
        };
        writer.initialize_catalog()?;
        info!(
            "DuckLake writer initialized (scope_bound={}, writer_pool_size={})",
            scope_bound,
            writer.ducklake.effective_writer_pool_size()
        );
        Ok(writer)
    }

    pub fn dropdown_catalog(&self) -> Option<std::sync::Arc<crate::catalog::DropdownCatalog>> {
        self.dropdown_catalog.clone()
    }

    pub fn scope_registry(&self) -> Option<&DuckLakeScopeResolver> {
        self.tenant_ducklake.as_ref()
    }

    /// `true` when DuckLake writes are partitioned per authenticated tenant (Postgres catalog + registry).
    pub fn tenant_scoped_ingest_enabled(&self) -> bool {
        self.use_tenant_scoped_ducklake()
    }

    fn initialize_catalog(&self) -> Result<()> {
        self.with_attached_conn(&self.ducklake, |conn| {
            if std::env::var("SPLAKE_RESET_DUCKLAKE").ok().as_deref() == Some("1") {
                self.reset_tables_for_dev(conn)?;
            }
            Ok(())
        })
    }

    fn conn_cache_key(dk: &DuckLakeConfig) -> String {
        format!(
            "{}|{}|{}|{}",
            dk.catalog_type, dk.metadata_path, dk.metadata_schema, dk.data_path
        )
    }

    fn get_or_create_pool(&self, dk: &DuckLakeConfig) -> Result<Arc<WriterPool>> {
        let key = Self::conn_cache_key(dk);
        let mut guard = self
            .writer_pools
            .lock()
            .map_err(|_| anyhow!("DuckLake writer pool map lock poisoned"))?;
        if let Some(pool) = guard.get(&key) {
            return Ok(Arc::clone(pool));
        }
        let size = dk.effective_writer_pool_size();
        let mut conns = Vec::with_capacity(size);
        // Attach sequentially: the first connection initializes DuckLake metadata tables; later
        // pool members ATTACH the already-initialized Postgres schema (with retry on races).
        for _ in 0..size {
            let conn = self.open_connection_for(dk)?;
            apply_ducklake_retry_settings(&conn)?;
            self.attach_ducklake_for(&conn, dk)?;
            self.ensure_schema_for(&conn, dk)?;
            conns.push(Mutex::new(conn));
        }
        let pool = Arc::new(WriterPool {
            conns,
            next: AtomicUsize::new(0),
        });
        guard.insert(key, Arc::clone(&pool));
        Ok(pool)
    }

    /// Borrow one connection from the per-scope writer pool (short map lock; pool slot for SQL).
    fn with_attached_conn<R>(
        &self,
        dk: &DuckLakeConfig,
        f: impl FnOnce(&Connection) -> Result<R>,
    ) -> Result<R> {
        let pool = self.get_or_create_pool(dk)?;
        pool.with_conn(f)
    }

    fn use_tenant_scoped_ducklake(&self) -> bool {
        self.tenant_ducklake.is_some() && self.ducklake.catalog_type == "postgres"
    }

    fn effective_ducklake(&self, scope: &DuckLakeScope) -> DuckLakeConfig {
        let mut dk = self.ducklake.clone();
        dk.metadata_schema = scope.metadata_schema.clone();
        dk.data_path = scope.data_path.clone();
        dk
    }

    fn tenant_bound_scope(&self) -> Option<DuckLakeScope> {
        if !self.scope_bound || self.ducklake.catalog_type != "postgres" {
            return None;
        }
        Some(DuckLakeScope {
            metadata_schema: self.ducklake.metadata_schema.clone(),
            data_path: self.ducklake.data_path.clone(),
        })
    }

    fn flatten_spans(batches: Vec<Vec<Span>>) -> Vec<Span> {
        batches.into_iter().flatten().collect()
    }

    fn flatten_logs(batches: Vec<Vec<Log>>) -> Vec<Log> {
        batches.into_iter().flatten().collect()
    }

    fn flatten_metrics(batches: Vec<Vec<Metric>>) -> Vec<Metric> {
        batches.into_iter().flatten().collect()
    }

    fn telemetry_columns_for_table(
        manifests: &[TelemetryColumnsManifest],
        table: TelemetryTable,
    ) -> Vec<PromotionColumn> {
        manifests
            .iter()
            .filter(|manifest| manifest.target.tables.contains(&table))
            .flat_map(|manifest| manifest.columns.iter().cloned())
            .collect()
    }

    fn apply_span_promotions(spans: &mut [Span], columns: &[PromotionColumn]) -> Result<()> {
        for span in spans {
            let events = span
                .events
                .iter()
                .map(|event| TelemetryPromotionEvent {
                    name: event.name.clone(),
                    attributes: event.attributes.clone(),
                })
                .collect::<Vec<_>>();
            let row = TelemetryPromotionRow {
                resource_attributes: &span.resource_attributes,
                attributes: &span.attributes,
                events: &events,
                http_request_body: span.http_request_body.as_deref(),
                http_response_body: span.http_response_body.as_deref(),
                metric_value: None,
            };
            let mut promoted = Vec::new();
            for column in columns {
                if let Some(value) = extract_telemetry_promoted_value(&row, column)? {
                    promoted.push((column.name.clone(), value));
                }
            }
            span.attributes.extend(promoted);
        }
        Ok(())
    }

    fn apply_log_promotions(logs: &mut [Log], columns: &[PromotionColumn]) -> Result<()> {
        for log in logs {
            let row = TelemetryPromotionRow {
                resource_attributes: &log.resource_attributes,
                attributes: &log.attributes,
                events: &[],
                http_request_body: None,
                http_response_body: None,
                metric_value: None,
            };
            let mut promoted = Vec::new();
            for column in columns {
                if let Some(value) = extract_telemetry_promoted_value(&row, column)? {
                    promoted.push((column.name.clone(), value));
                }
            }
            log.attributes.extend(promoted);
        }
        Ok(())
    }

    fn apply_metric_promotions(metrics: &mut [Metric], columns: &[PromotionColumn]) -> Result<()> {
        for metric in metrics {
            let row = TelemetryPromotionRow {
                resource_attributes: &metric.resource_attributes,
                attributes: &metric.attributes,
                events: &[],
                http_request_body: None,
                http_response_body: None,
                metric_value: Some(metric.value),
            };
            let mut promoted = Vec::new();
            for column in columns {
                if let Some(value) = extract_telemetry_promoted_value(&row, column)? {
                    promoted.push((column.name.clone(), value));
                }
            }
            metric.attributes.extend(promoted);
        }
        Ok(())
    }

    pub async fn write_span_batches(&self, batches: Vec<Vec<Span>>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        if self.use_tenant_scoped_ducklake() {
            let resolver = self.tenant_ducklake.as_ref().unwrap();
            if let Some(scope) = self.tenant_bound_scope() {
                let mut spans = Self::flatten_spans(batches);
                if spans.is_empty() {
                    return Ok(());
                }
                let manifests = resolver
                    .load_active_telemetry_columns_manifests_for_scope(&scope)
                    .await?;
                let columns = Self::telemetry_columns_for_table(&manifests, TelemetryTable::Traces);
                Self::apply_span_promotions(&mut spans, &columns)?;
                let schema = Arc::new(TraceTable::schema_with_promoted_columns(&columns));
                let dk = self.effective_ducklake(&scope);
                let record_batches = vec![Span::to_record_batch(&spans, schema.as_ref())?];
                self.write_record_batches_internal_with_ducklake(&dk, "traces", record_batches)
                    .await?;
                return Ok(());
            }
            let spans = Self::flatten_spans(batches);
            if spans.is_empty() {
                return Ok(());
            }
            let mut by_tenant: HashMap<String, Vec<Span>> = HashMap::new();
            for span in spans {
                let tid = span
                    .tenant_id
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "tenant-scoped ingest requires span.tenant_id from authenticated tenant"
                        )
                    })?
                    .clone();
                by_tenant.entry(tid).or_default().push(span);
            }
            for (tenant_id, mut tenant_spans) in by_tenant {
                let (scope, manifests) = resolver
                    .load_active_telemetry_columns_manifests(&tenant_id)
                    .await?;
                let columns = Self::telemetry_columns_for_table(&manifests, TelemetryTable::Traces);
                Self::apply_span_promotions(&mut tenant_spans, &columns)?;
                let schema = Arc::new(TraceTable::schema_with_promoted_columns(&columns));
                let dk = self.effective_ducklake(&scope);
                let record_batches = vec![Span::to_record_batch(&tenant_spans, schema.as_ref())?];
                self.write_record_batches_internal_with_ducklake(&dk, "traces", record_batches)
                    .await?;
            }
            Ok(())
        } else {
            let schema = self.spans_schema().await?;
            let mut record_batches = Vec::new();
            for batch in batches {
                if !batch.is_empty() {
                    record_batches.push(Span::to_record_batch(&batch, schema.as_ref())?);
                }
            }
            self.write_record_batches_internal("traces", record_batches)
                .await
        }
    }

    async fn write_tenant_log_batches(
        &self,
        scope: &DuckLakeScope,
        manifests: &[TelemetryColumnsManifest],
        batches: Vec<Vec<Log>>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let mut logs = Self::flatten_logs(batches);
        if logs.is_empty() {
            return Ok(());
        }
        let columns = Self::telemetry_columns_for_table(manifests, TelemetryTable::Logs);
        Self::apply_log_promotions(&mut logs, &columns)?;
        let schema = Arc::new(OtlpLogsTable::schema_with_promoted_columns(&columns));
        let dk = self.effective_ducklake(scope);
        let record_batches = vec![arrow::logs_to_record_batch(&logs, schema.as_ref())?];
        self.write_record_batches_internal_with_ducklake(&dk, "logs", record_batches)
            .await?;
        Ok(())
    }

    pub async fn write_log_batches(&self, batches: Vec<Vec<Log>>) -> Result<()> {
        if self.use_tenant_scoped_ducklake() {
            let resolver = self.tenant_ducklake.as_ref().unwrap();
            // Non-scope-bound writers (single-tenant / tests) use the configured DuckLake scope.
            let scope = self.tenant_bound_scope().unwrap_or_else(|| DuckLakeScope {
                metadata_schema: self.ducklake.metadata_schema.clone(),
                data_path: self.ducklake.data_path.clone(),
            });
            let manifests = if self.scope_bound {
                resolver
                    .load_active_telemetry_columns_manifests_for_scope(&scope)
                    .await?
            } else {
                resolver
                    .load_active_telemetry_columns_manifests("")
                    .await?
                    .1
            };
            return self
                .write_tenant_log_batches(&scope, &manifests, batches)
                .await;
        }
        let schema = self.logs_schema().await?;
        let mut record_batches = Vec::new();
        for batch in batches {
            if !batch.is_empty() {
                record_batches.push(arrow::logs_to_record_batch(&batch, schema.as_ref())?);
            }
        }
        self.write_record_batches_internal("logs", record_batches)
            .await
    }

    async fn write_tenant_metric_batches(
        &self,
        scope: &DuckLakeScope,
        manifests: &[TelemetryColumnsManifest],
        batches: Vec<Vec<Metric>>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let mut metrics = Self::flatten_metrics(batches);
        if metrics.is_empty() {
            return Ok(());
        }
        let columns = Self::telemetry_columns_for_table(manifests, TelemetryTable::Metrics);
        Self::apply_metric_promotions(&mut metrics, &columns)?;
        let schema = Arc::new(OtlpMetricsTable::schema_with_promoted_columns(&columns));
        let dk = self.effective_ducklake(scope);
        let record_batches = vec![arrow::metrics_to_record_batch(&metrics, schema.as_ref())?];
        self.write_record_batches_internal_with_ducklake(&dk, "metrics", record_batches)
            .await?;
        Ok(())
    }

    pub async fn write_metric_batches(&self, batches: Vec<Vec<Metric>>) -> Result<()> {
        if self.use_tenant_scoped_ducklake() {
            let resolver = self.tenant_ducklake.as_ref().unwrap();
            // Non-scope-bound writers (single-tenant / tests) use the configured DuckLake scope.
            let scope = self.tenant_bound_scope().unwrap_or_else(|| DuckLakeScope {
                metadata_schema: self.ducklake.metadata_schema.clone(),
                data_path: self.ducklake.data_path.clone(),
            });
            let manifests = if self.scope_bound {
                resolver
                    .load_active_telemetry_columns_manifests_for_scope(&scope)
                    .await?
            } else {
                resolver
                    .load_active_telemetry_columns_manifests("")
                    .await?
                    .1
            };
            return self
                .write_tenant_metric_batches(&scope, &manifests, batches)
                .await;
        }
        let schema = self.metrics_schema().await?;
        let mut record_batches = Vec::new();
        for batch in batches {
            if !batch.is_empty() {
                record_batches.push(arrow::metrics_to_record_batch(&batch, schema.as_ref())?);
            }
        }
        self.write_record_batches_internal("metrics", record_batches)
            .await
    }

    pub async fn write_span_record_batches(&self, record_batches: Vec<RecordBatch>) -> Result<()> {
        if self.use_tenant_scoped_ducklake() {
            let scope = self
                .tenant_ducklake
                .as_ref()
                .unwrap()
                .resolve_or_create("")
                .await?;
            let dk = self.effective_ducklake(&scope);
            self.write_record_batches_internal_with_ducklake(&dk, "traces", record_batches)
                .await
        } else {
            self.write_record_batches_internal("traces", record_batches)
                .await
        }
    }

    pub async fn write_log_record_batches(&self, record_batches: Vec<RecordBatch>) -> Result<()> {
        if self.use_tenant_scoped_ducklake() {
            let scope = self
                .tenant_ducklake
                .as_ref()
                .unwrap()
                .resolve_or_create("")
                .await?;
            let dk = self.effective_ducklake(&scope);
            return self
                .write_record_batches_internal_with_ducklake(&dk, "logs", record_batches)
                .await;
        }
        self.write_record_batches_internal("logs", record_batches)
            .await
    }

    pub async fn write_metric_record_batches(
        &self,
        record_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        if self.use_tenant_scoped_ducklake() {
            let scope = self
                .tenant_ducklake
                .as_ref()
                .unwrap()
                .resolve_or_create("")
                .await?;
            let dk = self.effective_ducklake(&scope);
            return self
                .write_record_batches_internal_with_ducklake(&dk, "metrics", record_batches)
                .await;
        }
        self.write_record_batches_internal("metrics", record_batches)
            .await
    }

    /// Apply additive telemetry promotion DDL inside one tenant DuckLake scope.
    ///
    /// `promotion apply` owns schema changes for promoted telemetry columns. It first materializes
    /// the hardcoded canonical telemetry tables if they do not exist, then runs the nullable
    /// `ALTER TABLE ADD COLUMN IF NOT EXISTS` statements generated from the tenant manifest.
    pub async fn apply_telemetry_column_promotion(
        &self,
        scope: &DuckLakeScope,
        spec: &TelemetryColumnsManifest,
    ) -> Result<Vec<String>> {
        let dk = self.effective_ducklake(scope);
        for table in &spec.target.tables {
            self.ensure_telemetry_table_for(&dk, table).await?;
        }
        let ddls = self.with_attached_conn(&dk, |conn| {
            let prefix = if dk.metadata_schema == "main" {
                dk.catalog_alias.clone()
            } else {
                format!(
                    "{}.{}",
                    quote_duckdb_ident(&dk.catalog_alias),
                    quote_duckdb_ident(&dk.metadata_schema)
                )
            };
            let ddls = telemetry_column_add_ddls(&prefix, spec)
                .map_err(|err| anyhow!("telemetry promotion validation failed: {err}"))?;
            for ddl in &ddls {
                conn.execute_batch(ddl)?;
            }
            Ok(ddls)
        })?;
        Ok(ddls)
    }

    /// Apply generated business table DDL inside one tenant DuckLake scope.
    ///
    /// Business promotion manifests own the physical table and current view. The runtime executes
    /// generated DDL in order so agents do not need to write tenant-specific `CREATE TABLE` SQL.
    pub async fn apply_business_table_promotion(
        &self,
        scope: &DuckLakeScope,
        spec: &BusinessTableManifest,
    ) -> Result<Vec<String>> {
        let dk = self.effective_ducklake(scope);
        let ddls = self.with_attached_conn(&dk, |conn| {
            // Prefer catalog.schema when metadata lives outside `main`; fall back to catalog-only
            // (matches write-path table name candidates when ATTACH uses METADATA_SCHEMA).
            let prefixes = if dk.metadata_schema == "main" {
                vec![dk.catalog_alias.clone()]
            } else {
                vec![
                    format!(
                        "{}.{}",
                        quote_duckdb_ident(&dk.catalog_alias),
                        quote_duckdb_ident(&dk.metadata_schema)
                    ),
                    dk.catalog_alias.clone(),
                ]
            };
            let mut last_err: Option<anyhow::Error> = None;
            for prefix in prefixes {
                let ddls = business_table_create_ddls(&prefix, spec)
                    .map_err(|err| anyhow!("business table promotion validation failed: {err}"))?;
                match ddls
                    .iter()
                    .try_for_each(|ddl| conn.execute_batch(ddl).map(|_| ()))
                {
                    Ok(()) => return Ok(ddls),
                    Err(err) => {
                        last_err = Some(anyhow!(
                            "business table promotion failed with prefix {prefix}: {err}"
                        ));
                    }
                }
            }
            Err(last_err.unwrap_or_else(|| anyhow!("business table promotion failed")))
        })?;
        Ok(ddls)
    }

    async fn ensure_telemetry_table_for(
        &self,
        dk: &DuckLakeConfig,
        table: &TelemetryTable,
    ) -> Result<()> {
        let (table_name, schema) = match table {
            TelemetryTable::Traces => ("traces", TraceTable::schema()),
            TelemetryTable::Logs => ("logs", OtlpLogsTable::schema()),
            TelemetryTable::Metrics => ("metrics", OtlpMetricsTable::schema()),
        };
        let arrow_schema = Arc::new(schema);
        let batch = RecordBatch::new_empty(arrow_schema);
        self.write_record_batches_internal_with_ducklake(dk, table_name, vec![batch])
            .await
    }

    pub async fn spans_schema(&self) -> Result<Arc<Schema>> {
        Ok(Arc::new(TraceTable::schema()))
    }

    pub async fn logs_schema(&self) -> Result<Arc<Schema>> {
        Ok(Arc::new(OtlpLogsTable::schema()))
    }

    pub async fn metrics_schema(&self) -> Result<Arc<Schema>> {
        Ok(Arc::new(OtlpMetricsTable::schema()))
    }

    async fn write_record_batches_internal(
        &self,
        table_name: &str,
        record_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        self.write_record_batches_internal_with_ducklake(&self.ducklake, table_name, record_batches)
            .await
    }

    async fn write_record_batches_internal_with_ducklake(
        &self,
        dk: &DuckLakeConfig,
        table_name: &str,
        record_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        if record_batches.is_empty() {
            return Ok(());
        }

        // Dropdown catalog from ingest batches **before** DuckLake INSERT (covers data inlining into Postgres).
        if table_name == "traces" {
            if let Some(ref cat) = self.dropdown_catalog {
                if self.config.dropdown_catalog.enabled {
                    if let Err(e) = cat.upsert_trace_batches(&record_batches).await {
                        warn!("dropdown catalog upsert failed (non-fatal): {}", e);
                    }
                }
            }
        }

        let temp_path = self.write_temp_parquet(table_name, &record_batches)?;
        let escaped_path = escape_sql_literal(temp_path.to_string_lossy().as_ref());
        let candidates = self.table_name_candidates_for(table_name, dk);
        let order_clause = self.insert_order_clause(table_name);
        let option_stmts: Vec<String> = candidates
            .iter()
            .flat_map(|qualified_table| {
                let scope = ducklake_set_option_scope_for_qualified(qualified_table);
                [
                    format!(
                        "CALL {}.set_option('target_file_size', '{}', {});",
                        self.ducklake.catalog_alias,
                        size_literal(self.config.maintenance.target_file_size_bytes),
                        scope
                    ),
                    format!(
                        "CALL {}.set_option('hive_file_pattern', true, {});",
                        self.ducklake.catalog_alias, scope
                    ),
                ]
            })
            .collect();
        let pool = self.get_or_create_pool(dk)?;
        let write_result = tokio::task::spawn_blocking(move || {
            pool.with_conn(|conn| {
                let mut last_err: Option<anyhow::Error> = None;
                for (i, qualified_table) in candidates.iter().enumerate() {
                    let ddl = format!(
                        "CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM read_parquet('{path}') LIMIT 0;",
                        table = qualified_table,
                        path = escaped_path
                    );
                    let insert = format!(
                        "INSERT INTO {table} SELECT * FROM read_parquet('{path}') {order_clause};",
                        table = qualified_table,
                        path = escaped_path,
                        order_clause = order_clause,
                    );
                    conn.execute_batch("BEGIN TRANSACTION;")?;
                    match conn
                        .execute_batch(&ddl)
                        .and_then(|_| conn.execute_batch(&insert))
                    {
                        Ok(_) => {
                            conn.execute_batch("COMMIT;")?;
                            // Apply options for this table (two stmts per candidate).
                            let base = i * 2;
                            if let Some(stmt) = option_stmts.get(base) {
                                if let Err(err) = conn.execute_batch(stmt) {
                                    warn!("DuckLake table option optimization skipped: {}", err);
                                }
                            }
                            if let Some(stmt) = option_stmts.get(base + 1) {
                                if let Err(err) = conn.execute_batch(stmt) {
                                    warn!("DuckLake table option optimization skipped: {}", err);
                                }
                            }
                            return Ok(());
                        }
                        Err(err) => {
                            let _ = conn.execute_batch("ROLLBACK;");
                            last_err = Some(anyhow!(
                                "DuckLake write failed for {}: {}",
                                qualified_table,
                                err
                            ));
                        }
                    }
                }
                Err(last_err.unwrap_or_else(|| anyhow!("DuckLake write failed")))
            })
        })
        .await
        .map_err(|e| anyhow!("DuckLake writer blocking task join failed: {e}"))?;
        if write_result.is_err() {
            let _ = std::fs::remove_file(&temp_path);
            return write_result;
        }
        let _ = std::fs::remove_file(&temp_path);
        Ok(())
    }

    fn write_temp_parquet(&self, table_name: &str, batches: &[RecordBatch]) -> Result<PathBuf> {
        let base_dir = std::env::temp_dir().join("splake-ducklake");
        std::fs::create_dir_all(&base_dir)?;
        let temp_path = base_dir.join(format!(
            "{}-{}.parquet",
            table_name,
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let file = std::fs::File::create(&temp_path)?;
        let mut writer = ArrowWriter::try_new(
            file,
            batches[0].schema(),
            Some(WriterProperties::builder().build()),
        )?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.close()?;
        Ok(temp_path)
    }

    fn open_connection_for(&self, dk: &DuckLakeConfig) -> Result<Connection> {
        let conn =
            Connection::open_in_memory().map_err(|e| anyhow!("DuckDB open failed: {}", e))?;
        conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
        configure_object_store(&conn, &self.config, &dk.data_path)?;
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
        if dk.catalog_type == "postgres" {
            conn.execute_batch("INSTALL postgres; LOAD postgres;")?;
        }
        if dk.catalog_type == "sqlite" {
            conn.execute_batch("INSTALL sqlite; LOAD sqlite;")?;
        }
        Ok(conn)
    }

    fn attach_ducklake_for(&self, conn: &Connection, dk: &DuckLakeConfig) -> Result<()> {
        let attach_target = ducklake_attach_target(dk);
        self.prepare_local_ducklake_paths_for(&attach_target, dk)?;

        let options = ducklake_attach_options(dk);
        let sql = format!(
            "ATTACH 'ducklake:{target}' AS {alias} ({opts});",
            target = escape_sql_literal(&attach_target),
            alias = dk.catalog_alias,
            opts = options.join(", ")
        );
        match conn.execute_batch(&sql) {
            Ok(()) => Ok(()),
            Err(err) => {
                let message = err.to_string();
                if catalog_is_attached(conn, &dk.catalog_alias) {
                    return Ok(());
                }
                // Writer-pool bootstrap can race DuckLake metadata CREATE TABLE on the same
                // Postgres schema. Retry once after the first connection finishes initializing.
                let retryable = message.to_lowercase().contains("already exists")
                    || message.contains("ducklake_metadata");
                if retryable {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    match conn.execute_batch(&sql) {
                        Ok(()) => return Ok(()),
                        Err(err2) if catalog_is_attached(conn, &dk.catalog_alias) => return Ok(()),
                        Err(err2) => {
                            return Err(anyhow!(
                                "DuckLake attach failed after retry: {err2} (first: {message})"
                            ));
                        }
                    }
                }
                Err(anyhow!("DuckLake attach failed: {message}"))
            }
        }
    }

    fn prepare_local_ducklake_paths_for(
        &self,
        attach_target: &str,
        dk: &DuckLakeConfig,
    ) -> Result<()> {
        if dk.catalog_type == "sqlite" {
            let raw = attach_target
                .strip_prefix("sqlite:")
                .unwrap_or(attach_target);
            let metadata_path = PathBuf::from(raw);
            if let Some(parent) = metadata_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
        }
        // Local file DATA_PATH must exist for both sqlite and postgres catalogs.
        if !dk.data_path.contains("://") {
            std::fs::create_dir_all(&dk.data_path)?;
        }
        Ok(())
    }

    fn ensure_schema_for(&self, conn: &Connection, dk: &DuckLakeConfig) -> Result<()> {
        if dk.metadata_schema == "main" {
            return Ok(());
        }
        conn.execute_batch(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{};",
            dk.catalog_alias, dk.metadata_schema
        ))?;
        Ok(())
    }

    fn qualified_table_name(&self, table_name: &str) -> String {
        self.qualified_table_name_for(table_name, &self.ducklake)
    }

    fn qualified_table_name_for(&self, table_name: &str, dk: &DuckLakeConfig) -> String {
        ducklake_qualified_table_name(dk, table_name)
    }

    fn table_name_candidates_for(&self, table_name: &str, dk: &DuckLakeConfig) -> Vec<String> {
        // Prefer catalog.schema.table when metadata lives in a non-main schema; fall back to
        // catalog.table if the engine rejects the three-part name. set_option scope must match
        // whichever form succeeds (see apply_table_options).
        vec![
            self.qualified_table_name_for(table_name, dk),
            format!("{}.{}", dk.catalog_alias, table_name),
        ]
    }

    #[allow(dead_code)]
    fn apply_table_options(&self, conn: &Connection, qualified_table: &str) {
        // Scope must match how the table was created (`catalog.table` vs `catalog.schema.table`).
        // TODO(bill): Avoid calling set_option on every write/maintenance cycle. Persist and compare
        // desired option values (e.g. in-memory cache or metadata bootstrap marker), then only update
        // when changed to reduce DuckLake metadata contention on Postgres.
        let scope = ducklake_set_option_scope_for_qualified(qualified_table);
        let stmts = [
            format!(
                "CALL {}.set_option('target_file_size', '{}', {});",
                self.ducklake.catalog_alias,
                size_literal(self.config.maintenance.target_file_size_bytes),
                scope
            ),
            format!(
                "CALL {}.set_option('hive_file_pattern', true, {});",
                self.ducklake.catalog_alias, scope
            ),
        ];
        for stmt in stmts {
            if let Err(err) = conn.execute_batch(&stmt) {
                warn!("DuckLake table option optimization skipped: {}", err);
            }
        }
    }

    fn insert_order_clause(&self, table_name: &str) -> &'static str {
        match table_name {
            "traces" => "ORDER BY record_date, app_id, session_id, timestamp",
            "logs" => "ORDER BY record_date, session_id, timestamp",
            "metrics" => "ORDER BY record_date, metric_name, timestamp",
            _ => "",
        }
    }

    fn reset_tables_for_dev(&self, conn: &Connection) -> Result<()> {
        for table in ["traces", "logs", "metrics"] {
            let qualified = self.qualified_table_name(table);
            conn.execute_batch(&format!("DROP TABLE IF EXISTS {qualified};"))?;
            conn.execute_batch(&format!(
                "DROP TABLE IF EXISTS {}.{};",
                self.ducklake.catalog_alias, table
            ))?;
        }
        info!("DuckLake tables reset because SPLAKE_RESET_DUCKLAKE=1");
        Ok(())
    }
}

fn escape_sql_literal(input: &str) -> String {
    input.replace('\'', "''")
}

fn catalog_is_attached(conn: &Connection, alias: &str) -> bool {
    let sql = format!(
        "SELECT 1 FROM duckdb_databases() WHERE database_name = '{}' LIMIT 1;",
        escape_sql_literal(alias)
    );
    conn.query_row(&sql, [], |_| Ok(())).is_ok()
}

/// Pin DuckLake extension conflict-retry defaults (official concurrent-write mechanism).
fn apply_ducklake_retry_settings(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "SET ducklake_max_retry_count = 10;\n\
         SET ducklake_retry_backoff = 1.5;\n\
         SET ducklake_retry_wait_ms = 100;",
    )?;
    Ok(())
}

pub(crate) fn ducklake_attach_target(dk: &DuckLakeConfig) -> String {
    match dk.catalog_type.as_str() {
        "postgres" => {
            if dk.metadata_path.starts_with("postgres:") {
                dk.metadata_path.clone()
            } else {
                format!("postgres:{}", dk.metadata_path)
            }
        }
        "sqlite" => {
            if dk.metadata_path.starts_with("sqlite:") {
                dk.metadata_path.clone()
            } else {
                format!("sqlite:{}", dk.metadata_path)
            }
        }
        _ => dk.metadata_path.clone(),
    }
}

/// ATTACH options shared by writer / query / compaction.
pub(crate) fn ducklake_attach_options(dk: &DuckLakeConfig) -> Vec<String> {
    let mut options = vec![format!("DATA_PATH '{}'", escape_sql_literal(&dk.data_path))];
    if dk.catalog_type == "postgres" && dk.metadata_schema != "main" {
        let schema = escape_sql_literal(&dk.metadata_schema);
        options.push(format!("METADATA_SCHEMA '{}'", schema));
        options.push(format!("META_SCHEMA '{}'", schema));
    }
    // Official SQLite multi-client guidance: WAL + busy timeout (DuckLake / sqlite extension).
    // 5s absorbs concurrent query-worker ATTACH / snapshot races better than 500ms.
    if dk.catalog_type == "sqlite" {
        options.push("META_JOURNAL_MODE 'WAL'".to_string());
        options.push("META_BUSY_TIMEOUT 5000".to_string());
    }
    if let Some(limit) = dk.data_inlining_row_limit {
        options.push(format!("DATA_INLINING_ROW_LIMIT {}", limit));
    }
    options
}

fn quote_duckdb_ident(input: &str) -> String {
    format!("\"{}\"", input.replace('"', "\"\""))
}

/// Fully qualified DuckLake table name used for CREATE / INSERT (`catalog.table` when
/// `metadata_schema` is `main`, else `catalog.metadata_schema.table`).
pub(crate) fn ducklake_qualified_table_name(cfg: &DuckLakeConfig, bare_table: &str) -> String {
    if cfg.metadata_schema == "main" {
        format!("{}.{}", cfg.catalog_alias, bare_table)
    } else {
        format!(
            "{}.{}.{}",
            cfg.catalog_alias, cfg.metadata_schema, bare_table
        )
    }
}

/// Scoping clause for `CALL <catalog>.set_option(...)` matching a qualified table name.
/// Two-part `catalog.table` → `table_name` only; three-part → `schema` + `table_name`.
pub(crate) fn ducklake_set_option_scope_for_qualified(qualified_table: &str) -> String {
    let parts: Vec<&str> = qualified_table.split('.').collect();
    match parts.len() {
        3 => {
            let s = escape_sql_literal(parts[1]);
            let t = escape_sql_literal(parts[2]);
            format!("schema => '{s}', table_name => '{t}'")
        }
        2 => {
            let t = escape_sql_literal(parts[1]);
            format!("table_name => '{t}'")
        }
        _ => {
            let t = escape_sql_literal(parts.last().copied().unwrap_or(""));
            format!("table_name => '{t}'")
        }
    }
}

fn size_literal(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    const GB: usize = 1024 * MB;
    if bytes >= GB && bytes.is_multiple_of(GB) {
        format!("{}GB", bytes / GB)
    } else if bytes >= MB && bytes.is_multiple_of(MB) {
        format!("{}MB", bytes / MB)
    } else if bytes >= KB && bytes.is_multiple_of(KB) {
        format!("{}KB", bytes / KB)
    } else {
        warn!(
            "target_file_size_bytes={} is not power-of-1024 aligned; using byte literal",
            bytes
        );
        format!("{}B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_option_scope_matches_qualified_table_shape() {
        assert_eq!(
            ducklake_set_option_scope_for_qualified("softprobe.traces"),
            "table_name => 'traces'"
        );
        assert_eq!(
            ducklake_set_option_scope_for_qualified("softprobe.tenant_a.traces"),
            "schema => 'tenant_a', table_name => 'traces'"
        );
    }

    #[tokio::test]
    async fn spans_schema_has_no_process_global_promoted_columns() {
        let config = Config::default();
        let writer = DuckLakeWriter {
            config,
            ducklake: DuckLakeConfig {
                catalog_type: "sqlite".to_string(),
                metadata_path: ":memory:".to_string(),
                data_path: "/tmp/unused".to_string(),
                catalog_alias: "softprobe".to_string(),
                metadata_schema: "main".to_string(),
                data_inlining_row_limit: None,
                writer_pool_size: 1,
            },
            dropdown_catalog: None,
            tenant_ducklake: None,
            scope_bound: false,
            writer_pools: Mutex::new(HashMap::new()),
        };

        let schema = writer.spans_schema().await.expect("schema");
        assert!(
            schema.field_with_name("division_name").is_err(),
            "promoted telemetry columns come from runtime-scoped promotion apply, not process config"
        );
    }
}
