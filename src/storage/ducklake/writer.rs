use crate::config::{Config, DuckLakeConfig};
use crate::promotion::TelemetryTable;
use crate::runtime_engine::{DuckLakeScope, DuckLakeScopeResolver};
use crate::storage::schema::tables::{
    OtlpLogsTable, OtlpMetricsTable, ScoreConfigTable, ScoreTable, TraceTable,
};
use crate::storage::schema::variant::parquet_select_with_variant_casts;
use ::arrow::datatypes::Schema;
use ::arrow::record_batch::RecordBatch;
use anyhow::{anyhow, Result};
use duckdb::Connection;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

use super::attach::{
    apply_ducklake_retry_settings, catalog_is_attached, ducklake_attach_options,
    ducklake_attach_target, ducklake_qualified_table_name, ducklake_set_option_scope_for_qualified,
    prepare_local_ducklake_paths,
};
use super::object_store::configure_object_store;
use super::util::{
    ensure_variant_column_types, escape_sql_literal, size_literal, WriteAttemptError,
};

pub(super) struct WriterPool {
    conns: Vec<Mutex<Connection>>,
    next: AtomicUsize,
}

impl WriterPool {
    pub(super) fn with_conn<R>(&self, f: impl FnOnce(&Connection) -> Result<R>) -> Result<R> {
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
    pub(super) config: Config,
    pub(super) ducklake: DuckLakeConfig,
    pub(super) dropdown_catalog: Option<std::sync::Arc<crate::catalog::DropdownCatalog>>,
    /// When set with `catalog_type = postgres`, commits route to per-tenant metadata schemas.
    pub(super) tenant_ducklake: Option<DuckLakeScopeResolver>,
    /// When true, this writer is permanently bound to `ducklake.metadata_schema` / `data_path`
    /// (built via [`IngestPipeline::build_tenant_storage`]). When false, postgres writers with a
    /// registry resolver route each batch by `span.tenant_id` even if config carries a non-main
    /// registry schema name.
    pub(super) scope_bound: bool,
    /// Per catalog-scope pools of reused ATTACH'd DuckDB connections.
    pub(super) writer_pools: Mutex<HashMap<String, Arc<WriterPool>>>,
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

    pub(super) async fn new_inner(
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

    pub(super) fn initialize_catalog(&self) -> Result<()> {
        self.with_attached_conn(&self.ducklake, |conn| {
            if std::env::var("SPLAKE_RESET_DUCKLAKE").ok().as_deref() == Some("1") {
                self.reset_tables_for_dev(conn)?;
            }
            Ok(())
        })
    }

    pub(super) fn conn_cache_key(dk: &DuckLakeConfig) -> String {
        format!(
            "{}|{}|{}|{}",
            dk.catalog_type, dk.metadata_path, dk.metadata_schema, dk.data_path
        )
    }

    pub(super) fn get_or_create_pool(&self, dk: &DuckLakeConfig) -> Result<Arc<WriterPool>> {
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
    pub(super) fn with_attached_conn<R>(
        &self,
        dk: &DuckLakeConfig,
        f: impl FnOnce(&Connection) -> Result<R>,
    ) -> Result<R> {
        let pool = self.get_or_create_pool(dk)?;
        pool.with_conn(f)
    }

    pub(super) fn use_tenant_scoped_ducklake(&self) -> bool {
        self.tenant_ducklake.is_some() && self.ducklake.catalog_type == "postgres"
    }

    pub(super) fn effective_ducklake(&self, scope: &DuckLakeScope) -> DuckLakeConfig {
        let mut dk = self.ducklake.clone();
        dk.metadata_schema = scope.metadata_schema.clone();
        dk.data_path = scope.data_path.clone();
        dk
    }

    pub(super) fn tenant_bound_scope(&self) -> Option<DuckLakeScope> {
        if !self.scope_bound || self.ducklake.catalog_type != "postgres" {
            return None;
        }
        Some(DuckLakeScope {
            metadata_schema: self.ducklake.metadata_schema.clone(),
            data_path: self.ducklake.data_path.clone(),
        })
    }

    pub(super) async fn ensure_telemetry_table_for(
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

    pub(super) async fn write_record_batches_internal(
        &self,
        table_name: &str,
        record_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        self.write_record_batches_internal_with_ducklake(&self.ducklake, table_name, record_batches)
            .await
    }

    pub(super) async fn write_record_batches_internal_with_ducklake(
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
        let select_prefix = parquet_select_with_variant_casts(table_name);
        let variant_table_name = table_name.to_string();
        let deduplicate_scores =
            table_name == ScoreTable::table_name() || table_name == ScoreConfigTable::table_name();
        let dedupe_id_column: Option<&'static str> = if !deduplicate_scores {
            None
        } else if table_name == ScoreConfigTable::table_name() {
            Some("config_id")
        } else {
            Some("score_id")
        };
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
                        "CREATE TABLE IF NOT EXISTS {table} AS {select} FROM read_parquet('{path}') LIMIT 0;",
                        table = qualified_table,
                        select = select_prefix,
                        path = escaped_path
                    );
                    // BY NAME keeps ingest shrink-safe: physical promoted columns absent from the
                    // active manifest (and thus from this Parquet batch) are filled with NULL
                    // instead of failing a positional column-count match.
                    let insert = if let Some(id_column) = dedupe_id_column {
                        format!(
                            "INSERT INTO {table} BY NAME
                             SELECT incoming.* FROM (
                               {select} FROM read_parquet('{path}')
                             ) incoming
                             WHERE NOT EXISTS (
                               SELECT 1 FROM {table} existing
                               WHERE existing.{id_column} = incoming.{id_column}
                             )
                             {order_clause};",
                            table = qualified_table,
                            select = select_prefix,
                            path = escaped_path,
                            order_clause = order_clause,
                            id_column = id_column,
                        )
                    } else {
                        format!(
                            "INSERT INTO {table} BY NAME {select} FROM read_parquet('{path}') {order_clause};",
                            table = qualified_table,
                            select = select_prefix,
                            path = escaped_path,
                            order_clause = order_clause,
                        )
                    };
                    conn.execute_batch("BEGIN TRANSACTION;")?;
                    let write_ok = (|| -> Result<(), WriteAttemptError> {
                        conn.execute_batch(&ddl).map_err(|e| {
                            WriteAttemptError::Retryable(anyhow!("CREATE TABLE failed: {e}"))
                        })?;
                        ensure_variant_column_types(conn, qualified_table, &variant_table_name)
                            .map_err(WriteAttemptError::from_variant_guard)?;
                        conn.execute_batch(&insert).map_err(|e| {
                            WriteAttemptError::Retryable(anyhow!("INSERT failed: {e}"))
                        })?;
                        Ok(())
                    })();
                    match write_ok {
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
                        Err(WriteAttemptError::Fatal(err)) => {
                            let _ = conn.execute_batch("ROLLBACK;");
                            // Legacy MAP / schema mismatch must not fall through to catalog.table —
                            // that would create a second VARIANT table while queries still read the
                            // unchanged three-part legacy table.
                            return Err(err);
                        }
                        Err(WriteAttemptError::Retryable(err)) => {
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

    pub(super) fn write_temp_parquet(
        &self,
        table_name: &str,
        batches: &[RecordBatch],
    ) -> Result<PathBuf> {
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

    pub(super) fn open_connection_for(&self, dk: &DuckLakeConfig) -> Result<Connection> {
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

    pub(super) fn attach_ducklake_for(&self, conn: &Connection, dk: &DuckLakeConfig) -> Result<()> {
        let attach_target = ducklake_attach_target(dk);
        prepare_local_ducklake_paths(dk, &attach_target)?;

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

    pub(super) fn ensure_schema_for(&self, conn: &Connection, dk: &DuckLakeConfig) -> Result<()> {
        if dk.metadata_schema == "main" {
            return Ok(());
        }
        conn.execute_batch(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{};",
            dk.catalog_alias, dk.metadata_schema
        ))?;
        Ok(())
    }

    pub(super) fn qualified_table_name(&self, table_name: &str) -> String {
        self.qualified_table_name_for(table_name, &self.ducklake)
    }

    pub(super) fn qualified_table_name_for(&self, table_name: &str, dk: &DuckLakeConfig) -> String {
        ducklake_qualified_table_name(dk, table_name)
    }

    pub(super) fn table_name_candidates_for(
        &self,
        table_name: &str,
        dk: &DuckLakeConfig,
    ) -> Vec<String> {
        // Prefer catalog.schema.table when metadata lives in a non-main schema; fall back to
        // catalog.table if the engine rejects the three-part name. set_option scope must match
        // whichever form succeeds (see write_record_batches_internal_with_ducklake).
        vec![
            self.qualified_table_name_for(table_name, dk),
            format!("{}.{}", dk.catalog_alias, table_name),
        ]
    }

    pub(super) fn insert_order_clause(&self, table_name: &str) -> &'static str {
        match table_name {
            "traces" => "ORDER BY record_date, app_id, session_id, timestamp",
            "logs" => "ORDER BY record_date, session_id, timestamp",
            "metrics" => "ORDER BY record_date, metric_name, timestamp",
            "scores" => "ORDER BY record_date, name, timestamp",
            _ => "",
        }
    }

    pub(super) fn reset_tables_for_dev(&self, conn: &Connection) -> Result<()> {
        for table in [
            "traces",
            "logs",
            "metrics",
            "scores",
            ScoreConfigTable::table_name(),
        ] {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DuckLakeConfig};

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
