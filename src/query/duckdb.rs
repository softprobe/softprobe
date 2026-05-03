use crate::config::Config;
use crate::query::cache::CacheSettings;
use crate::storage::ducklake::ducklake_qualified_table_name;
use crate::storage::iceberg::arrow::{
    logs_to_record_batch, metrics_to_record_batch, spans_to_record_batch,
};
use crate::storage::TieredStorage;
use anyhow::{anyhow, Result};
use base64::Engine;
use chrono::{DateTime, Utc};
use duckdb::types::Value as DuckValue;
use duckdb::{Connection, ToSql};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

pub struct DuckDBQueryEngine {
    _shared_connection: Arc<Mutex<Connection>>,
    workers: Arc<Vec<WorkerHandle>>,
    next_worker: AtomicUsize,
}

const CATALOG_ALIAS: &str = "iceberg_catalog";
const DUCKDB_SESSION_INIT_SQL: &str = include_str!("sql/duckdb_session_init.sql");

/// When a DuckLake catalog is attached, DuckDB treats identifiers containing substrings like
/// `union_spans` / `committed_spans` as special. That breaks `TEMP VIEW`s and even derived-table
/// aliases over native DuckLake tables. The `spb_` prefix is also reserved by DuckLake in this
/// DuckDB build. Rewrite the public `union_*` / `committed_*` surface to neutral `tm_*` names.
fn telemetry_view_names(kind: &str) -> (&'static str, &'static str, &'static str, &'static str) {
    match kind {
        "logs" => (
            "tm_cq_log",
            "tm_buf_log",
            "tm_all_log",
            "tm_icb_log",
        ),
        "metrics" => (
            "tm_cq_metric",
            "tm_buf_metric",
            "tm_all_metric",
            "tm_icb_metric",
        ),
        _ => (
            "tm_cq_span",
            "tm_buf_span",
            "tm_all_span",
            "tm_icb_span",
        ),
    }
}

fn is_sql_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn replace_standalone_ident(s: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(s.len().saturating_add(32));
    let mut last = 0;
    for (i, _) in s.match_indices(from) {
        let before_ok = s[..i]
            .chars()
            .next_back()
            .map(|c| !is_sql_ident_char(c))
            .unwrap_or(true);
        let end = i + from.len();
        let after_ok = s[end..]
            .chars()
            .next()
            .map(|c| !is_sql_ident_char(c))
            .unwrap_or(true);
        if before_ok && after_ok {
            out.push_str(&s[last..i]);
            out.push_str(to);
            last = end;
        }
    }
    out.push_str(&s[last..]);
    out
}

fn rewrite_reserved_telemetry_view_names(sql: &str) -> String {
    let mut s = sql.to_string();
    // Metrics/logs before spans so `union_metrics` is not partially consumed.
    const PAIRS: &[(&str, &str)] = &[
        ("union_metrics", "tm_all_metric"),
        ("committed_metrics", "tm_cq_metric"),
        ("buffer_metrics", "tm_buf_metric"),
        ("iceberg_metrics", "tm_icb_metric"),
        ("staged_metrics", "tm_stg_metric"),
        ("union_logs", "tm_all_log"),
        ("committed_logs", "tm_cq_log"),
        ("buffer_logs", "tm_buf_log"),
        ("iceberg_logs", "tm_icb_log"),
        ("staged_logs", "tm_stg_log"),
        ("union_spans", "tm_all_span"),
        ("committed_spans", "tm_cq_span"),
        ("buffer_spans", "tm_buf_span"),
        ("iceberg_spans", "tm_icb_span"),
        ("staged_spans", "tm_stg_span"),
    ];
    for &(from, to) in PAIRS {
        s = replace_standalone_ident(&s, from, to);
    }
    s
}

use once_cell::sync::Lazy;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;

#[derive(Default)]
struct ViewCounters {
    iceberg: AtomicU64,
    staged: AtomicU64,
    union_view: AtomicU64,
}

static VIEW_COUNTERS: Lazy<ViewCounters> = Lazy::new(ViewCounters::default);
static CACHE_HTTPFS_CONFIG_WARNED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
static PINNED_MODE_OPTION_WARNED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
#[derive(Debug, Clone)]
pub struct ViewCounterSnapshot {
    pub iceberg_recreates: u64,
    pub staged_recreates: u64,
    pub union_recreates: u64,
}

pub fn reset_view_counters() {
    VIEW_COUNTERS.iceberg.store(0, Ordering::Relaxed);
    VIEW_COUNTERS.staged.store(0, Ordering::Relaxed);
    VIEW_COUNTERS.union_view.store(0, Ordering::Relaxed);
}

pub fn view_counters_snapshot() -> ViewCounterSnapshot {
    ViewCounterSnapshot {
        iceberg_recreates: VIEW_COUNTERS.iceberg.load(Ordering::Relaxed),
        staged_recreates: VIEW_COUNTERS.staged.load(Ordering::Relaxed),
        union_recreates: VIEW_COUNTERS.union_view.load(Ordering::Relaxed),
    }
}

struct WorkerHandle {
    sender: mpsc::Sender<QueryRequest>,
}

struct QueryRequest {
    sql: String,
    respond_to: oneshot::Sender<Result<QueryResult>>,
}

struct ConnectionState {
    conn: Connection,
    prepared_kinds: HashSet<String>,
    staged_signatures: HashMap<String, String>,
    iceberg_sources: HashMap<String, IcebergSource>,
    iceberg_signatures: HashMap<String, String>,
    tiered_storage: Arc<dyn TieredStorage>,
    /// Last `TieredStorage::catalog_write_generation()` applied after reattach for this worker.
    last_seen_catalog_write_generation: u64,
    cache_httpfs_wrap_supported: bool,
    cache_httpfs_wrapped_s3: bool,
    cache_httpfs_wrapped_httpfs: bool,
}

#[derive(Clone)]
enum IcebergSource {
    Pinned {
        metadata_path: String,
        snapshot_id: Option<i64>,
        compression: Option<String>,
        signature: String,
        data_files_path: Option<String>,
    },
    Catalog,
    ScanUri(String),
    Stub(String),
}

struct PinnedMetadata {
    metadata_path: String,
    snapshot_id: Option<i64>,
    compression: Option<String>,
    signature: String,
    data_files_path: Option<String>,
}

#[derive(Clone)]
struct DuckDBCore {
    config: Config,
    cache: CacheSettings,
    tiered_storage: Arc<dyn TieredStorage>,
    runtime_handle: tokio::runtime::Handle,
}

/// Query result containing columns and rows
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub row_count: usize,
}

impl DuckDBQueryEngine {
    pub async fn new(config: &Config, tiered_storage: Arc<dyn TieredStorage>) -> Result<Self> {
        let core = DuckDBCore {
            config: config.clone(),
            cache: CacheSettings::new(config),
            tiered_storage: tiered_storage.clone(),
            runtime_handle: tokio::runtime::Handle::current(),
        };
        // Install extensions once to ensure they're available
        let temp_conn = core.open_connection()?;
        core.install_extensions(&temp_conn)?;
        drop(temp_conn); // Extensions are installed globally, connection no longer needed

        let worker_count = std::cmp::max(1, config.duckdb.max_connections);
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (tx, mut rx) = mpsc::channel::<QueryRequest>(32);
            let core = core.clone();
            std::thread::spawn(move || {
                // Each worker gets its own independent connection (not cloned)
                let connection = match core.open_connection() {
                    Ok(conn) => conn,
                    Err(err) => {
                        warn!("DuckDB worker failed to open connection: {}", err);
                        return;
                    }
                };
                let mut state = match core.init_connection_state_with(connection) {
                    Ok(state) => state,
                    Err(err) => {
                        warn!("DuckDB worker failed to initialize: {}", err);
                        return;
                    }
                };
                while let Some(request) = rx.blocking_recv() {
                    let result = core.execute_query_on_state(&mut state, &request.sql);
                    let _ = request.respond_to.send(result);
                }
            });
            workers.push(WorkerHandle { sender: tx });
        }

        // Keep a dummy connection for the _shared_connection field (for compatibility)
        let dummy_conn = core.open_connection()?;

        Ok(Self {
            _shared_connection: Arc::new(Mutex::new(dummy_conn)),
            workers: Arc::new(workers),
            next_worker: AtomicUsize::new(0),
        })
    }

    /// Execute arbitrary SQL query and return results as JSON
    /// Used by Grafana SQL API endpoint
    pub async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        let index = self.next_worker.fetch_add(1, Ordering::Relaxed);
        let worker = &self.workers[index % self.workers.len()];
        let (tx, rx) = oneshot::channel();
        let request = QueryRequest {
            sql: query.to_string(),
            respond_to: tx,
        };
        worker
            .sender
            .send(request)
            .await
            .map_err(|_| anyhow!("DuckDB worker channel closed"))?;
        rx.await
            .map_err(|_| anyhow!("DuckDB worker dropped response"))?
    }
}

impl DuckDBCore {
    fn open_connection(&self) -> Result<Connection> {
        Connection::open_in_memory().map_err(|err| anyhow!("DuckDB open failed: {}", err))
    }

    fn cache_dir(&self) -> Option<&Path> {
        self.config
            .ingest_engine
            .cache_dir
            .as_deref()
            .map(Path::new)
    }

    fn install_extensions(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch("INSTALL httpfs;")?;
        // Keep iceberg installed for backward-compatible fallback paths.
        conn.execute_batch("INSTALL iceberg;")?;
        // DuckLake is the primary committed storage path.
        conn.execute_batch("INSTALL ducklake;")?;
        if self.ducklake_config().catalog_type == "postgres" {
            conn.execute_batch("INSTALL postgres;")?;
        }
        if self.ducklake_config().catalog_type == "sqlite" {
            conn.execute_batch("INSTALL sqlite;")?;
        }
        Ok(())
    }

    fn init_connection_state_with(&self, conn: Connection) -> Result<ConnectionState> {
        self.configure_connection(&conn)?;
        self.attach_catalog_if_needed(&conn)?;
        Ok(ConnectionState {
            conn,
            prepared_kinds: HashSet::new(),
            staged_signatures: HashMap::new(),
            iceberg_sources: HashMap::new(),
            iceberg_signatures: HashMap::new(),
            tiered_storage: self.tiered_storage.clone(),
            last_seen_catalog_write_generation: 0,
            cache_httpfs_wrap_supported: true,
            cache_httpfs_wrapped_s3: false,
            cache_httpfs_wrapped_httpfs: false,
        })
    }

    fn execute_query_on_state(
        &self,
        state: &mut ConnectionState,
        query: &str,
    ) -> Result<QueryResult> {
        if self.config.ducklake.is_some() {
            let gen = state.tiered_storage.catalog_write_generation();
            if gen != state.last_seen_catalog_write_generation {
                self.force_reattach_catalog(&state.conn)?;
                state.prepared_kinds.clear();
                state.staged_signatures.clear();
                state.iceberg_sources.clear();
                state.iceberg_signatures.clear();
                state.last_seen_catalog_write_generation = gen;
            }
        }

        let query_prep = rewrite_reserved_telemetry_view_names(query);
        let query_run = if self.use_attached_catalog() {
            self.ducklake_inline_sql(&query_prep)
        } else {
            query_prep.clone()
        };
        if std::env::var("SOFTPROBE_LOG_SQL").ok().as_deref() == Some("1") {
            eprintln!("SOFTPROBE_LOG_SQL prep={query_prep}\nSOFTPROBE_LOG_SQL run={query_run}");
        }
        let diag = std::env::var("PERF_DIAG").ok().as_deref() == Some("1");
        let run_once = |state: &mut ConnectionState| -> Result<QueryResult> {
            let prepare_start = std::time::Instant::now();
            self.prepare_union_views_for_query(state, query_prep.as_str())?;
            self.try_wrap_cache_httpfs_filesystems(state);
            if diag {
                let elapsed = prepare_start.elapsed();
                println!("DIAG prepare_union_views: {:?}", elapsed);
            }

            let query_start = std::time::Instant::now();
            let mut stmt = state.conn.prepare(query_run.as_str())?;
            let mut query_rows = stmt.query([])?;
            let column_names = query_rows
                .as_ref()
                .map(|stmt_ref| {
                    (0..stmt_ref.column_count())
                        .filter_map(|idx| {
                            stmt_ref.column_name(idx).ok().map(|name| name.to_string())
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let mut rows = Vec::new();
            while let Some(row) = query_rows.next()? {
                let mut values = Vec::with_capacity(column_names.len());
                for idx in 0..column_names.len() {
                    let value: DuckValue = row.get(idx)?;
                    values.push(duck_value_to_json(value));
                }
                rows.push(values);
            }

            let result = QueryResult {
                columns: column_names,
                row_count: rows.len(),
                rows,
            };
            if diag {
                println!("DIAG execute_query: {:?}", query_start.elapsed());
            }
            Ok(result)
        };

        match run_once(state) {
            Ok(result) => Ok(result),
            Err(err) => {
                let message = err.to_string();
                if message.contains("__ducklake_metadata_")
                    && message.contains("does not exist")
                    && self.config.ducklake.is_some()
                {
                    warn!(
                        "DuckLake metadata catalog missing during query; force reattaching catalog and retrying once"
                    );
                    self.force_reattach_catalog(&state.conn)?;
                    state.prepared_kinds.clear();
                    state.staged_signatures.clear();
                    state.iceberg_sources.clear();
                    state.iceberg_signatures.clear();
                    return run_once(state);
                }
                Err(err)
            }
        }
    }

    fn try_wrap_cache_httpfs_filesystems(&self, state: &mut ConnectionState) {
        if self.cache.cache_dir.is_none() {
            return;
        }
        if std::env::var("PERF_DISABLE_CACHE_HTTPFS").ok().as_deref() == Some("1") {
            return;
        }
        if !state.cache_httpfs_wrap_supported {
            return;
        }

        if !state.cache_httpfs_wrapped_s3 {
            match state
                .conn
                .execute("SELECT cache_httpfs_wrap_cache_filesystem('s3');", [])
            {
                Ok(_) => {
                    state.cache_httpfs_wrapped_s3 = true;
                    info!("cache_httpfs wrapped filesystem: s3");
                }
                Err(err) => {
                    let message = err.to_string();
                    if message.contains("already wrapped") {
                        state.cache_httpfs_wrapped_s3 = true;
                        info!("cache_httpfs wrapped filesystem: s3 (already wrapped)");
                    } else if message.contains("hasn't been registered yet") {
                        // Will retry later once filesystem is registered by real usage.
                    } else if message.contains("does not exist")
                        || message.contains("Catalog Error")
                            && message.contains("cache_httpfs_wrap_cache_filesystem")
                    {
                        state.cache_httpfs_wrap_supported = false;
                        warn!("cache_httpfs wrap function not available in this DuckDB build; disk cache will remain unused");
                    }
                }
            }
        }

        if !state.cache_httpfs_wrapped_httpfs && state.cache_httpfs_wrap_supported {
            match state
                .conn
                .execute("SELECT cache_httpfs_wrap_cache_filesystem('httpfs');", [])
            {
                Ok(_) => {
                    state.cache_httpfs_wrapped_httpfs = true;
                    info!("cache_httpfs wrapped filesystem: httpfs");
                }
                Err(err) => {
                    let message = err.to_string();
                    if message.contains("already wrapped") {
                        state.cache_httpfs_wrapped_httpfs = true;
                        info!("cache_httpfs wrapped filesystem: httpfs (already wrapped)");
                    } else if message.contains("hasn't been registered yet") {
                        // Will retry later once filesystem is registered by real usage.
                    } else if message.contains("does not exist")
                        || message.contains("Catalog Error")
                            && message.contains("cache_httpfs_wrap_cache_filesystem")
                    {
                        state.cache_httpfs_wrap_supported = false;
                        warn!("cache_httpfs wrap function not available in this DuckDB build; disk cache will remain unused");
                    }
                }
            }
        }
    }

    fn configure_connection(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch(DUCKDB_SESSION_INIT_SQL)?;
        // Extension loading is connection-scoped. Match interactive production query behavior
        // by explicitly loading the DuckLake backend extension in each worker connection.
        match self.ducklake_config().catalog_type.as_str() {
            "postgres" => conn.execute_batch("LOAD postgres;")?,
            "sqlite" => conn.execute_batch("LOAD sqlite;")?,
            _ => {}
        }
        let dk = self.config.ducklake_or_default();
        crate::storage::ducklake::configure_httpfs_gcs_for_data_path(conn, &dk.data_path)?;

        // 1) Native object cache for parsed objects/metadata (best-effort; depends on DuckDB build).
        if let Err(err) = conn.execute("SET enable_object_cache = true;", []) {
            warn!("Failed to enable DuckDB object cache: {}", err);
        }

        if let Some(endpoint) = self.config.s3.endpoint.as_ref() {
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
        // Fetch credentials from config or instance metadata
        let (access_key, secret_key, session_token) =
            if let Some(access_key) = self.config.s3.access_key_id.as_ref() {
                // Use explicit credentials from config
                (
                    Some(access_key.clone()),
                    self.config.s3.secret_access_key.clone(),
                    None,
                )
            } else {
                // Try to fetch from instance metadata (IAM role)
                fetch_instance_metadata_credentials().unwrap_or((None, None, None))
            };

        if let Some(access_key) = access_key.as_ref() {
            conn.execute("SET s3_access_key_id = ?;", [access_key as &dyn ToSql])?;
        }
        if let Some(secret_key) = secret_key.as_ref() {
            conn.execute("SET s3_secret_access_key = ?;", [secret_key as &dyn ToSql])?;
        }
        if let Some(session_token) = session_token.as_ref() {
            conn.execute("SET s3_session_token = ?;", [session_token as &dyn ToSql])?;
        }
        conn.execute(
            "SET s3_region = ?;",
            [&self.config.storage.s3_region as &dyn ToSql],
        )?;

        // 2) Native external file cache for raw bytes (in-memory). This complements cache_httpfs'
        // on-disk persistence; we disable cache_httpfs in-memory caching to avoid double-caching.
        if let Err(err) = conn.execute("SET enable_external_file_cache = true;", []) {
            warn!("Failed to enable DuckDB external file cache: {}", err);
        }
        if let Err(err) = conn.execute("SET enable_http_metadata_cache = true;", []) {
            warn!("Failed to enable DuckDB HTTP metadata cache: {}", err);
        }
        if let Err(err) = conn.execute("SET parquet_metadata_cache = true;", []) {
            warn!("Failed to enable DuckDB parquet metadata cache: {}", err);
        }
        if let Err(err) = conn.execute("SET experimental_metadata_reuse = true;", []) {
            warn!("Failed to enable DuckDB metadata reuse: {}", err);
        }
        if self.cache.cache_dir.is_some()
            && std::env::var("PERF_DISABLE_CACHE_HTTPFS").ok().as_deref() == Some("1")
        {
            return Ok(());
        }

        if let Err(err) = self.cache.configure(conn) {
            // Avoid log spam when DuckDB build doesn't support wrapping 'httpfs' or cache_httpfs knobs.
            if !CACHE_HTTPFS_CONFIG_WARNED.swap(true, Ordering::Relaxed) {
                warn!("Failed to configure cache_httpfs: {}", err);
            }
        }

        Ok(())
    }

    fn prepare_union_views_for_query(
        &self,
        state: &mut ConnectionState,
        query: &str,
    ) -> Result<()> {
        let sql = query.to_lowercase();

        let uses_spans = sql.contains("tm_all_span")
            || sql.contains("tm_cq_span")
            || sql.contains("tm_icb_span")
            || sql.contains("tm_stg_span")
            || sql.contains("tm_buf_span");
        if uses_spans {
            let ducklake_need_buffer = self.config.ducklake.is_none()
                || sql.contains("tm_buf_span")
                || sql.contains("tm_all_span");
            self.prepare_union_view(state, "spans", "traces", ducklake_need_buffer)?;
        }

        let uses_logs = sql.contains("tm_all_log")
            || sql.contains("tm_cq_log")
            || sql.contains("tm_icb_log")
            || sql.contains("tm_stg_log")
            || sql.contains("tm_buf_log");
        if uses_logs {
            let ducklake_need_buffer = self.config.ducklake.is_none()
                || sql.contains("tm_buf_log")
                || sql.contains("tm_all_log");
            self.prepare_union_view(state, "logs", "logs", ducklake_need_buffer)?;
        }

        let uses_metrics = sql.contains("tm_all_metric")
            || sql.contains("tm_cq_metric")
            || sql.contains("tm_icb_metric")
            || sql.contains("tm_stg_metric")
            || sql.contains("tm_buf_metric");
        if uses_metrics {
            let ducklake_need_buffer = self.config.ducklake.is_none()
                || sql.contains("tm_buf_metric")
                || sql.contains("tm_all_metric");
            self.prepare_union_view(state, "metrics", "metrics", ducklake_need_buffer)?;
        }

        Ok(())
    }

    fn prepare_union_view(
        &self,
        state: &mut ConnectionState,
        kind: &str,
        table_name: &str,
        materialize_buffer: bool,
    ) -> Result<()> {
        let kind_key = kind.to_string();
        let kind_ready = state.prepared_kinds.contains(&kind_key);
        let diag = std::env::var("PERF_DIAG").ok().as_deref() == Some("1");
        let start = std::time::Instant::now();
        if self.config.ducklake.is_some() {
            // Catalog is attached once in `init_connection_state_with`. Re-running ATTACH here on
            // every query (even when it "succeeds" via already-attached) breaks DuckLake snapshot
            // binding for reads in this DuckDB build.
            self.create_committed_catalog_view(&state.conn, kind, table_name)?;
        } else {
            let (source, source_signature) =
                self.resolve_iceberg_source(state, &kind_key, table_name)?;
            let source_changed = state
                .iceberg_signatures
                .get(&kind_key)
                .map(|prev| prev != &source_signature)
                .unwrap_or(true);
            if source_changed || !kind_ready {
                let applied_source =
                    self.create_iceberg_view(&state.conn, kind, table_name, &source)?;
                let applied_signature = self.iceberg_source_signature(&applied_source, table_name)?;
                state
                    .iceberg_signatures
                    .insert(kind_key.clone(), applied_signature);
                state
                    .iceberg_sources
                    .insert(kind_key.clone(), applied_source);
                #[cfg(test)]
                {
                    VIEW_COUNTERS.iceberg.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if diag {
            println!(
                "DIAG committed_view({}) prepared in {:?}",
                table_name,
                start.elapsed()
            );
        }

        // Create buffer view (snapshot of in-memory data)
        // Uses sync snapshot (non-blocking) to access buffer data in real-time
        if !kind_ready && materialize_buffer {
            self.create_buffer_view_sync(&state.conn, kind, &state.tiered_storage)?;
        }

        // DuckLake: any TEMP VIEW / wrapper over attached tables breaks snapshot binding; union is
        // expanded inline in `ducklake_inline_sql` instead.
        if !kind_ready && self.config.ducklake.is_none() {
            let (vc, vb, vu, _) = telemetry_view_names(kind);
            let union_view = format!(
                "CREATE OR REPLACE TEMP VIEW {vu} AS \
                 SELECT * FROM {vc} \
                 UNION ALL SELECT * FROM {vb};",
                vu = vu,
                vc = vc,
                vb = vb,
            );
            let start = std::time::Instant::now();
            state.conn.execute_batch(&union_view)?;
            if diag {
                println!("DIAG union_view({}) created in {:?}", kind, start.elapsed());
            }
            #[cfg(test)]
            {
                VIEW_COUNTERS.union_view.fetch_add(1, Ordering::Relaxed);
            }
        }

        state.prepared_kinds.insert(kind_key);

        Ok(())
    }

    fn create_committed_catalog_view(
        &self,
        _conn: &Connection,
        _kind: &str,
        _table_name: &str,
    ) -> Result<()> {
        // DuckLake reads use fully qualified catalog tables inlined in `ducklake_inline_sql`.
        Ok(())
    }

    /// Must match [`crate::storage::ducklake::ducklake_qualified_table_name`] (writer DDL uses
    /// `catalog.table` when `metadata_schema` is `main`, not `catalog.main.table`).
    fn ducklake_qualified_table(&self, table: &str) -> String {
        ducklake_qualified_table_name(&self.ducklake_config(), table)
    }

    /// Replace internal telemetry aliases with real DuckLake table refs (and inline unions).
    fn ducklake_inline_sql(&self, sql: &str) -> String {
        let traces = self.ducklake_qualified_table("traces");
        let logs = self.ducklake_qualified_table("logs");
        let metrics = self.ducklake_qualified_table("metrics");
        let mut s = sql.to_string();
        s = replace_standalone_ident(&s, "tm_icb_metric", &metrics);
        s = replace_standalone_ident(&s, "tm_cq_metric", &metrics);
        s = replace_standalone_ident(
            &s,
            "tm_all_metric",
            &format!(
                "(SELECT * FROM {q} UNION ALL SELECT * FROM tm_buf_metric) AS __tm_u",
                q = metrics
            ),
        );
        s = replace_standalone_ident(&s, "tm_icb_log", &logs);
        s = replace_standalone_ident(&s, "tm_cq_log", &logs);
        s = replace_standalone_ident(
            &s,
            "tm_all_log",
            &format!(
                "(SELECT * FROM {q} UNION ALL SELECT * FROM tm_buf_log) AS __tm_u",
                q = logs
            ),
        );
        s = replace_standalone_ident(&s, "tm_icb_span", &traces);
        s = replace_standalone_ident(&s, "tm_cq_span", &traces);
        s = replace_standalone_ident(
            &s,
            "tm_all_span",
            &format!(
                "(SELECT * FROM {q} UNION ALL SELECT * FROM tm_buf_span) AS __tm_u",
                q = traces
            ),
        );
        s
    }

    fn use_attached_catalog(&self) -> bool {
        self.config.ducklake.is_some()
    }

    fn attach_catalog_if_needed(&self, conn: &Connection) -> Result<()> {
        if std::env::var("DUCKDB_TEST_ICEBERG_FALLBACK_PATH").is_ok() {
            // Tests can bypass REST catalog attach when using a local parquet stub.
            return Ok(());
        }
        if !self.use_attached_catalog() {
            return Ok(());
        }

        let sql = if self.config.ducklake.is_some() {
            let ducklake = self.ducklake_config();
            let attach_target = match ducklake.catalog_type.as_str() {
                "postgres" => {
                    if ducklake.metadata_path.starts_with("postgres:") {
                        ducklake.metadata_path.clone()
                    } else {
                        format!("postgres:{}", ducklake.metadata_path)
                    }
                }
                "sqlite" => {
                    if ducklake.metadata_path.starts_with("sqlite:") {
                        ducklake.metadata_path.clone()
                    } else {
                        format!("sqlite:{}", ducklake.metadata_path)
                    }
                }
                _ => ducklake.metadata_path.clone(),
            };
            self.prepare_local_ducklake_paths(&ducklake, &attach_target)?;
            let mut options = vec![format!(
                "DATA_PATH '{}'",
                escape_sql_literal(&ducklake.data_path)
            )];
            if let Some(limit) = ducklake.data_inlining_row_limit {
                options.push(format!("DATA_INLINING_ROW_LIMIT {}", limit));
            }
            if ducklake.catalog_type == "postgres" && ducklake.metadata_schema != "main" {
                let schema = escape_sql_literal(&ducklake.metadata_schema);
                options.push(format!("METADATA_SCHEMA '{}'", schema));
                options.push(format!("META_SCHEMA '{}'", schema));
            }
            format!(
                "ATTACH 'ducklake:{}' AS {} ({});",
                escape_sql_literal(&attach_target),
                ducklake.catalog_alias,
                options.join(", ")
            )
        } else {
            let endpoint = escape_sql_literal(&self.config.iceberg.catalog_uri);
            let warehouse = escape_sql_literal(&self.config.iceberg.warehouse);
            let mut options = vec![
                "TYPE ICEBERG".to_string(),
                format!("ENDPOINT '{}'", endpoint),
            ];
            if let Some(token) = self.config.iceberg.catalog_token.as_ref() {
                options.push(format!("TOKEN '{}'", escape_sql_literal(token)));
            } else {
                options.push("AUTHORIZATION_TYPE 'none'".to_string());
            }
            format!(
                "ATTACH '{}' AS {} ({});",
                warehouse,
                CATALOG_ALIAS,
                options.join(", ")
            )
        };
        match conn.execute_batch(&sql) {
            Ok(()) => Ok(()),
            Err(err) => {
                let message = err.to_string();
                if message.contains("already exists") || message.contains("already attached") {
                    Ok(())
                } else if self.config.ducklake.is_some()
                    && message.contains("__ducklake_metadata_")
                    && message.contains("does not exist")
                {
                    let ducklake = self.ducklake_config();
                    // Backward-compatible fallback for catalogs initialized without custom metadata schema.
                    let attach_target = match ducklake.catalog_type.as_str() {
                        "postgres" => {
                            if ducklake.metadata_path.starts_with("postgres:") {
                                ducklake.metadata_path.clone()
                            } else {
                                format!("postgres:{}", ducklake.metadata_path)
                            }
                        }
                        "sqlite" => {
                            if ducklake.metadata_path.starts_with("sqlite:") {
                                ducklake.metadata_path.clone()
                            } else {
                                format!("sqlite:{}", ducklake.metadata_path)
                            }
                        }
                        _ => ducklake.metadata_path.clone(),
                    };
                    let mut fallback_options = vec![format!(
                        "DATA_PATH '{}'",
                        escape_sql_literal(&ducklake.data_path)
                    )];
                    if let Some(limit) = ducklake.data_inlining_row_limit {
                        fallback_options.push(format!("DATA_INLINING_ROW_LIMIT {}", limit));
                    }
                    let fallback_sql = format!(
                        "ATTACH 'ducklake:{}' AS {} ({});",
                        escape_sql_literal(&attach_target),
                        ducklake.catalog_alias,
                        fallback_options.join(", ")
                    );
                    conn.execute_batch(&fallback_sql)
                        .map_err(|fallback_err| anyhow!("DuckDB ATTACH failed: {}", fallback_err))
                } else {
                    Err(anyhow!("DuckDB ATTACH failed: {}", err))
                }
            }
        }
    }

    fn force_reattach_catalog(&self, conn: &Connection) -> Result<()> {
        if !self.use_attached_catalog() {
            return Ok(());
        }
        let alias = self.catalog_alias();
        let detach_sql = format!("DETACH {};", alias);
        if let Err(err) = conn.execute_batch(&detach_sql) {
            let message = err.to_string();
            if !message.contains("not attached")
                && !message.contains("does not exist")
                && !message.contains("not found")
            {
                return Err(anyhow!("DuckDB DETACH failed for {}: {}", alias, err));
            }
        }
        self.attach_catalog_if_needed(conn)
    }

    fn prepare_local_ducklake_paths(
        &self,
        ducklake: &crate::config::DuckLakeConfig,
        attach_target: &str,
    ) -> Result<()> {
        if ducklake.catalog_type == "duckdb" || ducklake.catalog_type == "sqlite" {
            let raw = attach_target
                .strip_prefix("sqlite:")
                .unwrap_or(attach_target)
                .strip_prefix("duckdb:")
                .unwrap_or(attach_target);
            let metadata_path = std::path::PathBuf::from(raw);
            if let Some(parent) = metadata_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if !ducklake.data_path.contains("://") {
                std::fs::create_dir_all(&ducklake.data_path)?;
            }
        }
        Ok(())
    }

    /// Resolve the iceberg source for a given kind and table name.
    fn resolve_iceberg_source(
        &self,
        state: &ConnectionState,
        kind_key: &str,
        table_name: &str,
    ) -> Result<(IcebergSource, String)> {
        // DuckLake-backed runtime should always read committed data through the attached
        // catalog tables. This avoids legacy pinned metadata + iceberg_scan fallback logic,
        // which is Iceberg-specific and adds noise/latency in DuckLake deployments.
        if self.config.ducklake.is_some() {
            let signature = format!("catalog:{}:{}", self.catalog_schema().as_str(), table_name);
            return Ok((IcebergSource::Catalog, signature));
        }

        // Check if we've already resolved this source for this kind.
        if let Some(source) = state.iceberg_sources.get(kind_key) {
            if matches!(source, IcebergSource::Stub(_)) {
                // Re-resolve stub sources so we can switch to committed catalog tables
                // after the first optimizer commit.
            } else {
                if let IcebergSource::Pinned { .. } = source {
                    if let Some(pinned) = self.iceberg_pinned_metadata(table_name) {
                        // If we have pinned metadata, use it.
                        return Ok((
                            IcebergSource::Pinned {
                                metadata_path: pinned.metadata_path,
                                snapshot_id: pinned.snapshot_id,
                                compression: pinned.compression,
                                signature: pinned.signature.clone(),
                                data_files_path: pinned.data_files_path,
                            },
                            pinned.signature,
                        ));
                    }
                }
                // Otherwise, use the signature of the source.
                let signature = self.iceberg_source_signature(source, table_name)?;
                return Ok((source.clone(), signature));
            }
        }

        if let Ok(stub_path) = std::env::var("DUCKDB_TEST_ICEBERG_FALLBACK_PATH") {
            let signature = self.stub_signature(&stub_path)?;
            return Ok((IcebergSource::Stub(stub_path), signature));
        }

        let disable_pinned = std::env::var("DUCKDB_DISABLE_PINNED_METADATA")
            .ok()
            .as_deref()
            == Some("1");
        if !disable_pinned {
            if let Some(pinned) = self.iceberg_pinned_metadata(table_name) {
                if pinned.data_files_path.is_some() {
                    info!(
                        "Using pinned metadata for {} with data_files_path",
                        table_name
                    );
                } else {
                    debug!(
                        "Pinned metadata for {} exists but data_files_path is None; will query committed table via metadata scan",
                        table_name
                    );
                }
                return Ok((
                    IcebergSource::Pinned {
                        metadata_path: pinned.metadata_path,
                        snapshot_id: pinned.snapshot_id,
                        compression: pinned.compression,
                        signature: pinned.signature.clone(),
                        data_files_path: pinned.data_files_path,
                    },
                    pinned.signature,
                ));
            } else {
                debug!(
                    "No pinned metadata found for {}, will use catalog or scan",
                    table_name
                );
            }
        }

        if self.use_attached_catalog() {
            let signature = format!("catalog:{}:{}", self.catalog_schema().as_str(), table_name);
            return Ok((IcebergSource::Catalog, signature));
        }

        let uri = self.iceberg_table_uri(table_name);
        let signature = format!("scan_uri:{}", uri);
        Ok((IcebergSource::ScanUri(uri), signature))
    }

    fn iceberg_source_signature(&self, source: &IcebergSource, table_name: &str) -> Result<String> {
        match source {
            IcebergSource::Pinned { signature, .. } => {
                if let Some(pinned) = self.iceberg_pinned_metadata(table_name) {
                    Ok(pinned.signature)
                } else {
                    Ok(signature.clone())
                }
            }
            IcebergSource::Catalog => Ok(format!(
                "catalog:{}:{}",
                self.catalog_schema().as_str(),
                table_name
            )),
            IcebergSource::ScanUri(uri) => Ok(format!("scan_uri:{}", uri)),
            IcebergSource::Stub(path) => self.stub_signature(path),
        }
    }

    fn create_buffer_view_sync(
        &self,
        conn: &Connection,
        kind: &str,
        tiered_storage: &Arc<dyn TieredStorage>,
    ) -> Result<()> {
        // Get buffer snapshot and convert to RecordBatch (sync, non-blocking for snapshot)
        let batch = match kind {
            "spans" => {
                let Some(items) = tiered_storage.snapshot_buffered_spans_sync() else {
                    return self.create_empty_buffer_view(conn, kind);
                };
                if items.is_empty() {
                    return self.create_empty_buffer_view(conn, kind);
                }
                // Get schema via shared runtime handle (cached, fast).
                let schema = self
                    .runtime_handle
                    .block_on(tiered_storage.writer().spans_schema())?;
                spans_to_record_batch(&items, schema.as_ref())?
            }
            "logs" => {
                let Some(items) = tiered_storage.snapshot_buffered_logs_sync() else {
                    return self.create_empty_buffer_view(conn, kind);
                };
                if items.is_empty() {
                    return self.create_empty_buffer_view(conn, kind);
                }
                // Get schema via shared runtime handle (cached, fast).
                let schema = self
                    .runtime_handle
                    .block_on(tiered_storage.writer().logs_schema())?;
                logs_to_record_batch(&items, schema.as_ref())?
            }
            "metrics" => {
                let Some(items) = tiered_storage.snapshot_buffered_metrics_sync() else {
                    return self.create_empty_buffer_view(conn, kind);
                };
                if items.is_empty() {
                    return self.create_empty_buffer_view(conn, kind);
                }
                // Get schema via shared runtime handle (cached, fast).
                let schema = self
                    .runtime_handle
                    .block_on(tiered_storage.writer().metrics_schema())?;
                metrics_to_record_batch(&items, schema.as_ref())?
            }
            _ => return self.create_empty_buffer_view(conn, kind),
        };

        // Write batch to temp Parquet file
        let temp_path = format!(
            "/tmp/buffer_{}_{}.parquet",
            kind,
            Utc::now().timestamp_nanos_opt().unwrap_or(0)
        );

        let file = File::create(&temp_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Create view from temp file
        let (_, vb, _, _) = telemetry_view_names(kind);
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TEMP VIEW {vb} AS SELECT * FROM read_parquet('{path}');",
            vb = vb,
            path = temp_path
        ))?;

        Ok(())
    }

    fn create_empty_buffer_view(&self, conn: &Connection, kind: &str) -> Result<()> {
        let (vc, vb, _, _) = telemetry_view_names(kind);
        if self.config.ducklake.is_some() {
            let qual = self.ducklake_qualified_table(match kind {
                "logs" => "logs",
                "metrics" => "metrics",
                _ => "traces",
            });
            // `CREATE TEMP TABLE AS SELECT` from DuckLake can hit the same broken binder as TEMP
            // VIEW wrappers; `LIMIT 0` subquery-style reads work reliably.
            conn.execute_batch(&format!(
                "CREATE OR REPLACE TEMP VIEW {vb} AS SELECT * FROM {qual} LIMIT 0;",
                vb = vb,
                qual = qual,
            ))?;
        } else {
            conn.execute_batch(&format!(
                "CREATE OR REPLACE TEMP TABLE {vb} AS SELECT * FROM {vc} WHERE 1=0;",
                vb = vb,
                vc = vc,
            ))?;
        }
        Ok(())
    }

    fn create_iceberg_view(
        &self,
        conn: &Connection,
        kind: &str,
        table_name: &str,
        source: &IcebergSource,
    ) -> Result<IcebergSource> {
        let (vc, _, _, _) = telemetry_view_names(kind);
        match source {
            IcebergSource::Pinned {
                metadata_path,
                snapshot_id,
                compression,
                data_files_path,
                ..
            } => {
                if let Some(data_files_path) = data_files_path.as_ref() {
                    info!(
                        "Attempting to use data_files.json for {}: {:?}",
                        kind, data_files_path
                    );
                    if let Ok(contents) = std::fs::read_to_string(data_files_path) {
                        #[derive(serde::Deserialize)]
                        struct DataFiles {
                            files: Vec<String>,
                        }
                        if let Ok(files) = serde_json::from_str::<DataFiles>(&contents) {
                            info!(
                                "Loaded {} files from data_files.json for {}",
                                files.files.len(),
                                kind
                            );
                            if !files.files.is_empty() {
                                // Separate local and S3 files
                                let mut local_files = Vec::new();
                                let mut s3_files = Vec::new();

                                for path in &files.files {
                                    if path.contains("://") {
                                        // S3 path - check if cached locally
                                        if let Some(cache_dir) = self.cache_dir() {
                                            let filename = path.rsplit('/').next().unwrap_or("");
                                            // Use table_name, not kind, because cache structure uses table names
                                            let cached_path = cache_dir
                                                .join("iceberg_metadata")
                                                .join(table_name)
                                                .join("data")
                                                .join(filename);
                                            if cached_path.exists() {
                                                local_files.push(
                                                    cached_path.to_string_lossy().to_string(),
                                                );
                                            } else {
                                                // Not cached, will use S3 (slow but works)
                                                s3_files.push(path.clone());
                                            }
                                        } else {
                                            s3_files.push(path.clone());
                                        }
                                    } else {
                                        // Already a local path
                                        if std::path::Path::new(path).exists() {
                                            local_files.push(path.clone());
                                        }
                                    }
                                }

                                // Prefer local cache. If no local files exist, fall back to S3/HTTP paths.
                                let (files_to_use, skipped_s3_count, using_remote) =
                                    if !local_files.is_empty() {
                                        let skipped = s3_files.len();
                                        (local_files, skipped, false)
                                    } else {
                                        (s3_files, 0, true)
                                    };

                                if !files_to_use.is_empty() {
                                    let file_list = files_to_use
                                        .iter()
                                        .map(|path| format!("'{}'", escape_sql_literal(path)))
                                        .collect::<Vec<_>>()
                                        .join(", ");
                                    let used_count = files_to_use.len();
                                    if using_remote {
                                        info!(
                                            "Using {} remote files for iceberg_{} (no local cache available)",
                                            used_count, kind
                                        );
                                    } else {
                                        info!(
                                            "Using {} local cached files for iceberg_{} (skipped {} S3 files)",
                                            used_count, kind, skipped_s3_count
                                        );
                                    }
                                    let committed_view = format!(
                                        "CREATE OR REPLACE TEMP VIEW {vc} AS \
                                         SELECT * FROM read_parquet([{files}]);",
                                        vc = vc,
                                        files = file_list,
                                    );
                                    conn.execute_batch(&committed_view)?;
                                    self.create_legacy_iceberg_alias(conn, kind)?;
                                    return Ok(source.clone());
                                } else {
                                    warn!("No local files available for iceberg_{}, falling back to S3 (slow)", kind);
                                }
                            } else {
                                warn!("data_files.json for {} is empty", kind);
                            }
                        } else {
                            warn!(
                                "Failed to parse data_files.json for {}: {:?}",
                                kind, data_files_path
                            );
                        }
                    } else {
                        warn!(
                            "Failed to read data_files.json for {}: {:?}",
                            kind, data_files_path
                        );
                    }
                } else {
                    debug!(
                        "data_files_path is None for committed_{}, falling back to iceberg_scan metadata path",
                        kind
                    );
                }
                // Check if metadata_path is a local file path (not S3/HTTP)
                // iceberg_scan doesn't work with local paths, so we need to fall back
                if !metadata_path.contains("://") {
                    // Local metadata path - iceberg_scan won't work, fall back to catalog or S3
                    warn!("Pinned metadata has local path but iceberg_scan requires remote path for {}: {}", table_name, metadata_path);
                    let fallback = if self.use_attached_catalog() {
                        IcebergSource::Catalog
                    } else {
                        IcebergSource::ScanUri(self.iceberg_table_uri(table_name))
                    };
                    return self.create_iceberg_view(conn, kind, table_name, &fallback);
                }

                let mut options = vec![
                    "mode := 'metadata'".to_string(),
                    "allow_moved_paths := true".to_string(),
                ];
                if let Some(snapshot_id) = snapshot_id {
                    options.push(format!("snapshot_from_id := {}", snapshot_id));
                }
                if let Some(codec) = compression {
                    options.push(format!("metadata_compression_codec := '{}'", codec));
                }
                let escaped_uri = escape_sql_literal(metadata_path);
                let with_mode_sql = format!(
                    "CREATE OR REPLACE TEMP VIEW {vc} AS SELECT * FROM iceberg_scan('{uri}', {options});",
                    vc = vc,
                    uri = escaped_uri,
                    options = options.join(", "),
                );
                if let Err(err) = conn.execute_batch(&with_mode_sql) {
                    let err_msg = err.to_string();
                    if err_msg.contains("Unimplemented option mode") {
                        if !PINNED_MODE_OPTION_WARNED.swap(true, Ordering::Relaxed) {
                            warn!(
                                "Pinned metadata view uses unsupported iceberg_scan mode option; retrying without mode for {}",
                                table_name
                            );
                        } else {
                            debug!(
                                "Retrying pinned metadata view without iceberg_scan mode option for {}",
                                table_name
                            );
                        }
                        options.retain(|opt| !opt.starts_with("mode :="));
                        let without_mode_sql = format!(
                            "CREATE OR REPLACE TEMP VIEW {vc} AS SELECT * FROM iceberg_scan('{uri}', {options});",
                            vc = vc,
                            uri = escaped_uri,
                            options = options.join(", "),
                        );
                        match conn.execute_batch(&without_mode_sql) {
                            Ok(()) => {
                                self.create_legacy_iceberg_alias(conn, kind)?;
                                return Ok(source.clone());
                            }
                            Err(retry_err) => {
                                warn!(
                                    "Pinned metadata view retry without mode also failed for {}: {}",
                                    table_name, retry_err
                                );
                            }
                        }
                    }
                    warn!("Pinned metadata view failed for {}: {}", table_name, err);
                    let fallback = if self.use_attached_catalog() {
                        IcebergSource::Catalog
                    } else {
                        IcebergSource::ScanUri(self.iceberg_table_uri(table_name))
                    };
                    return self.create_iceberg_view(conn, kind, table_name, &fallback);
                }
                self.create_legacy_iceberg_alias(conn, kind)?;
                Ok(source.clone())
            }
            IcebergSource::Catalog => {
                self.attach_catalog_if_needed(conn)?;
                let alias = self.catalog_alias();
                let schema = self.catalog_schema();
                let committed_view = format!(
                    "CREATE OR REPLACE TEMP VIEW {vc} AS SELECT * FROM {alias}.{ns}.{table};",
                    vc = vc,
                    alias = alias,
                    ns = schema,
                    table = table_name,
                );
                match conn.execute_batch(&committed_view) {
                    Ok(()) => {
                        self.create_legacy_iceberg_alias(conn, kind)?;
                        Ok(source.clone())
                    }
                    Err(err) => {
                        let error_msg = err.to_string();
                        let alt_view = format!(
                            "CREATE OR REPLACE TEMP VIEW {vc} AS SELECT * FROM {alias}.{table};",
                            vc = vc,
                            alias = alias,
                            table = table_name,
                        );
                        if conn.execute_batch(&alt_view).is_ok() {
                            self.create_legacy_iceberg_alias(conn, kind)?;
                            return Ok(source.clone());
                        }
                        // Provide helpful error message if table doesn't exist
                        if error_msg.contains("does not exist")
                            || error_msg.contains("Catalog")
                            || (error_msg.contains("Table") && error_msg.contains("not found"))
                        {
                            // Before the first optimizer commit, DuckLake tables might not exist yet.
                            // Build an empty committed view with the staged schema to keep union-read working.
                            let staged_files = self.staged_files(kind);
                            if let Some(first_file) = staged_files.first() {
                                let empty_view = format!(
                                    "CREATE OR REPLACE TEMP VIEW {vc} AS \
                                     SELECT * FROM read_parquet('{path}') WHERE 1=0;",
                                    vc = vc,
                                    path = escape_sql_literal(first_file),
                                );
                                conn.execute_batch(&empty_view)?;
                                self.create_legacy_iceberg_alias(conn, kind)?;
                                return Ok(IcebergSource::Stub(first_file.clone()));
                            }
                            Err(anyhow!(
                                "Iceberg table '{}.{}.{}' does not exist in catalog. \
                                 This usually means:\n\
                                 1. No data has been ingested and flushed to Iceberg yet, or\n\
                                 2. The table name/namespace is incorrect, or\n\
                                 3. The catalog connection failed\n\
                                 \n\
                                 To fix: Ensure data has been ingested and flushed to Iceberg tables. \
                                 Check that the ingest pipeline is running and data is being written.\n\
                                 \n\
                                 Original error: {}",
                                CATALOG_ALIAS,
                                self.catalog_schema().as_str(),
                                table_name,
                                error_msg
                            ))
                        } else {
                            Err(anyhow!(
                                "Failed to create iceberg view for {}: {}",
                                table_name,
                                err
                            ))
                        }
                    }
                }
            }
            IcebergSource::ScanUri(uri) => {
                let committed_view = format!(
                    "CREATE OR REPLACE TEMP VIEW {vc} AS SELECT * FROM iceberg_scan('{uri}', allow_moved_paths := true);",
                    vc = vc,
                    uri = escape_sql_literal(uri),
                );
                conn.execute_batch(&committed_view)?;
                self.create_legacy_iceberg_alias(conn, kind)?;
                Ok(source.clone())
            }
            IcebergSource::Stub(path) => {
                let committed_view = format!(
                    "CREATE OR REPLACE TEMP VIEW {vc} AS SELECT * FROM read_parquet('{path}');",
                    vc = vc,
                    path = escape_sql_literal(path),
                );
                conn.execute_batch(&committed_view)?;
                self.create_legacy_iceberg_alias(conn, kind)?;
                Ok(source.clone())
            }
        }
    }

    fn create_legacy_iceberg_alias(&self, conn: &Connection, kind: &str) -> Result<()> {
        let (vc, _, _, v_ice) = telemetry_view_names(kind);
        conn.execute_batch(&format!(
            "CREATE OR REPLACE TEMP VIEW {v_ice} AS SELECT * FROM {vc};",
            v_ice = v_ice,
            vc = vc,
        ))?;
        Ok(())
    }

    fn stub_signature(&self, path: &str) -> Result<String> {
        let modified = std::fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .ok()
            .map(|timestamp| DateTime::<Utc>::from(timestamp).to_rfc3339())
            .unwrap_or_else(|| "unknown".to_string());
        Ok(format!("stub:{}:{}", path, modified))
    }

    fn staged_files(&self, kind: &str) -> Vec<String> {
        let mut files = match self.tiered_storage.list_staged_files(kind) {
            Ok(files) => files,
            Err(_) => return Vec::new(),
        };
        files.sort();
        files.dedup();
        files
            .into_iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect()
    }

    fn iceberg_pinned_metadata(&self, table: &str) -> Option<PinnedMetadata> {
        let cache_dir = self.cache_dir()?;
        let pointer_path = cache_dir
            .join("iceberg_metadata")
            .join(format!("{table}.json"));
        if !pointer_path.exists() {
            debug!("Pinned metadata file not found: {:?}", pointer_path);
            return None;
        }
        let contents = std::fs::read_to_string(&pointer_path).ok()?;
        #[derive(serde::Deserialize)]
        struct Pointer {
            metadata_file: Option<String>,
            metadata_location: Option<String>,
            snapshot_id: Option<i64>,
            data_files_path: Option<String>,
        }
        let pointer: Pointer = serde_json::from_str(&contents).ok()?;
        let pointer_modified = std::fs::metadata(&pointer_path)
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .map(|timestamp| DateTime::<Utc>::from(timestamp).to_rfc3339())
            .unwrap_or_else(|| "unknown".to_string());
        let mut metadata_path = None;
        if let Some(metadata_file) = pointer.metadata_file.as_ref() {
            let local_path = cache_dir
                .join("iceberg_metadata")
                .join(table)
                .join("metadata")
                .join(metadata_file);
            if local_path.exists() {
                metadata_path = Some(local_path.to_string_lossy().to_string());
            }
        }
        if metadata_path.is_none() {
            metadata_path = pointer.metadata_location;
        }
        let metadata_path = metadata_path?;
        let compression =
            if metadata_path.ends_with(".gz.metadata.json") || metadata_path.ends_with(".gz") {
                Some("gzip".to_string())
            } else {
                None
            };
        let data_files_modified = pointer
            .data_files_path
            .as_ref()
            .and_then(|path| std::fs::metadata(path).ok())
            .and_then(|metadata| metadata.modified().ok())
            .map(|timestamp| DateTime::<Utc>::from(timestamp).to_rfc3339())
            .unwrap_or_else(|| "unknown".to_string());
        let signature = format!(
            "pinned:{}:{:?}:{:?}:{}:{}",
            metadata_path, pointer.snapshot_id, compression, pointer_modified, data_files_modified
        );
        Some(PinnedMetadata {
            metadata_path,
            snapshot_id: pointer.snapshot_id,
            compression,
            signature,
            data_files_path: pointer.data_files_path,
        })
    }

    fn iceberg_table_uri(&self, table: &str) -> String {
        let warehouse = self.config.iceberg.warehouse.trim_end_matches('/');
        if warehouse.contains("://") {
            format!(
                "{}/{}/{}",
                warehouse,
                self.config.iceberg.namespace.as_str(),
                table
            )
        } else {
            let bucket = self.config.ingest_engine.wal_bucket.trim_end_matches('/');
            format!("s3://{}/{}/{}", bucket, warehouse, table)
        }
    }

    fn ducklake_config(&self) -> crate::config::DuckLakeConfig {
        self.config.ducklake_or_default()
    }

    fn catalog_alias(&self) -> String {
        if self.config.ducklake.is_some() {
            self.ducklake_config().catalog_alias
        } else {
            CATALOG_ALIAS.to_string()
        }
    }

    fn catalog_schema(&self) -> String {
        if self.config.ducklake.is_some() {
            self.ducklake_config().metadata_schema
        } else {
            self.config.iceberg.namespace.clone()
        }
    }
}

/// Fetch AWS credentials from EC2 instance metadata service
fn fetch_instance_metadata_credentials() -> Result<(Option<String>, Option<String>, Option<String>)>
{
    // Use blocking reqwest for synchronous call (this is called from sync context)
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()?;

    // First, get the IAM role name
    let role_url = "http://169.254.169.254/latest/meta-data/iam/security-credentials/";
    let role_response = match client.get(role_url).send() {
        Ok(resp) => resp,
        Err(_) => return Ok((None, None, None)), // Not on EC2 or metadata service unavailable
    };

    let role_name = role_response.text()?.trim().to_string();
    if role_name.is_empty() {
        return Ok((None, None, None));
    }

    // Fetch credentials for the role
    let creds_url = format!(
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}",
        role_name
    );
    let creds_response = client.get(&creds_url).send()?;
    let creds_json: serde_json::Value = creds_response.json()?;

    let access_key = creds_json["AccessKeyId"].as_str().map(|s| s.to_string());
    let secret_key = creds_json["SecretAccessKey"]
        .as_str()
        .map(|s| s.to_string());
    let session_token = creds_json["Token"].as_str().map(|s| s.to_string());

    Ok((access_key, secret_key, session_token))
}

fn duck_value_to_json(value: DuckValue) -> Value {
    match value {
        DuckValue::Null => Value::Null,
        DuckValue::Boolean(v) => Value::Bool(v),
        DuckValue::TinyInt(v) => Value::Number(v.into()),
        DuckValue::SmallInt(v) => Value::Number(v.into()),
        DuckValue::Int(v) => Value::Number(v.into()),
        DuckValue::BigInt(v) => Value::Number(v.into()),
        DuckValue::HugeInt(v) => Value::String(v.to_string()),
        DuckValue::UTinyInt(v) => Value::Number(v.into()),
        DuckValue::USmallInt(v) => Value::Number(v.into()),
        DuckValue::UInt(v) => Value::Number(v.into()),
        DuckValue::UBigInt(v) => Value::Number(v.into()),
        DuckValue::Float(v) => serde_json::Number::from_f64(v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        DuckValue::Double(v) => serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        DuckValue::Decimal(v) => Value::String(v.to_string()),
        DuckValue::Timestamp(unit, value) => Value::String(format!("{:?}:{}", unit, value)),
        DuckValue::Text(v) => Value::String(v),
        DuckValue::Blob(v) => Value::String(base64::engine::general_purpose::STANDARD.encode(v)),
        DuckValue::Date32(v) => Value::String(v.to_string()),
        DuckValue::Time64(unit, value) => Value::String(format!("{:?}:{}", unit, value)),
        DuckValue::Interval {
            months,
            days,
            nanos,
        } => Value::String(format!("months={months},days={days},nanos={nanos}")),
        DuckValue::List(v) => Value::Array(v.into_iter().map(duck_value_to_json).collect()),
        DuckValue::Enum(v) => Value::String(v),
        DuckValue::Struct(fields) => {
            let mut map = serde_json::Map::new();
            for (name, field) in fields.iter() {
                map.insert(name.clone(), duck_value_to_json(field.clone()));
            }
            Value::Object(map)
        }
        DuckValue::Array(v) => Value::Array(v.into_iter().map(duck_value_to_json).collect()),
        DuckValue::Map(entries) => {
            let mut map = serde_json::Map::new();
            for (key, value) in entries.iter() {
                map.insert(format!("{:?}", key), duck_value_to_json(value.clone()));
            }
            Value::Object(map)
        }
        DuckValue::Union(value) => duck_value_to_json(*value),
    }
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_standalone_ident_rewrites_tm_cq_span() {
        let s = "SELECT count(*) AS c FROM tm_cq_span";
        let out = replace_standalone_ident(s, "tm_cq_span", "softprobe.softprobe.traces");
        assert!(
            out.contains("softprobe.softprobe.traces"),
            "got {out}"
        );
        assert!(!out.contains("tm_cq_span"));
    }

    #[test]
    fn session_init_sql_contains_required_loads() {
        assert!(
            DUCKDB_SESSION_INIT_SQL.contains("LOAD httpfs;"),
            "expected session init to load httpfs"
        );
        assert!(
            DUCKDB_SESSION_INIT_SQL.contains("LOAD ducklake;"),
            "expected session init to load ducklake"
        );
        assert!(
            DUCKDB_SESSION_INIT_SQL.contains("SET unsafe_enable_version_guessing = true;"),
            "expected session init to enable unsafe_enable_version_guessing"
        );
    }
}
