use crate::config::Config;
use crate::query::cache::CacheSettings;
use crate::runtime_engine::DuckLakeScope;
use crate::storage::ducklake::ducklake_qualified_table_name;
use crate::storage::TieredStorage;
use anyhow::{anyhow, Result};
use base64::Engine;
use duckdb::types::Value as DuckValue;
use duckdb::{Connection, ToSql};
use serde_json::Value;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

pub struct DuckDBQueryEngine {
    _shared_connection: Arc<Mutex<Connection>>,
    workers: Vec<WorkerHandle>,
    next_worker: AtomicUsize,
    config: Config,
}

const DUCKDB_SESSION_INIT_SQL: &str = include_str!("sql/duckdb_session_init.sql");

/// When a DuckLake catalog is attached, DuckDB treats identifiers containing substrings like
/// `union_spans` / `committed_spans` as special. Rewrite the public surface to neutral `tm_*` names.
/// Buffer/staged aliases map to the committed tier (ingest is flush-through).
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
        ("buffer_metrics", "tm_cq_metric"),
        ("iceberg_metrics", "tm_cq_metric"),
        ("staged_metrics", "tm_cq_metric"),
        ("union_logs", "tm_all_log"),
        ("committed_logs", "tm_cq_log"),
        ("buffer_logs", "tm_cq_log"),
        ("iceberg_logs", "tm_cq_log"),
        ("staged_logs", "tm_cq_log"),
        ("union_spans", "tm_all_span"),
        ("committed_spans", "tm_cq_span"),
        ("buffer_spans", "tm_cq_span"),
        ("iceberg_spans", "tm_cq_span"),
        ("staged_spans", "tm_cq_span"),
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
    committed: AtomicU64,
    staged: AtomicU64,
    union_view: AtomicU64,
}

static VIEW_COUNTERS: Lazy<ViewCounters> = Lazy::new(ViewCounters::default);
static CACHE_HTTPFS_CONFIG_WARNED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
#[derive(Debug, Clone)]
pub struct ViewCounterSnapshot {
    pub committed_recreates: u64,
    pub staged_recreates: u64,
    pub union_recreates: u64,
}

pub fn reset_view_counters() {
    VIEW_COUNTERS.committed.store(0, Ordering::Relaxed);
    VIEW_COUNTERS.staged.store(0, Ordering::Relaxed);
    VIEW_COUNTERS.union_view.store(0, Ordering::Relaxed);
}

pub fn view_counters_snapshot() -> ViewCounterSnapshot {
    ViewCounterSnapshot {
        committed_recreates: VIEW_COUNTERS.committed.load(Ordering::Relaxed),
        staged_recreates: VIEW_COUNTERS.staged.load(Ordering::Relaxed),
        union_recreates: VIEW_COUNTERS.union_view.load(Ordering::Relaxed),
    }
}

struct WorkerHandle {
    sender: Option<mpsc::Sender<QueryRequest>>,
    join: Option<std::thread::JoinHandle<()>>,
}

struct QueryRequest {
    sql: String,
    respond_to: oneshot::Sender<Result<QueryResult>>,
}

struct ConnectionState {
    conn: Connection,
    cache_httpfs_wrap_supported: bool,
    cache_httpfs_wrapped_s3: bool,
    cache_httpfs_wrapped_httpfs: bool,
}

#[derive(Clone)]
struct DuckDBCore {
    config: Config,
    cache: CacheSettings,
}

/// Query result containing columns and rows
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub row_count: usize,
}

impl DuckDBQueryEngine {
    pub async fn new(config: &Config, _tiered_storage: Arc<dyn TieredStorage>) -> Result<Self> {
        let core = DuckDBCore {
            config: config.clone(),
            cache: CacheSettings::new(config),
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
            let join = std::thread::Builder::new()
                .name("softprobe-duckdb-query-worker".to_string())
                .spawn(move || {
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
                })
                .map_err(|err| anyhow!("DuckDB worker spawn failed: {}", err))?;
            workers.push(WorkerHandle {
                sender: Some(tx),
                join: Some(join),
            });
        }

        // Keep a dummy connection for the _shared_connection field (for compatibility)
        let dummy_conn = core.open_connection()?;

        Ok(Self {
            _shared_connection: Arc::new(Mutex::new(dummy_conn)),
            workers,
            next_worker: AtomicUsize::new(0),
            config: config.clone(),
        })
    }

    /// Execute arbitrary SQL query and return results as JSON
    /// Used by Grafana SQL API endpoint
    pub async fn execute_query(&self, query: &str) -> Result<QueryResult> {
        let index = self.next_worker.fetch_add(1, Ordering::Relaxed);
        let worker = &self.workers[index % self.workers.len()];
        let sender = worker
            .sender
            .as_ref()
            .ok_or_else(|| anyhow!("DuckDB worker channel closed"))?;
        let (tx, rx) = oneshot::channel();
        let request = QueryRequest {
            sql: query.to_string(),
            respond_to: tx,
        };
        sender
            .send(request)
            .await
            .map_err(|_| anyhow!("DuckDB worker channel closed"))?;
        rx.await
            .map_err(|_| anyhow!("DuckDB worker dropped response"))?
    }

    /// Execute one query with a tenant-specific DuckLake metadata schema.
    ///
    /// Worker connections intentionally keep the process-level DuckLake attachment for general
    /// agent SQL. Tenant-authenticated control endpoints use this one-shot path when they need to
    /// query the exact DuckLake scope resolved from Postgres control metadata.
    pub async fn execute_query_in_ducklake_scope(
        &self,
        query: &str,
        scope: &DuckLakeScope,
    ) -> Result<QueryResult> {
        let mut config = self.config.clone();
        let mut ducklake = config.ducklake_or_default();
        ducklake.metadata_schema = scope.metadata_schema.clone();
        ducklake.data_path = scope.data_path.clone();
        config.ducklake = Some(ducklake);

        let core = DuckDBCore {
            cache: CacheSettings::new(&config),
            config,
        };
        let conn = core.open_connection()?;
        let mut state = core.init_connection_state_with(conn)?;
        core.execute_query_on_state(&mut state, query)
    }
}

impl Drop for DuckDBQueryEngine {
    fn drop(&mut self) {
        // DuckDB/extension connections are not safe to leave on detached threads while the process
        // or test binary is exiting. Close every worker channel first so all workers can break out
        // of `blocking_recv`, then join them before this engine releases its final shared state.
        for worker in &mut self.workers {
            worker.sender.take();
        }
        for worker in &mut self.workers {
            if let Some(join) = worker.join.take() {
                if let Err(err) = join.join() {
                    warn!("DuckDB query worker panicked during shutdown: {:?}", err);
                }
            }
        }
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
        // Catalog visibility: postgres metadata is visible without reconnect; sqlite concurrency
        // is handled by DuckLake (WAL + busy timeout / ATTACH behavior). Softprobe does not
        // reattach or mem::forget connections after writes.

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
            let query_start = std::time::Instant::now();
            self.try_wrap_cache_httpfs_filesystems(state);
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
                if message.contains("No snapshot found in DuckLake") {
                    warn!(
                        "DuckLake snapshot not visible yet; retrying query once: {}",
                        message
                    );
                    std::thread::sleep(std::time::Duration::from_millis(50));
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

    /// Must match [`crate::storage::ducklake::ducklake_qualified_table_name`] (writer DDL uses
    /// `catalog.table` when `metadata_schema` is `main`, not `catalog.main.table`).
    fn ducklake_qualified_table(&self, table: &str) -> String {
        ducklake_qualified_table_name(&self.ducklake_config(), table)
    }

    /// Replace internal telemetry aliases with real DuckLake table refs.
    fn ducklake_inline_sql(&self, sql: &str) -> String {
        let traces = self.ducklake_qualified_table("traces");
        let logs = self.ducklake_qualified_table("logs");
        let metrics = self.ducklake_qualified_table("metrics");
        let mut s = sql.to_string();
        for name in ["tm_icb_metric", "tm_cq_metric", "tm_all_metric", "tm_buf_metric"] {
            s = replace_standalone_ident(&s, name, &metrics);
        }
        for name in ["tm_icb_log", "tm_cq_log", "tm_all_log", "tm_buf_log"] {
            s = replace_standalone_ident(&s, name, &logs);
        }
        for name in ["tm_icb_span", "tm_cq_span", "tm_all_span", "tm_buf_span"] {
            s = replace_standalone_ident(&s, name, &traces);
        }
        s
    }

    fn use_attached_catalog(&self) -> bool {
        true
    }

    fn attach_catalog_if_needed(&self, conn: &Connection) -> Result<()> {
        let sql = {
            let ducklake = self.ducklake_config();
            let attach_target = crate::storage::ducklake::ducklake_attach_target(&ducklake);
            self.prepare_local_ducklake_paths(&ducklake, &attach_target)?;
            let options = crate::storage::ducklake::ducklake_attach_options(&ducklake);
            format!(
                "ATTACH 'ducklake:{}' AS {} ({});",
                escape_sql_literal(&attach_target),
                ducklake.catalog_alias,
                options.join(", ")
            )
        };
        match conn.execute_batch(&sql) {
            Ok(()) => Ok(()),
            Err(err) => {
                let message = err.to_string();
                if message.contains("already exists") || message.contains("already attached") {
                    Ok(())
                } else if message.contains("__ducklake_metadata_")
                    && message.contains("does not exist")
                {
                    let ducklake = self.ducklake_config();
                    // Backward-compatible fallback for catalogs initialized without custom metadata schema.
                    let attach_target = crate::storage::ducklake::ducklake_attach_target(&ducklake);
                    let mut fallback_options = vec![format!(
                        "DATA_PATH '{}'",
                        escape_sql_literal(&ducklake.data_path)
                    )];
                    if ducklake.catalog_type == "sqlite" {
                        fallback_options.push("META_JOURNAL_MODE 'WAL'".to_string());
                        fallback_options.push("META_BUSY_TIMEOUT 500".to_string());
                    }
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

    fn prepare_local_ducklake_paths(
        &self,
        ducklake: &crate::config::DuckLakeConfig,
        attach_target: &str,
    ) -> Result<()> {
        if ducklake.catalog_type == "sqlite" {
            let raw = attach_target
                .strip_prefix("sqlite:")
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

    fn ducklake_config(&self) -> crate::config::DuckLakeConfig {
        self.config.ducklake_or_default()
    }

    fn catalog_alias(&self) -> String {
        self.ducklake_config().catalog_alias
    }

    fn catalog_schema(&self) -> String {
        self.ducklake_config().metadata_schema
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
        assert!(out.contains("softprobe.softprobe.traces"), "got {out}");
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
