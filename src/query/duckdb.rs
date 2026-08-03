use crate::config::Config;
use crate::query::cache::CacheSettings;
use crate::runtime_engine::DuckLakeScope;
use crate::storage::ducklake::ducklake_qualified_table_name;
use crate::storage::TieredStorage;
use anyhow::{anyhow, Result};
use base64::Engine;
use duckdb::types::Value as DuckValue;
use duckdb::Connection;
use serde_json::Value;
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

/// Self-heal bookkeeping for poisoned worker connections. Process-global on
/// purpose (same idiom as VIEW_COUNTERS): /health must answer "is self-heal
/// failing" across every tenant engine without holding a reference to each.
///
/// Because it is global, `consecutive_failures` is cleared by ANY successful
/// query, not just by a successful rebuild. Otherwise a single tenant whose
/// DuckLake scope is permanently unattachable (schema dropped, bucket deleted,
/// per-scope credentials rotated) would drive the counter past the /health
/// threshold and crashloop a pod that is serving every other tenant fine --
/// and a restart cannot fix a broken tenant scope, so it would never stop.
/// Reaching the threshold now requires that nothing anywhere is succeeding,
/// which is the process-level state a restart can actually resolve.
#[derive(Default)]
struct SelfHealCounters {
    rebuilds: AtomicU64,
    consecutive_failures: AtomicU64,
}

static SELF_HEAL: Lazy<SelfHealCounters> = Lazy::new(SelfHealCounters::default);

#[derive(Debug, Clone)]
pub struct SelfHealSnapshot {
    pub rebuilds: u64,
    pub consecutive_failures: u64,
}

pub fn self_heal_snapshot() -> SelfHealSnapshot {
    SelfHealSnapshot {
        rebuilds: SELF_HEAL.rebuilds.load(Ordering::Relaxed),
        consecutive_failures: SELF_HEAL.consecutive_failures.load(Ordering::Relaxed),
    }
}

/// Test-only: the counters are global, so the /health unhealthy branch is
/// otherwise unreachable from tests (mirrors [`reset_view_counters`]).
pub fn set_self_heal_failures_for_test(value: u64) {
    SELF_HEAL
        .consecutive_failures
        .store(value, Ordering::Relaxed);
}

/// How a query error relates to DuckDB's "database has been invalidated" state.
///
/// After an internal assertion failure (e.g. the ducklake extension's inlined
/// data reader crashing with "Attempted to access index 0 within vector of
/// size 0"), DuckDB invalidates the whole database object. Each worker owns an
/// independent in-memory database, so one bad query kills that worker's
/// connection permanently -- and round-robin dispatch keeps feeding requests
/// to the corpse. Under concurrent load every worker gets poisoned within
/// seconds and each query returns 503 until a human restarts the process.
/// That is the 2026-08-03 production outage; this classification is what lets
/// workers rebuild instead of staying dead.
///
/// Classification is anchored to the leading marker of the FIRST line, for the
/// same reason [`crate::api::llm::query::classify_storage_error`] is: DuckDB
/// echoes the offending statement (`LINE 1: ...`) into the message, and that
/// statement embeds caller-supplied literals. A `contains` check here would let
/// a filter value like `model_name = "database has been invalidated"` force two
/// full connection rebuilds -- each one a fresh in-memory database plus a
/// DuckLake ATTACH round-trip to Postgres, synchronously on the worker thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Poison {
    /// Not a poison-related failure.
    None,
    /// This query tripped the fatal error itself. Deterministic -- retrying
    /// would just poison the fresh connection too, so only rebuild.
    Triggered,
    /// This query landed on a connection an earlier query had poisoned.
    /// Innocent -- retry it once on the rebuilt connection.
    Collateral,
}

fn poison_kind(message: &str) -> Poison {
    // Only the first line: everything after it may contain echoed SQL.
    let head = message.trim_start().lines().next().unwrap_or("");
    // Order matters. The collateral message quotes the original internal error
    // in a trailing `Original error: "..."`, but that lives on a later line, so
    // first-line anchoring already separates the two. Checked first anyway to
    // keep the intent explicit.
    if head.starts_with("FATAL Error") {
        return Poison::Collateral;
    }
    if head.starts_with("INTERNAL Error") {
        return Poison::Triggered;
    }
    Poison::None
}

fn rebuild_worker_state(core: &DuckDBCore, index: usize) -> Option<ConnectionState> {
    match core
        .open_connection()
        .and_then(|conn| core.init_connection_state_with(conn))
    {
        Ok(state) => {
            SELF_HEAL.rebuilds.fetch_add(1, Ordering::Relaxed);
            SELF_HEAL.consecutive_failures.store(0, Ordering::Relaxed);
            info!("DuckDB query worker {index} rebuilt its connection after a fatal engine error");
            Some(state)
        }
        Err(err) => {
            // Counted so /health can turn "self-heal keeps failing" into an
            // unhealthy signal; nothing recovers from this state on its own.
            SELF_HEAL
                .consecutive_failures
                .fetch_add(1, Ordering::Relaxed);
            warn!("DuckDB query worker {index} failed to rebuild its poisoned connection: {err}");
            None
        }
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

        let worker_count = std::cmp::max(1, config.query.max_connections);
        let mut workers = Vec::with_capacity(worker_count);
        // Workers report startup outcome so a failed one cannot stay in the pool.
        // Previously a worker that could not open its connection just logged a
        // warning and returned; its channel closed, but round-robin dispatch kept
        // handing queries to that dead slot. Every request landing there failed
        // with "worker channel closed", which the API layer flattened into a bare
        // 503 -- a fixed fraction of requests failing while looking like a random
        // outage.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<usize, String>>();
        for index in 0..worker_count {
            let (tx, mut rx) = mpsc::channel::<QueryRequest>(32);
            let core = core.clone();
            let ready_tx = ready_tx.clone();
            let join = std::thread::Builder::new()
                .name("softprobe-duckdb-query-worker".to_string())
                .spawn(move || {
                    // Each worker gets its own independent connection (not cloned)
                    let connection = match core.open_connection() {
                        Ok(conn) => conn,
                        Err(err) => {
                            let _ = ready_tx
                                .send(Err(format!("worker {index} open_connection: {err}")));
                            return;
                        }
                    };
                    let mut state = match core.init_connection_state_with(connection) {
                        Ok(state) => state,
                        Err(err) => {
                            let _ = ready_tx
                                .send(Err(format!("worker {index} init_connection: {err}")));
                            return;
                        }
                    };
                    if ready_tx.send(Ok(index)).is_err() {
                        return; // engine construction already aborted
                    }
                    drop(ready_tx);
                    while let Some(request) = rx.blocking_recv() {
                        let mut result = core.execute_query_on_state(&mut state, &request.sql);
                        if let Err(err) = &result {
                            let kind = poison_kind(&err.to_string());
                            if kind != Poison::None {
                                // This worker's in-memory database is dead; every
                                // future statement would fail with the same FATAL
                                // error. Rebuild before touching the next request.
                                if let Some(fresh) = rebuild_worker_state(&core, index) {
                                    state = fresh;
                                    if kind == Poison::Collateral {
                                        result =
                                            core.execute_query_on_state(&mut state, &request.sql);
                                        if let Err(retry_err) = &result {
                                            if poison_kind(&retry_err.to_string()) != Poison::None {
                                                // The retry poisoned the fresh connection
                                                // too; rebuild again so the next request
                                                // does not inherit a dead one.
                                                if let Some(fresh) =
                                                    rebuild_worker_state(&core, index)
                                                {
                                                    state = fresh;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if result.is_ok() {
                            // Any success anywhere clears the global streak --
                            // see SelfHealCounters for why this must not be
                            // limited to successful rebuilds.
                            SELF_HEAL.consecutive_failures.store(0, Ordering::Relaxed);
                        }
                        let _ = request.respond_to.send(result);
                    }
                })
                .map_err(|err| anyhow!("DuckDB worker spawn failed: {}", err))?;
            workers.push(WorkerHandle {
                sender: Some(tx),
                join: Some(join),
            });
        }
        drop(ready_tx);

        // Fail fast: one unusable worker means a fixed slice of every future
        // request would fail, which is far harder to diagnose than not starting.
        let mut failures = Vec::new();
        for _ in 0..worker_count {
            match ready_rx.recv() {
                Ok(Ok(_)) => {}
                Ok(Err(msg)) => failures.push(msg),
                Err(_) => failures.push("worker exited before reporting readiness".to_string()),
            }
        }
        if !failures.is_empty() {
            warn!(
                "DuckDB query engine: {}/{} workers failed to start",
                failures.len(),
                worker_count
            );
            return Err(anyhow!(
                "DuckDB query engine failed to start {} of {} workers: {}",
                failures.len(),
                worker_count,
                failures.join("; ")
            ));
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
        config.ducklake.metadata_schema = scope.metadata_schema.clone();
        config.ducklake.data_path = scope.data_path.clone();

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
        let dk = &self.config.ducklake;
        crate::storage::ducklake::configure_object_store(conn, &self.config, &dk.data_path)?;

        // 1) Native object cache for parsed objects/metadata (best-effort; depends on DuckDB build).
        if let Err(err) = conn.execute("SET enable_object_cache = true;", []) {
            warn!("Failed to enable DuckDB object cache: {}", err);
        }

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
        let scores = self.ducklake_qualified_table("scores");
        let mut s = sql.to_string();
        for name in [
            "tm_icb_metric",
            "tm_cq_metric",
            "tm_all_metric",
            "tm_buf_metric",
        ] {
            s = replace_standalone_ident(&s, name, &metrics);
        }
        for name in ["tm_icb_log", "tm_cq_log", "tm_all_log", "tm_buf_log"] {
            s = replace_standalone_ident(&s, name, &logs);
        }
        for name in ["tm_icb_span", "tm_cq_span", "tm_all_span", "tm_buf_span"] {
            s = replace_standalone_ident(&s, name, &traces);
        }
        s = replace_standalone_ident(&s, "scores", &scores);
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
                        fallback_options.push("META_BUSY_TIMEOUT 5000".to_string());
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
        self.config.ducklake.clone()
    }
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
        // Keep VARCHAR/JSON-as-text as strings. Call sites that need objects
        // (VARIANT `CAST(... AS JSON)` attributes) parse in map_string_map.
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
                // `format!("{:?}", key)` emits Rust Debug output (`Text("k")`) instead of
                // the actual string value. Extract the logical key so JSON MAP objects
                // have correct keys regardless of the DuckValue variant.
                let key_str = match key {
                    DuckValue::Text(s) => s.clone(),
                    other => match duck_value_to_json(other.clone()) {
                        Value::String(s) => s,
                        v => v.to_string(),
                    },
                };
                map.insert(key_str, duck_value_to_json(value.clone()));
            }
            Value::Object(map)
        }
        DuckValue::Union(value) => duck_value_to_json(*value),
        // duckdb::types::Value is #[non_exhaustive]; keep forward-compatible.
        other => Value::String(format!("{other:?}")),
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
    fn duck_value_map_keys_are_plain_strings() {
        let entries = vec![
            (
                DuckValue::Text("logger_name".into()),
                DuckValue::Text("com.example".into()),
            ),
            (
                DuckValue::Text("sp.source".into()),
                DuckValue::Text("backend".into()),
            ),
        ];
        let json = duck_value_to_json(DuckValue::Map(entries.into()));
        let obj = json.as_object().expect("should be object");
        assert!(
            obj.contains_key("logger_name"),
            "key should be plain string, got: {json}"
        );
        assert!(
            obj.contains_key("sp.source"),
            "key should be plain string, got: {json}"
        );
        assert!(
            !json.to_string().contains("Text("),
            "keys must not contain Debug wrapper"
        );
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

    #[test]
    fn duck_value_keeps_json_looking_text_as_string() {
        let value = duck_value_to_json(DuckValue::Text(
            r#"{"Content-Type":"application/json"}"#.to_string(),
        ));
        assert_eq!(
            value,
            Value::String(r#"{"Content-Type":"application/json"}"#.to_string()),
            "VARCHAR JSON blobs (e.g. http headers) must remain strings"
        );
    }

    #[test]
    fn duck_value_keeps_plain_text() {
        let value = duck_value_to_json(DuckValue::Text("plain".to_string()));
        assert_eq!(value, Value::String("plain".to_string()));
    }

    #[test]
    fn poison_kind_matches_production_outage_messages() {
        // Verbatim from the 2026-08-03 incident logs.
        assert_eq!(
            poison_kind(
                "FATAL Error: Failed: database has been invalidated because of a previous \
                 fatal error. The database must be restarted prior to being used again.\n\
                 Original error: \"Attempted to access index 0 within vector of size 0\""
            ),
            Poison::Collateral
        );
        assert_eq!(
            poison_kind("INTERNAL Error: Attempted to access index 0 within vector of size 0"),
            Poison::Triggered
        );
    }

    #[test]
    fn poison_kind_ignores_ordinary_failures() {
        assert_eq!(
            poison_kind("Catalog Error: Table with name logs does not exist!"),
            Poison::None
        );
        assert_eq!(
            poison_kind("Connection Error: could not reach object store"),
            Poison::None
        );
        assert_eq!(
            poison_kind("Binder Error: column x not found"),
            Poison::None
        );
    }

    #[test]
    fn poison_kind_ignores_markers_in_echoed_sql() {
        // DuckDB echoes the offending statement, and that statement embeds
        // caller-supplied filter values. A `contains` check here let a filter
        // like model_name = "database has been invalidated" force two full
        // connection rebuilds (fresh database + DuckLake ATTACH) per request.
        assert_eq!(
            poison_kind(
                "Catalog Error: Table with name traces does not exist!\n\
                 LINE 1: ... WHERE model_name = 'database has been invalidated'"
            ),
            Poison::None
        );
        assert_eq!(
            poison_kind(
                "Binder Error: no such column\n\
                 LINE 1: ... WHERE user_id = 'INTERNAL Error'"
            ),
            Poison::None
        );
        // The collateral message quotes the internal error on a later line;
        // first-line anchoring must still classify it as collateral, not as
        // the triggering query.
        assert_eq!(
            poison_kind(
                "FATAL Error: Failed: database has been invalidated because of a previous \
                 fatal error.\nOriginal error: \"Attempted to access index 0 within vector \
                 of size 0\""
            ),
            Poison::Collateral
        );
    }
}
