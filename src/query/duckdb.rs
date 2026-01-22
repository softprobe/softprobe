use crate::config::Config;
use crate::query::cache::CacheSettings;
use anyhow::{anyhow, Result};
use base64::Engine;
use chrono::{DateTime, Utc};
use duckdb::types::Value as DuckValue;
use duckdb::{Connection, ToSql};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

pub struct DuckDBQueryEngine {
    _shared_connection: Arc<Mutex<Connection>>,
    workers: Arc<Vec<WorkerHandle>>,
    next_worker: AtomicUsize,
}

const CATALOG_ALIAS: &str = "iceberg_catalog";

use once_cell::sync::Lazy;
use std::sync::atomic::AtomicU64;

#[derive(Default)]
struct ViewCounters {
    iceberg: AtomicU64,
    staged: AtomicU64,
    wal: AtomicU64,
    union_view: AtomicU64,
}

static VIEW_COUNTERS: Lazy<ViewCounters> = Lazy::new(ViewCounters::default);
#[derive(Debug, Clone)]
pub struct ViewCounterSnapshot {
    pub iceberg_recreates: u64,
    pub staged_recreates: u64,
    pub wal_recreates: u64,
    pub union_recreates: u64,
}

pub fn reset_view_counters() {
    VIEW_COUNTERS.iceberg.store(0, Ordering::Relaxed);
    VIEW_COUNTERS.staged.store(0, Ordering::Relaxed);
    VIEW_COUNTERS.wal.store(0, Ordering::Relaxed);
    VIEW_COUNTERS.union_view.store(0, Ordering::Relaxed);
}

pub fn view_counters_snapshot() -> ViewCounterSnapshot {
    ViewCounterSnapshot {
        iceberg_recreates: VIEW_COUNTERS.iceberg.load(Ordering::Relaxed),
        staged_recreates: VIEW_COUNTERS.staged.load(Ordering::Relaxed),
        wal_recreates: VIEW_COUNTERS.wal.load(Ordering::Relaxed),
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
    wal_signatures: HashMap<String, String>,
    staged_signatures: HashMap<String, String>,
    iceberg_sources: HashMap<String, IcebergSource>,
    iceberg_signatures: HashMap<String, String>,
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

struct WalManifest {
    updated_at: Option<DateTime<Utc>>,
    files: Vec<String>,
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
    pub async fn new(config: &Config) -> Result<Self> {
        let core = DuckDBCore {
            config: config.clone(),
            cache: CacheSettings::new(config),
        };
        let base_connection = core.open_connection()?;
        core.install_extensions(&base_connection)?;
        let shared_connection = Arc::new(Mutex::new(base_connection));
        let worker_count = std::cmp::max(1, config.duckdb.max_connections);
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (tx, mut rx) = mpsc::channel::<QueryRequest>(32);
            let core = core.clone();
            let shared_connection = Arc::clone(&shared_connection);
            std::thread::spawn(move || {
                let connection = match shared_connection.lock() {
                    Ok(base) => base.try_clone(),
                    Err(poisoned) => poisoned.into_inner().try_clone(),
                };
                let connection = match connection {
                    Ok(connection) => connection,
                    Err(err) => {
                        warn!("DuckDB worker failed to clone connection: {}", err);
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

        Ok(Self {
            _shared_connection: shared_connection,
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

    pub async fn query_metadata(
        &self,
        _query: &str,
        _params: &[&dyn std::any::Any],
    ) -> Result<Vec<crate::api::query::RecordingMetadata>> {
        // TODO: Execute DuckDB query on Iceberg table (Phase 1.2)
        // - Use iceberg_scan() function
        // - Apply partition pruning automatically (record_date, category_type)
        // - Return metadata records with payload_file_uri, payload_file_offset, payload_row_group_index
        // See: docs/migration-to-iceberg-design.md lines 993-1004 for query pattern
        todo!("Implement DuckDB query execution - see design document v1.7")
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
        conn.execute_batch("INSTALL iceberg;")?;
        Ok(())
    }

    fn init_connection_state_with(&self, conn: Connection) -> Result<ConnectionState> {
        self.configure_connection(&conn)?;
        Ok(ConnectionState {
            conn,
            prepared_kinds: HashSet::new(),
            wal_signatures: HashMap::new(),
            staged_signatures: HashMap::new(),
            iceberg_sources: HashMap::new(),
            iceberg_signatures: HashMap::new(),
        })
    }

    fn execute_query_on_state(
        &self,
        state: &mut ConnectionState,
        query: &str,
    ) -> Result<QueryResult> {
        let diag = std::env::var("PERF_DIAG").ok().as_deref() == Some("1");
        let prepare_start = std::time::Instant::now();
        self.prepare_union_views_for_query(state, query)?;
        if diag {
            let elapsed = prepare_start.elapsed();
            println!("DIAG prepare_union_views: {:?}", elapsed);
        }

        let query_start = std::time::Instant::now();
        let mut stmt = state.conn.prepare(query)?;
        let mut query_rows = stmt.query([])?;
        let column_names = query_rows
            .as_ref()
            .map(|stmt_ref| {
                (0..stmt_ref.column_count())
                    .filter_map(|idx| stmt_ref.column_name(idx).ok().map(|name| name.to_string()))
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
    }

    fn configure_connection(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch("LOAD httpfs;")?;
        conn.execute_batch("LOAD iceberg;")?;
        conn.execute_batch("SET unsafe_enable_version_guessing = true;")?;

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
        if let Some(access_key) = self.config.s3.access_key_id.as_ref() {
            conn.execute("SET s3_access_key_id = ?;", [access_key as &dyn ToSql])?;
        }
        if let Some(secret_key) = self.config.s3.secret_access_key.as_ref() {
            conn.execute("SET s3_secret_access_key = ?;", [secret_key as &dyn ToSql])?;
        }
        conn.execute(
            "SET s3_region = ?;",
            [&self.config.storage.s3_region as &dyn ToSql],
        )?;

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
            warn!("Failed to configure cache_httpfs: {}", err);
        }

        Ok(())
    }

    fn prepare_union_views(&self, state: &mut ConnectionState) -> Result<()> {
        // View creation dominates warm-query latency when recreated per query.
        // The perf diagnostics and tests validate that these views are stable and
        // only rebuilt when the WAL/staged signatures change or on schema changes.
        self.prepare_union_view(state, "spans", "traces")?;
        self.prepare_union_view(state, "logs", "logs")?;
        self.prepare_union_view(state, "metrics", "metrics")?;
        Ok(())
    }

    fn prepare_union_views_for_query(
        &self,
        state: &mut ConnectionState,
        query: &str,
    ) -> Result<()> {
        let sql = query.to_lowercase();
        let mut prepared_any = false;

        let uses_spans = sql.contains("union_spans")
            || sql.contains("iceberg_spans")
            || sql.contains("staged_spans")
            || sql.contains("wal_spans");
        if uses_spans {
            self.prepare_union_view(state, "spans", "traces")?;
            prepared_any = true;
        }

        let uses_logs = sql.contains("union_logs")
            || sql.contains("iceberg_logs")
            || sql.contains("staged_logs")
            || sql.contains("wal_logs");
        if uses_logs {
            self.prepare_union_view(state, "logs", "logs")?;
            prepared_any = true;
        }

        let uses_metrics = sql.contains("union_metrics")
            || sql.contains("iceberg_metrics")
            || sql.contains("staged_metrics")
            || sql.contains("wal_metrics");
        if uses_metrics {
            self.prepare_union_view(state, "metrics", "metrics")?;
            prepared_any = true;
        }

        if !prepared_any {
            self.prepare_union_views(state)?;
        }

        Ok(())
    }

    fn prepare_union_view(
        &self,
        state: &mut ConnectionState,
        kind: &str,
        table_name: &str,
    ) -> Result<()> {
        let kind_key = kind.to_string();
        let kind_ready = state.prepared_kinds.contains(&kind_key);
        let diag = std::env::var("PERF_DIAG").ok().as_deref() == Some("1");
        let (source, source_signature) =
            self.resolve_iceberg_source(state, &kind_key, table_name)?;
        let source_changed = state
            .iceberg_signatures
            .get(&kind_key)
            .map(|prev| prev != &source_signature)
            .unwrap_or(true);
        if source_changed || !kind_ready {
            let start = std::time::Instant::now();
            let applied_source =
                self.create_iceberg_view(&state.conn, kind, table_name, &source)?;
            if diag {
                println!(
                    "DIAG iceberg_view({}) created in {:?}",
                    table_name,
                    start.elapsed()
                );
            }
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

        let staged_view = if let Some(glob) = self.parquet_glob(kind) {
            format!(
                "CREATE OR REPLACE TEMP VIEW staged_{kind} AS SELECT * FROM read_parquet('{glob}');",
                kind = kind,
                glob = escape_sql_literal(&glob),
            )
        } else {
            format!(
                "CREATE OR REPLACE TEMP VIEW staged_{kind} AS SELECT * FROM iceberg_{kind} WHERE 1=0;",
                kind = kind,
            )
        };
        let staged_signature = staged_view.clone();
        let staged_key = kind_key.clone();
        let staged_changed = state
            .staged_signatures
            .get(&staged_key)
            .map(|prev| prev != &staged_signature)
            .unwrap_or(true);
        if staged_changed || !kind_ready {
            let start = std::time::Instant::now();
            state.conn.execute_batch(&staged_view)?;
            if diag {
                println!(
                    "DIAG staged_view({}) recreated in {:?}",
                    kind,
                    start.elapsed()
                );
            }
            state.staged_signatures.insert(staged_key, staged_signature);
            #[cfg(test)]
            {
                VIEW_COUNTERS.staged.fetch_add(1, Ordering::Relaxed);
            }
        }

        let wal_files = self.wal_files(kind);
        let wal_signature = self.wal_signature(kind, &wal_files);
        let wal_view = if !wal_files.is_empty() {
            let file_list = wal_files
                .iter()
                .map(|path| format!("'{}'", escape_sql_literal(path)))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "CREATE OR REPLACE TEMP VIEW wal_{kind} AS \
                 SELECT * FROM read_parquet([{files}]);",
                kind = kind,
                files = file_list,
            )
        } else {
            format!(
                "CREATE OR REPLACE TEMP VIEW wal_{kind} AS SELECT * FROM iceberg_{kind} WHERE 1=0;",
                kind = kind,
            )
        };
        let wal_key = kind_key.clone();
        let wal_changed = state
            .wal_signatures
            .get(&wal_key)
            .map(|prev| prev != &wal_signature)
            .unwrap_or(true);
        if wal_changed || !kind_ready {
            let start = std::time::Instant::now();
            state.conn.execute_batch(&wal_view)?;
            if diag {
                println!(
                    "DIAG wal_view({}) recreated in {:?}, files={}",
                    kind,
                    start.elapsed(),
                    wal_files.len()
                );
            }
            state.wal_signatures.insert(wal_key, wal_signature);
            #[cfg(test)]
            {
                VIEW_COUNTERS.wal.fetch_add(1, Ordering::Relaxed);
            }
        }

        if !kind_ready {
            let union_view = format!(
                "CREATE OR REPLACE TEMP VIEW union_{kind} AS \
                 SELECT * FROM iceberg_{kind} \
                 UNION ALL SELECT * FROM staged_{kind} \
                 UNION ALL SELECT * FROM wal_{kind};",
                kind = kind,
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

    fn use_attached_catalog(&self) -> bool {
        self.config.iceberg.catalog_type == "rest"
    }

    fn attach_catalog_if_needed(&self, conn: &Connection) -> Result<()> {
        if std::env::var("DUCKDB_TEST_ICEBERG_FALLBACK_PATH").is_ok() {
            // Tests can bypass REST catalog attach when using a local parquet stub.
            return Ok(());
        }
        if !self.use_attached_catalog() {
            return Ok(());
        }

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

        let sql = format!(
            "ATTACH '{}' AS {} ({});",
            warehouse,
            CATALOG_ALIAS,
            options.join(", ")
        );
        match conn.execute_batch(&sql) {
            Ok(()) => Ok(()),
            Err(err) => {
                let message = err.to_string();
                if message.contains("already exists") {
                    Ok(())
                } else {
                    Err(anyhow!("DuckDB ATTACH failed: {}", err))
                }
            }
        }
    }

    fn resolve_iceberg_source(
        &self,
        state: &ConnectionState,
        kind_key: &str,
        table_name: &str,
    ) -> Result<(IcebergSource, String)> {
        if let Some(source) = state.iceberg_sources.get(kind_key) {
            if let IcebergSource::Pinned { .. } = source {
                if let Some(pinned) = self.iceberg_pinned_metadata(table_name) {
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
            let signature = self.iceberg_source_signature(source, table_name)?;
            return Ok((source.clone(), signature));
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

        if self.use_attached_catalog() {
            let signature = format!(
                "catalog:{}:{}",
                self.config.iceberg.namespace.as_str(),
                table_name
            );
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
                self.config.iceberg.namespace.as_str(),
                table_name
            )),
            IcebergSource::ScanUri(uri) => Ok(format!("scan_uri:{}", uri)),
            IcebergSource::Stub(path) => self.stub_signature(path),
        }
    }

    fn create_iceberg_view(
        &self,
        conn: &Connection,
        kind: &str,
        table_name: &str,
        source: &IcebergSource,
    ) -> Result<IcebergSource> {
        match source {
            IcebergSource::Pinned {
                metadata_path,
                snapshot_id,
                compression,
                data_files_path,
                ..
            } => {
                if let Some(data_files_path) = data_files_path.as_ref() {
                    if let Ok(contents) = std::fs::read_to_string(data_files_path) {
                        #[derive(serde::Deserialize)]
                        struct DataFiles {
                            files: Vec<String>,
                        }
                        if let Ok(files) = serde_json::from_str::<DataFiles>(&contents) {
                            if !files.files.is_empty() {
                                let file_list = files
                                    .files
                                    .iter()
                                    .map(|path| format!("'{}'", escape_sql_literal(path)))
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                let iceberg_view = format!(
                                    "CREATE OR REPLACE TEMP VIEW iceberg_{kind} AS \
                                     SELECT * FROM read_parquet([{files}]);",
                                    kind = kind,
                                    files = file_list,
                                );
                                conn.execute_batch(&iceberg_view)?;
                                return Ok(source.clone());
                            }
                        }
                    }
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
                let options_sql = format!(", {}", options.join(", "));
                let iceberg_view = format!(
                    "CREATE OR REPLACE TEMP VIEW iceberg_{kind} AS SELECT * FROM iceberg_scan('{uri}'{options});",
                    kind = kind,
                    uri = escape_sql_literal(metadata_path),
                    options = options_sql,
                );
                if let Err(err) = conn.execute_batch(&iceberg_view) {
                    warn!("Pinned metadata view failed for {}: {}", table_name, err);
                    let fallback = if self.use_attached_catalog() {
                        IcebergSource::Catalog
                    } else {
                        IcebergSource::ScanUri(self.iceberg_table_uri(table_name))
                    };
                    return self.create_iceberg_view(conn, kind, table_name, &fallback);
                }
                Ok(source.clone())
            }
            IcebergSource::Catalog => {
                self.attach_catalog_if_needed(conn)?;
                let iceberg_view = format!(
                    "CREATE OR REPLACE TEMP VIEW iceberg_{kind} AS SELECT * FROM {alias}.{ns}.{table};",
                    kind = kind,
                    alias = CATALOG_ALIAS,
                    ns = self.config.iceberg.namespace.as_str(),
                    table = table_name,
                );
                conn.execute_batch(&iceberg_view)?;
                Ok(source.clone())
            }
            IcebergSource::ScanUri(uri) => {
                let iceberg_view = format!(
                    "CREATE OR REPLACE TEMP VIEW iceberg_{kind} AS SELECT * FROM iceberg_scan('{uri}', allow_moved_paths := true);",
                    kind = kind,
                    uri = escape_sql_literal(uri),
                );
                conn.execute_batch(&iceberg_view)?;
                Ok(source.clone())
            }
            IcebergSource::Stub(path) => {
                let iceberg_view = format!(
                    "CREATE OR REPLACE TEMP VIEW iceberg_{kind} AS SELECT * FROM read_parquet('{path}');",
                    kind = kind,
                    path = escape_sql_literal(path),
                );
                conn.execute_batch(&iceberg_view)?;
                Ok(source.clone())
            }
        }
    }

    fn stub_signature(&self, path: &str) -> Result<String> {
        let modified = std::fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .ok()
            .map(|timestamp| DateTime::<Utc>::from(timestamp).to_rfc3339())
            .unwrap_or_else(|| "unknown".to_string());
        Ok(format!("stub:{}:{}", path, modified))
    }

    fn parquet_glob(&self, kind: &str) -> Option<String> {
        let cache_dir = self.cache_dir()?;
        let parquet_dir = cache_dir.join(kind);
        if !parquet_dir.exists() {
            return None;
        }
        let mut files = Vec::new();
        if collect_parquet_files(&parquet_dir, &mut files).is_err() || files.is_empty() {
            return None;
        }
        Some(format!("{}/**/*.parquet", parquet_dir.to_string_lossy()))
    }

    fn wal_files(&self, kind: &str) -> Vec<String> {
        let Some(cache_dir) = self.cache_dir() else {
            return Vec::new();
        };
        let wal_dir = cache_dir.join("wal").join(kind);
        let watermark = self.wal_watermark(kind);
        let mut files = Vec::new();
        if let Some(manifest) = self.read_wal_manifest(kind) {
            let mut merged = manifest
                .files
                .into_iter()
                .filter(|path| std::path::Path::new(path).exists())
                .collect::<Vec<_>>();
            let manifest_cutoff = manifest.updated_at.unwrap_or(watermark);
            let mut extra = Vec::new();
            if collect_parquet_files(&wal_dir, &mut extra).is_ok() {
                for path in extra {
                    if let Ok(metadata) = std::fs::metadata(&path) {
                        if let Ok(modified) = metadata.modified() {
                            let modified = DateTime::<Utc>::from(modified);
                            if modified > manifest_cutoff && modified > watermark {
                                merged.push(path.to_string_lossy().to_string());
                            }
                        }
                    }
                }
            }
            merged.sort();
            merged.dedup();
            return merged;
        }
        if collect_parquet_files(&wal_dir, &mut files).is_err() {
            return Vec::new();
        }

        files
            .into_iter()
            .filter_map(|path| {
                let metadata = std::fs::metadata(&path).ok()?;
                let modified = metadata.modified().ok()?;
                let modified = DateTime::<Utc>::from(modified);
                if modified > watermark {
                    Some(path.to_string_lossy().to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    fn wal_signature(&self, kind: &str, files: &[String]) -> String {
        let Some(cache_dir) = self.cache_dir() else {
            return String::new();
        };
        let manifest_path = cache_dir.join("wal_manifest").join(format!("{kind}.json"));
        if let Ok(metadata) = std::fs::metadata(&manifest_path) {
            if let Ok(modified) = metadata.modified() {
                let modified = DateTime::<Utc>::from(modified);
                return modified.to_rfc3339();
            }
        }
        let mut sorted = files.to_vec();
        sorted.sort();
        sorted.join("|")
    }

    fn read_wal_manifest(&self, kind: &str) -> Option<WalManifest> {
        let cache_dir = self.cache_dir()?;
        let manifest_path = cache_dir.join("wal_manifest").join(format!("{kind}.json"));
        let contents = std::fs::read_to_string(&manifest_path).ok()?;
        #[derive(serde::Deserialize)]
        struct Manifest {
            updated_at: Option<String>,
            files: Vec<String>,
        }
        let manifest: Manifest = serde_json::from_str(&contents).ok()?;
        if manifest.files.is_empty() {
            None
        } else {
            let updated_at = manifest
                .updated_at
                .as_deref()
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc));
            Some(WalManifest {
                updated_at,
                files: manifest.files,
            })
        }
    }

    fn wal_watermark(&self, kind: &str) -> DateTime<Utc> {
        let Some(cache_dir) = self.cache_dir() else {
            return DateTime::<Utc>::from(std::time::SystemTime::UNIX_EPOCH);
        };
        let sanitized = self.config.ingest_engine.wal_prefix.replace('/', "_");
        let path = cache_dir
            .join("wal_watermarks")
            .join(sanitized)
            .join(format!("{}.txt", kind));
        match std::fs::read_to_string(&path) {
            Ok(contents) => DateTime::parse_from_rfc3339(contents.trim())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| DateTime::<Utc>::from(std::time::SystemTime::UNIX_EPOCH)),
            Err(err) => {
                warn!("Failed to read WAL watermark {:?}: {}", path, err);
                DateTime::<Utc>::from(std::time::SystemTime::UNIX_EPOCH)
            }
        }
    }

    fn iceberg_pinned_metadata(&self, table: &str) -> Option<PinnedMetadata> {
        let cache_dir = self.cache_dir()?;
        let pointer_path = cache_dir
            .join("iceberg_metadata")
            .join(format!("{table}.json"));
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
}

fn collect_parquet_files(dir: &std::path::Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, out)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    Ok(())
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
