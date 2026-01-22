use crate::config::Config;
use anyhow::{anyhow, Result};
use duckdb::{Connection, ToSql};
use once_cell::sync::Lazy;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::warn;

#[derive(Clone)]
pub struct CacheSettings {
    pub cache_dir: Option<PathBuf>,
}

impl CacheSettings {
    pub fn new(config: &Config) -> Self {
        Self {
            cache_dir: config.ingest_engine.cache_dir.as_ref().map(PathBuf::from),
        }
    }

    pub fn configure(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch("LOAD httpfs;")?;
        conn.execute_batch("LOAD iceberg;")?;
        conn.execute_batch("SET unsafe_enable_version_guessing = true;")?;

        if let Some(cache_dir) = &self.cache_dir {
            configure_cache_httpfs(cache_dir, conn)?;
        }

        Ok(())
    }
}

fn configure_cache_httpfs(cache_dir: &PathBuf, conn: &Connection) -> Result<()> {
    let cache_path = cache_dir.join("duckdb_http_cache");
    std::fs::create_dir_all(&cache_path)?;
    let cache_dir_str = cache_path.to_string_lossy().to_string();
    if let Err(err) = conn.execute(
        "SET external_file_cache_directory = ?;",
        [&cache_dir_str as &dyn ToSql],
    ) {
        handle_config_error(err, "external_file_cache_directory")?;
    }
    if let Err(err) = conn.execute("SET external_file_cache_size = '2GB';", []) {
        if !is_unrecognized_config_error(&err) {
            return Err(anyhow!(err));
        }
    }
    if let Err(err) = conn.execute(
        "SET s3_cache_directory = ?;",
        [&cache_dir_str as &dyn ToSql],
    ) {
        handle_config_error(err, "s3_cache_directory")?;
    }
    if let Err(err) = conn.execute("SET s3_cache_size = '2GB';", []) {
        if !is_unrecognized_config_error(&err) {
            return Err(anyhow!(err));
        }
    }
    conn.execute_batch("LOAD cache_httpfs;")?;
    conn.execute("SET cache_httpfs_cache_block_size = 8388608;", [])?;
    conn.execute("SET cache_httpfs_max_in_mem_cache_block_count = 4096;", [])?;
    conn.execute(
        "SET cache_httpfs_disk_cache_reader_enable_memory_cache = 1;",
        [],
    )?;
    conn.execute(
        "SET cache_httpfs_disk_cache_reader_mem_cache_block_count = 4096;",
        [],
    )?;
    conn.execute(
        "SET cache_httpfs_cache_directory = ?;",
        [&cache_dir_str as &dyn ToSql],
    )?;
    conn.execute("SET cache_httpfs_type = 'on_disk';", [])?;
    wrap_filesystem(conn)?;
    Ok(())
}

fn wrap_filesystem(conn: &Connection) -> Result<()> {
    if let Err(err) = conn.execute("SELECT cache_httpfs_wrap_cache_filesystem('httpfs');", []) {
        let message = err.to_string();
        if !message.contains("already wrapped") {
            return Err(anyhow!(err));
        }
    }
    if let Err(err) = conn.execute("SELECT cache_httpfs_wrap_cache_filesystem('s3');", []) {
        let message = err.to_string();
        if !message.contains("already wrapped") {
            return Err(anyhow!(err));
        }
    }
    if std::env::var("PERF_CACHE_PROFILE").ok().as_deref() == Some("1") {
        let first = !CACHE_PROFILE_INIT.swap(true, Ordering::Relaxed);
        if first {
            conn.execute("SET cache_httpfs_profile_type = 'temp';", [])?;
            conn.execute("SELECT cache_httpfs_clear_profile();", [])?;
        }
    }
    Ok(())
}

static CACHE_PROFILE_INIT: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
static CACHE_HTTPFS_UNSUPPORTED_WARNED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

fn handle_config_error(err: duckdb::Error, setting: &str) -> Result<()> {
    if is_unrecognized_config_error(&err) {
        if !CACHE_HTTPFS_UNSUPPORTED_WARNED.swap(true, Ordering::Relaxed) {
            warn!(
                "DuckDB does not expose cache_httpfs setting {}; skipping disk cache setup",
                setting,
            );
        }
        return Ok(());
    }
    Err(anyhow!(err))
}

fn is_unrecognized_config_error(err: &duckdb::Error) -> bool {
    err.to_string()
        .contains("unrecognized configuration parameter")
}
