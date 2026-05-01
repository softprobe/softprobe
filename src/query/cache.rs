use crate::config::Config;
use anyhow::{anyhow, Result};
use duckdb::{Connection, ToSql};
use once_cell::sync::Lazy;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

const DUCKDB_CACHE_HTTPFS_INIT_SQL: &str = include_str!("sql/duckdb_cache_httpfs_init.sql");

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
    // 3) cache_httpfs for persistent on-disk caching + glob caching (useful when we use globs like **/*.parquet).
    // We keep cache_httpfs' disk cache enabled, but disable its in-memory caching so we don't double-cache
    // with DuckDB's native external_file_cache.
    //
    // Note: cache_httpfs is not bundled by default; we must INSTALL it before LOAD.
    conn.execute_batch(DUCKDB_CACHE_HTTPFS_INIT_SQL)
        .map_err(|err| {
            anyhow!(
                "Failed to initialize cache_httpfs via community registry: {}. \
            Ensure DuckDB is built with extension support and the community registry is available.",
                err
            )
        })?;
    // Enable glob result caching (best-effort; setting may vary by extension version).
    let _ = conn.execute("SET cache_httpfs_enable_glob_cache = true;", []);
    conn.execute(
        "SET cache_httpfs_cache_directory = ?;",
        [&cache_dir_str as &dyn ToSql],
    )?;
    wrap_filesystem(conn)?;
    Ok(())
}

/// Wrap S3 and httpfs filesystems with cache_httpfs for persistent on-disk caching.
/// Filesystems are registered lazily by DuckDB when first used, so wrapping may fail
/// until S3/httpfs paths are actually queried. This is non-fatal - caching will work
/// once filesystems are registered through actual usage.
fn wrap_filesystem(conn: &Connection) -> Result<()> {
    // Wrap S3 filesystem (will succeed once S3 is used in a query)
    if let Err(err) = conn.execute("SELECT cache_httpfs_wrap_cache_filesystem('s3');", []) {
        let message = err.to_string();
        if message.contains("already wrapped") {
            // Already wrapped is fine (idempotent)
        } else if message.contains("hasn't been registered yet") {
            // S3 filesystem not registered yet - will be registered on first S3 query
            // This is expected and non-fatal. Caching will work once S3 is used.
        } else {
            return Err(anyhow!(
                "Failed to wrap S3 filesystem with cache_httpfs: {}. \
                Unexpected error - ensure DuckDB 1.4.3+ with cache_httpfs support.",
                err
            ));
        }
    }

    // Wrap httpfs filesystem (will succeed once httpfs is used in a query)
    if let Err(err) = conn.execute("SELECT cache_httpfs_wrap_cache_filesystem('httpfs');", []) {
        let message = err.to_string();
        if message.contains("already wrapped") {
            // Already wrapped is fine (idempotent)
        } else if message.contains("hasn't been registered yet") {
            // httpfs filesystem not registered yet - will be registered on first HTTP query
            // This is expected and non-fatal. Caching will work once httpfs is used.
        } else {
            return Err(anyhow!(
                "Failed to wrap httpfs filesystem with cache_httpfs: {}. \
                Unexpected error - ensure DuckDB 1.4.3+ with cache_httpfs support.",
                err
            ));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_httpfs_init_sql_installs_before_loads() {
        let install = DUCKDB_CACHE_HTTPFS_INIT_SQL
            .find("INSTALL cache_httpfs")
            .expect("expected INSTALL cache_httpfs in init SQL");
        let load = DUCKDB_CACHE_HTTPFS_INIT_SQL
            .find("LOAD cache_httpfs")
            .expect("expected LOAD cache_httpfs in init SQL");
        assert!(
            install < load,
            "expected INSTALL cache_httpfs to appear before LOAD cache_httpfs"
        );
    }
}
