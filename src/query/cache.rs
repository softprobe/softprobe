use crate::config::Config;
use anyhow::{anyhow, Result};
use duckdb::{Connection, ToSql};
use once_cell::sync::Lazy;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

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
    conn.execute("SET external_file_cache_directory = ?;", [&cache_dir_str as &dyn ToSql])?;
    conn.execute("SET external_file_cache_size = '2GB';", [])?;
    conn.execute("SET s3_cache_directory = ?;", [&cache_dir_str as &dyn ToSql])?;
    conn.execute("SET s3_cache_size = '2GB';", [])?;
    conn.execute_batch("LOAD cache_httpfs;")?;
    conn.execute("SET cache_httpfs_cache_block_size = 8388608;", [])?;
    conn.execute("SET cache_httpfs_max_in_mem_cache_block_count = 4096;", [])?;
    conn.execute("SET cache_httpfs_disk_cache_reader_enable_memory_cache = 1;", [])?;
    conn.execute("SET cache_httpfs_disk_cache_reader_mem_cache_block_count = 4096;", [])?;
    conn.execute("SET cache_httpfs_cache_directory = ?;", [&cache_dir_str as &dyn ToSql])?;
    conn.execute("SET cache_httpfs_type = 'on_disk';", [])?;
    wrap_filesystem(conn)?;
    Ok(())
}

static CACHE_PROFILE_INIT: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

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
