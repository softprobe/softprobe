//! Shared [`softprobe_runtime::config::Config`] for integration tests (local DuckLake, no MinIO).

use softprobe_runtime::config::Config;
use tempfile::TempDir;

/// Minimal OSS-style config: file-backed DuckLake under `temp`, maintenance/compaction off for quiet tests.
pub fn file_backed_oss_config(temp: &TempDir) -> Config {
    let mut config = Config::default();
    config.compaction.enabled = false;
    config.compaction.metadata_maintenance_enabled = false;
    config.ingest_engine.cache_dir = Some(
        temp.path()
            .join("cache")
            .to_string_lossy()
            .into_owned(),
    );
    config.ingest_engine.wal_dir = Some(
        temp.path()
            .join("wal")
            .to_string_lossy()
            .into_owned(),
    );
    config.ingest_engine.optimizer_interval_seconds = 3600;
    config.span_buffering.max_buffer_spans = 10_000;
    config.span_buffering.flush_interval_seconds = 3600;

    let duck_dir = temp.path().join("ducklake");
    std::fs::create_dir_all(duck_dir.join("data")).expect("ducklake data");

    let mut dl = config.ducklake_or_default();
    dl.metadata_path = duck_dir
        .join("metadata.ducklake")
        .to_string_lossy()
        .into_owned();
    dl.data_path = duck_dir
        .join("data")
        .to_string_lossy()
        .into_owned()
        + "/";
    config.ducklake = Some(dl);

    config
}
