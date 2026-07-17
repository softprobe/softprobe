//! Shared [`softprobe_runtime::config::Config`] for integration tests (local DuckLake, no MinIO).

use softprobe_runtime::config::Config;
use tempfile::TempDir;

/// Minimal file-backed DuckLake config under `temp`, maintenance/compaction off for quiet tests.
pub fn file_backed_test_config(temp: &TempDir) -> Config {
    let mut config = Config::default();
    config.compaction.enabled = false;
    config.compaction.metadata_maintenance_enabled = false;
    config.ingest_engine.cache_dir = Some(temp.path().join("cache").to_string_lossy().into_owned());

    let duck_dir = temp.path().join("ducklake");
    std::fs::create_dir_all(duck_dir.join("data")).expect("ducklake data");

    let mut dl = config.ducklake_or_default();
    dl.catalog_type = "sqlite".to_string();
    dl.metadata_path = duck_dir
        .join("metadata.sqlite")
        .to_string_lossy()
        .into_owned();
    dl.data_path = duck_dir.join("data").to_string_lossy().into_owned() + "/";
    config.ducklake = Some(dl);

    config
}
