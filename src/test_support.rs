//! OSS router + config helpers for `#[cfg(test)]` modules (`cargo test --lib` / `llvm-cov --lib`).
//! Mirrors `tests/util/config.rs` so unit tests do not depend on the integration-test crate.

use crate::api::ingestion::traces::ingest_traces;
use crate::api::{create_router, AppPipeline, AppState};
use crate::config::Config;
use axum::routing::post;
use axum::Router;
use tempfile::TempDir;

/// File-backed DuckLake under `temp`; compaction and metadata maintenance disabled.
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

/// Router + [`AppState`] from [`create_router`] (same wiring as `AppPipeline::into_router`).
pub async fn oss_router_and_state() -> anyhow::Result<(Router, AppState, TempDir)> {
    let temp = TempDir::new()?;
    let config = file_backed_oss_config(&temp);
    let app = AppPipeline::new(&config).await?;
    let (router, state) = create_router(
        app.storage,
        app.query_engine,
        Some(app.span_buffer),
        Some(app.log_buffer),
        Some(app.metric_buffer),
        post(ingest_traces),
        None,
        None,
    )
    .await?;
    Ok((router, state, temp))
}

/// Builds the OSS router (same as `AppPipeline::into_router()`).
pub async fn oss_router() -> anyhow::Result<(Router, TempDir)> {
    let (router, _, temp) = oss_router_and_state().await?;
    Ok((router, temp))
}

/// [`crate::storage::Storage`] for unit tests that need tiered storage without building a router.
pub async fn sample_storage() -> anyhow::Result<(crate::storage::Storage, TempDir)> {
    let temp = TempDir::new()?;
    let config = file_backed_oss_config(&temp);
    let app = AppPipeline::new(&config).await?;
    Ok((app.storage, temp))
}
