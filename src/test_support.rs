//! Local router + config helpers for `#[cfg(test)]` modules (`make test-quick` / `cargo test --lib` / `llvm-cov --lib`).
//! Mirrors `tests/util/config.rs` so unit tests do not depend on the integration-test crate.

use crate::api::ingestion::traces::ingest_traces;
use crate::api::{create_router, AppPipeline, AppState};
use crate::config::Config;
use axum::routing::post;
use axum::Router;
use std::sync::Arc;
use tempfile::TempDir;

/// File-backed DuckLake under `temp`; compaction and metadata maintenance disabled.
pub fn file_backed_test_config(temp: &TempDir) -> Config {
    let mut config = Config::default();
    config.maintenance.enabled = false;
    config.maintenance.metadata_enabled = false;
    config.query.cache_dir = Some(temp.path().join("cache").to_string_lossy().into_owned());

    let duck_dir = temp.path().join("ducklake");
    std::fs::create_dir_all(duck_dir.join("data")).expect("ducklake data");

    config.ducklake.catalog_type = "sqlite".to_string();
    config.ducklake.metadata_path = duck_dir
        .join("metadata.sqlite")
        .to_string_lossy()
        .into_owned();
    config.ducklake.data_path = duck_dir.join("data").to_string_lossy().into_owned() + "/";

    config
}

/// Router + [`AppState`] from [`create_router`] (same wiring as `AppPipeline::into_router`).
pub async fn local_router_and_state() -> anyhow::Result<(Router, AppState, TempDir)> {
    let temp = TempDir::new()?;
    let config = file_backed_test_config(&temp);
    let config = Arc::new(config);
    let app = AppPipeline::new(config.as_ref()).await?;
    let (router, state) = create_router(
        config.clone(),
        app.storage,
        app.query_engine,
        post(ingest_traces),
        None,
        None,
    )
    .await?;
    Ok((router, state, temp))
}

/// Builds the local test router (same as `AppPipeline::into_router()`).
pub async fn local_router() -> anyhow::Result<(Router, TempDir)> {
    let (router, _, temp) = local_router_and_state().await?;
    Ok((router, temp))
}

/// [`crate::storage::Storage`] for unit tests that need storage without building a router.
pub async fn sample_storage() -> anyhow::Result<(crate::storage::Storage, TempDir)> {
    let temp = TempDir::new()?;
    let config = file_backed_test_config(&temp);
    let app = AppPipeline::new(&config).await?;
    Ok((app.storage, temp))
}
