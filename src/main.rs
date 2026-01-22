use axum::extract::DefaultBodyLimit;
use std::net::SocketAddr;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, decompression::RequestDecompressionLayer, trace::TraceLayer};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

pub mod api;
pub mod compaction;
pub mod config;
pub mod ingest_engine;
pub mod models;
pub mod query;
pub mod storage;

use config::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "datalake=info,tower_http=info".into()),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!(
        "Starting SoftProbe OTLP Backend v{}",
        env!("CARGO_PKG_VERSION")
    );

    // Load configuration
    let config = Config::load()?;
    info!("Configuration loaded: {:?}", config);

    // Initialize storage components
    let storage = storage::create_storage(&config).await?;
    let query_engine = query::create_query_engine(&config).await?;

    // Initialize span buffer with Iceberg writer
    let span_buffer = storage::create_span_buffer(
        &config,
        storage.iceberg_writer.clone(),
        storage.ingest_engine.clone(),
    )
    .await?;

    // Initialize log buffer with Iceberg writer
    let log_buffer = storage::create_log_buffer(
        &config,
        storage.iceberg_writer.clone(),
        storage.ingest_engine.clone(),
    )
    .await?;

    // Initialize metric buffer with Iceberg writer
    let metric_buffer = storage::create_metric_buffer(
        &config,
        storage.iceberg_writer.clone(),
        storage.ingest_engine.clone(),
    )
    .await?;

    // Start background maintenance jobs (metadata + compaction)
    if let Some(_handle) = compaction::scheduler::start_maintenance_scheduler(&config).await? {
        info!("Maintenance scheduler started");
    }

    // Initialize API handlers
    let app = api::create_router(
        storage,
        query_engine,
        Some(span_buffer),
        Some(log_buffer),
        Some(metric_buffer),
    )
    .await?
    .layer(
        ServiceBuilder::new()
            .layer(TraceLayer::new_for_http())
            .layer(CorsLayer::permissive())
            // Decompress request bodies (gzip, deflate, brotli, zstd)
            .layer(RequestDecompressionLayer::new())
            .layer(DefaultBodyLimit::max(config.server.max_body_size))
            .into_inner(),
    );

    // Start HTTP server
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server.port));
    info!("Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("OTLP collector service initialized successfully");

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}
