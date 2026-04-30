use axum::extract::DefaultBodyLimit;
use axum::middleware::from_fn_with_state;
use axum::routing::post;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::api::{self, HostedRuntime};
use softprobe_runtime::authn::Resolver;
use softprobe_runtime::config::Config;
use softprobe_runtime::grpc_otlp;
use softprobe_runtime::hosted::{hosted_auth_middleware, hosted_post_v1_traces, hosted_routes};
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::session_redis::RedisStore;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, decompression::RequestDecompressionLayer, trace::TraceLayer};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "softprobe_runtime=info,tower_http=info".into()),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting softprobe-runtime v{}", env!("CARGO_PKG_VERSION"));

    let config = Config::load()?;
    info!("Configuration loaded");

    let pipeline = IngestPipeline::new(&config).await?;
    let storage = pipeline.storage.clone();
    let query_engine =
        softprobe_runtime::query::create_query_engine(&config, Arc::new(storage.clone())).await?;

    if let Some(_handle) =
        softprobe_runtime::compaction::scheduler::start_maintenance_scheduler(&config).await?
    {
        info!("Maintenance scheduler started");
    }

    let hosted = hosted_runtime_from_env().await?;

    let traces = if hosted.is_some() {
        post(hosted_post_v1_traces)
    } else {
        post(ingest_traces)
    };

    let (mut app, state) = api::create_router(
        storage.clone(),
        query_engine,
        Some(pipeline.storage.span_buffer.clone()),
        Some(pipeline.storage.log_buffer.clone()),
        Some(pipeline.storage.metric_buffer.clone()),
        traces,
        hosted.clone(),
    )
    .await?;

    if hosted.is_some() {
        app = app.merge(hosted_routes().with_state(state.clone()));
    }

    let app = app
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive())
                .layer(RequestDecompressionLayer::new())
                .layer(DefaultBodyLimit::max(config.server.max_body_size))
                .into_inner(),
        )
        .layer(from_fn_with_state(state.clone(), hosted_auth_middleware));

    // OTLP/gRPC (4317). Set `SOFTPROBE_GRPC_DISABLE=1` to skip (e.g. port conflicts in tests).
    if !std::env::var("SOFTPROBE_GRPC_DISABLE")
        .map(|v| v == "1")
        .unwrap_or(false)
    {
        let grpc_port: u16 = std::env::var("OTEL_GRPC_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4317);
        let grpc_addr = SocketAddr::from(([0, 0, 0, 0], grpc_port));
        let st = state.clone();
        tokio::spawn(async move {
            if let Err(e) = grpc_otlp::run_trace_grpc_server(grpc_addr, st).await {
                tracing::error!("gRPC server exited: {e}");
            }
        });
    }

    let listen: SocketAddr = std::env::var("SOFTPROBE_LISTEN_ADDR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], config.server.port)));

    info!("HTTP listening on {listen}");
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn hosted_runtime_from_env() -> anyhow::Result<Option<HostedRuntime>> {
    let auth = match std::env::var("SOFTPROBE_AUTH_URL") {
        Ok(v) if !v.is_empty() => v,
        _ => return Ok(None),
    };
    let redis_host = match std::env::var("REDIS_HOST") {
        Ok(v) if !v.is_empty() => v,
        _ => return Ok(None),
    };
    let port: u16 = std::env::var("REDIS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6379);
    let pw = std::env::var("REDIS_PASSWORD").ok().filter(|s| !s.is_empty());
    let store =
        RedisStore::connect_host_port(&redis_host, port, pw.as_deref(), "global", Duration::from_secs(86_400))
            .await?;
    let resolver = Resolver::new(auth, Duration::from_secs(60));
    Ok(Some(HostedRuntime {
        resolver,
        session_store: Arc::new(tokio::sync::Mutex::new(store)),
    }))
}
