use axum::extract::DefaultBodyLimit;
use axum::middleware::from_fn_with_state;
use axum::routing::post;
use softprobe_runtime::api::{self, ControlPlaneRuntime};
use softprobe_runtime::authn::Resolver;
use softprobe_runtime::config::Config;
use softprobe_runtime::grpc_otlp;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::runtime_api::{
    runtime_auth_middleware, runtime_control_routes, runtime_post_v1_traces,
};
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

    let config = Arc::new(Config::load()?);
    info!("Configuration loaded");

    let pipeline = IngestPipeline::new(config.as_ref()).await?;
    let storage = pipeline.storage.clone();
    let dropdown_catalog = pipeline.dropdown_catalog.clone();
    let query_engine =
        softprobe_runtime::query::create_query_engine(config.as_ref(), Arc::new(storage.clone()))
            .await?;

    if let Some(_handle) = softprobe_runtime::compaction::scheduler::start_maintenance_scheduler(
        config.as_ref(),
        dropdown_catalog.clone(),
    )
    .await?
    {
        info!("Maintenance scheduler started");
    }

    let control_plane = control_plane_runtime_from_env(config.clone()).await?;
    let traces = post(runtime_post_v1_traces);

    let (mut app, state) = api::create_router(
        config.clone(),
        storage.clone(),
        query_engine,
        traces,
        Some(control_plane.clone()),
        dropdown_catalog,
    )
    .await?;
    app = app.merge(runtime_control_routes().with_state(state.clone()));

    let app = app
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive())
                .layer(RequestDecompressionLayer::new())
                .layer(DefaultBodyLimit::max(config.server.max_body_size))
                .into_inner(),
        )
        .layer(from_fn_with_state(state.clone(), runtime_auth_middleware));

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

async fn control_plane_runtime_from_env(
    _config: Arc<Config>,
) -> anyhow::Result<ControlPlaneRuntime> {
    let redis_host = required_env("REDIS_HOST")?;
    let port: u16 = std::env::var("REDIS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6379);
    let pw = std::env::var("REDIS_PASSWORD")
        .ok()
        .filter(|s| !s.is_empty());
    let store = RedisStore::connect_host_port(
        &redis_host,
        port,
        pw.as_deref(),
        Duration::from_secs(86_400),
    )
    .await?;
    let auth_url = match optional_env("SOFTPROBE_AUTH_URL") {
        Some(url) => url,
        None => {
            const DEFAULT_LOCAL_AUTH: &str = "http://127.0.0.1:8091/validate";
            info!(
                "SOFTPROBE_AUTH_URL not set; using default local auth stub {}",
                DEFAULT_LOCAL_AUTH
            );
            DEFAULT_LOCAL_AUTH.to_string()
        }
    };
    let resolver = Resolver::new(auth_url, Duration::from_secs(60));
    Ok(ControlPlaneRuntime {
        resolver,
        session_store: Arc::new(tokio::sync::Mutex::new(store)),
    })
}

fn required_env(name: &str) -> anyhow::Result<String> {
    let value = std::env::var(name).unwrap_or_default();
    let value = value.trim().to_string();
    if value.is_empty() {
        anyhow::bail!("{name} is required in control-plane-only runtime mode");
    }
    Ok(value)
}

fn optional_env(name: &str) -> Option<String> {
    let value = std::env::var(name).unwrap_or_default();
    let value = value.trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}
