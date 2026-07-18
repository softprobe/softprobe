use axum::routing::post;
use softprobe_runtime::api;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::config::Config;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::query;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use uuid::Uuid;

pub async fn start_test_server() -> (String, TempDir) {
    let mut config = Config::default();
    config.object_store.endpoint = Some("http://localhost:9000".to_string());
    config.object_store.region = "us-east-1".to_string();

    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    let cache_dir = TempDir::new().expect("tempdir");
    config.query.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
    let dl_dir = cache_dir.path().join("ducklake");
    std::fs::create_dir_all(&dl_dir).expect("ducklake dir");
    config.ducklake.catalog_type = "sqlite".to_string();
    config.ducklake.metadata_path = dl_dir
        .join(format!("metadata-{}.sqlite", Uuid::new_v4()))
        .to_string_lossy()
        .to_string();
    config.ducklake.data_path = dl_dir.join("data").to_string_lossy().to_string();
    std::fs::create_dir_all(&config.ducklake.data_path).expect("ducklake data dir");

    let config = Arc::new(config);
    let pipeline = IngestPipeline::new(config.as_ref())
        .await
        .expect("pipeline");
    let query_engine = query::create_query_engine(
        config.as_ref(),
        std::sync::Arc::new(pipeline.storage.clone()),
    )
    .await
    .expect("query engine");

    let (app, _) = api::create_router(
        config.clone(),
        pipeline.storage.clone(),
        query_engine,
        post(ingest_traces),
        None,
        None,
    )
    .await
    .expect("router");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let addr = listener.local_addr().expect("addr");
    let base_url = format!("http://{}", addr);

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    (base_url, cache_dir)
}
