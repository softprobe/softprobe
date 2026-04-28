use splake::api;
use splake::config::Config;
use splake::ingest_engine::IngestPipeline;
use splake::query;
use tempfile::TempDir;
use tokio::net::TcpListener;

pub async fn start_test_server() -> (String, TempDir) {
    let mut config = Config::default();
    config.iceberg.catalog_type = "rest".to_string();
    config.iceberg.catalog_uri = "http://localhost:8181/catalog".to_string();
    config.iceberg.warehouse = "default".to_string();
    config.iceberg.force_close_after_append = true;
    config.ingest_engine.optimizer_interval_seconds = 1;
    config.s3.endpoint = Some("http://localhost:9000".to_string());
    config.s3.access_key_id = Some("minioadmin".to_string());
    config.s3.secret_access_key = Some("minioadmin".to_string());
    config.storage.s3_region = "us-east-1".to_string();

    std::env::set_var("S3_ENDPOINT", "http://localhost:9000");
    std::env::set_var("S3_ACCESS_KEY", "minioadmin");
    std::env::set_var("S3_SECRET_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");

    let cache_dir = TempDir::new().expect("tempdir");
    config.ingest_engine.cache_dir = Some(cache_dir.path().to_string_lossy().to_string());
    config.ingest_engine.wal_dir =
        Some(cache_dir.path().join("wal").to_string_lossy().to_string());

    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let query_engine =
        query::create_query_engine(&config, std::sync::Arc::new(pipeline.storage.clone()))
            .await
            .expect("query engine");

    let app = api::create_router(
        pipeline.storage.clone(),
        query_engine,
        Some(pipeline.storage.span_buffer.clone()),
        Some(pipeline.storage.log_buffer.clone()),
        Some(pipeline.storage.metric_buffer.clone()),
    )
    .await
    .expect("router");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let addr = listener.local_addr().expect("addr");
    let base_url = format!("http://{}", addr);

    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .expect("serve");
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    (base_url, cache_dir)
}
