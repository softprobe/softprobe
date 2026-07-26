//! PostgreSQL adapter for the shared promotion E2E contract.

use async_trait::async_trait;
use axum::middleware::from_fn_with_state;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::{create_router, ControlPlaneRuntime};
use softprobe_runtime::authn::Resolver;
use softprobe_runtime::config::Config;
use softprobe_runtime::ingest_engine::IngestPipeline;
use softprobe_runtime::runtime_api::{
    runtime_auth_middleware, runtime_control_routes, runtime_post_v1_traces,
};
use softprobe_runtime::runtime_engine::{DuckLakeScopeResolver, ScopeProvisioningRequest};
use softprobe_runtime::session_redis::RedisStore;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio_postgres::NoTls;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::util::promotion_contract::{
    contract_apply_ingest_query, contract_business_compatibility, contract_shrink_safe,
    contract_update_and_idempotency, PromotionContractBackend,
};

const POSTGRES_DSN: &str =
    "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake";

struct PostgresBackend {
    _temp: TempDir,
    _mock: MockServer,
    router: Router,
    metadata_path: String,
    data_path: String,
    metadata_schema: String,
    api_key: String,
}

async fn setup() -> PostgresBackend {
    let mock = MockServer::start().await;
    let temp = TempDir::new().expect("tempdir");
    let suffix = Uuid::new_v4().simple().to_string();
    let short = &suffix[..8];
    let tenant_id = format!("tenant-promo-{short}");
    let metadata_schema = format!("sp_promo_data_{short}");
    let data_path = temp
        .path()
        .join("tenant-data")
        .to_string_lossy()
        .to_string();

    let mut config = Config::default();
    config.maintenance.enabled = false;
    config.maintenance.metadata_enabled = false;
    config.query.cache_dir = Some(temp.path().join("cache").to_string_lossy().into());
    config.ducklake.catalog_type = "postgres".to_string();
    config.ducklake.metadata_path = POSTGRES_DSN.to_string();
    config.ducklake.catalog_alias = "softprobe".to_string();
    config.ducklake.metadata_schema = format!("sp_promo_reg_{short}");
    config.ducklake.data_path = data_path.clone();
    config.ducklake.data_inlining_row_limit = Some(0);

    let resolver = DuckLakeScopeResolver::connect(&config)
        .await
        .expect("resolver")
        .expect("postgres resolver");
    resolver
        .provision_scope(ScopeProvisioningRequest {
            scope_id: tenant_id.clone(),
            metadata_schema: metadata_schema.clone(),
            data_path: data_path.clone(),
        })
        .await
        .expect("provision tenant");

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "data": { "tenantId": tenant_id, "resources": [] }
        })))
        .mount(&mock)
        .await;

    let pipeline = IngestPipeline::new(&config).await.expect("pipeline");
    let query_engine =
        softprobe_runtime::query::create_query_engine(&config, Arc::new(pipeline.storage.clone()))
            .await
            .expect("query engine");
    let redis_port = crate::util::redis::test_redis_port();
    let redis = RedisStore::connect_host_port("127.0.0.1", redis_port, None, Duration::from_secs(3600))
        .await
        .unwrap_or_else(|e| {
            panic!("redis from make setup-local (127.0.0.1:{redis_port}): {e}")
        });
    let control = ControlPlaneRuntime {
        resolver: Resolver::new(format!("{}/", mock.uri()), Duration::from_secs(60)),
        session_store: Arc::new(tokio::sync::Mutex::new(redis)),
    };
    let metadata_path = config.ducklake.metadata_path.clone();
    let (router, state) = create_router(
        Arc::new(config),
        pipeline.storage,
        query_engine,
        post(runtime_post_v1_traces),
        Some(control),
        None,
    )
    .await
    .expect("router");
    let router = router
        .merge(runtime_control_routes().with_state(state.clone()))
        .layer(from_fn_with_state(state, runtime_auth_middleware));

    PostgresBackend {
        _temp: temp,
        _mock: mock,
        router,
        metadata_path,
        data_path,
        metadata_schema,
        api_key: "promotion-contract-key".to_string(),
    }
}

impl PostgresBackend {
    fn attach(&self) -> duckdb::Connection {
        let connection = duckdb::Connection::open_in_memory().expect("duckdb");
        connection
            .execute_batch("INSTALL ducklake; INSTALL postgres; LOAD postgres;")
            .expect("extensions");
        connection
            .execute_batch(&format!(
                "ATTACH 'ducklake:postgres:{}' AS softprobe \
                 (DATA_PATH '{}', METADATA_SCHEMA '{}', META_SCHEMA '{}', \
                  DATA_INLINING_ROW_LIMIT 0);",
                self.metadata_path.replace('\'', "''"),
                self.data_path.replace('\'', "''"),
                self.metadata_schema.replace('\'', "''"),
                self.metadata_schema.replace('\'', "''"),
            ))
            .expect("attach");
        connection
    }

    async fn count_specs(&self, status: &str) -> i64 {
        let (client, connection) = tokio_postgres::connect(POSTGRES_DSN, NoTls)
            .await
            .expect("postgres");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .query_one(
                &format!(
                    r#"SELECT count(*)::bigint FROM "{}".promotion_specs
                       WHERE status = $1 AND target_kind = 'telemetry_columns'"#,
                    self.metadata_schema
                ),
                &[&status],
            )
            .await
            .expect("count specs")
            .get(0)
    }
}

#[async_trait]
impl PromotionContractBackend for PostgresBackend {
    fn router(&self) -> &Router {
        &self.router
    }

    fn bearer_token(&self) -> Option<&str> {
        Some(&self.api_key)
    }

    async fn query_promoted(&self, session_id: &str, columns: &[&str]) -> Vec<Option<String>> {
        let connection = self.attach();
        let sql = format!(
            "SELECT {} FROM softprobe.{}.traces WHERE session_id = '{}'",
            columns.join(", "),
            self.metadata_schema,
            session_id.replace('\'', "''")
        );
        connection
            .query_row(&sql, [], |row| {
                (0..columns.len())
                    .map(|index| row.get::<_, Option<String>>(index))
                    .collect()
            })
            .expect("query promoted columns")
    }

    async fn column_exists(&self, table: &str, column: &str) -> bool {
        let connection = self.attach();
        let sql = format!(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_catalog = 'softprobe' AND table_schema = '{}' \
             AND table_name = '{table}' AND column_name = '{column}'",
            self.metadata_schema
        );
        connection
            .query_row(&sql, [], |row| row.get::<_, i64>(0))
            .unwrap_or(0)
            > 0
    }

    async fn active_telemetry_count(&self) -> i64 {
        self.count_specs("active").await
    }

    async fn inactive_telemetry_count(&self) -> i64 {
        self.count_specs("inactive").await
    }
}

#[tokio::test]
async fn postgres_apply_ingest_query_contract() {
    contract_apply_ingest_query(&setup().await).await;
}

#[tokio::test]
async fn postgres_update_idempotency_contract() {
    contract_update_and_idempotency(&setup().await).await;
}

#[tokio::test]
async fn postgres_shrink_contract() {
    contract_shrink_safe(&setup().await).await;
}

#[tokio::test]
async fn postgres_business_contract() {
    contract_business_compatibility(&setup().await).await;
}
