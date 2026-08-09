//! SQLite adapter for the shared promotion E2E contract (no external infrastructure).

use async_trait::async_trait;
use axum::middleware::from_fn;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::config::Config;
use softprobe_runtime::runtime_api::runtime_control_routes;
use std::sync::Arc;
use tempfile::TempDir;

use crate::util::config::file_backed_test_config;
use crate::util::promotion_contract::{
    apply_manifest, contract_apply_ingest_query, contract_business_compatibility,
    contract_shrink_safe, contract_update_and_idempotency, ingest_otlp, PromotionContractBackend,
    MANIFEST_V1,
};
use crate::util::tenant::inject_local_sqlite_tenant as inject_tenant;

struct SqliteBackend {
    _temp: TempDir,
    router: Router,
    metadata_path: String,
    data_path: String,
}

async fn build_router(config: Config) -> Router {
    let (router, state) =
        softprobe_runtime::api::create_router(Arc::new(config), post(ingest_traces), None)
            .await
            .expect("router");
    router
        .merge(runtime_control_routes().with_state(state))
        .layer(from_fn(inject_tenant))
}

async fn setup() -> SqliteBackend {
    let temp = TempDir::new().expect("tempdir");
    let config = file_backed_test_config(&temp);
    let metadata_path = config.ducklake.metadata_path.clone();
    let data_path = config.ducklake.data_path.clone();
    let router = build_router(config).await;
    SqliteBackend {
        _temp: temp,
        router,
        metadata_path,
        data_path,
    }
}

impl SqliteBackend {
    fn attach(&self) -> duckdb::Connection {
        attach(&self.metadata_path, &self.data_path)
    }
}

fn attach(metadata_path: &str, data_path: &str) -> duckdb::Connection {
    let connection = duckdb::Connection::open_in_memory().expect("duckdb");
    connection
        .execute_batch("INSTALL ducklake; INSTALL sqlite; LOAD ducklake; LOAD sqlite;")
        .expect("extensions");
    connection
        .execute_batch(&format!(
            "ATTACH 'ducklake:sqlite:{}' AS softprobe \
             (DATA_PATH '{}', META_JOURNAL_MODE 'WAL', META_BUSY_TIMEOUT 5000, \
              DATA_INLINING_ROW_LIMIT 0);",
            metadata_path.replace('\'', "''"),
            data_path.replace('\'', "''"),
        ))
        .expect("attach");
    connection
}

#[async_trait]
impl PromotionContractBackend for SqliteBackend {
    fn router(&self) -> &Router {
        &self.router
    }

    fn bearer_token(&self) -> Option<&str> {
        None
    }

    async fn query_promoted(&self, session_id: &str, columns: &[&str]) -> Vec<Option<String>> {
        let connection = self.attach();
        let sql = format!(
            "SELECT {} FROM softprobe.traces WHERE session_id = '{}'",
            columns.join(", "),
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
             WHERE table_catalog = 'softprobe' AND table_name = '{table}' \
             AND column_name = '{column}'"
        );
        connection
            .query_row(&sql, [], |row| row.get::<_, i64>(0))
            .unwrap_or(0)
            > 0
    }

    async fn active_telemetry_count(&self) -> i64 {
        count_specs(&self.attach(), "active")
    }

    async fn inactive_telemetry_count(&self) -> i64 {
        count_specs(&self.attach(), "inactive")
    }
}

fn count_specs(connection: &duckdb::Connection, status: &str) -> i64 {
    connection
        .query_row(
            &format!(
                "SELECT count(*) FROM softprobe.promotion_specs \
                 WHERE status = '{status}' AND target_kind = 'telemetry_columns'"
            ),
            [],
            |row| row.get(0),
        )
        .unwrap_or(0)
}

#[tokio::test]
async fn sqlite_apply_ingest_query_contract() {
    contract_apply_ingest_query(&setup().await).await;
}

#[tokio::test]
async fn sqlite_update_idempotency_contract() {
    contract_update_and_idempotency(&setup().await).await;
}

#[tokio::test]
async fn sqlite_shrink_contract() {
    contract_shrink_safe(&setup().await).await;
}

#[tokio::test]
async fn sqlite_business_contract() {
    contract_business_compatibility(&setup().await).await;
}

#[tokio::test]
async fn sqlite_specs_persist_across_writer_rebuild() {
    let backend = setup().await;
    assert_eq!(
        apply_manifest(&backend, MANIFEST_V1).await.0,
        axum::http::StatusCode::OK
    );
    let replacement = SqliteBackend {
        _temp: backend._temp,
        router: build_router({
            let mut config = Config::default();
            config.maintenance.enabled = false;
            config.maintenance.metadata_enabled = false;
            config.query.cache_dir = Some(
                std::env::temp_dir()
                    .join("sqlite-promo-rebuild-cache")
                    .to_string_lossy()
                    .into(),
            );
            config.ducklake.catalog_type = "sqlite".to_string();
            config.ducklake.metadata_path = backend.metadata_path.clone();
            config.ducklake.data_path = backend.data_path.clone();
            config
        })
        .await,
        metadata_path: backend.metadata_path,
        data_path: backend.data_path,
    };
    assert_eq!(replacement.active_telemetry_count().await, 1);
    assert_eq!(
        ingest_otlp(&replacement, "persisted-contract", "checkout-api", None).await,
        axum::http::StatusCode::OK
    );
    assert_eq!(
        replacement
            .query_promoted("persisted-contract", &["service_name"])
            .await[0]
            .as_deref(),
        Some("checkout-api")
    );
}
