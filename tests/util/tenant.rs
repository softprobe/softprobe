//! Shared axum middleware that injects a local-SQLite tenant for router-level tests.

use axum::middleware::Next;
use softprobe_runtime::authn::TenantInfo;

pub const LOCAL_SQLITE_TENANT_ID: &str = "local-sqlite-tenant";

/// Header used by multi-tenant Prom isolation tests to select the injected tenant.
pub const TEST_TENANT_HEADER: &str = "x-test-tenant-id";

pub async fn inject_local_sqlite_tenant(
    mut request: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    let tenant_id = request
        .headers()
        .get(TEST_TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or(LOCAL_SQLITE_TENANT_ID)
        .to_string();
    request.extensions_mut().insert(TenantInfo {
        tenant_id,
        bucket_name: String::new(),
        dataset_id: String::new(),
    });
    next.run(request).await
}
