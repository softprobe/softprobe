//! Shared axum middleware that injects a fixed local-SQLite tenant for router-level promotion tests.

use axum::middleware::Next;
use softprobe_runtime::authn::TenantInfo;

pub const LOCAL_SQLITE_TENANT_ID: &str = "local-sqlite-tenant";

pub async fn inject_local_sqlite_tenant(
    mut request: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    request.extensions_mut().insert(TenantInfo {
        tenant_id: LOCAL_SQLITE_TENANT_ID.to_string(),
        bucket_name: String::new(),
        dataset_id: String::new(),
    });
    next.run(request).await
}
