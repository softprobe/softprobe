//! Integration-style tests for [`softprobe_runtime::authn::Resolver`] against a stub HTTP service.

use softprobe_runtime::authn::Resolver;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn resolve_empty_key_fails() {
    let server = MockServer::start().await;
    let r = Resolver::new(server.uri(), Duration::from_secs(60));
    let err = r.resolve("").await.expect_err("empty key");
    assert!(
        err.to_string().contains("empty API key"),
        "{err}"
    );
}

#[tokio::test]
async fn resolve_calls_auth_json_contract() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "data": {
                "tenantId": "tenant-a",
                "resources": [{
                    "resourceType": "BIGQUERY_STORAGE",
                    "configJson": "{\"dataset_id\":\"ds1\",\"bucket_name\":\"bucket-x\"}"
                }]
            }
        })))
        .mount(&server)
        .await;

    let r = Resolver::new(format!("{}/", server.uri()), Duration::from_secs(60));
    let info = r.resolve("secret-key").await.expect("resolve");
    assert_eq!(info.tenant_id, "tenant-a");
    assert_eq!(info.dataset_id, "ds1");
    assert_eq!(info.bucket_name, "bucket-x");

    // Cache hit — no extra HTTP request if second resolve fast enough
    let info2 = r.resolve("secret-key").await.expect("cached");
    assert_eq!(info2.tenant_id, "tenant-a");
}

#[tokio::test]
async fn resolve_invalid_success_payload_fails() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": false
        })))
        .mount(&server)
        .await;

    let r = Resolver::new(format!("{}/", server.uri()), Duration::from_secs(60));
    let err = r.resolve("k").await.expect_err("invalid key");
    assert!(
        err.to_string().contains("invalid API key"),
        "{err}"
    );
}
