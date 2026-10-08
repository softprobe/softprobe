//! Backend-neutral end-to-end promotion contract.
//!
//! PostgreSQL and SQLite fixtures implement only setup/query primitives; every lifecycle scenario,
//! manifest, OTLP request, and assertion is defined once here.

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

pub const MANIFEST_V1: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: service_name
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: service.name
"#;

pub const MANIFEST_V2: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: service_name
    type: string
    nullable: true
    source:
      from: resource_attribute
      key: service.name
  - name: division_name
    type: string
    nullable: true
    source:
      from: attribute
      key: division.name
"#;

pub const BUSINESS_V1: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
"#;

pub const BUSINESS_V1_INCOMPATIBLE: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: total_cents
    type: string
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
"#;

pub const BUSINESS_V1_ADDITIVE: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
  - name: coupon_code
    type: string
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.coupon
"#;

pub const BUSINESS_V1_REQUIRED_ADDITIVE: &str = r#"
specVersion: softprobe.promotion.v1
target:
  kind: business_table
  table: checkout_orders
  version: 1
rowSelector:
  attribute:
    key: sp.workflow
    equals: checkout
columns:
  - name: total_cents
    type: int64
    nullable: true
    source:
      from: http_response_body
      json_path: $.order.total_cents
  - name: order_id
    type: string
    nullable: false
    source:
      from: attribute
      key: order.id
"#;

#[async_trait]
pub trait PromotionContractBackend {
    fn router(&self) -> &Router;
    fn bearer_token(&self) -> Option<&str>;

    async fn query_promoted(&self, session_id: &str, columns: &[&str]) -> Vec<Option<String>>;
    async fn column_exists(&self, table: &str, column: &str) -> bool;
    async fn active_telemetry_count(&self) -> i64;
    async fn inactive_telemetry_count(&self) -> i64;
}

pub async fn apply_manifest<B: PromotionContractBackend + Sync>(
    backend: &B,
    manifest_yaml: &str,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/promotions/apply")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = backend.bearer_token() {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = backend
        .router()
        .clone()
        .oneshot(
            builder
                .body(Body::from(
                    serde_json::to_vec(&json!({ "manifestYaml": manifest_yaml })).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .expect("apply request");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("apply body")
        .to_bytes();
    let json = serde_json::from_slice(&body)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&body) }));
    (status, json)
}

pub async fn ingest_otlp<B: PromotionContractBackend + Sync>(
    backend: &B,
    session_id: &str,
    service_name: &str,
    division_name: Option<&str>,
) -> StatusCode {
    let mut span_attributes = vec![string_kv("sp.session.id", session_id)];
    if let Some(division) = division_name {
        span_attributes.push(string_kv("division.name", division));
    }
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", service_name),
                    string_kv("sp.app.id", "promotion-contract"),
                ],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "promotion.contract".to_string(),
                    version: "1.0.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: Uuid::new_v4().as_bytes().to_vec(),
                    span_id: Uuid::new_v4().as_bytes()[..8].to_vec(),
                    name: "promotion_contract_span".to_string(),
                    kind: span::SpanKind::Internal as i32,
                    start_time_unix_nano: 1_640_995_200_000_000_000,
                    end_time_unix_nano: 1_640_995_260_000_000_000,
                    attributes: span_attributes,
                    status: Some(Status {
                        code: 1,
                        message: String::new(),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let mut body = Vec::new();
    request.encode(&mut body).expect("encode OTLP");
    let mut builder = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header(header::CONTENT_TYPE, "application/x-protobuf");
    if let Some(token) = backend.bearer_token() {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    backend
        .router()
        .clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .expect("ingest request")
        .status()
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

pub async fn contract_apply_ingest_query<B: PromotionContractBackend + Sync>(backend: &B) {
    let (status, body) = apply_manifest(backend, MANIFEST_V1).await;
    assert_eq!(status, StatusCode::OK, "apply failed: {body}");
    assert!(backend.column_exists("traces", "service_name").await);
    let session = format!("contract-apply-{}", Uuid::new_v4());
    assert_eq!(
        ingest_otlp(backend, &session, "checkout-api", None).await,
        StatusCode::OK
    );
    let values = backend.query_promoted(&session, &["service_name"]).await;
    assert_eq!(values[0].as_deref(), Some("checkout-api"));
    assert_eq!(backend.active_telemetry_count().await, 1);
}

pub async fn contract_update_and_idempotency<B: PromotionContractBackend + Sync>(backend: &B) {
    assert_eq!(apply_manifest(backend, MANIFEST_V1).await.0, StatusCode::OK);
    assert_eq!(apply_manifest(backend, MANIFEST_V1).await.0, StatusCode::OK);
    assert_eq!(backend.active_telemetry_count().await, 1);
    assert_eq!(backend.inactive_telemetry_count().await, 0);

    assert_eq!(apply_manifest(backend, MANIFEST_V2).await.0, StatusCode::OK);
    assert!(backend.column_exists("traces", "service_name").await);
    assert!(backend.column_exists("traces", "division_name").await);
    assert_eq!(backend.active_telemetry_count().await, 1);
    assert_eq!(backend.inactive_telemetry_count().await, 1);

    let session = format!("contract-v2-{}", Uuid::new_v4());
    assert_eq!(
        ingest_otlp(backend, &session, "checkout-api", Some("payments")).await,
        StatusCode::OK
    );
    let values = backend
        .query_promoted(&session, &["service_name", "division_name"])
        .await;
    assert_eq!(values[0].as_deref(), Some("checkout-api"));
    assert_eq!(values[1].as_deref(), Some("payments"));

    assert_eq!(apply_manifest(backend, MANIFEST_V2).await.0, StatusCode::OK);
    assert_eq!(backend.active_telemetry_count().await, 1);
}

pub async fn contract_shrink_safe<B: PromotionContractBackend + Sync>(backend: &B) {
    assert_eq!(apply_manifest(backend, MANIFEST_V2).await.0, StatusCode::OK);
    assert_eq!(
        ingest_otlp(backend, "contract-wide", "checkout-api", Some("payments")).await,
        StatusCode::OK
    );
    assert_eq!(apply_manifest(backend, MANIFEST_V1).await.0, StatusCode::OK);
    let narrow = format!("contract-narrow-{}", Uuid::new_v4());
    assert_eq!(
        ingest_otlp(backend, &narrow, "checkout-api", Some("payments")).await,
        StatusCode::OK
    );
    let values = backend
        .query_promoted(&narrow, &["service_name", "division_name"])
        .await;
    assert_eq!(values[0].as_deref(), Some("checkout-api"));
    assert_eq!(values[1], None);
}

pub async fn contract_business_compatibility<B: PromotionContractBackend + Sync>(backend: &B) {
    let (initial, body) = apply_manifest(backend, BUSINESS_V1).await;
    assert_eq!(initial, StatusCode::OK, "business apply failed: {body}");
    assert!(
        backend
            .column_exists("checkout_orders_v1", "total_cents")
            .await
    );

    let (bad, body) = apply_manifest(backend, BUSINESS_V1_INCOMPATIBLE).await;
    assert_eq!(bad, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "business_column_type_changed");

    let (required, body) = apply_manifest(backend, BUSINESS_V1_REQUIRED_ADDITIVE).await;
    assert_eq!(required, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "business_required_column_added");

    let (additive, body) = apply_manifest(backend, BUSINESS_V1_ADDITIVE).await;
    assert_eq!(additive, StatusCode::OK, "{body}");
    assert!(
        backend
            .column_exists("checkout_orders_v1", "coupon_code")
            .await
    );
}
