//! Lib-only coverage that is not duplicated by `tests/integration/http_api.rs`.
//! HTTP status/envelope smoke lives in `http_api`; keep RuntimeEngine, score
//! validation, OpenAPI schema, SQL-error, and pure telemetry compile checks here.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::api::telemetry::{
    compile_details_sql, compile_search_sql, TelemetryDetailsTarget, TelemetryFilter,
    TelemetryFilterExpr, TelemetrySearchRequest, TelemetrySearchScope, TelemetrySort,
    TelemetrySortDirection, TelemetryTimeRange,
};
use crate::authn::TenantInfo;
use crate::test_support::local_router_and_state;
use std::sync::Arc;

fn test_tenant() -> TenantInfo {
    TenantInfo {
        tenant_id: "unit-test-tenant".to_string(),
        bucket_name: "unit-bucket".to_string(),
        dataset_id: "unit-dataset".to_string(),
    }
}

#[tokio::test]
async fn unit_runtime_engine_manager_cache_hit_same_arc() {
    let (_router, state, _t) = local_router_and_state().await.expect("router");
    let t = test_tenant();
    let e1 = state.engine_for_tenant(&t).await.expect("engine");
    let e2 = state.engine_for_tenant(&t).await.expect("engine");
    assert!(Arc::ptr_eq(&e1, &e2));
}

#[tokio::test]
async fn unit_runtime_engine_manager_single_flight_build_once() {
    let (_router, state, _t) = local_router_and_state().await.expect("router");
    let tenant_id = "unit-test-single-flight".to_string();
    let (a, b, c, d) = tokio::join!(
        state.engine_for_id(&tenant_id),
        state.engine_for_id(&tenant_id),
        state.engine_for_id(&tenant_id),
        state.engine_for_id(&tenant_id),
    );
    let e1 = a.expect("engine");
    let e2 = b.expect("engine");
    let e3 = c.expect("engine");
    let e4 = d.expect("engine");
    assert!(Arc::ptr_eq(&e1, &e2));
    assert!(Arc::ptr_eq(&e1, &e3));
    assert!(Arc::ptr_eq(&e1, &e4));
    assert_eq!(state.engines.build_count(), 1);
}

#[tokio::test]
async fn unit_score_create_is_validated_and_idempotent() {
    let (router, _state, _t) = local_router_and_state().await.expect("router");
    let body = json!({
        "score_id": "score-unit-1",
        "timestamp": "2026-07-18T23:22:00Z",
        "trace_id": "trace-unit-1",
        "span_id": null,
        "session_id": null,
        "name": "correctness",
        "data_type": "numeric",
        "numeric_value": 0.9,
        "string_value": null,
        "boolean_value": null,
        "source": "evaluator",
        "metadata": { "evaluator": "unit-test" }
    })
    .to_string();

    let create = || {
        Request::builder()
            .method("POST")
            .uri("/v1/llm/scores")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.clone()))
            .unwrap()
    };
    let first = router
        .clone()
        .oneshot(create())
        .await
        .expect("first score request");
    assert_eq!(first.status(), StatusCode::CREATED);

    let retry = router.clone().oneshot(create()).await.expect("score retry");
    assert_eq!(retry.status(), StatusCode::OK);

    let invalid = Request::builder()
        .method("POST")
        .uri("/v1/llm/scores")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "score_id": "score-unit-invalid",
                "timestamp": "2026-07-18T23:22:00Z",
                "name": "correctness",
                "data_type": "numeric",
                "numeric_value": 0.9,
                "source": "evaluator"
            })
            .to_string(),
        ))
        .unwrap();
    let invalid_response = router
        .oneshot(invalid)
        .await
        .expect("invalid score request");
    assert_eq!(invalid_response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn unit_score_config_list_seeds_defaults_and_validates_scores() {
    let (router, _state, _t) = local_router_and_state().await.expect("router");

    let list_req = Request::builder()
        .method("GET")
        .uri("/v1/llm/score-configs")
        .body(Body::empty())
        .unwrap();
    let list_resp = router
        .clone()
        .oneshot(list_req)
        .await
        .expect("list score configs");
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .expect("list body");
    let list_json: serde_json::Value = serde_json::from_slice(&list_body).expect("list json");
    let items = list_json["items"].as_array().expect("items");
    assert!(items.len() >= 3, "expected seeded defaults, got {items:?}");

    let config_id = items
        .iter()
        .find(|item| item["name"] == "correctness")
        .and_then(|item| item["config_id"].as_str())
        .expect("correctness config")
        .to_string();

    let ok_score = Request::builder()
        .method("POST")
        .uri("/v1/llm/scores")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "score_id": "score-anno-1",
                "timestamp": "2026-07-18T23:22:00Z",
                "span_id": "span-unit-1",
                "name": "correctness",
                "data_type": "boolean",
                "boolean_value": true,
                "source": "annotation",
                "config_id": config_id,
            })
            .to_string(),
        ))
        .unwrap();
    let ok_resp = router.clone().oneshot(ok_score).await.expect("anno score");
    assert_eq!(ok_resp.status(), StatusCode::CREATED);

    let bad_score = Request::builder()
        .method("POST")
        .uri("/v1/llm/scores")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "score_id": "score-anno-bad",
                "timestamp": "2026-07-18T23:22:00Z",
                "span_id": "span-unit-1",
                "name": "correctness",
                "data_type": "boolean",
                "boolean_value": true,
                "source": "annotation",
                "config_id": "missing-config",
            })
            .to_string(),
        ))
        .unwrap();
    let bad_resp = router.oneshot(bad_score).await.expect("bad config score");
    assert_eq!(bad_resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn unit_openapi_llm_schema_contracts() {
    // Deeper than http_api openapi smoke: pin LLM operationIds / schemas for codegen.
    let (router, _state, _t) = local_router_and_state().await.expect("router");

    let req = Request::builder()
        .uri("/openapi.json")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::OK);
    let openapi_body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("openapi body");
    let openapi: serde_json::Value =
        serde_json::from_slice(&openapi_body).expect("valid openapi json");
    let score_post = &openapi["paths"]["/v1/llm/scores"]["post"];
    assert_eq!(score_post["operationId"], "createScore");
    assert_eq!(
        score_post["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/CreateScoreRequest"
    );
    assert_eq!(
        openapi["components"]["schemas"]["CreateScoreRequest"]["properties"]["data_type"]["enum"],
        json!(["numeric", "categorical", "boolean", "text"])
    );
    assert_eq!(
        openapi["paths"]["/v1/llm/observations/search"]["post"]["operationId"],
        "searchObservations"
    );
    assert_eq!(
        openapi["paths"]["/v1/llm/score-configs"]["get"]["operationId"],
        "listScoreConfigs"
    );
    assert_eq!(
        openapi["paths"]["/v1/llm/score-configs"]["post"]["operationId"],
        "createScoreConfig"
    );
    assert_eq!(
        openapi["paths"]["/v1/llm/observations/{span_id}"]["get"]["operationId"],
        "getObservation"
    );
    assert_eq!(
        openapi["paths"]["/v1/llm/traces/{trace_id}"]["get"]["operationId"],
        "getTrace"
    );
    assert_eq!(
        openapi["paths"]["/v1/llm/sessions/{session_id}"]["get"]["operationId"],
        "getSession"
    );
    assert!(openapi["components"]["schemas"]["ObservationSearchRequest"].is_object());
    assert!(openapi["components"]["schemas"]["TraceDetail"].is_object());
    assert!(openapi["components"]["schemas"]["SessionDetail"].is_object());
}

#[tokio::test]
async fn unit_query_sql_invalid_returns_500() {
    // http_api covers empty SQL (400) and SELECT 1 (200); this pins engine error mapping.
    let (router, _t) = crate::test_support::local_router().await.expect("router");
    let req = Request::builder()
        .method("POST")
        .uri("/v1/query/sql")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "sql": "SELECT )syntax_error(" }).to_string(),
        ))
        .unwrap();
    let resp = router.oneshot(req).await.expect("oneshot");
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn unit_telemetry_search_compiles_session_summary_with_safe_filters() {
    let request = TelemetrySearchRequest {
        version: 1,
        scope: TelemetrySearchScope::Sessions,
        time_range: Some(TelemetryTimeRange {
            from: "2026-05-03T10:00:00Z".to_string(),
            to: "2026-05-03T11:00:00Z".to_string(),
        }),
        filter: Some(TelemetryFilterExpr::And {
            and: vec![
                TelemetryFilterExpr::Predicate(TelemetryFilter {
                    field: "service.name".to_string(),
                    op: "eq".to_string(),
                    value: Some(json!("checkout-api")),
                }),
                TelemetryFilterExpr::Predicate(TelemetryFilter {
                    field: "http_request_path".to_string(),
                    op: "prefix".to_string(),
                    value: Some(json!("/api/checkout")),
                }),
                TelemetryFilterExpr::Predicate(TelemetryFilter {
                    field: "http_response_status_code".to_string(),
                    op: "gte".to_string(),
                    value: Some(json!(500)),
                }),
            ],
        }),
        columns: vec!["session_id".to_string(), "trace_count".to_string()],
        sort: vec![TelemetrySort {
            field: "timestamp".to_string(),
            direction: TelemetrySortDirection::Desc,
        }],
        limit: Some(50),
        cursor: None,
    };

    let sql = compile_search_sql(&request).expect("compile search");

    assert!(sql.contains("FROM union_spans"));
    assert!(sql.contains("GROUP BY session_id"));
    assert!(sql.contains("app_id = 'checkout-api'"));
    assert!(sql.contains("http_request_path LIKE '/api/checkout%'"));
    assert!(sql.contains("http_response_status_code >= 500"));
    assert!(sql.contains("LIMIT 50"));
}

#[test]
fn unit_telemetry_search_rejects_unknown_fields_and_sql_fragments() {
    let mut request = TelemetrySearchRequest {
        version: 1,
        scope: TelemetrySearchScope::Traces,
        time_range: Some(TelemetryTimeRange {
            from: "2026-05-03T10:00:00Z".to_string(),
            to: "2026-05-03T11:00:00Z".to_string(),
        }),
        filter: Some(TelemetryFilterExpr::Predicate(TelemetryFilter {
            field: "attributes['x']; DROP TABLE traces; --".to_string(),
            op: "eq".to_string(),
            value: Some(json!("bad")),
        })),
        columns: vec![],
        sort: vec![],
        limit: Some(10),
        cursor: None,
    };

    assert!(compile_search_sql(&request).is_err());

    request.filter = Some(TelemetryFilterExpr::Predicate(TelemetryFilter {
        field: "session_id".to_string(),
        op: "contains".to_string(),
        value: Some(json!("sess")),
    }));
    assert!(compile_search_sql(&request).is_err());
}

#[test]
fn unit_telemetry_details_compiles_correlated_signal_queries() {
    let target = TelemetryDetailsTarget {
        kind: "session".to_string(),
        id: "sess_abc".to_string(),
    };

    let compiled = compile_details_sql(&target, None, 100).expect("compile details");

    assert!(compiled.spans.contains("FROM union_spans"));
    assert!(compiled.spans.contains("http_request_body"));
    assert!(compiled.spans.contains("http_response_body"));
    assert!(compiled.logs.contains("FROM union_logs"));
    assert!(compiled.metrics.contains("FROM union_metrics"));
    assert!(compiled.spans.contains("session_id = 'sess_abc'"));
    assert!(compiled.logs.contains("session_id = 'sess_abc'"));
    assert!(compiled
        .metrics
        .contains("CAST(attributes['sp.session.id'] AS VARCHAR) = 'sess_abc'"));
}
