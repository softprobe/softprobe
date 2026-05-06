use softprobe_runtime::promotion::{
    extract_business_promoted_row, parse_promotion_manifest, BusinessPromotionInput,
    PromotionManifest, TelemetryPromotionEvent,
};
use std::collections::HashMap;

#[test]
fn extracts_business_row_with_session_trace_span_and_timestamp_anchors() {
    let manifest = parse_promotion_manifest(
        r#"
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
  - name: order_id
    type: string
    nullable: false
    source:
      from: http_request_body
      json_path: $.order.id
  - name: decision
    type: string
    nullable: true
    source:
      from: event_attribute
      event_name: fraud.checked
      key: fraud.decision
"#,
    )
    .expect("valid manifest");
    let PromotionManifest::BusinessTable(spec) = manifest else {
        panic!("expected business table manifest");
    };
    let attributes = HashMap::from([("sp.workflow".to_string(), "checkout".to_string())]);
    let events = vec![TelemetryPromotionEvent {
        name: "fraud.checked".to_string(),
        attributes: HashMap::from([("fraud.decision".to_string(), "allow".to_string())]),
    }];
    let input = BusinessPromotionInput {
        session_id: "session-a",
        trace_id: "trace-a",
        span_id: "span-a",
        event_name: Some("fraud.checked"),
        event_timestamp: Some("2026-05-06T17:10:00Z"),
        service_name: Some("checkout-api"),
        source_signal: "event",
        source_timestamp: "2026-05-06T17:10:00Z",
        attributes: &attributes,
        events: &events,
        http_request_body: Some(r#"{"order":{"id":"ord_999"}}"#),
        http_response_body: None,
    };

    let row = extract_business_promoted_row(&spec, &input)
        .expect("extract row")
        .expect("selector matches");

    assert_eq!(row.session_id, "session-a");
    assert_eq!(row.trace_id, "trace-a");
    assert_eq!(row.span_id, "span-a");
    assert_eq!(row.event_name.as_deref(), Some("fraud.checked"));
    assert_eq!(row.event_timestamp.as_deref(), Some("2026-05-06T17:10:00Z"));
    assert_eq!(row.source_timestamp, "2026-05-06T17:10:00Z");
    assert_eq!(row.values["order_id"], "ord_999");
    assert_eq!(row.values["decision"], "allow");
}
