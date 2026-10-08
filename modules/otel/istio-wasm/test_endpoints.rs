use std::time::Duration;
use std::collections::HashMap;
use prost::Message;
use serde_json::json;

// Include the same protobuf definitions as the main project
mod otel {
    pub mod opentelemetry {
        pub mod proto {
            pub mod common {
                pub mod v1 {
                    include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.common.v1.rs"));
                }
            }
            pub mod resource {
                pub mod v1 {
                    include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.resource.v1.rs"));
                }
            }
            pub mod trace {
                pub mod v1 {
                    include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.trace.v1.rs"));
                }
            }
        }
    }
}

use otel::opentelemetry::proto::trace::v1::{TracesData, ResourceSpans, ScopeSpans, Span};
use otel::opentelemetry::proto::resource::v1::Resource;
use otel::opentelemetry::proto::common::v1::{KeyValue, AnyValue, any_value};

#[tokio::main]
async fn main() {
    println!("Testing o.softprobe.ai endpoints");
    println!("================================");
    
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");
    
    // Test /v1/traces endpoint
    println!("\n1. Testing /v1/traces endpoint:");
    test_traces_endpoint(&client).await;
    
    // Test /v1/inject endpoint  
    println!("\n2. Testing /v1/inject endpoint:");
    test_inject_endpoint(&client).await;
}

async fn test_traces_endpoint(client: &reqwest::Client) {
    // Create TracesData with a span
    let span = Span {
        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
        name: "test_span".to_string(),
        attributes: vec![
            KeyValue {
                key: "sp.span.type".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("extract".to_string())),
                }),
            },
            KeyValue {
                key: "http.request.body".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("{\"request_key\":\"request_value\"}".to_string()))
                })
            },
            KeyValue {
                key: "http.response.body".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("{\"response_key\":\"response_value\"}".to_string()))
                })
            }
        ],
        ..Default::default()
    };
    
    let traces_data = TracesData {
        resource_spans: vec![
            ResourceSpans {
                resource: Some(Resource::default()),
                scope_spans: vec![
                    ScopeSpans {
                        spans: vec![span],
                        ..Default::default()
                    }
                ],
                ..Default::default()
            }
        ],
    };
    let protobuf_data = traces_data.encode_to_vec();
    
    println!("  Sending {} bytes of valid protobuf data", protobuf_data.len());
    
    match client
        .post("https://o.softprobe.ai/v1/traces")
        .header("Content-Type", "application/x-protobuf")
        .body(protobuf_data)
        .send()
        .await
    {
        Ok(response) => {
            let status = response.status();
            let headers: HashMap<String, String> = response
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                .collect();
            
            println!("  Status: {}", status);
            println!("  Headers: {:?}", headers);
            
            if status == 200 {
                println!("  ✅ SUCCESS: /v1/traces returned 200");
            } else {
                println!("  ⚠️  /v1/traces returned: {}", status);
            }
        }
        Err(e) => {
            println!("  ❌ ERROR: Failed to call /v1/traces: {}", e);
        }
    }
}

async fn test_inject_endpoint(client: &reqwest::Client) {
    // Create TracesData with a span for inject
    let span = Span {
        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
        name: "agent_inject".to_string(),
        attributes: vec![
            KeyValue {
                key: "sp.span.type".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("inject".to_string())),
                }),
            },
            KeyValue {
                key: "http.request.body".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("{\"request_key\":\"request_value\"}".to_string()))
                })
            }
        ],
        ..Default::default()
    };
    
    let traces_data = TracesData {
        resource_spans: vec![
            ResourceSpans {
                resource: Some(Resource::default()),
                scope_spans: vec![
                    ScopeSpans {
                        spans: vec![span],
                        ..Default::default()
                    }
                ],
                ..Default::default()
            }
        ],
    };
    let protobuf_data = traces_data.encode_to_vec();
    
    println!("  Sending {} bytes of valid protobuf data", protobuf_data.len());
    
    match client
        .post("https://o.softprobe.ai/v1/inject")
        .header("Content-Type", "application/x-protobuf")
        .body(protobuf_data)
        .send()
        .await
    {
        Ok(response) => {
            let status = response.status();
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            
            println!("  Status: {}", status);
            println!("  Content-Type: {}", content_type);
            
            let body_bytes = response.bytes().await.unwrap_or_default();
            println!("  Body size: {} bytes", body_bytes.len());
            
            if status == 200 {
                if content_type.contains("application/x-protobuf") || content_type.contains("application/protobuf") {
                    println!("  ✅ SUCCESS: /v1/inject returned 200 with protobuf content-type");

                    // Decode the response body as TracesData and convert to JSON manually
                    match TracesData::decode(body_bytes.as_ref()) {
                        Ok(traces_data) => {
                            // Manual conversion to JSON since TracesData doesn't implement Serialize
                            let json_obj = json!({
                                "resource_spans": traces_data.resource_spans.iter().map(|rs| {
                                    json!({
                                        "resource": rs.resource.as_ref().map(|r| json!({
                                            "attributes": r.attributes.iter().map(|kv| {
                                                json!({
                                                    "key": kv.key,
                                                    "value": kv.value.as_ref().map(|v| match &v.value {
                                                        Some(any_value::Value::StringValue(s)) => json!({"string_value": s}),
                                                        Some(any_value::Value::IntValue(i)) => json!({"int_value": i}),
                                                        Some(any_value::Value::BoolValue(b)) => json!({"bool_value": b}),
                                                        Some(any_value::Value::DoubleValue(d)) => json!({"double_value": d}),
                                                        Some(any_value::Value::BytesValue(b)) => json!({"bytes_value": base64::encode(b)}),
                                                        _ => json!(null)
                                                    })
                                                })
                                            }).collect::<Vec<_>>()
                                        })),
                                        "scope_spans": rs.scope_spans.iter().map(|ss| {
                                            json!({
                                                "spans": ss.spans.iter().map(|span| {
                                                    json!({
                                                        "trace_id": base64::encode(&span.trace_id),
                                                        "span_id": base64::encode(&span.span_id), 
                                                        "name": span.name,
                                                        "attributes": span.attributes.iter().map(|kv| {
                                                            json!({
                                                                "key": kv.key,
                                                                "value": kv.value.as_ref().map(|v| match &v.value {
                                                                    Some(any_value::Value::StringValue(s)) => json!({"string_value": s}),
                                                                    Some(any_value::Value::IntValue(i)) => json!({"int_value": i}),
                                                                    Some(any_value::Value::BoolValue(b)) => json!({"bool_value": b}),
                                                                    Some(any_value::Value::DoubleValue(d)) => json!({"double_value": d}),
                                                                    Some(any_value::Value::BytesValue(b)) => json!({"bytes_value": base64::encode(b)}),
                                                                    _ => json!(null)
                                                                })
                                                            })
                                                        }).collect::<Vec<_>>()
                                                    })
                                                }).collect::<Vec<_>>()
                                            })
                                        }).collect::<Vec<_>>()
                                    })
                                }).collect::<Vec<_>>()
                            });
                            println!("  Response JSON:\n{}", serde_json::to_string_pretty(&json_obj).unwrap());
                        }
                        Err(e) => {
                            println!("  ❌ ERROR: Failed to decode response as TracesData: {}", e);
                            println!("  Raw body bytes: {:?}", &body_bytes[..std::cmp::min(50, body_bytes.len())]);
                        }
                    }
                    
                } else {
                    println!("  ⚠️  /v1/inject returned 200 but wrong content-type: {}", content_type);
                }
            } else if status == 404 {
                println!("  ✅ SUCCESS: /v1/inject returned 404 (cache miss)");
            } else {
                println!("  ⚠️  /v1/inject returned unexpected status: {}", status);
            }
            
            if body_bytes.len() < 200 {
                println!("  Body content: {}", String::from_utf8_lossy(&body_bytes));
            }
        }
        Err(e) => {
            println!("  ❌ ERROR: Failed to call /v1/inject: {}", e);
        }
    }
}