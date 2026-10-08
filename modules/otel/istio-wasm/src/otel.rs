use std::collections::HashMap;
// Note: SystemTime is not available in WASM runtime, will use proxy-wasm host functions
use prost::Message;
use proxy_wasm;
// use std::sync::atomic::{AtomicU64, Ordering};

// Include generated protobuf types
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

// Re-export commonly used types
pub use opentelemetry::proto::common::v1::{AnyValue, KeyValue, any_value};
pub use opentelemetry::proto::resource::v1::Resource;
pub use opentelemetry::proto::trace::v1::{TracesData, ResourceSpans, ScopeSpans, Span, Status, span};

#[derive(Clone)]
pub struct SpanBuilder {
    trace_id: Vec<u8>,
    parent_span_id: Option<Vec<u8>>,
    current_span_id: Vec<u8>,  // 添加当前 span ID 字段
    service_name: String,
    traffic_direction: String,  // 添加traffic_direction字段
    public_key: String,
    session_id: String
}

impl SpanBuilder {
    pub fn new() -> Self {
        Self {
            trace_id: generate_trace_id(),
            parent_span_id: None,
            current_span_id: generate_span_id(),  // 初始化当前 span ID
            service_name: "default-service".to_string(),
            traffic_direction: "outbound".to_string(),  // 默认值
            public_key: String::new(),
            session_id: String::new()
        }
    }
    // 添加设置service_name的方法
    pub fn with_service_name(mut self, service_name: String) -> Self {
        self.service_name = service_name;
        self
    }

    // 添加设置traffic_direction的方法
    pub fn with_traffic_direction(mut self, traffic_direction: String) -> Self {
        self.traffic_direction = traffic_direction;
        self
    }

    // 添加设置api_key的方法
    pub fn with_public_key(mut self, public_key: String) -> Self {
        self.public_key = public_key;
        self
    }

    /// Check if session_id is present and not empty
    pub fn has_session_id(&self) -> bool {
        !self.session_id.is_empty()
    }

    /// Get current session_id string (may be empty if not set)
    pub fn get_session_id(&self) -> &str {
        &self.session_id
    }

    /// Get trace_id as hex string
    pub fn get_current_span_id_hex(&self) -> String {
        self.current_span_id.iter().map(|b| format!("{:02x}", b)).collect::<String>()
    }

    pub fn get_trace_id_hex(&self) -> String {
        self.trace_id.iter().map(|b| format!("{:02x}", b)).collect::<String>()
    }

    pub fn with_context(mut self, headers: &HashMap<String, String>) -> Self {
        // Extract trace context from tracestate x-sp-traceparent if present
        if let Some(tracestate) = headers.get("tracestate") {
            crate::sp_info!("with_context Found tracestate header {}", tracestate);
            
            // 解析 tracestate 中的 x-sp-traceparent
            for entry in tracestate.split(',') {
                let entry = entry.trim();
                if let Some(value) = entry.strip_prefix("x-sp-traceparent=") {
                    crate::sp_debug!("Found x-sp-traceparent entry in tracestate {}", value);
                    // 解析完整的 traceparent 格式: 00-trace_id-span_id-01
                    if let Some((trace_id, span_id)) = parse_traceparent(value) {
                        self.trace_id = trace_id;
                        self.parent_span_id = Some(span_id);
                        crate::sp_debug!("Parsed trace context from x-sp-traceparent");
                        break;
                    }
                }
                // 解析 tracestate 中的 x-sp-session-id（如果存在）
                if self.session_id.is_empty() {
                    if let Some(sid) = entry.strip_prefix("x-sp-session-id=") {
                        crate::sp_debug!("Found x-sp-session-id entry in tracestate {}", sid);
                        self.session_id = sid.to_string();
                    }
                }
            }
        }

        // 如果没有从 tracestate 中解析到 trace context，尝试从标准的 traceparent 头部解析
        if self.trace_id.is_empty() {
            if let Some(traceparent) = headers.get("traceparent") {
                crate::sp_debug!("Found traceparent header {}", traceparent);
                // 解析标准的 traceparent 格式: 00-trace_id-span_id-01
                if let Some((trace_id, span_id)) = parse_traceparent(traceparent) {
                    self.trace_id = trace_id;
                    self.parent_span_id = Some(span_id);
                    crate::sp_debug!("Parsed trace context from traceparent");
                }
            }
        }

        // Get session ID from headers directly
        crate::sp_debug!("Looking for session_id in headers");
        let session_id_found = headers.get("x-sp-session-id")
            .or_else(|| headers.get("sp_session_id"))
            .or_else(|| headers.get("x-session-id"));

        if let Some(session_id) = session_id_found {
            let masked = if session_id.len() > 4 { "****" } else { "" };
            crate::sp_debug!("Found session_id in headers: {}", masked);
            self.session_id = session_id.clone();
        } else {
            // 如果未在 headers 中找到，则尝试从 tracestate 中解析 x-sp-session-id
            if let Some(tracestate) = headers.get("tracestate") {
                for entry in tracestate.split(',') {
                    let entry = entry.trim();
                    if let Some(sid) = entry.strip_prefix("x-sp-session-id=") {
                        crate::sp_debug!("Found session_id in tracestate: ****");
                        self.session_id = sid.to_string();
                        break;
                    }
                }
            }
            // 如果依然没有，则生成新的，并在后续注入阶段补充到 tracestate 中
            if self.session_id.is_empty() {
                crate::sp_debug!("No session_id found in headers or tracestate, generating new one");
                self.session_id = generate_session_id();
                crate::sp_debug!("Generated session_id: sp-session-**** (will be added into tracestate during injection)");
            }
        }

        // If no valid trace context found, generate new one
        if self.trace_id.is_empty() {
            self.trace_id = generate_trace_id();
        }
        
        self
    }

    #[allow(dead_code)]
    pub fn create_inject_span(
        &self,
        request_headers: &HashMap<String, String>,
        request_body: &[u8],
        url_host: Option<&str>,
        url_path: Option<&str>,
    ) -> TracesData {
        let span_id = self.current_span_id.clone();  // 使用 SpanBuilder 中的 current_span_id
        let mut attributes = Vec::new();

        // Add service name attribute
        let service_name = if self.service_name.is_empty() {
            "default-service".to_string()
        } else {
            self.service_name.clone()
        };

        attributes.push(KeyValue {
            key: "sp.service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(service_name)),
            }),
        });

        // Add traffic direction attribute
        attributes.push(KeyValue {
            key: "sp.traffic.direction".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(self.traffic_direction.clone())),
            }),
        });

        // Add API key attribute if present
        log::debug!("DEBUG: public_key value: '{}'", self.public_key);
        if !self.public_key.is_empty() {
            log::debug!("DEBUG: Adding public_key attribute");
            attributes.push(KeyValue {
                key: "sp.public.key".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(self.public_key.clone())),
                }),
            });
        } else {
            log::debug!("DEBUG: public_key is empty, not adding attribute");
        }

        // Add span type attribute
        attributes.push(KeyValue {
            key: "sp.span.type".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("inject".to_string())),
            }),
        });

        // Add session ID attribute if present
        if !self.session_id.is_empty() {
            attributes.push(KeyValue {
                key: "sp.session.id".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(self.session_id.clone())),
                }),
            });
        }
        
        // Add request headers as attributes
        for (key, value) in request_headers {
            if !should_skip_header(key) {
                attributes.push(KeyValue {
                    key: format!("http.request.header.{}", key.to_lowercase()),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(value.clone())),
                    }),
                });
            }
        }

        // Add url attributes if available
        if let Some(path) = url_path {
            attributes.push(KeyValue {
                key: "url.path".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(path.to_string())),
                }),
            });
        }
        if let Some(host) = url_host {
            attributes.push(KeyValue {
                key: "url.host".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(host.to_string())),
                }),
            });
        }

        // Add request body if present and text-based
        if !request_body.is_empty() {
            let body_value = if is_text_content(request_headers) {
                String::from_utf8_lossy(request_body).to_string()
            } else {
                use base64::{Engine as _, engine::general_purpose};
                general_purpose::STANDARD.encode(request_body)
            };

            attributes.push(KeyValue {
                key: "http.request.body".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(body_value)),
                }),
            });
        }

        let span = Span {
            trace_id: self.trace_id.clone(),
            span_id,
            parent_span_id: self.parent_span_id.clone().unwrap_or_default(),
            name: url_path.unwrap_or("unknown_path").to_string(),
            kind: span::SpanKind::Client as i32,
            start_time_unix_nano: get_current_timestamp_nanos(),
            end_time_unix_nano: get_current_timestamp_nanos(),
            attributes,
            flags: 0,
            ..Default::default()
        };

        self.create_traces_data(span)
    }

    pub fn create_extract_span(
        &self,
        request_headers: &HashMap<String, String>,
        request_body: &[u8],
        response_headers: &HashMap<String, String>,
        response_body: &[u8],
        url_host: Option<&str>,
        url_path: Option<&str>,
        request_start_time: Option<u64>,  // Add request start time parameter
    ) -> TracesData {
        let span_id = self.current_span_id.clone();
        let mut attributes = Vec::new();

        crate::sp_debug!("Building extract span: service_name set {}", self.service_name);
        attributes.push(KeyValue {
            key: "sp.service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(self.service_name.clone())),
            }),
        });

        // Add traffic direction attribute
        crate::sp_debug!("Building extract span: traffic_direction set {}", self.traffic_direction);
        attributes.push(KeyValue {
            key: "sp.traffic.direction".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(self.traffic_direction.clone())),
            }),
        });

        // Add extract span type attribute
        attributes.push(KeyValue {
            key: "sp.span.type".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("extract".to_string())),
            }),
        });

        // Add session ID attribute if present
        if !self.session_id.is_empty() {
            crate::sp_debug!("Building extract span: session_id present: {}", self.session_id);
            attributes.push(KeyValue {
                key: "sp.session.id".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(self.session_id.clone())),
                }),
            });
        } else {
            crate::sp_debug!("session_id is empty, not adding attribute");
        }

        // Add request headers
        for (key, value) in request_headers {
            if !should_skip_header(key) {
                attributes.push(KeyValue {
                    key: format!("http.request.header.{}", key.to_lowercase()),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(value.clone())),
                    }),
                });
            }
        }

        // Add url attributes if available
        if let Some(path) = url_path {
            attributes.push(KeyValue {
                key: "url.path".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(path.to_string())),
                }),
            });
        }
        if let Some(host) = url_host {
            attributes.push(KeyValue {
                key: "url.host".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(host.to_string())),
                }),
            });
        }

        // Add request body
        if !request_body.is_empty() {
            let body_value = if is_text_content(request_headers) {
                String::from_utf8_lossy(request_body).to_string()
            } else {
                use base64::{Engine as _, engine::general_purpose};
                general_purpose::STANDARD.encode(request_body)
            };

            attributes.push(KeyValue {
                key: "http.request.body".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(body_value)),
                }),
            });
        }

        // Add response headers
        for (key, value) in response_headers {
            if !should_skip_header(key) {
                attributes.push(KeyValue {
                    key: format!("http.response.header.{}", key.to_lowercase()),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(value.clone())),
                    }),
                });
            }
        }

        // Add response status code
        if let Some(status) = response_headers.get(":status") {
            if let Ok(status_code) = status.parse::<i64>() {
                attributes.push(KeyValue {
                    key: "http.response.status_code".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::IntValue(status_code)),
                    }),
                });
            }
        }

        // Add response body
        if !response_body.is_empty() {
            let body_value = if is_text_content(response_headers) {
                String::from_utf8_lossy(response_body).to_string()
            } else {
                use base64::{Engine as _, engine::general_purpose};
                general_purpose::STANDARD.encode(response_body)
            };

            attributes.push(KeyValue {
                key: "http.response.body".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(body_value)),
                }),
            });
        }

        let span = Span {
            trace_id: self.trace_id.clone(),
            span_id,
            parent_span_id: self.parent_span_id.clone().unwrap_or_default(),
            name: url_path.unwrap_or("unknown_path").to_string(),
            kind: span::SpanKind::Server as i32,
            start_time_unix_nano: request_start_time.unwrap_or_else(|| get_current_timestamp_nanos()),
            end_time_unix_nano: get_current_timestamp_nanos(),
            attributes,
            status: Some(Status {
                code: 1, // STATUS_CODE_OK
                message: String::new(),
            }),
            flags: 0,
            ..Default::default()
        };

        self.create_traces_data(span)
    }

    fn create_traces_data(&self, span: Span) -> TracesData {
        // Create resource with service.name attribute
        let service_name = if self.service_name.is_empty() {
            "default-service".to_string()
        } else {
            self.service_name.clone()
        };
        let mut attributes = Vec::new();

        log::debug!("DEBUG: public_key value: '{}'", self.public_key);
        if !self.public_key.is_empty() {
            log::debug!("DEBUG: Adding public_key attribute");
            attributes.push(KeyValue {
                key: "sp.public.key".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(self.public_key.clone())),
                }),
            });
        } else {
            log::debug!("DEBUG: public_key is empty, not adding attribute");
        }

        attributes.push(KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(service_name)),
            }),
        });

        let resource_type_value = "sp-envoy-proxy".to_string();
        attributes.push(KeyValue {
            key: "sp.resource.type".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(resource_type_value.clone())),
            }),
        });

        let resource = Resource {
            attributes,
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };

        TracesData {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource),
                scope_spans: vec![ScopeSpans {
                    spans: vec![span],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// Generate W3C traceparent header value
    /// Format: 00-{trace_id}-{span_id}-{trace_flags}
    pub fn generate_traceparent(&self, span_id: &[u8]) -> String {
        let version = "00";
        let trace_id_hex = hex_encode(&self.trace_id);
        let span_id_hex = hex_encode(span_id);
        let trace_flags = "01"; // sampled flag set

        format!("{}-{}-{}-{}", version, trace_id_hex, span_id_hex, trace_flags)
    }

    }


// 保留原有的protobuf序列化函数
pub fn serialize_traces_data(traces_data: &TracesData) -> Result<Vec<u8>, prost::EncodeError> {
    let mut buf = Vec::new();
    traces_data.encode(&mut buf)?;
    Ok(buf)
}

fn generate_trace_id() -> Vec<u8> {
    let mut trace_id = vec![0u8; 16];
    
    // Use current timestamp as source of randomness
    let now_nanos = get_current_timestamp_nanos();
    let secs = (now_nanos / 1_000_000_000) as u64;
    let nanos = (now_nanos % 1_000_000_000) as u64;
    
    // Fill first 8 bytes with seconds
    trace_id[0..8].copy_from_slice(&secs.to_be_bytes());
    // Fill last 8 bytes with nanoseconds
    trace_id[8..16].copy_from_slice(&nanos.to_be_bytes());
    
    trace_id
}

pub fn generate_span_id() -> Vec<u8> {
    let mut span_id = vec![0u8; 8];
    
    // Use current timestamp as source of randomness
    let now_nanos = get_current_timestamp_nanos();
    
    // Add some variation to make it different from trace ID
    let varied_nanos = now_nanos ^ 0xCAFEBABE;
    span_id.copy_from_slice(&varied_nanos.to_be_bytes());
    
    span_id
}

fn parse_traceparent(traceparent: &str) -> Option<(Vec<u8>, Vec<u8>)> {
    let parts: Vec<&str> = traceparent.split('-').collect();
    if parts.len() != 4 {
        return None;
    }
    
    let trace_id = hex_decode(parts[1])?;
    let span_id = hex_decode(parts[2])?;
    
    Some((trace_id, span_id))
}

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    
    let mut result = Vec::new();
    for i in (0..hex.len()).step_by(2) {
        if let Ok(byte) = u8::from_str_radix(&hex[i..i+2], 16) {
            result.push(byte);
        } else {
            return None;
        }
    }
    
    Some(result)
}

pub fn get_current_timestamp_nanos() -> u64 {
    match proxy_wasm::hostcalls::get_current_time() {
        Ok(system_time) => {
            // Convert SystemTime to nanoseconds
            system_time.duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos() as u64)
                .unwrap_or_else(|_| {
                    // If system_time is before UNIX_EPOCH, use fallback
                    use std::sync::atomic::{AtomicU64, Ordering};
                    static TIMESTAMP_COUNTER: AtomicU64 = AtomicU64::new(1609459200000000000_u64); // Start at Jan 1, 2021
                    TIMESTAMP_COUNTER.fetch_add(1000000, Ordering::Relaxed)
                })
        },
        Err(_) => {
            // Fallback to counter-based approach if host function fails
            use std::sync::atomic::{AtomicU64, Ordering};
            static TIMESTAMP_COUNTER: AtomicU64 = AtomicU64::new(1609459200000000000_u64); // Start at Jan 1, 2021
            TIMESTAMP_COUNTER.fetch_add(1000000, Ordering::Relaxed)
        }
    }
}

fn should_skip_header(key: &str) -> bool {
    matches!(key.to_lowercase().as_str(), 
        "authorization" | "cookie" | "set-cookie" | 
        "x-public-key" | "x-auth-token" | "bearer" |
        "proxy-authorization"
    )
}

fn is_text_content(headers: &HashMap<String, String>) -> bool {
    if let Some(content_type) = headers.get("content-type") {
        content_type.starts_with("text/") || 
        content_type.starts_with("application/json") ||
        content_type.starts_with("application/xml") ||
        content_type.starts_with("application/x-www-form-urlencoded")
    } else {
        false
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn generate_session_id() -> String {
    // Generate a UUID-like session ID in the format: sp-session-f43fdfa5-3ab8-4548-895e-26a0c28ec54a
    let mut uuid_bytes = vec![0u8; 16];
    
    // Use current timestamp as source of randomness
    let now_nanos = get_current_timestamp_nanos();
    let secs = (now_nanos / 1_000_000_000) as u64;
    let nanos = (now_nanos % 1_000_000_000) as u64;
    
    // Fill first 8 bytes with seconds
    uuid_bytes[0..8].copy_from_slice(&secs.to_be_bytes());
    // Fill last 8 bytes with nanoseconds + some variation
    let varied_nanos = nanos ^ 0xDEADBEEF;
    uuid_bytes[8..16].copy_from_slice(&varied_nanos.to_be_bytes());
    
    // Format as UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    format!(
        "sp-session-{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        uuid_bytes[0], uuid_bytes[1], uuid_bytes[2], uuid_bytes[3],
        uuid_bytes[4], uuid_bytes[5],
        uuid_bytes[6], uuid_bytes[7],
        uuid_bytes[8], uuid_bytes[9],
        uuid_bytes[10], uuid_bytes[11], uuid_bytes[12], uuid_bytes[13], uuid_bytes[14], uuid_bytes[15]
    )
}