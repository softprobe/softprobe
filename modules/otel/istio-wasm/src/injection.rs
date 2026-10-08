// no imports needed

#[derive(Debug, Clone)]
pub struct AgentResponse {
    pub status_code: u32,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

// note: helper function from older attempt removed as unused in the current module structure

#[allow(dead_code)]
fn log_span_details(traces_data: &crate::otel::TracesData) {
    if let Some(resource_spans) = traces_data.resource_spans.first() {
        if let Some(scope_spans) = resource_spans.scope_spans.first() {
            for (i, span) in scope_spans.spans.iter().enumerate() {
                let trace_id_hex = span
                    .trace_id
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>();
                let span_id_hex = span
                    .span_id
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>();
                let parent_span_id_hex = span
                    .parent_span_id
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>();

                log::info!(
                    "SP: Span[{}] - trace_id: {}, span_id: {}, parent_span_id: {}, name: {}",
                    i, trace_id_hex, span_id_hex, parent_span_id_hex, span.name
                );
            }
        }
    }
}

// Helper function to parse OTEL injection response
pub fn parse_otel_injection_response(response_body: &[u8]) -> Result<Option<AgentResponse>, String> {
    use crate::otel::TracesData;
    use prost::Message;

    crate::sp_debug!("Starting protobuf decode ({} bytes)", response_body.len());

    // Decode OTEL protobuf response
    let traces_data = TracesData::decode(response_body).map_err(|e| {
        log::error!("SP: Protobuf decode failed: {}", e);
        format!("Serialization error: {}", e)
    })?;

    crate::sp_debug!("Decoded protobuf with {} resource spans", traces_data.resource_spans.len());

    // Extract agent HTTP response from span attributes
    for (i, resource_span) in traces_data.resource_spans.iter().enumerate() {
        crate::sp_debug!("Processing resource span {}, found {} scope spans", i, resource_span.scope_spans.len());
        for (j, scope_span) in resource_span.scope_spans.iter().enumerate() {
            crate::sp_debug!("Processing scope span {}, found {} spans", j, scope_span.spans.len());
            for (k, span) in scope_span.spans.iter().enumerate() {
                crate::sp_debug!("Processing span {}, name: '{}', {} attributes", k, span.name, span.attributes.len());
                
                if let Some(agent_response) = extract_agent_response_from_span(span) {
                    return Ok(Some(agent_response));
                }
            }
        }
    }

    crate::sp_debug!("No agent response found in any spans");
    Ok(None)
}

fn extract_agent_response_from_span(span: &crate::otel::Span) -> Option<AgentResponse> {
    let mut status_code = 200u32;
    let mut headers = Vec::new();
    let mut body = Vec::new();

    for attr in &span.attributes {
        match attr.key.as_str() {
            "http.response.status_code" => {
                if let Some(value) = &attr.value {
                    if let Some(crate::otel::any_value::Value::IntValue(code)) = &value.value {
                        status_code = *code as u32;
                    }
                }
            }
            key if key.starts_with("http.response.header.") => {
                let header_name = &key[21..]; // Remove "http.response.header." prefix
                if let Some(value) = &attr.value {
                    if let Some(crate::otel::any_value::Value::StringValue(header_value)) =
                        &value.value
                    {
                        headers.push((header_name.to_string(), header_value.clone()));
                    }
                }
            }
            "http.response.body" => {
                if let Some(value) = &attr.value {
                    if let Some(crate::otel::any_value::Value::StringValue(body_str)) = &value.value
                    {
                        // Decode base64 if it's binary data, otherwise use as-is
                        body = if is_base64_encoded(body_str) {
                            use base64::{engine::general_purpose, Engine as _};
                            general_purpose::STANDARD
                                .decode(body_str)
                                .unwrap_or_else(|_| body_str.as_bytes().to_vec())
                        } else {
                            body_str.as_bytes().to_vec()
                        };
                    }
                }
            }
            _ => {}
        }
    }

    // If we found response data, return it
    if !body.is_empty() || !headers.is_empty() {
        crate::sp_debug!("Agent response in span: status={}, headers={}, body_bytes={}", status_code, headers.len(), body.len());
        Some(AgentResponse {
            status_code,
            headers,
            body,
        })
    } else {
        crate::sp_debug!("No agent response data found in span");
        None
    }
}

fn is_base64_encoded(s: &str) -> bool {
    // Simple heuristic: if string is longer than 100 chars and contains typical base64 chars
    s.len() > 100
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=')
}