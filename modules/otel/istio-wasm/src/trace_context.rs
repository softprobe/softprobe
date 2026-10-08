use std::collections::HashMap;

/// Parse traceparent value in format: 00-trace_id-span_id-01
pub fn parse_traceparent_value(traceparent: &str) -> Option<(Vec<u8>, Vec<u8>)> {
    let parts: Vec<&str> = traceparent.split('-').collect();
    if parts.len() != 4 {
        return None;
    }

    let trace_id = hex_decode(parts[1])?;
    let span_id = hex_decode(parts[2])?;

    Some((trace_id, span_id))
}

/// Helper function to decode hex string to bytes
pub fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }

    let mut bytes = Vec::new();
    for i in (0..hex.len()).step_by(2) {
        if let Ok(byte) = u8::from_str_radix(&hex[i..i + 2], 16) {
            bytes.push(byte);
        } else {
            return None;
        }
    }
    Some(bytes)
}

/// Extract and propagate W3C Trace Context from response headers
pub fn extract_and_propagate_trace_context(
    request_headers: &HashMap<String, String>,
    response_headers: &HashMap<String, String>,
) {
    // Extract trace context from request headers
    if let Some(tracestate) = request_headers.get("tracestate") {
        crate::sp_debug!("Found tracestate in request: {}", tracestate);

        // Parse x-sp-traceparent from tracestate
        for entry in tracestate.split(',') {
            let entry = entry.trim();
            if let Some(value) = entry.strip_prefix("x-sp-traceparent=") {
                if let Some((trace_id, parent_span_id)) = parse_traceparent_value(value) {
                    let trace_id_hex = trace_id
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>();
                    let parent_id_hex = parent_span_id
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>();
                    
                    crate::sp_debug!("Extracted trace context from x-sp-traceparent: {}, trace_id: {}, parent_span_id: {}", value, trace_id_hex, parent_id_hex);
                    break;
                }
            }
        }
    }

    // Check response headers for traceparent
    if let Some(_traceparent) = response_headers.get("traceparent") {
        crate::sp_debug!("Found traceparent in response headers");
        crate::sp_debug!("Would propagate trace context to response");
    } else {
        crate::sp_debug!("No traceparent found in response headers");
    }
}