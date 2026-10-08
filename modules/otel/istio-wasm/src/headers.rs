use std::collections::HashMap;

/// Detect service name from headers or configuration
pub fn detect_service_name(
    request_headers: &HashMap<String, String>,
    config_service_name: &str,
) -> String {
    // Use configured service_name if it's not default
    if !config_service_name.is_empty() && config_service_name != "default-service" {
        crate::sp_debug!("Using configured service_name: {}", config_service_name);
        return config_service_name.to_string();
    }

    let current_service_headers = vec!["x-sp-service-name"];
    for header_name in current_service_headers {
        if let Some(header_value) = request_headers.get(header_name) {
            if !header_value.is_empty() {
                crate::sp_debug!("Got service_name from header: {} -> {}", header_name, header_value);
                return header_value.clone();
            }
        }
    }
    config_service_name.to_string()
}

/// Build new tracestate with x-sp-traceparent entry
pub fn build_new_tracestate(
    request_headers: &HashMap<String, String>,
    traceparent_value: &str,
    session_id: &str,
) -> String {
    let mut tracestate_entries = Vec::new();
    let mut has_sp_session_id = false;

    if let Some(existing_tracestate) = request_headers.get("tracestate") {
        // Parse existing tracestate, preserve other entries
        for entry in existing_tracestate.split(',') {
            let entry = entry.trim();
            if entry.starts_with("x-sp-session-id=") {
                has_sp_session_id = true;
                tracestate_entries.push(entry.to_string());
            } else if !entry.starts_with("x-sp-traceparent=") {
                tracestate_entries.push(entry.to_string());
            }
        }
    }

    // Add x-sp-traceparent entry with full traceparent format
    tracestate_entries.insert(0, format!("x-sp-traceparent={}", traceparent_value));

    // If tracestate does not contain x-sp-session-id and we have a session_id, add it
    if !has_sp_session_id && !session_id.is_empty() {
        tracestate_entries.insert(1, format!("x-sp-session-id={}", session_id));
    }

    let new_tracestate = tracestate_entries.join(",");
    crate::sp_debug!("Adding x-sp-traceparent/x-sp-session-id to tracestate: {}", new_tracestate);

    new_tracestate
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_detect_service_name_with_configured_name() {
        let headers = HashMap::new();
        let config_name = "my-service";
        
        let result = detect_service_name(&headers, config_name);
        assert_eq!(result, "my-service");
    }

    #[test]
    fn test_detect_service_name_with_default_config() {
        let headers = HashMap::new();
        let config_name = "default-service";
        
        let result = detect_service_name(&headers, config_name);
        assert_eq!(result, "default-service");
    }

    #[test]
    fn test_detect_service_name_from_header() {
        let mut headers = HashMap::new();
        headers.insert("x-sp-service-name".to_string(), "header-service".to_string());
        let config_name = "default-service";
        
        let result = detect_service_name(&headers, config_name);
        assert_eq!(result, "header-service");
    }

    #[test]
    fn test_detect_service_name_header_overrides_config() {
        let mut headers = HashMap::new();
        headers.insert("x-sp-service-name".to_string(), "header-service".to_string());
        let config_name = "my-service";
        
        let result = detect_service_name(&headers, config_name);
        assert_eq!(result, "my-service"); // Config takes precedence if not default
    }

    #[test]
    fn test_detect_service_name_empty_header() {
        let mut headers = HashMap::new();
        headers.insert("x-sp-service-name".to_string(), "".to_string());
        let config_name = "default-service";
        
        let result = detect_service_name(&headers, config_name);
        assert_eq!(result, "default-service");
    }

    #[test]
    fn test_build_new_tracestate_with_no_existing() {
        let mut headers = HashMap::new();
        let traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
        let result = build_new_tracestate(&headers, traceparent, "");
        assert!(result.starts_with("x-sp-traceparent="));
    }

    #[test]
    fn test_build_new_tracestate_with_existing_entries() {
        let mut headers = HashMap::new();
        headers.insert("tracestate".to_string(), "vendor1=value1,vendor2=value2".to_string());
        let traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
        let result = build_new_tracestate(&headers, traceparent, "");
        assert!(result.contains("vendor1=value1"));
        assert!(result.contains("vendor2=value2"));
        assert!(result.starts_with("x-sp-traceparent="));
    }

    #[test]
    fn test_build_new_tracestate_replaces_existing_sp_entry() {
        let mut headers = HashMap::new();
        headers.insert("tracestate".to_string(), "x-sp-traceparent=old-value,vendor1=value1".to_string());
        let traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
        let result = build_new_tracestate(&headers, traceparent, "");
        assert!(result.starts_with("x-sp-traceparent="));
        assert!(result.contains("vendor1=value1"));
        assert!(!result.contains("old-value"));
    }

    #[test]
    fn test_build_new_tracestate_handles_whitespace() {
        let mut headers = HashMap::new();
        headers.insert("tracestate".to_string(), " vendor1=value1 , vendor2=value2 ".to_string());
        let traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
        let result = build_new_tracestate(&headers, traceparent, "");
        assert!(result.contains("vendor1=value1"));
        assert!(result.contains("vendor2=value2"));
    }

    #[test]
    fn test_build_new_tracestate_empty_existing() {
        let mut headers = HashMap::new();
        headers.insert("tracestate".to_string(), "".to_string());
        let traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
        let result = build_new_tracestate(&headers, traceparent, "");
        assert!(result.starts_with("x-sp-traceparent="));
    }
}