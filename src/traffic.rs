use crate::config::Config;
use proxy_wasm::traits::Context;
use regex::Regex;
use std::collections::HashMap;
// use url::Url; // no longer needed here

pub trait TrafficAnalyzer {
    fn detect_traffic_direction(&self, config: &Config) -> String;
    fn is_from_istio_ingressgateway(&self) -> bool;
    fn should_collect_by_rules(&self, config: &Config, request_headers: &HashMap<String, String>) -> bool;
}

pub trait RequestHeadersAccess {
    fn get_context_property(&self, path: Vec<&str>) -> Option<Vec<u8>>;
    fn get_request_header(&self, name: &str) -> Option<String>;
}

impl<T: Context> TrafficAnalyzer for T where T: RequestHeadersAccess {
    fn detect_traffic_direction(&self, config: &Config) -> String {
        // Method 1: Use configured traffic direction if available
        if let Some(ref direction) = config.traffic_direction {
            crate::sp_debug!("Using configured traffic direction: {}", direction);
            return match direction.as_str() {
                "server" => "inbound".to_string(),
                "client" => "outbound".to_string(),
                _ => direction.clone(),
            };
        }

        // Method 2: Check if this is client or server role
        // Client (发起请求) → outbound, Server (接收请求) → inbound
        
        // Check if this is a client making outbound requests
        if let Some(upstream_host) = self.get_context_property(vec!["upstream_host"]) {
            if let Ok(host) = String::from_utf8(upstream_host) {
                crate::sp_debug!("Detected upstream_host: {} → client role (outbound)", host);
                return "outbound".to_string();
            }
        }

        // Check cluster name for client/server role indication
        if let Some(cluster_name) = self.get_context_property(vec!["cluster_name"]) {
            if let Ok(cluster) = String::from_utf8(cluster_name) {
                crate::sp_debug!("Detected cluster_name: {}", cluster);
                if cluster.starts_with("outbound|") {
                    crate::sp_debug!("Client role detected from cluster name → outbound");
                    return "outbound".to_string();
                } else if cluster.starts_with("inbound|") {
                    crate::sp_debug!("Server role detected from cluster name → inbound");
                    return "inbound".to_string();
                }
            }
        }

        // Log request protocol for debugging (but don't use it for direction detection)
        if let Some(request_protocol) = self.get_context_property(vec!["request", "protocol"]) {
            if let Ok(protocol) = String::from_utf8(request_protocol) {
                crate::sp_debug!("Request protocol: {}", protocol);
                // Note: HTTP protocol presence doesn't indicate traffic direction
                // Both inbound and outbound traffic can have HTTP protocol info
            }
        }

        // Check connection mTLS info for server role (more reliable than protocol)
        if let Some(connection_mtls) = self.get_context_property(vec!["connection", "mtls"]) {
            if let Ok(mtls_info) = String::from_utf8(connection_mtls) {
                crate::sp_debug!("Connection mTLS info: {}", mtls_info);
                // 如果有客户端证书信息，通常表示这是服务端接收请求
                if mtls_info.contains("client") {
                    crate::sp_debug!("Server role detected (has client cert info) → inbound");
                    return "inbound".to_string();
                }
            }
        }

        // Method 2: Check listener direction
        if let Some(listener_direction) = self.get_context_property(vec!["listener_direction"]) {
            if let Ok(direction) = String::from_utf8(listener_direction) {
                crate::sp_debug!("Detected listener_direction: {}", direction);
                return direction;
            }
        }

        // Method 2: Check metadata direction
        if let Some(metadata) = self.get_context_property(vec![
            "metadata",
            "filter_metadata",
            "envoy.common",
            "direction",
        ]) {
            if let Ok(direction) = String::from_utf8(metadata) {
                crate::sp_debug!("Detected direction from metadata: {}", direction);
                return direction;
            }
        }

        // Method 3: Check by port range (source address)
        if let Some(downstream_local_address) = self.get_context_property(vec!["source", "address"]) {
            if let Ok(address) = String::from_utf8(downstream_local_address) {
                crate::sp_debug!("Detected downstream address: {}", address);
                if address.contains(":15006") {
                    return "inbound".to_string();
                }
                if address.contains(":15001") {
                    return "outbound".to_string();
                }
            }
        }

        // Method 5: Heuristic using request headers
        if self.get_request_header("x-forwarded-for").is_some() {
            crate::sp_debug!("Found x-forwarded-for header, likely inbound traffic");
            return "inbound".to_string();
        }

        // Note: host/authority header indicates the target service, not the source
        // In SERVER mode, if we receive a request with host header, it's inbound traffic
        // In CLIENT mode, if we're making a request to a host, it's outbound traffic
        // Since we can't reliably determine client vs server role from headers alone,
        // we should rely on other methods above rather than host header heuristics

        crate::sp_debug!("Could not determine traffic direction, using 'auto'");
        "auto".to_string()
    }

    fn is_from_istio_ingressgateway(&self) -> bool {
        let ingress_patterns = [
            ("node", "metadata", "WORKLOAD_NAME"),
            ("node", "metadata", "app"),
            ("node", "metadata", "NAME"),
        ];

        for pattern in &ingress_patterns {
            if let Some(value) = self.get_context_property(vec![pattern.0, pattern.1, pattern.2]) {
                if let Ok(value_str) = String::from_utf8(value) {
                    if value_str.contains("istio-ingressgateway") {
                        return true;
                    }
                }
            }
        }

        // Check cluster metadata
        if let Some(cluster_metadata) = self.get_context_property(vec!["cluster_metadata"]) {
            if let Ok(metadata) = String::from_utf8(cluster_metadata) {
                if metadata.contains("istio-ingressgateway") {
                    return true;
                }
            }
        }

        // Check source workload
        if let Some(source_workload) = self.get_context_property(vec!["source", "workload", "name"]) {
            if let Ok(workload) = String::from_utf8(source_workload) {
                if workload.contains("istio-ingressgateway") {
                    return true;
                }
            }
        }

        // Check node ID
        if let Some(node_id) = self.get_context_property(vec!["node", "id"]) {
            if let Ok(id) = String::from_utf8(node_id) {
                if id.contains("istio-ingressgateway") {
                    return true;
                }
            }
        }

        // Check peer metadata header
        if let Some(peer_metadata) = self.get_request_header("x-envoy-peer-metadata-id") {
            if peer_metadata.contains("istio-ingressgateway") {
                return true;
            }
        }

        // Check labels
        if let Some(labels) = self.get_context_property(vec!["node", "metadata", "LABELS"]) {
            if let Ok(labels_str) = String::from_utf8(labels) {
                if labels_str.contains("istio-ingressgateway") {
                    return true;
                }
            }
        }

        false
    }

    fn should_collect_by_rules(&self, config: &Config, request_headers: &HashMap<String, String>) -> bool {
        should_collect_by_rules_for_headers(config, request_headers)
    }

}

// Implement RequestHeadersAccess for concrete contexts (e.g., SpHttpContext) in their modules

pub(crate) fn should_collect_by_rules_for_headers(
    config: &Config,
    request_headers: &HashMap<String, String>,
) -> bool {
    // First check exemption rules
    if is_exempted_for_headers(config, request_headers) {
        crate::sp_debug!("Request is exempted from collection");
        return false;
    }

    // If no rules configured, collect all requests
    if config.collection_rules.is_empty() {
        crate::sp_debug!("No collection rules configured, collecting all requests");
        return true;
    }

    crate::sp_debug!("Checking collection rules, total rules: {}", config.collection_rules.len());

    // Try inbound rules matching
    let inbound_matched = check_inbound_rules(config, request_headers);
    if inbound_matched {
        crate::sp_debug!("Request matched inbound rules, collecting");
        return true;
    }

    // Try outbound rules matching
    let outbound_matched = check_outbound_rules(config, request_headers);
    if outbound_matched {
        crate::sp_debug!("Request matched outbound rules, collecting");
        return true;
    }

    // Check if any rules are configured
    let has_server_rules = config
        .collection_rules
        .iter()
        .any(|rule| !rule.http.server.path.is_empty());
    let has_client_rules = config
        .collection_rules
        .iter()
        .any(|rule| !rule.http.client.is_empty());

    if !has_server_rules && !has_client_rules {
        crate::sp_debug!("No specific rules configured, collecting all requests");
        return true;
    }

    crate::sp_debug!("No rules matched, not collecting");
    false
}

pub(crate) fn is_exempted_for_headers(
    config: &Config,
    request_headers: &HashMap<String, String>,
) -> bool {
    if config.exemption_rules.is_empty() {
        return false;
    }

    let request_host = request_headers
        .get("host")
        .or_else(|| request_headers.get(":authority"))
        .cloned();
    let request_path = request_headers.get(":path").cloned();

    let (client_host, client_path) = crate::http_helpers::extract_client_info(request_headers);

    crate::sp_debug!(
        "Checking exemption - request_host: {:?}, request_path: {:?}, client_host: {:?}, client_path: {:?}",
        request_host, request_path, client_host, client_path
    );

    for rule in &config.exemption_rules {
        let host_matched = check_host_patterns(&rule.host_patterns, &request_host, &client_host);
        let path_matched = check_path_patterns(&rule.path_patterns, &request_path, &client_path);

        if host_matched && path_matched {
            crate::sp_info!(
                "Request exempted by rule - hostPatterns: {:?}, pathPatterns: {:?}",
                rule.host_patterns, rule.path_patterns
            );
            return true;
        }
    }

    false
}

fn check_inbound_rules(config: &Config, request_headers: &HashMap<String, String>) -> bool {
    if let Some(request_path) = request_headers.get(":path") {
        crate::sp_debug!("Checking inbound rules for path: {}", request_path);

        for (i, rule) in config.collection_rules.iter().enumerate() {
            if !rule.http.server.path.is_empty() {
                crate::sp_debug!("Checking inbound rule {}: serverPath='{}'", i, rule.http.server.path);
                if match_pattern(&rule.http.server.path, request_path) {
                    crate::sp_debug!("Inbound request matched server_path: {}", rule.http.server.path);
                    return true;
                }
            }
        }
    }
    false
}

fn check_outbound_rules(config: &Config, request_headers: &HashMap<String, String>) -> bool {
    let (client_host, client_path) = crate::http_helpers::extract_client_info(request_headers);
    crate::sp_debug!("Checking outbound rules with client_host: {:?}, client_path: {:?}", client_host, client_path);

    for (i, rule) in config.collection_rules.iter().enumerate() {
        if !rule.http.client.is_empty() {
            for client_config in &rule.http.client {
                crate::sp_debug!("Checking outbound rule {}: clientHost={}, clientPaths={:?}", i, client_config.host, client_config.paths);

                // Check client host
                if let Some(ref actual_client_host) = client_host {
                    if !match_pattern(&client_config.host, actual_client_host) {
                        crate::sp_debug!("Client host mismatch: expected={}, actual={}", client_config.host, actual_client_host);
                        continue;
                    }
                } else {
                    crate::sp_debug!("No client host info available, but rule requires it");
                    continue;
                }

                crate::sp_debug!("Client host matched");

                // Check client paths if configured
                if !client_config.paths.is_empty() {
                    if let Some(ref actual_client_path) = client_path {
                        let matched = client_config.paths.iter().any(|client_path| {
                            let matches = match_pattern(client_path, actual_client_path);
                            crate::sp_debug!("Client path match: pattern='{}' result={}", client_path, matches);
                            matches
                        });
                        if !matched {
                            crate::sp_debug!("Client paths did not match");
                            continue;
                        }
                    } else {
                        crate::sp_debug!("No client path info available");
                        continue;
                    }
                }

                crate::sp_debug!("Outbound request matched all criteria - client_host: {}, client_paths: {:?}", client_config.host, client_config.paths);
                return true;
            }
        }
    }
    false
}

// client info extraction is provided by crate::http_helpers::extract_client_info

fn check_host_patterns(
    host_patterns: &[String],
    request_host: &Option<String>,
    client_host: &Option<String>,
) -> bool {
    if host_patterns.is_empty() {
        return true;
    }

    // Check inbound request host
    if let Some(ref host) = request_host {
        for pattern in host_patterns {
            if match_pattern(pattern, host) {
                crate::sp_debug!("Host pattern '{}' matched request host '{}'", pattern, host);
                return true;
            }
        }
    }

    // Check outbound client host
    if let Some(ref host) = client_host {
        for pattern in host_patterns {
            if match_pattern(pattern, host) {
                crate::sp_debug!("Host pattern '{}' matched client host '{}'", pattern, host);
                return true;
            }
        }
    }

    false
}

fn check_path_patterns(
    path_patterns: &[String],
    request_path: &Option<String>,
    client_path: &Option<String>,
) -> bool {
    if path_patterns.is_empty() {
        return true;
    }

    // Check inbound request path
    if let Some(ref path) = request_path {
        for pattern in path_patterns {
            if match_pattern(pattern, path) {
                crate::sp_debug!("Path pattern '{}' matched request path '{}'", pattern, path);
                return true;
            }
        }
    }

    // Check outbound client path
    if let Some(ref path) = client_path {
        for pattern in path_patterns {
            if match_pattern(pattern, path) {
                crate::sp_debug!("Path pattern '{}' matched client path '{}'", pattern, path);
                return true;
            }
        }
    }

    false
}

fn match_pattern(pattern: &str, text: &str) -> bool {
    crate::sp_debug!("Matching pattern '{}' against text '{}'", pattern, text);
    match Regex::new(pattern) {
        Ok(re) => {
            let result = re.is_match(text);
            crate::sp_debug!("Regex match result: {}", result);
            result
        }
        Err(e) => {
            crate::sp_warn!("Invalid regex pattern '{}': {}", pattern, e);
            let result = pattern == text;
            crate::sp_debug!("Fallback to exact match: {}", result);
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        ClientConfig, CollectionRule, Config, ExemptionRule, HttpCollectionRule, ServerConfig,
    };

    fn demo_config() -> Config {
        Config {
            collection_rules: vec![
                CollectionRule {
                    http: HttpCollectionRule {
                        server: ServerConfig {
                            path: "^\\/(api|airline).*".to_string(),
                        },
                        client: vec![],
                    },
                },
                CollectionRule {
                    http: HttpCollectionRule {
                        server: ServerConfig {
                            path: String::new(),
                        },
                        client: vec![ClientConfig {
                            host: ".*".to_string(),
                            paths: vec!["^\\/(api|airline).*".to_string()],
                        }],
                    },
                },
            ],
            exemption_rules: vec![ExemptionRule::default()],
            ..Config::default()
        }
    }

    fn headers(path: &str) -> HashMap<String, String> {
        HashMap::from([
            (":authority".to_string(), "demo-ota.onpremis.svc.cluster.local".to_string()),
            (":path".to_string(), path.to_string()),
        ])
    }

    #[test]
    fn collects_api_paths() {
        assert!(should_collect_by_rules_for_headers(
            &demo_config(),
            &headers("/api/orders")
        ));
    }

    #[test]
    fn collects_airline_paths() {
        assert!(should_collect_by_rules_for_headers(
            &demo_config(),
            &headers("/airline-api/v1/flights")
        ));
    }

    #[test]
    fn skips_non_matching_paths() {
        assert!(!should_collect_by_rules_for_headers(
            &demo_config(),
            &headers("/health")
        ));
    }

    #[test]
    fn skips_otel_logs_even_when_host_matches_client_rule() {
        assert!(!should_collect_by_rules_for_headers(
            &demo_config(),
            &headers("/v1/logs")
        ));
    }

    #[test]
    fn missing_session_id_does_not_change_collection_decision() {
        let request_headers = headers("/v1/logs");
        assert!(!request_headers.contains_key("x-sp-session-id"));
        assert!(!should_collect_by_rules_for_headers(&demo_config(), &request_headers));
    }
}
