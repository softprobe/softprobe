use serde_json;

#[derive(Debug, Clone)]
pub struct CollectionRule {
    pub http: HttpCollectionRule,
}

#[derive(Debug, Clone)]
pub struct HttpCollectionRule {
    pub server: ServerConfig,
    pub client: Vec<ClientConfig>,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub host: String,
    pub paths: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ExemptionRule {
    pub host_patterns: Vec<String>,
    pub path_patterns: Vec<String>,
}

impl Default for ExemptionRule {
    fn default() -> Self {
        Self {
            host_patterns: vec![],
            path_patterns: vec![
                "/v1/traces".to_string(),
                "/api/traces".to_string(),
                "/v1/metrics".to_string(),
                "/api/metrics".to_string(),
                "/v1/logs".to_string(),
                "/api/logs".to_string(),
                "/otlp/v1/traces".to_string(),
                "/otlp/v1/metrics".to_string(),
                "/otlp/v1/logs".to_string(),
            ],
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub sp_backend_url: String,
    pub service_name: String,
    pub traffic_direction: Option<String>,
    pub collection_rules: Vec<CollectionRule>,
    pub exemption_rules: Vec<ExemptionRule>,
    pub public_key: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sp_backend_url: "https://o.softprobe.ai".to_string(),
            traffic_direction: None,
            service_name: "default-service".to_string(),
            collection_rules: vec![],
            exemption_rules: vec![],
            public_key: String::new(),
        }
    }
}

impl Config {
    pub fn parse_from_json(&mut self, config_bytes: &[u8]) -> bool {
        if let Ok(config_str) = std::str::from_utf8(config_bytes) {
            if let Ok(config_json) = serde_json::from_str::<serde_json::Value>(config_str) {
                self.parse_backend_url(&config_json);
                self.parse_traffic_direction(&config_json);
                self.parse_service_name(&config_json);
                self.parse_public_key(&config_json);
                self.parse_collection_rules(&config_json);
                self.parse_exemption_rules(&config_json);
                return true;
            }
        }
        false
    }

    fn parse_backend_url(&mut self, config_json: &serde_json::Value) {
        if let Some(backend_url) = config_json.get("sp_backend_url").and_then(|v| v.as_str()) {
            self.sp_backend_url = backend_url.to_string();
            crate::sp_info!("Configured backend URL: {}", self.sp_backend_url);
        }
    }

    fn parse_traffic_direction(&mut self, config_json: &serde_json::Value) {
        if let Some(direction) = config_json
            .get("traffic_direction")
            .and_then(|v| v.as_str())
        {
            self.traffic_direction = Some(direction.to_string());
            crate::sp_info!("Configured traffic direction: {:?}", self.traffic_direction);
        }
    }

    fn parse_service_name(&mut self, config_json: &serde_json::Value) {
        if let Some(service_name) = config_json.get("service_name").and_then(|v| v.as_str()) {
            self.service_name = service_name.to_string();
            crate::sp_info!("Configured service name: {}", self.service_name);
        }
    }

    fn parse_public_key(&mut self, config_json: &serde_json::Value) {
        if let Some(public_key) = config_json.get("public_key").and_then(|v| v.as_str()) {
            self.public_key = public_key.to_string();
            let masked = if self.public_key.len() > 4 { "****" } else { "" };
            crate::sp_info!("Public key configured: {}", masked);
        }
    }

    fn parse_collection_rules(&mut self, config_json: &serde_json::Value) {
        if let Some(rules) = config_json.get("collectionRules") {
            let (server_paths, client_configs) = self.extract_collection_data(rules);
            self.create_collection_rules(server_paths, client_configs);
        }
    }

    fn extract_collection_data(&self, rules: &serde_json::Value) -> (Vec<String>, Vec<(String, Vec<String>)>) {
        let mut server_paths = Vec::new();
        let mut client_configs = Vec::new();

        // Extract server paths
        if let Some(server_obj) = rules.get("http").and_then(|v| v.get("server")) {
            if let Some(server_array) = server_obj.as_array() {
                for server_entry in server_array {
                    if let Some(path) = server_entry.get("path").and_then(|v| v.as_str()) {
                        server_paths.push(path.to_string());
                    }
                }
            }
        }

        // Extract client configs
        if let Some(client_obj) = rules.get("http").and_then(|v| v.get("client")) {
            if let Some(client_array) = client_obj.as_array() {
                for client_entry in client_array {
                    if let Some(host) = client_entry.get("host").and_then(|v| v.as_str()) {
                        let mut paths = Vec::new();
                        if let Some(paths_obj) = client_entry.get("paths") {
                            if let Some(paths_array) = paths_obj.as_array() {
                                for path_entry in paths_array {
                                    if let Some(path) = path_entry.as_str() {
                                        paths.push(path.to_string());
                                    }
                                }
                            }
                        }
                        client_configs.push((host.to_string(), paths));
                    }
                }
            }
        }

        (server_paths, client_configs)
    }

    fn create_collection_rules(&mut self, server_paths: Vec<String>, client_configs: Vec<(String, Vec<String>)>) {
        // Create rules for each server path
        for server_path in server_paths {
            crate::sp_info!("Added server collection rule: {}", server_path);
            self.collection_rules.push(CollectionRule {
                http: HttpCollectionRule {
                    server: ServerConfig {
                        path: server_path,
                    },
                    client: vec![],
                },
            });
        }

        // Create rules for each client config
        for (client_host, client_paths) in &client_configs {
            crate::sp_info!("Added client collection rule: host={}, paths={:?}", client_host, client_paths);
            self.collection_rules.push(CollectionRule {
                http: HttpCollectionRule {
                    server: ServerConfig {
                        path: String::new(),
                    },
                    client: vec![ClientConfig {
                        host: client_host.clone(),
                        paths: client_paths.clone(),
                    }],
                },
            });
        }
    }

    fn parse_exemption_rules(&mut self, config_json: &serde_json::Value) {
        if let Some(exemption_rules) = config_json.get("exemptionRules") {
            if let Some(exemption_array) = exemption_rules.as_array() {
                for exemption_entry in exemption_array {
                    let (host_patterns, path_patterns) = self.extract_exemption_patterns(exemption_entry);
                    
                    if !path_patterns.is_empty() {
                        crate::sp_info!("Added exemption rule: hostPatterns={:?}, pathPatterns={:?}", host_patterns, path_patterns);
                        self.exemption_rules.push(ExemptionRule {
                            host_patterns,
                            path_patterns,
                        });
                    }
                }
            }
        } else {
            // Add default exemption rule if none configured
            let default_rule = ExemptionRule::default();
            crate::sp_info!("Added default exemption rule: pathPatterns={:?}", default_rule.path_patterns);
            self.exemption_rules.push(default_rule);
        }
    }

    fn extract_exemption_patterns(&self, exemption_entry: &serde_json::Value) -> (Vec<String>, Vec<String>) {
        let mut host_patterns = Vec::new();
        let mut path_patterns = Vec::new();

        if let Some(hosts) = exemption_entry.get("hostPatterns") {
            if let Some(hosts_array) = hosts.as_array() {
                for host_entry in hosts_array {
                    if let Some(host) = host_entry.as_str() {
                        host_patterns.push(host.to_string());
                    }
                }
            }
        }

        if let Some(paths) = exemption_entry.get("pathPatterns") {
            if let Some(paths_array) = paths.as_array() {
                for path_entry in paths_array {
                    if let Some(path) = path_entry.as_str() {
                        path_patterns.push(path.to_string());
                    }
                }
            }
        } else {
            // Use default path patterns if none specified
            path_patterns = ExemptionRule::default().path_patterns;
        }

        (host_patterns, path_patterns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.sp_backend_url, "https://o.softprobe.ai");
        assert_eq!(config.service_name, "default-service");
        assert!(config.traffic_direction.is_none());
        assert!(config.collection_rules.is_empty());
        assert!(config.public_key.is_empty());
    }

    #[test]
    fn test_exemption_rule_default() {
        let rule = ExemptionRule::default();
        assert!(rule.host_patterns.is_empty());
        assert!(rule.path_patterns.contains(&"/v1/traces".to_string()));
        assert!(rule.path_patterns.contains(&"/api/traces".to_string()));
    }

    #[test]
    fn test_config_parse_backend_url() {
        let mut config = Config::default();
        let json_config = json!({
            "sp_backend_url": "https://custom.backend.com"
        });
        let config_str = serde_json::to_string(&json_config).unwrap();
        
        assert!(config.parse_from_json(config_str.as_bytes()));
        assert_eq!(config.sp_backend_url, "https://custom.backend.com");
    }

    #[test]
    fn test_config_parse_service_name() {
        let mut config = Config::default();
        let json_config = json!({
            "service_name": "test-service"
        });
        let config_str = serde_json::to_string(&json_config).unwrap();
        
        assert!(config.parse_from_json(config_str.as_bytes()));
        assert_eq!(config.service_name, "test-service");
    }

    #[test]
    fn test_config_parse_traffic_direction() {
        let mut config = Config::default();
        let json_config = json!({
            "traffic_direction": "outbound"
        });
        let config_str = serde_json::to_string(&json_config).unwrap();
        
        assert!(config.parse_from_json(config_str.as_bytes()));
        assert_eq!(config.traffic_direction, Some("outbound".to_string()));
    }

    #[test]
    fn test_config_parse_api_key() {
        let mut config = Config::default();
        let json_config = json!({
            "api_key": "test-api-key-123"
        });
        let config_str = serde_json::to_string(&json_config).unwrap();
        
        assert!(config.parse_from_json(config_str.as_bytes()));
        assert_eq!(config.public_key, "test-api-key-123");
    }

    #[test]
    fn test_config_parse_collection_rules() {
        let mut config = Config::default();
        let json_config = json!({
            "collectionRules": {
                "http": {
                    "server": [
                        {"path": "/api/test"}
                    ],
                    "client": [
                        {
                            "host": "example.com",
                            "paths": ["/api/endpoint1", "/api/endpoint2"]
                        }
                    ]
                }
            }
        });
        let config_str = serde_json::to_string(&json_config).unwrap();
        
        assert!(config.parse_from_json(config_str.as_bytes()));
        assert_eq!(config.collection_rules.len(), 2);
        
        // Check server rule
        assert_eq!(config.collection_rules[0].http.server.path, "/api/test");
        assert!(config.collection_rules[0].http.client.is_empty());
        
        // Check client rule
        assert!(config.collection_rules[1].http.server.path.is_empty());
        assert_eq!(config.collection_rules[1].http.client.len(), 1);
        assert_eq!(config.collection_rules[1].http.client[0].host, "example.com");
        assert_eq!(config.collection_rules[1].http.client[0].paths.len(), 2);
    }

    #[test]
    fn test_config_parse_exemption_rules() {
        let mut config = Config::default();
        let json_config = json!({
            "exemptionRules": [
                {
                    "hostPatterns": ["internal.com", "localhost"],
                    "pathPatterns": ["/health", "/metrics"]
                }
            ]
        });
        let config_str = serde_json::to_string(&json_config).unwrap();
        
        assert!(config.parse_from_json(config_str.as_bytes()));
        assert_eq!(config.exemption_rules.len(), 1);
        assert_eq!(config.exemption_rules[0].host_patterns.len(), 2);
        assert_eq!(config.exemption_rules[0].path_patterns.len(), 2);
        assert!(config.exemption_rules[0].host_patterns.contains(&"internal.com".to_string()));
        assert!(config.exemption_rules[0].path_patterns.contains(&"/health".to_string()));
    }

    #[test]
    fn test_config_parse_invalid_json() {
        let mut config = Config::default();
        let original_backend = config.sp_backend_url.clone();
        
        // Test with invalid JSON
        assert!(!config.parse_from_json(b"invalid json"));
        assert_eq!(config.sp_backend_url, original_backend);
        
        // Test with invalid UTF-8
        assert!(!config.parse_from_json(&[0xFF, 0xFE]));
        assert_eq!(config.sp_backend_url, original_backend);
    }

    #[test]
    fn test_config_parse_empty_exemption_rules() {
        let mut config = Config::default();
        let json_config = json!({});
        let config_str = serde_json::to_string(&json_config).unwrap();
        
        assert!(config.parse_from_json(config_str.as_bytes()));
        // Should add default exemption rule
        assert_eq!(config.exemption_rules.len(), 1);
        assert!(config.exemption_rules[0].path_patterns.contains(&"/v1/traces".to_string()));
    }
}