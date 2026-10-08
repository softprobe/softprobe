use std::collections::HashMap;
use url::Url;

/// Extract client information from request headers
pub fn extract_client_info(request_headers: &HashMap<String, String>) -> (Option<String>, Option<String>) {
    let mut client_host = None;
    let mut client_path = None;

    // Extract from Referer header
    if let Some(referer) = request_headers.get("referer") {
        crate::sp_debug!("Found referer header: {}", referer);
        if let Ok(url) = Url::parse(referer) {
            client_host = url.host_str().map(|h| h.to_string());
            client_path = Some(url.path().to_string());
            crate::sp_debug!("Parsed referer host={:?} path={:?}", client_host, client_path);
        } else {
            crate::sp_debug!("Failed to parse referer as URL: {}", referer);
        }
    }

    // Extract from Origin header (if Referer doesn't exist)
    if client_host.is_none() {
        if let Some(origin) = request_headers.get("origin") {
            crate::sp_debug!("Found origin header: {}", origin);
            if let Ok(url) = Url::parse(origin) {
                client_host = url.host_str().map(|h| h.to_string());
                crate::sp_debug!("Parsed origin host={:?}", client_host);
            } else {
                crate::sp_debug!("Failed to parse origin as URL: {}", origin);
            }
        }
    }

    // Extract from Host header (as fallback)
    if client_host.is_none() {
        if let Some(host) = request_headers.get("host") {
            crate::sp_debug!("Found host header: {}", host);
            if let Ok(url) = Url::parse(&format!("http://{}", host)) {
                client_host = url.host_str().map(|h| h.to_string());
                crate::sp_debug!("Parsed host header host={:?}", client_host);
            }
        }
    }

    // Get client domain from Host header
    if client_host.is_none() {
        client_host = request_headers
            .get("host")
            .or_else(|| request_headers.get(":authority"))
            .cloned();
    }

    // Get client path directly from request path
    if client_path.is_none() {
        client_path = request_headers.get(":path").cloned();
    }

    crate::sp_debug!("Final client info computed host={:?} path={:?}", client_host, client_path);
    (client_host, client_path)
}

/// Get backend authority from URL
pub fn get_backend_authority(backend_url: &str) -> String {
    match Url::parse(backend_url) {
        Ok(url) => {
            if let Some(host) = url.host_str() {
                // For HTTPS, don't include the default port 443
                // For HTTP, don't include the default port 80
                match url.port() {
                    Some(port) => {
                        let default_port = match url.scheme() {
                            "https" => 443,
                            "http" => 80,
                            _ => 80,
                        };
                        if port == default_port {
                            host.to_string()
                        } else {
                            format!("{}:{}", host, port)
                        }
                    }
                    None => host.to_string(),
                }
            } else {
                "o.softprobe.ai".to_string()
            }
        }
        Err(_) => "o.softprobe.ai".to_string(),
    }
}

/// Build Envoy cluster name from backend URL
pub fn get_backend_cluster_name(backend_url: &str) -> String {
    match Url::parse(backend_url) {
        Ok(url) => {
            if let Some(host) = url.host_str() {
                let port = match url.scheme() {
                    "https" => url.port().unwrap_or(443),
                    "http" => url.port().unwrap_or(80),
                    _ => url.port().unwrap_or(80),
                };
                format!("outbound|{}||{}", port, host)
            } else {
                "outbound|443||o.softprobe.ai".to_string()
            }
        }
        Err(_) => "outbound|443||o.softprobe.ai".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_extract_client_info_from_referer() {
        let mut headers = HashMap::new();
        headers.insert("referer".to_string(), "https://example.com/page?param=value".to_string());
        
        let (host, path) = extract_client_info(&headers);
        assert_eq!(host, Some("example.com".to_string()));
        assert_eq!(path, Some("/page".to_string()));
    }

    #[test]
    fn test_extract_client_info_from_origin() {
        let mut headers = HashMap::new();
        headers.insert("origin".to_string(), "https://api.example.com".to_string());
        
        let (host, path) = extract_client_info(&headers);
        assert_eq!(host, Some("api.example.com".to_string()));
        assert_eq!(path, None);
    }

    #[test]
    fn test_extract_client_info_from_host_header() {
        let mut headers = HashMap::new();
        headers.insert("host".to_string(), "service.internal".to_string());
        
        let (host, path) = extract_client_info(&headers);
        assert_eq!(host, Some("service.internal".to_string()));
        assert_eq!(path, None);
    }

    #[test]
    fn test_extract_client_info_from_authority() {
        let mut headers = HashMap::new();
        headers.insert(":authority".to_string(), "api.service.com:8080".to_string());
        
        let (host, path) = extract_client_info(&headers);
        assert_eq!(host, Some("api.service.com:8080".to_string()));
        assert_eq!(path, None);
    }

    #[test]
    fn test_extract_client_info_with_path() {
        let mut headers = HashMap::new();
        headers.insert("host".to_string(), "service.internal".to_string());
        headers.insert(":path".to_string(), "/api/v1/users".to_string());
        
        let (host, path) = extract_client_info(&headers);
        assert_eq!(host, Some("service.internal".to_string()));
        assert_eq!(path, Some("/api/v1/users".to_string()));
    }

    #[test]
    fn test_extract_client_info_referer_priority() {
        let mut headers = HashMap::new();
        headers.insert("referer".to_string(), "https://referer.com/page".to_string());
        headers.insert("origin".to_string(), "https://origin.com".to_string());
        headers.insert("host".to_string(), "host.com".to_string());
        
        let (host, path) = extract_client_info(&headers);
        assert_eq!(host, Some("referer.com".to_string()));
        assert_eq!(path, Some("/page".to_string()));
    }

    #[test]
    fn test_extract_client_info_invalid_referer() {
        let mut headers = HashMap::new();
        headers.insert("referer".to_string(), "invalid-url".to_string());
        headers.insert("host".to_string(), "fallback.com".to_string());
        
        let (host, path) = extract_client_info(&headers);
        assert_eq!(host, Some("fallback.com".to_string()));
        assert_eq!(path, None);
    }

    #[test]
    fn test_get_backend_authority_https_default_port() {
        let authority = get_backend_authority("https://o.softprobe.ai");
        assert_eq!(authority, "o.softprobe.ai");
    }

    #[test]
    fn test_get_backend_authority_https_custom_port() {
        let authority = get_backend_authority("https://o.softprobe.ai:8443");
        assert_eq!(authority, "o.softprobe.ai:8443");
    }

    #[test]
    fn test_get_backend_authority_http_default_port() {
        let authority = get_backend_authority("http://example.com");
        assert_eq!(authority, "example.com");
    }

    #[test]
    fn test_get_backend_authority_http_custom_port() {
        let authority = get_backend_authority("http://example.com:8080");
        assert_eq!(authority, "example.com:8080");
    }

    #[test]
    fn test_get_backend_authority_invalid_url() {
        let authority = get_backend_authority("invalid-url");
        assert_eq!(authority, "o.softprobe.ai");
    }

    #[test]
    fn test_get_backend_cluster_name_https() {
        let cluster = get_backend_cluster_name("https://o.softprobe.ai");
        assert_eq!(cluster, "outbound|443||o.softprobe.ai");
    }

    #[test]
    fn test_get_backend_cluster_name_https_custom_port() {
        let cluster = get_backend_cluster_name("https://o.softprobe.ai:8443");
        assert_eq!(cluster, "outbound|8443||o.softprobe.ai");
    }

    #[test]
    fn test_get_backend_cluster_name_http() {
        let cluster = get_backend_cluster_name("http://example.com");
        assert_eq!(cluster, "outbound|80||example.com");
    }

    #[test]
    fn test_get_backend_cluster_name_http_custom_port() {
        let cluster = get_backend_cluster_name("http://example.com:3000");
        assert_eq!(cluster, "outbound|3000||example.com");
    }

    #[test]
    fn test_get_backend_cluster_name_invalid_url() {
        let cluster = get_backend_cluster_name("invalid-url");
        assert_eq!(cluster, "outbound|443||o.softprobe.ai");
    }

    #[test]
    fn test_extract_client_info_no_headers() {
        let headers = HashMap::new();
        
        let (host, path) = extract_client_info(&headers);
        assert_eq!(host, None);
        assert_eq!(path, None);
    }
}