use proxy_wasm::traits::*;
use proxy_wasm::types::*;
use std::collections::HashMap;

use crate::config::Config;
use crate::otel::{SpanBuilder, serialize_traces_data};
use crate::headers::{detect_service_name, build_new_tracestate};
use crate::http_helpers::{get_backend_authority, get_backend_cluster_name};
use crate::trace_context::extract_and_propagate_trace_context;
use crate::traffic::TrafficAnalyzer;

pub struct SpHttpContext {
    pub(crate) _context_id: u32,
    pub(crate) request_headers: HashMap<String, String>,
    pub(crate) request_body: Vec<u8>,
    pub(crate) response_headers: HashMap<String, String>,
    pub(crate) response_body: Vec<u8>,
    pub(crate) span_builder: SpanBuilder,
    pub(crate) pending_inject_call_token: Option<u32>,
    pub(crate) pending_save_call_token: Option<u32>,
    pub(crate) injected: bool,
    pub(crate) config: Config,
    pub(crate) url_host: Option<String>,
    pub(crate) url_path: Option<String>,
    pub(crate) is_from_ingressgateway: bool,  // Cache to avoid calling get_request_header during response phase
    pub(crate) request_start_time: Option<u64>,  // Store request start time in nanoseconds
}

impl SpHttpContext {
   pub fn new(context_id: u32, config: Config) -> Self {
        let mut span_builder = SpanBuilder::new();
        span_builder = span_builder
            .with_service_name(config.service_name.clone())
            .with_traffic_direction(
                config
                    .traffic_direction
                    .clone()
                    .unwrap_or_else(|| "auto".to_string()),
            );
        Self {
            _context_id: context_id,
            config,
            request_headers: HashMap::new(),
            request_body: Vec::new(),
            response_headers: HashMap::new(),
            response_body: Vec::new(),
            span_builder,
            pending_inject_call_token: None,
            pending_save_call_token: None,
            injected: false,
            url_host: None,
            url_path: None,
            is_from_ingressgateway: false,  // Initialize to false, will be set during request processing
            request_start_time: None,  // Initialize to None, will be set when request starts
        }
    }
    // Dispatch injection HTTP call (disabled)
    fn dispatch_injection_lookup(&mut self) -> Result<u32, String> {
        Err("Injection lookup is disabled".to_string())
    }

    fn update_url_info(&mut self) {
        // url.path from property system, fallback to :path header
        if let Some(prop) = self.get_property(vec!["request", "path"]) {
            if let Ok(path) = String::from_utf8(prop) {
                if !path.is_empty() {
                    self.url_path = Some(path);
                }
            }
        }
        if self.url_path.is_none() {
            if let Some(path_hdr) = self.request_headers.get(":path") {
                self.url_path = Some(path_hdr.clone());
            }
        }

        // url.host from :authority or host header
        let authority_or_host = self
            .request_headers
            .get(":authority")
            .cloned()
            .or_else(|| self.request_headers.get("host").cloned());

        // Keep port if present (use raw header value)
        if let Some(authority_value) = authority_or_host {
            if !authority_value.is_empty() {
                self.url_host = Some(authority_value);
            }
        }
    }

    fn dispatch_async_extraction_save(&mut self) {
        crate::sp_debug!("Starting async extraction save (host={:?}, path={:?})", self.url_host, self.url_path);

        // Early skip: Next.js RSC / prefetch requests
        if self.is_rsc_or_prefetch() {
            crate::sp_debug!("RSC/prefetch request detected, skipping trace upload");
            return;
        }

        // Check if this is a static resource request
        if self.is_static_resource() {
            crate::sp_debug!("Static resource detected, skipping trace upload");
            return;
        }

        crate::sp_debug!("Session ID present: {}", self.span_builder.has_session_id());

        // Collection rules are authoritative. Requests that do not match, including
        // telemetry endpoints such as /v1/logs, must not create or upload spans.
        if !self.should_collect_by_rules(&self.config, &self.request_headers) {
            crate::sp_debug!("Data extraction skipped based on collection rules");
            return;
        }

        crate::sp_debug!("Storing agent data asynchronously (backend={})", self.config.sp_backend_url);

        // Create extract span using references to avoid cloning
        let traces_data = self.span_builder.create_extract_span(
            &self.request_headers,
            &self.request_body,
            &self.response_headers,
            &self.response_body,
            self.url_host.as_deref(),
            self.url_path.as_deref(),
            self.request_start_time,  // Pass the stored request start time
        );

        // Serialize to protobuf
        let otel_data = match serialize_traces_data(&traces_data) {
            Ok(bytes) => bytes,
            Err(e) => {
                crate::sp_error!("Serialization error: {}", e);
                return;
            }
        };

        // Get backend authority from configured URL
        let authority = get_backend_authority(&self.config.sp_backend_url);

        // Prepare HTTP headers for the async save call
        let content_length = otel_data.len().to_string();
        let http_headers = vec![
            (":method", "POST"),
            (":path", "/v1/traces"),
            (":authority", &authority),
            ("content-type", "application/x-protobuf"),
            ("content-length", &content_length),
            ("x-public-key", &self.config.public_key),
        ];

        // Fire and forget async call to /v1/traces endpoint for storage
        let cluster_name = get_backend_cluster_name(&self.config.sp_backend_url);
        let timeout = std::time::Duration::from_secs(5);

        match self.dispatch_http_call(
            &cluster_name,
            http_headers,
            Some(&otel_data),
            vec![],
            timeout,
        ) {
            Ok(call_id) => {
                crate::sp_info!("Extraction: HTTP call dispatched successfully (call_id={})", call_id);
                self.pending_save_call_token = Some(call_id);
            }
            Err(status) => {
                let error_msg = format!(
                    "SP Extraction: Failed to dispatch HTTP call, status: {:?}",
                    status
                );
                crate::sp_error!("{}", error_msg);
            }
        }
    }

    fn inject_trace_context_headers(&mut self) {

        // Generate trace context
        let current_span_id_hex = self.span_builder.get_current_span_id_hex();
        let trace_id_hex = self.span_builder.get_trace_id_hex();
        let traceparent_value = format!("00-{}-{}-01", trace_id_hex, current_span_id_hex);

        // Build new tracestate
        let session_id = self.span_builder.get_session_id().to_string();
        let new_tracestate = build_new_tracestate(&self.request_headers, &traceparent_value, &session_id);

        // Update headers
        self.remove_http_request_header("tracestate");
        self.add_http_request_header("tracestate", &new_tracestate);

        // Check if traceparent exists
        let has_traceparent = self.get_http_request_headers()
            .iter()
            .any(|(k, _)| k.to_lowercase() == "traceparent");

        if !has_traceparent {
            self.add_http_request_header("traceparent", &traceparent_value);
            self.request_headers.insert("traceparent".to_string(), traceparent_value.clone());
        }

        // Update local cache
        self.request_headers.insert("tracestate".to_string(), new_tracestate.clone());

        // Handle x-sp-num header
        let current_sp_num = self.request_headers
            .get("x-sp-num")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        
        let new_sp_num = current_sp_num + 1;
        let new_sp_num_str = new_sp_num.to_string();
        
        self.add_http_request_header("x-sp-num", &new_sp_num_str);
        self.request_headers.insert("x-sp-num".to_string(), new_sp_num_str.clone());
        log::info!("inject_trace_context_headers: traceparent={}, x-sp-num={}", traceparent_value, new_sp_num_str);
    }

    fn extract_and_propagate_trace_context_impl(&mut self) {
        extract_and_propagate_trace_context(
            &self.request_headers,
            &self.response_headers,
        );

        // Check response headers for traceparent
        if let Some(traceparent) = self.response_headers.get("traceparent") {
            crate::sp_debug!("Found traceparent in response {}", traceparent);
            self.propagate_trace_context_to_response();
        }
    }

    fn propagate_trace_context_to_response(&mut self) {
        // Generate a new span ID for the response
        let span_id = crate::otel::generate_span_id();
        let traceparent = self.span_builder.generate_traceparent(&span_id);
        crate::sp_debug!("Propagating traceparent to response {}", traceparent);
        let _ = self.add_http_response_header("traceparent", &traceparent);
    }
}

// Provide header/property access to TrafficAnalyzer
impl crate::traffic::RequestHeadersAccess for SpHttpContext {
    fn get_context_property(&self, path: Vec<&str>) -> Option<Vec<u8>> {
        self.get_property(path)
    }

    fn get_request_header(&self, name: &str) -> Option<String> {
        // Prefer live headers from host to work before local cache is populated
        self.get_http_request_header(name)
            .or_else(|| self.request_headers.get(name).cloned())
    }
}

impl Context for SpHttpContext {
    fn on_http_call_response(
        &mut self,
        token_id: u32,
        _num_headers: usize,
        body_size: usize,
        _num_trailers: usize,
    ) {
        crate::sp_debug!("HTTP call response received: token={}, body_size={}", token_id, body_size);

        // Get response status
        let status_code = self
            .get_http_call_response_header(":status")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0);

        // Get response body
        let response_body = if body_size > 0 {
            self.get_http_call_response_body(0, body_size)
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        // Check if this is the response to our async save call
        if let Some(pending_save_token) = self.pending_save_call_token {
            if pending_save_token == token_id {
                crate::sp_debug!("Processing async save response (status_code={})", status_code);
                self.pending_save_call_token = None;

                if status_code >= 200 && status_code < 300 {
                    crate::sp_info!("Async save completed (status: {})", status_code);
                } else {
                    crate::sp_error!("Async save failed with status: {}", status_code);
                }
                return;
            }
        }

        // Check if this is the response to our injection lookup call
        if let Some(pending_token) = self.pending_inject_call_token {
            if pending_token == token_id {
                crate::sp_debug!("Processing injection lookup response (status_code={})", status_code);
                self.pending_inject_call_token = None;

                if status_code == 200 && body_size > 0 {
                    // Parse injection response
                    match crate::injection::parse_otel_injection_response(&response_body) {
                        Ok(Some(injected_response)) => {
                            let headers_refs: Vec<(&str, &str)> = injected_response
                                .headers
                                .iter()
                                .map(|(k, v)| (k.as_str(), v.as_str()))
                                .collect();

                            let body = if injected_response.body.is_empty() {
                                None
                            } else {
                                Some(injected_response.body.as_slice())
                            };

                            self.send_http_response(
                                injected_response.status_code,
                                headers_refs,
                                body,
                            );
                            return;
                        }
                        _ => {
                            crate::sp_debug!("No injection data found");
                        }
                    }
                }

                // Resume the paused request
                self.resume_http_request();
            }
        }
    }
}

impl HttpContext for SpHttpContext {
    fn on_http_request_headers(&mut self, _num_headers: usize, end_of_stream: bool) -> Action {
        // Record request start time as early as possible
        if self.request_start_time.is_none() {
            self.request_start_time = Some(crate::otel::get_current_timestamp_nanos());
        }
        
        let traffic_direction = crate::traffic::TrafficAnalyzer::detect_traffic_direction(self, &self.config);
        crate::sp_debug!("{} request headers callback invoked", traffic_direction);
        
        // Get initial request headers
        let mut initial_headers = HashMap::new();
        for (key, value) in self.get_http_request_headers() {
            crate::sp_debug!("on_http_request_headers request header: {}: {}", key, value);
            initial_headers.insert(key, value);
        }

        // Copy to request_headers cache
        self.request_headers = initial_headers.clone();
        
        // Cache the ingressgateway check result to avoid calling get_request_header during response phase
        self.is_from_ingressgateway = crate::traffic::TrafficAnalyzer::is_from_istio_ingressgateway(self);
        
        // Check if from istio-ingressgateway, skip if so
        if self.is_from_ingressgateway {
            crate::sp_debug!("Skipping processing for traffic from istio-ingressgateway");
            return Action::Continue;
        }

        // Detect service name
        let detected_service_name = detect_service_name(&self.request_headers, &self.config.service_name);
        let public_key = self.config.public_key.clone();

        // Update url info
        self.update_url_info();

        // Update span builder
        self.span_builder = self
            .span_builder
            .clone()
            .with_service_name(detected_service_name)
            .with_traffic_direction(traffic_direction)
            .with_public_key(public_key)
            .with_context(&initial_headers);

        // Inject trace context headers
        self.inject_trace_context_headers();

        // If no body, perform injection lookup now
        if end_of_stream {
            match self.dispatch_injection_lookup() {
                Ok(call_id) => {
                    self.pending_inject_call_token = Some(call_id);
                    return Action::Pause;
                }
                Err(e) => {
                    crate::sp_error!("Injection lookup error: {}, continuing", e);
                }
            }
        }

        Action::Continue
    }

    fn on_http_request_body(&mut self, body_size: usize, end_of_stream: bool) -> Action {
        if self.is_from_ingressgateway {
            return Action::Continue;
        }

        // Buffer request body
        if let Some(body) = self.get_http_request_body(0, body_size) {
            self.request_body.extend_from_slice(&body);
        }

        if end_of_stream {
            match self.dispatch_injection_lookup() {
                Ok(call_id) => {
                    self.pending_inject_call_token = Some(call_id);
                    return Action::Pause;
                }
                Err(e) => {
                    crate::sp_error!("Injection lookup error: {}, continuing", e);
                }
            }
        }

        Action::Continue
    }

    fn on_http_response_headers(&mut self, num_headers: usize, end_of_stream: bool) -> Action {
        crate::sp_debug!("proxied response headers - num_headers: {}, end_of_stream: {}", num_headers, end_of_stream);
        
        if self.is_from_ingressgateway || self.injected {
            return Action::Continue;
        }

        // Skip header processing if no headers are expected
        if num_headers == 0 {
            crate::sp_debug!("No response headers to process, skipping header capture");
            return Action::Continue;
        }

        // Capture response headers
        for (key, value) in self.get_http_response_headers() {
            self.response_headers.insert(key, value);
        }

        // Extract and propagate trace context
        self.extract_and_propagate_trace_context_impl();

        // If there's no response body, perform async extraction save now, fire and forget
        if end_of_stream {
            self.dispatch_async_extraction_save();   
        }

        Action::Continue
    }

    fn on_http_response_body(&mut self, body_size: usize, end_of_stream: bool) -> Action {
        crate::sp_debug!("proxied response body - body_size: {}, end_of_stream: {}", body_size, end_of_stream);

        if self.is_from_ingressgateway || self.injected {
            return Action::Continue;
        }

        // Buffer response body
        if let Some(body) = self.get_http_response_body(0, body_size) {
            self.response_body.extend_from_slice(&body);
        }

        if end_of_stream {
            if let Some(status) = self.response_headers.get(":status") {
                crate::sp_debug!("Processing response (status: {})", status);
                self.dispatch_async_extraction_save();
            }   
        }

        Action::Continue
    }
}

impl SpHttpContext {
    /// Check if the current request is for static resources based on URL path and Content-Type
    fn is_static_resource(&self) -> bool {
        is_static_resource(self.url_path.as_deref(), &self.response_headers)
    }

    /// Heuristic: detect Next.js RSC / prefetch requests to skip upload
    fn is_rsc_or_prefetch(&self) -> bool {
        // Check URL path query
        if let Some(ref path) = self.url_path {
            if path.contains("_rsc=") {
                crate::sp_debug!("Detected RSC query in path: {}", path);
                return true;
            }
        }
        // Additional framework-internal heuristics (e.g., Next.js App Router)
        if let Some(ref path) = self.url_path {
            if path.contains("/_next/data/") || path.starts_with("/_next/") {
                crate::sp_debug!("Detected Next.js internal path: {}", path);
                return true;
            }
        }
        // Detect RSC Accept header used by React Server Components
        let accept_lower = self
            .request_headers
            .get("accept")
            .map(|v| v.to_ascii_lowercase())
            .or_else(|| self.request_headers.get("Accept").map(|v| v.to_ascii_lowercase()));
        if let Some(a) = accept_lower {
            if a.contains("text/x-component") {
                crate::sp_debug!("Detected RSC Accept header: {}", a);
                return true;
            }
        }
        // Headers-based detection (may vary by framework/version)
        let has_rsc_header = self.request_headers.get("rsc").is_some() || self.request_headers.get("Rsc").is_some();
        let has_next_prefetch = self.request_headers.get("next-router-prefetch").is_some() || self.request_headers.get("Next-Router-Prefetch").is_some();
        let has_purpose_prefetch = self
            .request_headers
            .get("purpose")
            .map(|v| v.eq_ignore_ascii_case("prefetch"))
            .unwrap_or(false);
        let has_sec_purpose_prefetch = self
            .request_headers
            .get("sec-purpose")
            .map(|v| v.eq_ignore_ascii_case("prefetch"))
            .or_else(|| self.request_headers.get("Sec-Purpose").map(|v| v.eq_ignore_ascii_case("prefetch")))
            .unwrap_or(false);
        let has_next_state_tree = self.request_headers.get("next-router-state-tree").is_some()
            || self.request_headers.get("Next-Router-State-Tree").is_some();

        if has_rsc_header || has_next_prefetch || has_purpose_prefetch || has_sec_purpose_prefetch || has_next_state_tree {
            crate::sp_debug!(
                "Detected prefetch/RSC headers (rsc={}, next-router-prefetch={}, purpose-prefetch={}, sec-purpose-prefetch={}, next-router-state-tree={})",
                has_rsc_header, has_next_prefetch, has_purpose_prefetch, has_sec_purpose_prefetch, has_next_state_tree
            );
            return true;
        }
        false
    }
}

/// Check if the request is for static resources based on URL path and Content-Type
fn is_static_resource(url_path: Option<&str>, response_headers: &HashMap<String, String>) -> bool {
    // Check URL path extension
    if let Some(path) = url_path {
        let static_extensions = [
            ".js", ".css", ".html", ".htm", ".svg", 
            ".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico",
            ".ttf", ".woff", ".woff2", ".map"
        ];
        
        for ext in &static_extensions {
            if path.ends_with(ext) {
                crate::sp_debug!("Static resource detected by URL extension: {}", path);
                return true;
            }
        }
    }
    
    // Check Content-Type header
    if let Some(content_type) = response_headers.get("content-type") {
        let static_content_types = [
            "text/javascript", "application/javascript", "application/x-javascript",
            "text/css", "text/html", "image/svg+xml", 
            "image/png", "image/jpeg", "image/gif", "image/webp", "image/x-icon",
            "font/ttf", "font/woff", "font/woff2", "application/octet-stream"
        ];
        
        for ctype in &static_content_types {
            if content_type.starts_with(ctype) {
                crate::sp_debug!("Static resource detected by Content-Type: {}", content_type);
                return true;
            }
        }
    }
    
    false
}
