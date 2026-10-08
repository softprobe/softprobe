use proxy_wasm::traits::*;
use proxy_wasm::types::*;

mod otel;
mod config;
mod traffic;
mod headers;
mod injection;
mod context;
mod http_helpers;
mod trace_context;
mod logging;

use crate::config::Config;
use crate::context::SpHttpContext;
// Main entry point for the WASM module
proxy_wasm::main! {{
    // It's required to set the log level explicitly for the WASM module log to work correctly
    proxy_wasm::set_log_level(LogLevel::Debug);
    sp_info!("SP-Istio Agent WASM module loaded.");
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> {
        Box::new(SpRootContext::new())
    });
}}

struct SpRootContext {
    config: Config,
}

impl SpRootContext {
    fn new() -> Self {
        Self {
            config: Config::default(),
        }
    }
}

impl Context for SpRootContext {}

impl RootContext for SpRootContext {
    fn get_type(&self) -> Option<ContextType> {
        Some(ContextType::HttpContext)
    }

    fn create_http_context(&self, context_id: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(SpHttpContext::new(
            context_id,
            self.config.clone(),
        )))
    }

    fn on_configure(&mut self, _plugin_configuration_size: usize) -> bool {
        if let Some(config_bytes) = self.get_plugin_configuration() {
            self.config.parse_from_json(&config_bytes);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sp_root_context_creation() {
        let root_context = SpRootContext::new();
        assert_eq!(root_context.get_type(), Some(ContextType::HttpContext));
    }

    #[test]
    fn test_http_context_creation() {
        let root_context = SpRootContext::new();
        let http_context = root_context.create_http_context(123);
        assert!(http_context.is_some());
    }

    #[test]
    fn test_configuration_handling() {
        let mut root_context = SpRootContext::new();
        // Test with empty configuration
        assert!(root_context.on_configure(0));
    }
}