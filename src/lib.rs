// Re-export key modules for testing
#[cfg(test)]
mod test_support;

pub mod api;
pub mod authn;
pub mod capture_export;
pub mod catalog;
pub mod compaction;
pub mod config;
pub mod grpc_otlp;
pub mod ingest_engine;
pub mod inject;
pub mod models;
pub mod query;
pub mod runtime_api;
pub mod session_redis;
pub mod storage;
pub mod tenant_ducklake;
