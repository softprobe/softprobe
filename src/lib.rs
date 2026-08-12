// Re-export key modules for testing
#[cfg(test)]
mod test_support;

pub mod api;
pub mod authn;
pub mod catalog;
pub mod compaction;
pub mod compat;
pub mod config;
pub mod control_plane;
pub mod grpc_otlp;
pub mod ingest_engine;
pub mod metrics_fidelity;
pub mod models;
pub mod promotion;
pub mod query;
pub mod runtime_api;
pub mod runtime_engine;
pub mod storage;
