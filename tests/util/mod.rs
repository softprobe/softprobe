pub mod config;
pub mod otlp;
pub mod promotion_contract;
pub mod sp_llm_manifests;
pub mod tenant;

// E2E-only helpers. `integration_perf` needs pipeline + storage_config; the rest
// are for `integration-e2e` modules in the main `tests` binary.
#[cfg(feature = "integration-e2e")]
pub mod http;
#[cfg(feature = "integration-e2e")]
pub mod perf;
#[cfg(feature = "integration-e2e")]
pub mod pipeline;
#[cfg(feature = "integration-e2e")]
pub mod poll;
#[cfg(feature = "integration-e2e")]
pub mod storage_config;
