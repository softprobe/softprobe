pub mod authn_contract;
pub mod http_api;
pub mod schema_promotion_unit;

#[cfg(feature = "integration-e2e")]
pub mod iceberg;
#[cfg(feature = "integration-e2e")]
pub mod integration;
#[cfg(feature = "integration-e2e")]
pub mod metrics;
#[cfg(feature = "integration-e2e")]
pub mod schema_promotion;
