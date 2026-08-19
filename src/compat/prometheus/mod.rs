//! Prometheus HTTP API adapter (thin: parse → backend/promql → encode).

mod encode;
mod handlers;
mod params;

pub mod diff_normalize;
pub mod gold_overview;

pub use gold_overview::GOLD_OVERVIEW_EXPRS;
pub use handlers::prometheus_routes;
