//! Prometheus HTTP API adapter (thin: parse → backend/promql → encode).

mod encode;
mod handlers;
mod params;

pub mod diff_normalize;

pub use handlers::prometheus_routes;
pub use params::{pairs_from_query, parse_time_ms};
