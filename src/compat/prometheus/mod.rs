//! Prometheus HTTP API adapter (thin: parse → backend/promql → encode).

mod encode;
mod handlers;
mod params;

pub mod diff_normalize;

pub use handlers::prometheus_routes;
