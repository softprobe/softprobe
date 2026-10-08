//! Tempo query-only compatibility adapters.

pub mod encode;
pub mod handlers;
pub mod params;
pub mod traceql;

pub use handlers::tempo_routes;
