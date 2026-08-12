//! Shared Grafana/Prometheus/Loki/Tempo compatibility layer.
//!
//! Protocol HTTP adapters (Phases 1–3) stay thin: parse the wire request,
//! call typed backends with a [`TenantContext`], and encode protocol responses.
//! Auth, projection, ordering, and error classes live here — not under a
//! single protocol module.

pub mod backends;
pub mod capability;
pub mod envelopes;
pub mod errors;
pub mod ordering;
pub mod projection;
pub mod stubs;
pub mod tenant;

pub use capability::{load_capability_v0, CapabilityManifest};
pub use errors::{CompatError, CompatErrorCode};
pub use tenant::{ProtocolScope, QueryLimits, TenantContext};
