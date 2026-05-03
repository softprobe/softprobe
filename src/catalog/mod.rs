//! Postgres-backed UI dropdown catalog (EAV) stored alongside DuckLake metadata.

mod dropdown;

pub use dropdown::{resolve_trace_tenant_id, DropdownCatalog};
