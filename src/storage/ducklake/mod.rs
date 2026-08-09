// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

mod attach;
mod object_store;
mod otlp;
mod promotion;
mod scores;
mod util;
mod writer;

pub use object_store::{configure_httpfs_gcs_for_data_path, configure_object_store};
pub use writer::DuckLakeWriter;

pub(crate) use attach::{
    ducklake_attach_options, ducklake_attach_target, ducklake_qualified_table_name,
    ducklake_set_option_scope_for_qualified, prepare_local_ducklake_paths,
};
pub(crate) use util::{escape_sql_literal, size_literal};
