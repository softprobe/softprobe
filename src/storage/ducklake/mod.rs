// ============================================================================
// TENANT BINDING CONSTITUTION (HARD RULE)
// Tenant identity is allowed only at auth/configuration/instantiation boundaries.
// Operational APIs MUST NOT accept tenant_id parameters.
// After binding tenant context, use tenant-scoped instances/contexts only.
// ============================================================================

mod attach;
mod metrics_layout_write;
mod object_store;
mod otlp;
mod promotion;
mod scores;
mod util;
mod writer;

pub(crate) use metrics_layout_write::{
    layout_catalog_prefix, write_metrics_layout_txn, DEFAULT_MAX_LABELS_PER_SERIES,
};

pub use object_store::{configure_httpfs_gcs_for_data_path, configure_object_store};
pub use writer::DuckLakeWriter;

pub(crate) use attach::{
    configure_duckdb_resources, ducklake_attach_options, ducklake_attach_target,
    ducklake_qualified_table_name, ducklake_set_option_scope_for_qualified,
    prepare_local_ducklake_paths, COMPACTION_DUCKDB_MEMORY, COMPACTION_DUCKDB_THREADS,
    QUERY_DUCKDB_MEMORY, QUERY_DUCKDB_THREADS,
};
pub(crate) use util::{escape_sql_literal, size_literal};
