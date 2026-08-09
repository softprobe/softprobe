use crate::config::Config;
use anyhow::Result;
use duckdb::{Connection, ToSql};
use tracing::warn;

use super::util::escape_sql_literal;

/// Configure DuckDB httpfs for the configured object store + `data_path`.
///
/// Credentials come from the environment (never YAML):
/// - `gs://` → `GCS_HMAC_*` / `GCP_HMAC_*`
/// - `s3://` → `AWS_*` (or EC2 instance metadata)
///
/// See <https://duckdb.org/docs/current/guides/network_cloud_storage/gcs_import.html>.
pub fn configure_object_store(conn: &Connection, config: &Config, data_path: &str) -> Result<()> {
    if data_path.starts_with("gs://") {
        let creds = config.resolve_object_store_credentials(data_path);
        let (Some(key_id), Some(secret)) = (creds.access_key_id, creds.secret_access_key) else {
            warn!(
                "DuckLake data_path is {} but GCS_HMAC_ACCESS_KEY_ID/GCS_HMAC_SECRET are unset; gs:// I/O may return HTTP 403",
                data_path
            );
            return Ok(());
        };
        let kid = escape_sql_literal(&key_id);
        let sec = escape_sql_literal(&secret);
        let sql = format!(
            "CREATE OR REPLACE SECRET gcs_hmac (TYPE GCS, KEY_ID '{kid}', SECRET '{sec}');"
        );
        conn.execute_batch(&sql)?;
        return Ok(());
    }

    if let Some(endpoint) = config.object_store.endpoint.as_ref() {
        let trimmed = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        conn.execute("SET s3_endpoint = ?;", [&trimmed as &dyn ToSql])?;
        conn.execute("SET s3_url_style = 'path';", [])?;
        if endpoint.starts_with("http://") {
            conn.execute("SET s3_use_ssl = false;", [])?;
        } else if endpoint.starts_with("https://") {
            conn.execute("SET s3_use_ssl = true;", [])?;
        }
    }

    let creds = config.resolve_object_store_credentials(data_path);
    if let Some(access_key) = creds.access_key_id.as_ref() {
        conn.execute("SET s3_access_key_id = ?;", [access_key as &dyn ToSql])?;
    }
    if let Some(secret) = creds.secret_access_key.as_ref() {
        conn.execute("SET s3_secret_access_key = ?;", [secret as &dyn ToSql])?;
    }
    if let Some(token) = creds.session_token.as_ref() {
        conn.execute("SET s3_session_token = ?;", [token as &dyn ToSql])?;
    }
    conn.execute(
        "SET s3_region = ?;",
        [&config.object_store.region as &dyn ToSql],
    )?;
    Ok(())
}

/// Backward-compatible alias used by older call sites / docs.
pub fn configure_httpfs_gcs_for_data_path(conn: &Connection, data_path: &str) -> Result<()> {
    let config = Config::default();
    configure_object_store(conn, &config, data_path)
}

