use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Softprobe runtime configuration.
///
/// Secrets for object storage are **not** stored in YAML. Resolve them from the
/// environment (`AWS_*` for `s3://`, `GCS_HMAC_*` / `GCP_HMAC_*` for `gs://`).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub object_store: ObjectStoreConfig,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub maintenance: MaintenanceConfig,
    /// Required DuckLake catalog + data warehouse settings.
    pub ducklake: DuckLakeConfig,
    #[serde(default)]
    pub dropdown_catalog: DropdownCatalogConfig,
}

/// Postgres EAV table ([`crate::catalog::DropdownCatalog`]) for control-plane UI filter dropdowns.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DropdownCatalogConfig {
    #[serde(default = "default_dropdown_catalog_enabled")]
    pub enabled: bool,
    #[serde(default = "default_dropdown_catalog_active_days")]
    pub active_values_days: u32,
    #[serde(default = "default_dropdown_catalog_maintenance_prune")]
    pub maintenance_prune_enabled: bool,
    /// Max (entity_type, entity_value) pairs per single Postgres `INSERT … VALUES …`.
    #[serde(default = "default_dropdown_catalog_upsert_batch_size")]
    pub upsert_batch_size: usize,
    #[serde(default)]
    pub skip_entity_columns: Vec<String>,
}

impl Default for DropdownCatalogConfig {
    fn default() -> Self {
        Self {
            enabled: default_dropdown_catalog_enabled(),
            active_values_days: default_dropdown_catalog_active_days(),
            maintenance_prune_enabled: default_dropdown_catalog_maintenance_prune(),
            upsert_batch_size: default_dropdown_catalog_upsert_batch_size(),
            skip_entity_columns: Vec::new(),
        }
    }
}

fn default_dropdown_catalog_enabled() -> bool {
    false
}

fn default_dropdown_catalog_active_days() -> u32 {
    7
}

fn default_dropdown_catalog_maintenance_prune() -> bool {
    true
}

fn default_dropdown_catalog_upsert_batch_size() -> usize {
    500
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_server_port")]
    pub port: u16,
    #[serde(default = "default_server_host")]
    pub host: IpAddr,
    #[serde(default = "default_server_max_body_size")]
    pub max_body_size: usize,
    #[serde(default)]
    pub worker_threads: Option<usize>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: default_server_port(),
            host: default_server_host(),
            max_body_size: default_server_max_body_size(),
            worker_threads: None,
        }
    }
}

fn default_server_port() -> u16 {
    8090
}

fn default_server_host() -> IpAddr {
    "0.0.0.0".parse().expect("valid default host")
}

fn default_server_max_body_size() -> usize {
    100 * 1024 * 1024
}

/// Non-secret object-store connection settings (region / custom endpoint).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectStoreConfig {
    #[serde(default = "default_object_store_region")]
    pub region: String,
    /// Custom S3-compatible endpoint (MinIO, R2). Omit for AWS/GCS native paths.
    #[serde(default)]
    pub endpoint: Option<String>,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            region: default_object_store_region(),
            endpoint: None,
        }
    }
}

fn default_object_store_region() -> String {
    "us-east-1".to_string()
}

/// DuckDB query-engine worker settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryConfig {
    #[serde(default = "default_query_max_connections")]
    pub max_connections: usize,
    /// Directory for DuckDB `cache_httpfs` on-disk cache (query path).
    #[serde(default = "default_query_cache_dir")]
    pub cache_dir: Option<String>,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_connections: default_query_max_connections(),
            cache_dir: default_query_cache_dir(),
        }
    }
}

fn default_query_max_connections() -> usize {
    10
}

fn default_query_cache_dir() -> Option<String> {
    Some("/var/tmp/softprobe/duckdb".to_string())
}

/// Compaction + metadata maintenance scheduling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceConfig {
    /// Run `ducklake_merge_adjacent_files` compaction.
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_target_file_size_bytes")]
    pub target_file_size_bytes: usize,
    #[serde(default = "default_interval_seconds")]
    pub interval_seconds: u64,
    #[serde(default = "default_true")]
    pub metadata_enabled: bool,
    #[serde(default = "default_metadata_interval_seconds")]
    pub metadata_interval_seconds: u64,
    #[serde(default = "default_max_snapshot_age_seconds")]
    pub max_snapshot_age_seconds: u64,
    /// When true (and metadata maintenance runs), call `ducklake_cleanup_old_files`.
    #[serde(default = "default_true")]
    pub remove_orphan_files_enabled: bool,
    #[serde(default = "default_remove_orphan_older_than_seconds")]
    pub remove_orphan_older_than_seconds: u64,
    /// Open-day live Parquet file soft cap before TWCS merges (AC-F4).
    #[serde(default = "default_open_day_file_cap")]
    pub open_day_file_cap: usize,
    /// Max TWCS merge waves per table per maintenance pass (open day).
    #[serde(default = "default_max_waves_per_table")]
    pub max_waves_per_table: usize,
    /// `max_compacted_files` for a single open-day merge CALL when near the cap.
    #[serde(default = "default_max_compacted_files_per_wave")]
    pub max_compacted_files_per_wave: u64,
    /// `max_compacted_files` for closed-day merge CALLs.
    #[serde(default = "default_closed_day_max_compacted_files")]
    pub closed_day_max_compacted_files: u64,
    /// Max closed-day TWCS waves per table per pass.
    #[serde(default = "default_closed_day_max_waves")]
    pub closed_day_max_waves: usize,
    /// Only merge live files smaller than this (`max_file_size` on DuckLake merge).
    #[serde(default = "default_max_merge_file_size_bytes")]
    pub max_merge_file_size_bytes: u64,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            target_file_size_bytes: default_target_file_size_bytes(),
            interval_seconds: default_interval_seconds(),
            metadata_enabled: true,
            metadata_interval_seconds: default_metadata_interval_seconds(),
            max_snapshot_age_seconds: default_max_snapshot_age_seconds(),
            remove_orphan_files_enabled: true,
            remove_orphan_older_than_seconds: default_remove_orphan_older_than_seconds(),
            open_day_file_cap: default_open_day_file_cap(),
            max_waves_per_table: default_max_waves_per_table(),
            max_compacted_files_per_wave: default_max_compacted_files_per_wave(),
            closed_day_max_compacted_files: default_closed_day_max_compacted_files(),
            closed_day_max_waves: default_closed_day_max_waves(),
            max_merge_file_size_bytes: default_max_merge_file_size_bytes(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_target_file_size_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_interval_seconds() -> u64 {
    // Flush-through OTLP creates many small files under demo/Grafana churn;
    // merge every 5m by default so query scans do not wait an hour.
    300
}

fn default_metadata_interval_seconds() -> u64 {
    // Expire unused snapshot history often; Prom does not time-travel.
    60
}

fn default_max_snapshot_age_seconds() -> u64 {
    // Prom does not use DuckLake time-travel; keep a short overlap for in-flight readers.
    60
}

fn default_remove_orphan_older_than_seconds() -> u64 {
    60
}

fn default_open_day_file_cap() -> usize {
    2
}

fn default_max_waves_per_table() -> usize {
    32
}

fn default_max_compacted_files_per_wave() -> u64 {
    32
}

fn default_closed_day_max_compacted_files() -> u64 {
    256
}

fn default_closed_day_max_waves() -> usize {
    64
}

fn default_max_merge_file_size_bytes() -> u64 {
    8 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckLakeConfig {
    /// Catalog backend: `postgres` (production / multi-tenant) or `sqlite` (local multi-client).
    /// `duckdb` is rejected — DuckLake documents it as single-client only.
    #[serde(default = "default_ducklake_catalog_type")]
    pub catalog_type: String,
    #[serde(default = "default_ducklake_metadata_path")]
    pub metadata_path: String,
    #[serde(default = "default_ducklake_data_path")]
    pub data_path: String,
    #[serde(default = "default_ducklake_catalog_alias")]
    pub catalog_alias: String,
    #[serde(default = "default_ducklake_metadata_schema")]
    pub metadata_schema: String,
    /// Prefer Parquet (TWCS can merge). Opt-in `Some(10000)` only for scores/inlined-reader tests.
    #[serde(default = "default_data_inlining_row_limit")]
    pub data_inlining_row_limit: Option<u64>,
    /// Number of reused ATTACH'd DuckDB writer connections per catalog scope key.
    #[serde(default = "default_writer_pool_size")]
    pub writer_pool_size: usize,
}

impl Default for DuckLakeConfig {
    fn default() -> Self {
        Self {
            catalog_type: default_ducklake_catalog_type(),
            metadata_path: default_ducklake_metadata_path(),
            data_path: default_ducklake_data_path(),
            catalog_alias: default_ducklake_catalog_alias(),
            metadata_schema: default_ducklake_metadata_schema(),
            data_inlining_row_limit: default_data_inlining_row_limit(),
            writer_pool_size: default_writer_pool_size(),
        }
    }
}

fn default_ducklake_catalog_type() -> String {
    "sqlite".to_string()
}

fn default_ducklake_metadata_path() -> String {
    "./warehouse/ducklake/metadata.sqlite".to_string()
}

fn default_ducklake_data_path() -> String {
    "./warehouse/ducklake/data/".to_string()
}

fn default_ducklake_catalog_alias() -> String {
    "softprobe".to_string()
}

fn default_ducklake_metadata_schema() -> String {
    "main".to_string()
}

fn default_data_inlining_row_limit() -> Option<u64> {
    // VARIANT shredding (series.labels, traces) only works on Parquet. Skinny
    // samples/postings/hist used to inline into Postgres and skip TWCS merge.
    Some(0)
}

fn default_writer_pool_size() -> usize {
    4
}

impl DuckLakeConfig {
    /// Effective writer pool size, clamped to 1..=16.
    pub fn effective_writer_pool_size(&self) -> usize {
        self.writer_pool_size.clamp(1, 16)
    }
}

/// Resolved object-store credentials (never loaded from YAML).
#[derive(Debug, Clone, Default)]
pub struct ObjectStoreCredentials {
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
}

impl ObjectStoreCredentials {
    pub fn is_complete(&self) -> bool {
        self.access_key_id
            .as_deref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
            && self
                .secret_access_key
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false)
    }
}

impl Config {
    /// Reject unsupported DuckLake catalog backends (official multi-client = postgres or sqlite).
    pub fn validate_ducklake_catalog(&self) -> anyhow::Result<()> {
        match self.ducklake.catalog_type.as_str() {
            "postgres" | "sqlite" => Ok(()),
            "duckdb" => anyhow::bail!(
                "ducklake.catalog_type=duckdb is unsupported (DuckLake single-client only). \
                 Use sqlite for local multi-client concurrency or postgres for production."
            ),
            other => {
                anyhow::bail!("unsupported ducklake.catalog_type={other}; use postgres or sqlite")
            }
        }
    }

    /// Single query worker + single writer connection for in-process / local tests.
    /// Keeps production defaults (`max_connections=10`, `writer_pool_size=4`) elsewhere.
    pub fn shrink_pools_for_tests(&mut self) {
        self.query.max_connections = 1;
        self.ducklake.writer_pool_size = 1;
    }

    pub fn load() -> anyhow::Result<Self> {
        let config_file =
            std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config.yaml".to_string());

        let mut config = if std::path::Path::new(&config_file).exists() {
            let config_str = std::fs::read_to_string(&config_file)?;
            serde_yaml::from_str(&config_str)?
        } else if std::env::var("CONFIG_FILE").is_ok() {
            anyhow::bail!(
                "CONFIG_FILE={config_file} does not exist. Provide a valid path or unset CONFIG_FILE."
            );
        } else {
            Config::default()
        };

        config.apply_env_overrides()?;
        config.validate_ducklake_catalog()?;
        Ok(config)
    }

    fn apply_env_overrides(&mut self) -> anyhow::Result<()> {
        if let Ok(port) = std::env::var("PORT") {
            self.server.port = port
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid PORT={port}: {e}"))?;
        }

        if let Ok(region) = std::env::var("S3_REGION") {
            if region.trim().is_empty() {
                anyhow::bail!("S3_REGION is set but empty");
            }
            self.object_store.region = region;
        }

        if let Ok(raw) = std::env::var("SOFTPROBE_MAX_HTTP_BODY_BYTES") {
            let n: usize = raw
                .trim()
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid SOFTPROBE_MAX_HTTP_BODY_BYTES={raw}: {e}"))?;
            if n == 0 {
                anyhow::bail!("SOFTPROBE_MAX_HTTP_BODY_BYTES must be > 0");
            }
            self.server.max_body_size = n;
        }
        Ok(())
    }

    /// Resolve credentials for `data_path` from the environment (never from YAML).
    ///
    /// - `gs://` → `GCS_HMAC_ACCESS_KEY_ID` / `GCS_HMAC_SECRET` (or `GCP_HMAC_*`)
    /// - `s3://` → `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` [/ `AWS_SESSION_TOKEN`],
    ///   then EC2 instance metadata when unset
    pub fn resolve_object_store_credentials(&self, data_path: &str) -> ObjectStoreCredentials {
        if data_path.starts_with("gs://") {
            return ObjectStoreCredentials {
                access_key_id: std::env::var("GCS_HMAC_ACCESS_KEY_ID")
                    .or_else(|_| std::env::var("GCP_HMAC_ACCESS_KEY_ID"))
                    .ok(),
                secret_access_key: std::env::var("GCS_HMAC_SECRET")
                    .or_else(|_| std::env::var("GCP_HMAC_SECRET"))
                    .ok(),
                session_token: None,
            };
        }
        if data_path.starts_with("s3://") || self.object_store.endpoint.is_some() {
            let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok();
            let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
            let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
            if access_key_id.is_some() && secret_access_key.is_some() {
                return ObjectStoreCredentials {
                    access_key_id,
                    secret_access_key,
                    session_token,
                };
            }
            return fetch_instance_metadata_credentials().unwrap_or_default();
        }
        ObjectStoreCredentials::default()
    }
}

fn fetch_instance_metadata_credentials() -> anyhow::Result<ObjectStoreCredentials> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()?;

    let role_url = "http://169.254.169.254/latest/meta-data/iam/security-credentials/";
    let role_response = match client.get(role_url).send() {
        Ok(resp) => resp,
        Err(_) => return Ok(ObjectStoreCredentials::default()),
    };

    let role_name = role_response.text()?.trim().to_string();
    if role_name.is_empty() {
        return Ok(ObjectStoreCredentials::default());
    }

    let creds_url = format!(
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}",
        role_name
    );
    let creds_response = client.get(&creds_url).send()?;
    let creds_json: serde_json::Value = creds_response.json()?;

    Ok(ObjectStoreCredentials {
        access_key_id: creds_json["AccessKeyId"].as_str().map(|s| s.to_string()),
        secret_access_key: creds_json["SecretAccessKey"]
            .as_str()
            .map(|s| s.to_string()),
        session_token: creds_json["Token"].as_str().map(|s| s.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::Config;
    use crate::compaction::twcs::TwcsPolicy;
    use std::sync::Mutex;

    static CONFIG_TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn default_roundtrip_yaml() {
        let c = Config::default();
        let yaml = serde_yaml::to_string(&c).expect("serialize");
        let parsed: Config = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(parsed.server.port, c.server.port);
        assert_eq!(parsed.object_store.region, c.object_store.region);
        assert_eq!(parsed.ducklake.catalog_type, c.ducklake.catalog_type);
    }

    #[test]
    fn maintenance_defaults_favor_frequent_compaction() {
        let c = Config::default();
        assert_eq!(c.maintenance.interval_seconds, 300);
        assert_eq!(c.maintenance.metadata_interval_seconds, 60);
        assert!(c.maintenance.enabled);
        assert_eq!(c.maintenance.target_file_size_bytes, 64 * 1024 * 1024);
        assert_eq!(c.maintenance.open_day_file_cap, 2);
        assert_eq!(c.maintenance.max_waves_per_table, 32);
        assert_eq!(c.maintenance.max_compacted_files_per_wave, 32);
        assert_eq!(c.maintenance.closed_day_max_compacted_files, 256);
        assert_eq!(c.maintenance.closed_day_max_waves, 64);
        assert_eq!(c.maintenance.max_merge_file_size_bytes, 8 * 1024 * 1024);
        assert_eq!(TwcsPolicy::from(&c.maintenance), TwcsPolicy::default());
    }

    /// AC-N1 / T-N1: default snapshot retention is 60s, not 7d (or 1h).
    #[test]
    fn default_max_snapshot_age_seconds_is_one_minute() {
        let c = Config::default();
        assert_eq!(c.maintenance.max_snapshot_age_seconds, 60);
        assert_ne!(c.maintenance.max_snapshot_age_seconds, 604800);
        assert_ne!(c.maintenance.max_snapshot_age_seconds, 3600);
        assert_eq!(c.ducklake.data_inlining_row_limit, Some(0));
    }

    /// AC-F7 / T-F7: skinny tables write Parquet; inlining is opt-in.
    #[test]
    fn default_data_inlining_row_limit_is_zero() {
        let c = Config::default();
        assert_eq!(c.ducklake.data_inlining_row_limit, Some(0));
    }

    #[test]
    fn minimal_yaml_only_requires_ducklake() {
        let yaml = r#"
ducklake:
  catalog_type: sqlite
  metadata_path: /tmp/meta.sqlite
  data_path: /tmp/data/
"#;
        let c: Config = serde_yaml::from_str(yaml).expect("minimal ok");
        assert_eq!(c.server.port, 8090);
        assert_eq!(c.query.max_connections, 10);
        assert_eq!(c.ducklake.metadata_path, "/tmp/meta.sqlite");
    }

    #[test]
    fn reject_legacy_top_level_keys() {
        let yaml = r#"
storage:
  s3_region: us-east-1
ducklake:
  catalog_type: sqlite
"#;
        let err = serde_yaml::from_str::<Config>(yaml).expect_err("legacy rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("unknown field") || msg.contains("storage"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn reject_unused_duckdb_knobs() {
        let yaml = r#"
query:
  max_connections: 2
  max_memory_per_query: "2GB"
ducklake:
  catalog_type: sqlite
"#;
        let err = serde_yaml::from_str::<Config>(yaml).expect_err("unused knobs rejected");
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn load_reads_config_file_from_env() {
        let _lock = CONFIG_TEST_MUTEX.lock().expect("lock");
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("unit-test-config.yaml");
        let original = Config::default();
        std::fs::write(&path, serde_yaml::to_string(&original).expect("yaml")).expect("write");

        let prev = std::env::var("CONFIG_FILE").ok();
        std::env::set_var("CONFIG_FILE", path.to_str().expect("utf8 path"));
        let loaded = Config::load().expect("load");
        match prev {
            Some(p) => std::env::set_var("CONFIG_FILE", p),
            None => std::env::remove_var("CONFIG_FILE"),
        }

        assert_eq!(loaded.server.port, original.server.port);
        assert_eq!(loaded.ducklake.data_path, original.ducklake.data_path);
    }

    #[test]
    fn load_fails_when_config_file_missing() {
        let _lock = CONFIG_TEST_MUTEX.lock().expect("lock");
        let prev = std::env::var("CONFIG_FILE").ok();
        std::env::set_var("CONFIG_FILE", "/tmp/does-not-exist-softprobe-config.yaml");
        let err = Config::load().expect_err("missing file");
        match prev {
            Some(p) => std::env::set_var("CONFIG_FILE", p),
            None => std::env::remove_var("CONFIG_FILE"),
        }
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn env_overrides_port_and_region() {
        let _lock = CONFIG_TEST_MUTEX.lock().expect("lock");
        let prev_port = std::env::var("PORT").ok();
        let prev_region = std::env::var("S3_REGION").ok();
        let prev_body = std::env::var("SOFTPROBE_MAX_HTTP_BODY_BYTES").ok();

        std::env::set_var("PORT", "9191");
        std::env::set_var("S3_REGION", "eu-west-1");
        std::env::remove_var("SOFTPROBE_MAX_HTTP_BODY_BYTES");

        let mut c = Config::default();
        c.apply_env_overrides().expect("overrides");

        match prev_port {
            Some(p) => std::env::set_var("PORT", p),
            None => std::env::remove_var("PORT"),
        }
        match prev_region {
            Some(p) => std::env::set_var("S3_REGION", p),
            None => std::env::remove_var("S3_REGION"),
        }
        match prev_body {
            Some(p) => std::env::set_var("SOFTPROBE_MAX_HTTP_BODY_BYTES", p),
            None => std::env::remove_var("SOFTPROBE_MAX_HTTP_BODY_BYTES"),
        }

        assert_eq!(c.server.port, 9191);
        assert_eq!(c.object_store.region, "eu-west-1");
    }

    #[test]
    fn env_overrides_max_http_body_bytes() {
        let _lock = CONFIG_TEST_MUTEX.lock().expect("lock");
        let prev = std::env::var("SOFTPROBE_MAX_HTTP_BODY_BYTES").ok();
        std::env::set_var("SOFTPROBE_MAX_HTTP_BODY_BYTES", "5242880");
        let mut c = Config::default();
        c.apply_env_overrides().expect("overrides");
        match prev {
            Some(p) => std::env::set_var("SOFTPROBE_MAX_HTTP_BODY_BYTES", p),
            None => std::env::remove_var("SOFTPROBE_MAX_HTTP_BODY_BYTES"),
        }
        assert_eq!(c.server.max_body_size, 5 * 1024 * 1024);
    }

    #[test]
    fn reject_duckdb_catalog_type() {
        let mut c = Config::default();
        c.ducklake.catalog_type = "duckdb".to_string();
        let err = c.validate_ducklake_catalog().expect_err("duckdb rejected");
        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn reject_config_file_without_ducklake_block() {
        let _lock = CONFIG_TEST_MUTEX.lock().expect("lock");
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("no-ducklake.yaml");
        let yaml = r#"
server:
  port: 8090
object_store:
  region: us-east-1
"#;
        std::fs::write(&path, yaml).expect("write");
        let prev = std::env::var("CONFIG_FILE").ok();
        std::env::set_var("CONFIG_FILE", path.to_str().expect("utf8"));
        let err = Config::load().expect_err("missing ducklake must fail");
        match prev {
            Some(p) => std::env::set_var("CONFIG_FILE", p),
            None => std::env::remove_var("CONFIG_FILE"),
        }
        let msg = err.to_string();
        assert!(
            msg.contains("ducklake") || msg.contains("missing field"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn resolve_gcs_credentials_from_env() {
        let _lock = CONFIG_TEST_MUTEX.lock().expect("lock");
        let prev_id = std::env::var("GCS_HMAC_ACCESS_KEY_ID").ok();
        let prev_secret = std::env::var("GCS_HMAC_SECRET").ok();
        std::env::set_var("GCS_HMAC_ACCESS_KEY_ID", "gcs-key");
        std::env::set_var("GCS_HMAC_SECRET", "gcs-secret");
        let c = Config::default();
        let creds = c.resolve_object_store_credentials("gs://bucket/path/");
        match prev_id {
            Some(p) => std::env::set_var("GCS_HMAC_ACCESS_KEY_ID", p),
            None => std::env::remove_var("GCS_HMAC_ACCESS_KEY_ID"),
        }
        match prev_secret {
            Some(p) => std::env::set_var("GCS_HMAC_SECRET", p),
            None => std::env::remove_var("GCS_HMAC_SECRET"),
        }
        assert_eq!(creds.access_key_id.as_deref(), Some("gcs-key"));
        assert_eq!(creds.secret_access_key.as_deref(), Some("gcs-secret"));
        assert!(creds.is_complete());
    }
}
