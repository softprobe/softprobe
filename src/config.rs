use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub ingest_engine: IngestEngineConfig,
    pub compaction: CompactionConfig,
    pub duckdb: DuckDBConfig,
    pub s3: S3Config,
    #[serde(default)]
    pub ducklake: Option<DuckLakeConfig>,
    #[serde(default)]
    pub dropdown_catalog: DropdownCatalogConfig,
}

/// Postgres EAV table ([`crate::catalog::DropdownCatalog`]) for control-plane UI filter dropdowns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropdownCatalogConfig {
    #[serde(default = "default_dropdown_catalog_enabled")]
    pub enabled: bool,
    #[serde(default = "default_dropdown_catalog_active_days")]
    pub active_values_days: u32,
    #[serde(default = "default_dropdown_catalog_maintenance_prune")]
    pub maintenance_prune_enabled: bool,
    /// Max (entity_type, entity_value) pairs per single Postgres `INSERT … VALUES …` (fewer round-trips under high ingest).
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
pub struct ServerConfig {
    pub port: u16,
    pub host: IpAddr,
    pub max_body_size: usize,
    pub worker_threads: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub s3_region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestEngineConfig {
    #[serde(default = "default_ingest_cache_dir")]
    pub cache_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    pub enabled: bool,
    pub target_file_size_bytes: usize,    // 64MB
    pub compaction_interval_seconds: u64, // 3600 (1 hour)
    #[serde(default = "default_metadata_maintenance_enabled")]
    pub metadata_maintenance_enabled: bool,
    #[serde(default = "default_metadata_maintenance_interval_seconds")]
    pub metadata_maintenance_interval_seconds: u64,
    #[serde(default = "default_metadata_max_snapshot_age_seconds")]
    pub metadata_max_snapshot_age_seconds: u64,
    #[serde(default = "default_metadata_remove_orphan_files_enabled")]
    pub metadata_remove_orphan_files_enabled: bool,
    #[serde(default = "default_metadata_remove_orphan_older_than_seconds")]
    pub metadata_remove_orphan_older_than_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuckDBConfig {
    pub max_connections: usize,          // 10
    pub max_memory_per_query: String,    // "2GB"
    pub max_query_duration_seconds: u64, // 30
    pub enable_spill_to_disk: bool,
    pub spill_directory: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Prefer inlining small collector batches into the catalog over tiny Parquet files.
    /// Default 10000: validated on Postgres+GCS stress (near-zero data parquet for batches ≤10k).
    #[serde(default = "default_data_inlining_row_limit")]
    pub data_inlining_row_limit: Option<u64>,
    /// Number of reused ATTACH'd DuckDB writer connections per catalog scope key.
    /// Enables same-tenant concurrent commits (DuckLake/Postgres retries handle conflicts).
    /// Default 4 (clamped 1..=16). Prefer ≤4 under heavy inlining; 8+ caused 503 storms in stress.
    #[serde(default = "default_writer_pool_size")]
    pub writer_pool_size: usize,
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
    Some(10_000)
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

fn default_metadata_maintenance_enabled() -> bool {
    true
}

fn default_ingest_cache_dir() -> Option<String> {
    Some("/var/tmp/softprobe/duckdb".to_string())
}

fn default_metadata_maintenance_interval_seconds() -> u64 {
    3600
}

fn default_metadata_max_snapshot_age_seconds() -> u64 {
    7 * 24 * 3600
}

fn default_metadata_remove_orphan_files_enabled() -> bool {
    true
}

fn default_metadata_remove_orphan_older_than_seconds() -> u64 {
    3600
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                port: 8090,
                host: "0.0.0.0".parse().unwrap(),
                max_body_size: 100 * 1024 * 1024, // 100MB
                worker_threads: None,
            },
            storage: StorageConfig {
                s3_region: "us-east-1".to_string(),
            },
            ingest_engine: IngestEngineConfig {
                cache_dir: default_ingest_cache_dir(),
            },
            compaction: CompactionConfig {
                enabled: true,
                target_file_size_bytes: 64 * 1024 * 1024, // 64MB
                compaction_interval_seconds: 3600,
                metadata_maintenance_enabled: true,
                metadata_maintenance_interval_seconds: 3600,
                metadata_max_snapshot_age_seconds: 7 * 24 * 3600,
                metadata_remove_orphan_files_enabled: true,
                metadata_remove_orphan_older_than_seconds: 3600,
            },
            duckdb: DuckDBConfig {
                max_connections: 10,
                max_memory_per_query: "2GB".to_string(),
                max_query_duration_seconds: 30,
                enable_spill_to_disk: true,
                spill_directory: "/tmp/duckdb_spill".to_string(),
            },
            s3: S3Config {
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
            },
            ducklake: Some(DuckLakeConfig {
                catalog_type: default_ducklake_catalog_type(),
                metadata_path: default_ducklake_metadata_path(),
                data_path: default_ducklake_data_path(),
                catalog_alias: default_ducklake_catalog_alias(),
                metadata_schema: default_ducklake_metadata_schema(),
                data_inlining_row_limit: default_data_inlining_row_limit(),
                writer_pool_size: default_writer_pool_size(),
            }),
            dropdown_catalog: DropdownCatalogConfig::default(),
        }
    }
}

impl Config {
    pub fn ducklake_or_default(&self) -> DuckLakeConfig {
        self.ducklake.clone().unwrap_or(DuckLakeConfig {
            catalog_type: default_ducklake_catalog_type(),
            metadata_path: default_ducklake_metadata_path(),
            data_path: default_ducklake_data_path(),
            catalog_alias: default_ducklake_catalog_alias(),
            metadata_schema: default_ducklake_metadata_schema(),
            data_inlining_row_limit: default_data_inlining_row_limit(),
            writer_pool_size: default_writer_pool_size(),
        })
    }

    /// Reject unsupported DuckLake catalog backends (official multi-client = postgres or sqlite).
    pub fn validate_ducklake_catalog(&self) -> anyhow::Result<()> {
        let catalog_type = self.ducklake_or_default().catalog_type;
        match catalog_type.as_str() {
            "postgres" | "sqlite" => Ok(()),
            "duckdb" => anyhow::bail!(
                "ducklake.catalog_type=duckdb is unsupported (DuckLake single-client only). \
                 Use sqlite for local multi-client concurrency or postgres for production."
            ),
            other => anyhow::bail!(
                "unsupported ducklake.catalog_type={other}; use postgres or sqlite"
            ),
        }
    }

    pub fn load() -> anyhow::Result<Self> {
        // Load from environment variables or config file
        // Priority: environment > config file > defaults

        // Try to load from config file first
        let config_file =
            std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config.yaml".to_string());

        if std::path::Path::new(&config_file).exists() {
            let config_str = std::fs::read_to_string(&config_file)?;
            let mut config: Config = serde_yaml::from_str(&config_str)?;

            // Override with environment variables if present
            config.apply_env_overrides();
            config.validate_ducklake_catalog()?;

            Ok(config)
        } else {
            // Use defaults with environment overrides
            let mut config = Config::default();
            config.apply_env_overrides();
            config.validate_ducklake_catalog()?;
            Ok(config)
        }
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(port) = std::env::var("PORT") {
            if let Ok(p) = port.parse() {
                self.server.port = p;
            }
        }

        if let Ok(region) = std::env::var("S3_REGION") {
            self.storage.s3_region = region;
        }

        if let Ok(raw) = std::env::var("SOFTPROBE_MAX_HTTP_BODY_BYTES") {
            if let Ok(n) = raw.trim().parse::<usize>() {
                if n > 0 {
                    self.server.max_body_size = n;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    use std::sync::Mutex;

    static CONFIG_TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn default_roundtrip_yaml() {
        let c = Config::default();
        let yaml = serde_yaml::to_string(&c).expect("serialize");
        let parsed: Config = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(parsed.server.port, c.server.port);
        assert_eq!(parsed.storage.s3_region, c.storage.s3_region);
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
        c.apply_env_overrides();

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
        assert_eq!(c.storage.s3_region, "eu-west-1");
    }

    #[test]
    fn env_overrides_max_http_body_bytes() {
        let _lock = CONFIG_TEST_MUTEX.lock().expect("lock");
        let prev = std::env::var("SOFTPROBE_MAX_HTTP_BODY_BYTES").ok();
        std::env::set_var("SOFTPROBE_MAX_HTTP_BODY_BYTES", "5242880");
        let mut c = Config::default();
        c.apply_env_overrides();
        match prev {
            Some(p) => std::env::set_var("SOFTPROBE_MAX_HTTP_BODY_BYTES", p),
            None => std::env::remove_var("SOFTPROBE_MAX_HTTP_BODY_BYTES"),
        }
        assert_eq!(c.server.max_body_size, 5 * 1024 * 1024);
    }

    #[test]
    fn reject_duckdb_catalog_type() {
        let mut c = Config::default();
        let mut dl = c.ducklake_or_default();
        dl.catalog_type = "duckdb".to_string();
        c.ducklake = Some(dl);
        let err = c.validate_ducklake_catalog().expect_err("duckdb rejected");
        assert!(err.to_string().contains("unsupported"));
    }
}