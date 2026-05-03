use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub span_buffering: SpanBufferConfig,
    pub ingest_engine: IngestEngineConfig,
    pub compaction: CompactionConfig,
    pub duckdb: DuckDBConfig,
    pub s3: S3Config,
    /// Omitted in YAML uses `IcebergConfig::default` (DuckLake deployments do not need a real Iceberg catalog).
    #[serde(default)]
    pub iceberg: IcebergConfig,
    #[serde(default)]
    pub ducklake: Option<DuckLakeConfig>,
    #[serde(default)]
    pub schema_promotion: Option<SchemaPromotionConfig>,
    #[serde(default)]
    pub dropdown_catalog: DropdownCatalogConfig,
}

/// Postgres EAV table ([`crate::catalog::DropdownCatalog`]) for hosted UI filter dropdowns.
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
pub struct SpanBufferConfig {
    pub max_buffer_bytes: usize,     // 128MB - hard limit on buffer size
    pub max_buffer_spans: usize,     // 1000 - alternative span count limit
    pub flush_interval_seconds: u64, // 60 - flush every minute
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestEngineConfig {
    #[serde(default = "default_wal_bucket")]
    pub wal_bucket: String,
    #[serde(default = "default_wal_prefix")]
    pub wal_prefix: String,
    #[serde(default = "default_ingest_cache_dir")]
    pub cache_dir: Option<String>,
    #[serde(default = "default_wal_dir")]
    pub wal_dir: Option<String>,
    #[serde(default = "default_wal_manifest_update_interval_seconds")]
    pub wal_manifest_update_interval_seconds: u64,
    #[serde(default = "default_wal_manifest_max_pending_files")]
    pub wal_manifest_max_pending_files: usize,
    #[serde(default = "default_optimizer_interval_seconds")]
    pub optimizer_interval_seconds: u64,
    #[serde(default)]
    pub replay_wal_on_startup: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    pub enabled: bool,
    pub min_files_to_compact: usize,      // 5
    pub target_file_size_bytes: usize,    // 64MB
    pub compaction_interval_seconds: u64, // 3600 (1 hour)
    #[serde(default = "default_metadata_maintenance_enabled")]
    pub metadata_maintenance_enabled: bool,
    #[serde(default = "default_metadata_maintenance_interval_seconds")]
    pub metadata_maintenance_interval_seconds: u64,
    #[serde(default = "default_metadata_min_snapshots_to_keep")]
    pub metadata_min_snapshots_to_keep: usize,
    #[serde(default = "default_metadata_max_snapshot_age_seconds")]
    pub metadata_max_snapshot_age_seconds: u64,
    #[serde(default = "default_metadata_rewrite_manifests_enabled")]
    pub metadata_rewrite_manifests_enabled: bool,
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
pub struct IcebergConfig {
    pub catalog_type: String, // "s3", "glue", or "rest"
    pub catalog_uri: String,
    #[serde(default)]
    pub catalog_token: Option<String>, // Bearer token for REST catalog (e.g., Cloudflare R2)
    #[serde(default = "default_iceberg_namespace")]
    pub namespace: String,
    #[serde(default = "default_warehouse")]
    pub warehouse: String, // Warehouse location (s3://path or warehouse ID)
    pub write_target_file_size_bytes: usize, // 64MB
    pub write_row_group_size_bytes: usize,   // 128MB
    pub write_page_size_bytes: usize,        // 1MB
    pub force_close_after_append: bool,      // testing: close file after each append
}

impl Default for IcebergConfig {
    fn default() -> Self {
        Self {
            catalog_type: "s3".to_string(),
            catalog_uri: "s3://softprobe-recordings".to_string(),
            catalog_token: None,
            namespace: default_iceberg_namespace(),
            warehouse: "s3://warehouse".to_string(),
            write_target_file_size_bytes: 64 * 1024 * 1024,
            write_row_group_size_bytes: 128 * 1024 * 1024,
            write_page_size_bytes: 1024 * 1024,
            force_close_after_append: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuckLakeConfig {
    #[serde(default = "default_ducklake_catalog_type")]
    pub catalog_type: String, // duckdb, postgres, sqlite
    #[serde(default = "default_ducklake_metadata_path")]
    pub metadata_path: String,
    #[serde(default = "default_ducklake_data_path")]
    pub data_path: String,
    #[serde(default = "default_ducklake_catalog_alias")]
    pub catalog_alias: String,
    #[serde(default = "default_ducklake_metadata_schema")]
    pub metadata_schema: String,
    #[serde(default)]
    pub data_inlining_row_limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaPromotionConfig {
    #[serde(default)]
    pub traces: Option<TablePromotionConfig>,
    #[serde(default)]
    pub logs: Option<TablePromotionConfig>,
    #[serde(default)]
    pub metrics: Option<TablePromotionConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablePromotionConfig {
    /// Attributes to promote from the main attributes MAP
    #[serde(default)]
    pub attributes: Vec<PromotedColumn>,
    /// Attributes to promote from resource_attributes MAP
    #[serde(default)]
    pub resource_attributes: Vec<PromotedColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotedColumn {
    /// OTel attribute key (e.g., "user.id", "sp.user.id", "department")
    pub attribute_key: String,
    /// Column name in Iceberg table (defaults to attribute_key if not specified)
    #[serde(default)]
    pub column_name: Option<String>,
    /// Data type (auto-detected from first value if not specified)
    #[serde(default)]
    pub data_type: Option<PromotedDataType>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PromotedDataType {
    String,
    Int,
    Double,
    Boolean,
}

fn default_warehouse() -> String {
    "s3://warehouse".to_string()
}

fn default_iceberg_namespace() -> String {
    "default".to_string()
}

fn default_ducklake_catalog_type() -> String {
    "duckdb".to_string()
}

fn default_ducklake_metadata_path() -> String {
    "./warehouse/ducklake/metadata.ducklake".to_string()
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

fn default_metadata_maintenance_enabled() -> bool {
    true
}

fn default_wal_bucket() -> String {
    "warehouse".to_string()
}

fn default_wal_prefix() -> String {
    "wal".to_string()
}

fn default_ingest_cache_dir() -> Option<String> {
    Some("/var/tmp/softprobe/duckdb".to_string())
}

fn default_wal_dir() -> Option<String> {
    default_ingest_cache_dir()
}

fn default_wal_manifest_update_interval_seconds() -> u64 {
    10
}

fn default_wal_manifest_max_pending_files() -> usize {
    500
}

fn default_optimizer_interval_seconds() -> u64 {
    300
}

fn default_metadata_maintenance_interval_seconds() -> u64 {
    3600
}

fn default_metadata_min_snapshots_to_keep() -> usize {
    5
}

fn default_metadata_max_snapshot_age_seconds() -> u64 {
    7 * 24 * 3600
}

fn default_metadata_rewrite_manifests_enabled() -> bool {
    true
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
            span_buffering: SpanBufferConfig {
                max_buffer_bytes: 128 * 1024 * 1024, // 128MB
                max_buffer_spans: 10000,             // 10K spans
                flush_interval_seconds: 60,
            },
            ingest_engine: IngestEngineConfig {
                wal_bucket: "warehouse".to_string(),
                wal_prefix: "wal".to_string(),
                cache_dir: default_ingest_cache_dir(),
                wal_dir: default_wal_dir(),
                wal_manifest_update_interval_seconds: default_wal_manifest_update_interval_seconds(
                ),
                wal_manifest_max_pending_files: default_wal_manifest_max_pending_files(),
                optimizer_interval_seconds: 300,
                replay_wal_on_startup: false,
            },
            compaction: CompactionConfig {
                enabled: true,
                min_files_to_compact: 5,
                target_file_size_bytes: 64 * 1024 * 1024, // 64MB
                compaction_interval_seconds: 3600,
                metadata_maintenance_enabled: true,
                metadata_maintenance_interval_seconds: 3600,
                metadata_min_snapshots_to_keep: 5,
                metadata_max_snapshot_age_seconds: 7 * 24 * 3600,
                metadata_rewrite_manifests_enabled: true,
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
            iceberg: IcebergConfig::default(),
            ducklake: Some(DuckLakeConfig {
                catalog_type: default_ducklake_catalog_type(),
                metadata_path: default_ducklake_metadata_path(),
                data_path: default_ducklake_data_path(),
                catalog_alias: default_ducklake_catalog_alias(),
                metadata_schema: default_ducklake_metadata_schema(),
                data_inlining_row_limit: Some(0),
            }),
            schema_promotion: None,
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
            data_inlining_row_limit: Some(0),
        })
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

            Ok(config)
        } else {
            // Use defaults with environment overrides
            let mut config = Config::default();
            config.apply_env_overrides();
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

        if let Ok(namespace) = std::env::var("ICEBERG_NAMESPACE") {
            if !namespace.trim().is_empty() {
                self.iceberg.namespace = namespace;
            }
        }

        // Add more environment variable overrides as needed
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
        std::fs::write(&path, serde_yaml::to_string(&original).expect("yaml"))
            .expect("write");

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
        let prev_ns = std::env::var("ICEBERG_NAMESPACE").ok();

        std::env::set_var("PORT", "9191");
        std::env::set_var("S3_REGION", "eu-west-1");
        std::env::set_var("ICEBERG_NAMESPACE", "acctests");

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
        match prev_ns {
            Some(p) => std::env::set_var("ICEBERG_NAMESPACE", p),
            None => std::env::remove_var("ICEBERG_NAMESPACE"),
        }

        assert_eq!(c.server.port, 9191);
        assert_eq!(c.storage.s3_region, "eu-west-1");
        assert_eq!(c.iceberg.namespace, "acctests");
    }
}
