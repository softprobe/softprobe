use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub span_buffering: SpanBufferConfig,
    pub compaction: CompactionConfig,
    pub duckdb: DuckDBConfig,
    pub s3: S3Config,
    pub iceberg: IcebergConfig,
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
    pub max_buffer_bytes: usize,             // 128MB - hard limit on buffer size
    pub max_buffer_spans: usize,             // 1000 - alternative span count limit
    pub flush_interval_seconds: u64,         // 60 - flush every minute
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    pub enabled: bool,
    pub min_files_to_compact: usize,         // 5
    pub target_file_size_bytes: usize,      // 64MB
    pub compaction_interval_seconds: u64,   // 3600 (1 hour)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuckDBConfig {
    pub max_connections: usize,              // 10
    pub max_memory_per_query: String,       // "2GB"
    pub max_query_duration_seconds: u64,    // 30
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
    pub catalog_type: String,                // "s3", "glue", or "rest"
    pub catalog_uri: String,
    #[serde(default)]
    pub catalog_token: Option<String>,       // Bearer token for REST catalog (e.g., Cloudflare R2)
    #[serde(default = "default_warehouse")]
    pub warehouse: String,                   // Warehouse location (s3://path or warehouse ID)
    pub write_target_file_size_bytes: usize, // 64MB
    pub write_row_group_size_bytes: usize,   // 128MB
    pub write_page_size_bytes: usize,        // 1MB
    pub force_close_after_append: bool,      // testing: close file after each append
}

fn default_warehouse() -> String {
    "s3://warehouse".to_string()
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
                max_buffer_spans: 10000, // 10K spans
                flush_interval_seconds: 60,
            },
            compaction: CompactionConfig {
                enabled: true,
                min_files_to_compact: 5,
                target_file_size_bytes: 64 * 1024 * 1024, // 64MB
                compaction_interval_seconds: 3600,
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
            iceberg: IcebergConfig {
                catalog_type: "s3".to_string(),
                catalog_uri: "s3://softprobe-recordings".to_string(),
                catalog_token: None,
                warehouse: "s3://warehouse".to_string(),
                write_target_file_size_bytes: 64 * 1024 * 1024, // 64MB
                write_row_group_size_bytes: 128 * 1024 * 1024, // 128MB
                write_page_size_bytes: 1024 * 1024, // 1MB
                force_close_after_append: false,
            },
        }
    }
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        // Load from environment variables or config file
        // Priority: environment > config file > defaults
        
        // Try to load from config file first
        let config_file = std::env::var("CONFIG_FILE")
            .unwrap_or_else(|_| "config.yaml".to_string());
        
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
        
        // Add more environment variable overrides as needed
    }
}

