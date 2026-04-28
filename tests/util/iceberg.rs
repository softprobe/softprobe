use splake::config::Config;

pub fn load_test_config() -> Config {
    if let Ok(config_file) = std::env::var("CONFIG_FILE") {
        if std::path::Path::new(&config_file).exists() {
            println!("Loading test config from CONFIG_FILE: {}", config_file);
            let mut config = Config::load().expect("Failed to load config");
            assign_unique_ducklake_paths(&mut config);
            return config;
        }
    }
    let test_type = std::env::var("ICEBERG_TEST_TYPE").unwrap_or_else(|_| "local".to_string());

    let config_file = match test_type.as_str() {
        "r2" => "tests/config/test-r2.yaml",
        _ => "tests/config/test.yaml",
    };

    println!("Loading test config from: {}", config_file);
    std::env::set_var("CONFIG_FILE", config_file);
    let mut config = Config::load().expect("Failed to load test config");
    assign_unique_ducklake_paths(&mut config);
    config
}

fn assign_unique_ducklake_paths(config: &mut Config) {
    if config.ducklake.is_none() {
        config.ducklake = Some(config.ducklake_or_default());
    }
    if let Some(ducklake) = config.ducklake.as_mut() {
        let base = std::env::temp_dir().join(format!("splake-tests-{}", uuid::Uuid::new_v4()));
        let _ = std::fs::create_dir_all(&base);
        ducklake.metadata_path = base
            .join("metadata.ducklake")
            .to_string_lossy()
            .to_string();
        ducklake.data_path = base.join("data").to_string_lossy().to_string();
        let _ = std::fs::create_dir_all(&ducklake.data_path);
    }
}

pub fn ensure_wal_bucket(config: &mut Config) {
    if config.ingest_engine.wal_bucket != "your-bucket-name" {
        return;
    }

    let warehouse = config.iceberg.warehouse.trim();
    let mut candidate = warehouse
        .rsplit('/')
        .next()
        .unwrap_or(warehouse)
        .to_string();
    if let Some(after_underscore) = candidate.rsplit('_').next() {
        if !after_underscore.is_empty() {
            candidate = after_underscore.to_string();
        }
    }

    assert!(
        !candidate.is_empty() && candidate != "your-bucket-name",
        "wal_bucket is a placeholder and could not be derived from iceberg.warehouse: {}",
        warehouse
    );
    config.ingest_engine.wal_bucket = candidate;
}

/// Check if minio hostname resolves (needed for local testing when REST catalog returns minio:9000 URLs)
/// 
/// This is informational only - if minio doesn't resolve and tests fail with connection errors,
/// users should:
/// - Add `127.0.0.1 minio` to `/etc/hosts` (requires sudo), OR
/// - Run tests in Docker where `minio` hostname resolves, OR
/// - Manually drop and recreate tables if they have stale metadata with wrong endpoints
fn check_minio_hostname() -> bool {
    use std::net::ToSocketAddrs;
    "minio:9000".to_socket_addrs().is_ok()
}

/// Warn if minio hostname doesn't resolve (informational only, no automatic actions)
pub fn warn_if_minio_unresolvable() {
    if !check_minio_hostname() {
        eprintln!("⚠️  INFO: 'minio' hostname does not resolve.");
        eprintln!("   If tests fail with 'minio:9000' connection errors, you can:");
        eprintln!("   1. Add '127.0.0.1 minio' to /etc/hosts (requires sudo)");
        eprintln!("   2. Run tests in Docker where 'minio' hostname resolves");
        eprintln!("   3. Manually drop and recreate tables if they have stale metadata with wrong endpoints");
    }
}
