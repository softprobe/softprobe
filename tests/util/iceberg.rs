use softprobe_otlp_backend::config::Config;

pub fn load_test_config() -> Config {
    if let Ok(config_file) = std::env::var("CONFIG_FILE") {
        if std::path::Path::new(&config_file).exists() {
            println!("Loading test config from CONFIG_FILE: {}", config_file);
            return Config::load().expect("Failed to load config");
        }
    }
    let test_type = std::env::var("ICEBERG_TEST_TYPE").unwrap_or_else(|_| "local".to_string());

    let config_file = match test_type.as_str() {
        "r2" => "tests/config/test-r2.yaml",
        _ => "tests/config/test.yaml",
    };

    println!("Loading test config from: {}", config_file);
    std::env::set_var("CONFIG_FILE", config_file);
    Config::load().expect("Failed to load test config")
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
