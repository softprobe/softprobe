use softprobe_runtime::config::Config;

pub fn load_test_config() -> Config {
    if let Ok(config_file) = std::env::var("CONFIG_FILE") {
        if std::path::Path::new(&config_file).exists() {
            println!("Loading test config from CONFIG_FILE: {}", config_file);
            let mut config = Config::load().expect("Failed to load config");
            assign_unique_ducklake_paths(&mut config);
            warn_if_config_needs_minio_hostname(&config);
            return config;
        }
    }
    let test_type = std::env::var("E2E_BACKEND").unwrap_or_else(|_| "local".to_string());

    let config_file = match test_type.as_str() {
        "r2" => "tests/config/test-r2.yaml",
        "gcs" => "tests/config/test-gcs.yaml",
        _ => "tests/config/test.yaml",
    };

    println!("Loading test config from: {}", config_file);
    std::env::set_var("CONFIG_FILE", config_file);
    let mut config = Config::load().expect("Failed to load test config");
    assign_unique_ducklake_paths(&mut config);
    warn_if_config_needs_minio_hostname(&config);
    config
}

fn assign_unique_ducklake_paths(config: &mut Config) {
    let backend = std::env::var("E2E_BACKEND").unwrap_or_else(|_| "local".to_string());
    let run_id = uuid::Uuid::new_v4();

    match backend.as_str() {
        "gcs" => {
            let prefix = std::env::var("GCS_E2E_PREFIX").unwrap_or_else(|_| {
                let bucket = std::env::var("GCS_BUCKET")
                    .unwrap_or_else(|_| "softprobe-datalake-ducklake".to_string());
                format!("gs://{bucket}/ducklake/e2e/{run_id}/")
            });
            let prefix = if prefix.ends_with('/') {
                prefix
            } else {
                format!("{prefix}/")
            };
            println!("GCS e2e data_path prefix: {prefix}");
            // Keep sqlite metadata local; only object data goes to GCS.
            let base = std::env::temp_dir().join(format!("splake-gcs-e2e-{run_id}"));
            let _ = std::fs::create_dir_all(&base);
            config.ducklake.catalog_type = "sqlite".to_string();
            config.ducklake.metadata_path =
                base.join("metadata.sqlite").to_string_lossy().to_string();
            config.ducklake.data_path = prefix;
            config.ducklake.metadata_schema = "main".to_string();
        }
        _ => {
            let base = std::env::temp_dir().join(format!("splake-tests-{run_id}"));
            let _ = std::fs::create_dir_all(&base);
            config.ducklake.metadata_path =
                base.join("metadata.sqlite").to_string_lossy().to_string();
            // For local/r2: keep configured remote data_path when it already points at object store;
            // otherwise isolate under temp.
            if !config.ducklake.data_path.contains("://") {
                config.ducklake.data_path = base.join("data").to_string_lossy().to_string();
                let _ = std::fs::create_dir_all(&config.ducklake.data_path);
            } else if backend == "local" {
                // Local MinIO tests still need unique prefixes under the shared warehouse bucket.
                let unique = format!(
                    "{}e2e/{}/",
                    config.ducklake.data_path.trim_end_matches('/'),
                    run_id
                );
                // Prefer inserting under .../ducklake/... when present.
                if config.ducklake.data_path.contains("/ducklake/") {
                    config.ducklake.data_path = config.ducklake.data_path.replacen(
                        "/ducklake/",
                        &format!("/ducklake/e2e/{run_id}/"),
                        1,
                    );
                } else {
                    config.ducklake.data_path = unique;
                }
            }
        }
    }
}

fn endpoint_uses_minio_hostname(endpoint: &str) -> bool {
    // Match docker-compose host `minio`, not substrings like `minion`.
    let rest = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    let host = rest
        .split('/')
        .next()
        .unwrap_or(rest)
        .split(':')
        .next()
        .unwrap_or(rest);
    host.eq_ignore_ascii_case("minio")
}

fn minio_hostname_resolves() -> bool {
    use std::net::ToSocketAddrs;
    "minio:9000".to_socket_addrs().is_ok()
}

/// Only when this config talks to hostname `minio` (e.g. test-docker.yaml).
/// Host-local configs use localhost:9000 — no warning.
fn warn_if_config_needs_minio_hostname(config: &Config) {
    let Some(endpoint) = config.object_store.endpoint.as_deref() else {
        return;
    };
    if !endpoint_uses_minio_hostname(endpoint) || minio_hostname_resolves() {
        return;
    }
    eprintln!(
        "warning: object_store.endpoint is `{endpoint}` but hostname `minio` does not resolve"
    );
    eprintln!("  Add `127.0.0.1 minio` to /etc/hosts, or use tests/config/test.yaml (localhost).");
}
