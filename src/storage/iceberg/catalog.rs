use crate::config::Config;
use anyhow::Result;
use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Shared Iceberg REST catalog - single instance for all tables
pub struct IcebergCatalog {
    catalog: Arc<RestCatalog>,
}

impl IcebergCatalog {
    /// Initialize the Iceberg REST catalog with configuration
    pub async fn new(config: &Config) -> Result<Self> {
        info!("Initializing Iceberg REST catalog: {}", config.iceberg.catalog_uri);

        // Configure REST catalog properties
        let mut catalog_props = HashMap::new();
        catalog_props.insert(REST_CATALOG_PROP_URI.to_string(), config.iceberg.catalog_uri.clone());

        // Warehouse location
        let warehouse_uri = std::env::var("ICEBERG_WAREHOUSE")
            .unwrap_or_else(|_| config.iceberg.warehouse.clone());
        catalog_props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri.clone());
        info!("Using Iceberg warehouse: {}", warehouse_uri);

        // Authentication token (for Cloudflare R2, etc.)
        if let Some(ref token) = config.iceberg.catalog_token {
            info!("Adding bearer token for catalog authentication (token: {}...)",
                &token.chars().take(10).collect::<String>());
            catalog_props.insert("token".to_string(), token.clone());
        }

        // TLS validation (development only!)
        let http_client = if std::env::var("ICEBERG_DISABLE_TLS_VALIDATION").is_ok() {
            info!("⚠️  TLS certificate validation DISABLED (ICEBERG_DISABLE_TLS_VALIDATION set)");
            info!("⚠️  This should ONLY be used in sandboxed test environments with TLS interception");
            reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .build()?
        } else {
            reqwest::Client::builder().build()?
        };

        // S3 configuration
        if let Some(ref endpoint) = config.s3.endpoint {
            catalog_props.insert("s3.endpoint".to_string(), endpoint.clone());
            info!("Using S3 endpoint: {}", endpoint);
        }
        if let Some(ref access_key) = config.s3.access_key_id {
            catalog_props.insert("s3.access-key-id".to_string(), access_key.clone());
        }
        if let Some(ref secret_key) = config.s3.secret_access_key {
            catalog_props.insert("s3.secret-access-key".to_string(), secret_key.clone());
        }
        catalog_props.insert("s3.region".to_string(), config.storage.s3_region.clone());

        // Build catalog
        let catalog = RestCatalogBuilder::default()
            .with_client(http_client)
            .load("rest-catalog", catalog_props)
            .await?;

        info!("Iceberg REST catalog initialized successfully");

        Ok(Self {
            catalog: Arc::new(catalog),
        })
    }

    /// Get reference to the underlying RestCatalog
    pub fn catalog(&self) -> &Arc<RestCatalog> {
        &self.catalog
    }
}
