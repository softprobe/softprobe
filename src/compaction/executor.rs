use crate::config::Config;
use crate::storage::iceberg::IcebergCatalog;
use anyhow::{anyhow, Result};
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent, TableRequirement, TableUpdate};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct MaintenanceExecutor {
    config: Config,
    catalog: Arc<iceberg_catalog_rest::RestCatalog>,
    http_client: reqwest::Client,
    catalog_api_base_url: String,
    catalog_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MaintenanceSummary {
    pub tables: Vec<TableMaintenanceResult>,
}

#[derive(Debug, Clone)]
pub struct TableMaintenanceResult {
    pub table: TableIdent,
    pub metadata: MetadataMaintenanceResult,
    pub compaction: CompactionResult,
    pub rewrite_manifests: ActionResult,
    pub remove_orphan_files: ActionResult,
}

#[derive(Debug, Clone)]
pub struct MetadataMaintenanceResult {
    pub expired_snapshots: usize,
    pub skipped: bool,
}

#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub status: CompactionStatus,
}

#[derive(Debug, Clone)]
pub struct ActionResult {
    pub status: ActionStatus,
}

#[derive(Debug, Clone)]
pub enum ActionStatus {
    Completed,
    Skipped,
    Unsupported,
}

#[derive(Debug, Clone)]
pub enum CompactionStatus {
    Completed,
    Skipped,
    Unsupported,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct CommitTableRequest {
    identifier: TableIdent,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct RewriteDataFilesRequest {
    options: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Default)]
struct CatalogConfigResponse {
    #[serde(default)]
    overrides: HashMap<String, String>,
    #[serde(default)]
    defaults: HashMap<String, String>,
}

async fn resolve_catalog_api_base_url(
    http_client: &reqwest::Client,
    catalog_uri: &str,
    warehouse: &str,
    catalog_token: Option<&str>,
) -> Result<String> {
    let base_uri = catalog_uri.trim_end_matches('/').to_string();
    let config_url = format!("{}/v1/config", base_uri);

    let mut request = http_client.get(&config_url);
    if !warehouse.is_empty() {
        request = request.query(&[("warehouse", warehouse)]);
    }
    if let Some(token) = catalog_token {
        request = request.bearer_auth(token);
    }

    let diag = std::env::var("MAINT_DIAG").ok().as_deref() == Some("1");
    if diag {
        println!(
            "MAINT_DIAG config_lookup url={} warehouse_param={}",
            config_url,
            if warehouse.is_empty() { "no" } else { "yes" }
        );
    }

    let response = request.send().await;
    let response = match response {
        Ok(resp) if resp.status().is_success() => resp,
        Ok(resp) => {
            warn!(
                "Catalog config lookup failed ({}), falling back to base URI",
                resp.status()
            );
            if diag {
                println!(
                    "MAINT_DIAG config_lookup_failed status={} fallback_base={}/v1",
                    resp.status(),
                    base_uri
                );
            }
            return Ok(format!("{}/v1", base_uri));
        }
        Err(err) => {
            warn!(
                "Catalog config lookup failed ({}), falling back to base URI",
                err
            );
            if diag {
                println!(
                    "MAINT_DIAG config_lookup_error err={} fallback_base={}/v1",
                    err, base_uri
                );
            }
            return Ok(format!("{}/v1", base_uri));
        }
    };

    let status = response.status();
    let raw = response.text().await.unwrap_or_default();
    if diag {
        let preview = if raw.len() > 800 {
            &raw[..800]
        } else {
            raw.as_str()
        };
        println!(
            "MAINT_DIAG config_lookup_ok status={} body_preview={}",
            status, preview
        );
    }
    let config: CatalogConfigResponse = serde_json::from_str(&raw).unwrap_or_default();
    let resolved_uri = config
        .overrides
        .get("uri")
        .cloned()
        .unwrap_or_else(|| base_uri.clone())
        .trim_end_matches('/')
        .to_string();
    // Some catalogs (notably Cloudflare R2) return the resolved prefix under overrides instead
    // of defaults (e.g. overrides: { "prefix": "<uuid>" }).
    if let Some(prefix) = config
        .overrides
        .get("prefix")
        .or_else(|| config.defaults.get("prefix"))
    {
        if !prefix.is_empty() {
            if diag {
                println!(
                    "MAINT_DIAG config_prefix prefix={} resolved_uri={}",
                    prefix, resolved_uri
                );
            }
            return Ok(format!("{}/v1/{}", resolved_uri, prefix));
        }
    }

    if diag {
        println!("MAINT_DIAG config_no_prefix resolved_uri={}", resolved_uri);
    }
    Ok(format!("{}/v1", resolved_uri))
}

impl MaintenanceExecutor {
    pub async fn new(config: &Config) -> Result<Self> {
        let catalog = IcebergCatalog::new(config).await?;

        let http_client = if std::env::var("ICEBERG_DISABLE_TLS_VALIDATION").is_ok() {
            reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .build()?
        } else {
            reqwest::Client::builder().build()?
        };

        let catalog_token = config.iceberg.catalog_token.clone();
        let warehouse =
            std::env::var("ICEBERG_WAREHOUSE").unwrap_or_else(|_| config.iceberg.warehouse.clone());
        let catalog_api_base_url = resolve_catalog_api_base_url(
            &http_client,
            config.iceberg.catalog_uri.as_str(),
            warehouse.as_str(),
            catalog_token.as_deref(),
        )
        .await?;
        if std::env::var("MAINT_DIAG").ok().as_deref() == Some("1") {
            println!(
                "MAINT_DIAG catalog_api_base_url={} catalog_uri={} warehouse={}",
                catalog_api_base_url,
                config.iceberg.catalog_uri.as_str(),
                warehouse
            );
        }

        Ok(Self {
            config: config.clone(),
            catalog: catalog.catalog().clone(),
            http_client,
            catalog_api_base_url,
            catalog_token,
        })
    }

    pub async fn run_once(&self) -> Result<MaintenanceSummary> {
        let namespace = iceberg::NamespaceIdent::from_strs(["default"])?;
        let tables = self.catalog.list_tables(&namespace).await?;
        self.run_once_for_tables(&tables).await
    }

    pub async fn run_once_for_tables(&self, tables: &[TableIdent]) -> Result<MaintenanceSummary> {
        let mut results = Vec::new();
        for table_ident in tables {
            results.push(self.maintain_table(table_ident).await?);
        }

        Ok(MaintenanceSummary { tables: results })
    }

    async fn maintain_table(&self, table_ident: &TableIdent) -> Result<TableMaintenanceResult> {
        let table = self.catalog.load_table(table_ident).await?;

        let metadata = if self.config.compaction.metadata_maintenance_enabled {
            self.expire_snapshots(table_ident, &table).await?
        } else {
            MetadataMaintenanceResult {
                expired_snapshots: 0,
                skipped: true,
            }
        };

        let rewrite_manifests = if self.config.compaction.metadata_maintenance_enabled
            && self.config.compaction.metadata_rewrite_manifests_enabled
        {
            self.rewrite_manifests(table_ident).await?
        } else {
            ActionResult {
                status: ActionStatus::Skipped,
            }
        };

        let remove_orphan_files = if self.config.compaction.metadata_maintenance_enabled
            && self.config.compaction.metadata_remove_orphan_files_enabled
        {
            self.remove_orphan_files(table_ident).await?
        } else {
            ActionResult {
                status: ActionStatus::Skipped,
            }
        };

        let compaction = if self.config.compaction.enabled {
            self.compact_data_files(table_ident).await?
        } else {
            CompactionResult {
                status: CompactionStatus::Skipped,
            }
        };

        Ok(TableMaintenanceResult {
            table: table_ident.clone(),
            metadata,
            compaction,
            rewrite_manifests,
            remove_orphan_files,
        })
    }

    async fn expire_snapshots(
        &self,
        table_ident: &TableIdent,
        table: &Table,
    ) -> Result<MetadataMaintenanceResult> {
        let snapshots: Vec<_> = table.metadata().snapshots().collect();
        let metadata_log_before: HashSet<String> = table
            .metadata()
            .metadata_log()
            .iter()
            .map(|entry| entry.metadata_file.clone())
            .collect();
        if snapshots.is_empty() {
            return Ok(MetadataMaintenanceResult {
                expired_snapshots: 0,
                skipped: true,
            });
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow!("time error: {}", e))?
            .as_millis() as i64;
        let max_age_ms = (self.config.compaction.metadata_max_snapshot_age_seconds as i64) * 1000;
        let cutoff_ms = now_ms.saturating_sub(max_age_ms);

        let mut snapshots_sorted = snapshots.clone();
        snapshots_sorted.sort_by_key(|snapshot| snapshot.timestamp_ms());

        let mut keep_ids: HashSet<i64> = HashSet::new();
        let min_keep = self.config.compaction.metadata_min_snapshots_to_keep;
        for snapshot in snapshots_sorted.iter().rev().take(min_keep) {
            keep_ids.insert(snapshot.snapshot_id());
        }
        if max_age_ms > 0 {
            for snapshot in &snapshots_sorted {
                if snapshot.timestamp_ms() >= cutoff_ms {
                    keep_ids.insert(snapshot.snapshot_id());
                }
            }
        }
        if let Some(current) = table.metadata().current_snapshot() {
            keep_ids.insert(current.snapshot_id());
        }

        let expire_ids: Vec<i64> = snapshots_sorted
            .iter()
            .filter_map(|snapshot| {
                let id = snapshot.snapshot_id();
                if keep_ids.contains(&id) {
                    None
                } else {
                    Some(id)
                }
            })
            .collect();

        if expire_ids.is_empty() {
            return Ok(MetadataMaintenanceResult {
                expired_snapshots: 0,
                skipped: true,
            });
        }

        info!(
            "Expiring {} snapshots for {}",
            expire_ids.len(),
            table_ident
        );

        let current_snapshot_id = table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());

        let mut updates = Vec::new();
        for snapshot_id in &expire_ids {
            updates.push(TableUpdate::RemoveSnapshots {
                snapshot_ids: vec![*snapshot_id],
            });
        }
        updates.push(TableUpdate::SetProperties {
            updates: HashMap::from([
                (
                    "history.expire.max-snapshot-age-ms".to_string(),
                    max_age_ms.to_string(),
                ),
                (
                    "history.expire.min-snapshots-to-keep".to_string(),
                    self.config
                        .compaction
                        .metadata_min_snapshots_to_keep
                        .to_string(),
                ),
                (
                    "write.metadata.previous-versions-max".to_string(),
                    self.config
                        .compaction
                        .metadata_min_snapshots_to_keep
                        .to_string(),
                ),
            ]),
        });

        let requirements = vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".to_string(),
            snapshot_id: current_snapshot_id,
        }];

        let request = CommitTableRequest {
            identifier: table_ident.clone(),
            requirements,
            updates,
        };

        self.post_table_commit(table_ident, &request).await?;

        let table_after = self.catalog.load_table(table_ident).await?;
        let metadata_log_after: HashSet<String> = table_after
            .metadata()
            .metadata_log()
            .iter()
            .map(|entry| entry.metadata_file.clone())
            .collect();

        for metadata_file in metadata_log_before.difference(&metadata_log_after) {
            if let Err(err) = table_after.file_io().delete(metadata_file).await {
                warn!("Failed to delete metadata file {}: {}", metadata_file, err);
            }
        }

        Ok(MetadataMaintenanceResult {
            expired_snapshots: expire_ids.len(),
            skipped: false,
        })
    }

    async fn compact_data_files(&self, table_ident: &TableIdent) -> Result<CompactionResult> {
        let mut options = HashMap::new();
        options.insert(
            "min-input-files".to_string(),
            serde_json::Value::Number(serde_json::Number::from(
                self.config.compaction.min_files_to_compact as u64,
            )),
        );
        options.insert(
            "target-file-size-bytes".to_string(),
            serde_json::Value::Number(serde_json::Number::from(
                self.config.compaction.target_file_size_bytes as u64,
            )),
        );

        let request = RewriteDataFilesRequest { options };
        let url = format!(
            "{}/namespaces/{}/tables/{}/actions/rewrite-data-files",
            self.catalog_api_base_url,
            table_ident.namespace().to_url_string(),
            table_ident.name(),
        );

        let mut req = self.http_client.post(url).json(&request);
        if let Some(token) = &self.catalog_token {
            req = req.bearer_auth(token);
        }

        let response = req.send().await?;
        if response.status().is_success() {
            info!("Compaction request accepted for {}", table_ident);
            return Ok(CompactionResult {
                status: CompactionStatus::Completed,
            });
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status.as_u16() == 404
            || status.as_u16() == 501
            || (status.as_u16() == 400 && body.contains("No route for request"))
        {
            warn!(
                "Compaction action not supported by catalog for {}",
                table_ident
            );
            return Ok(CompactionResult {
                status: CompactionStatus::Unsupported,
            });
        }

        Err(anyhow!(
            "Compaction request failed for {}: {} - {}",
            table_ident,
            status,
            body
        ))
    }

    async fn post_table_commit(
        &self,
        table_ident: &TableIdent,
        request: &CommitTableRequest,
    ) -> Result<()> {
        let url = format!(
            "{}/namespaces/{}/tables/{}",
            self.catalog_api_base_url,
            table_ident.namespace().to_url_string(),
            table_ident.name()
        );
        if std::env::var("MAINT_DIAG").ok().as_deref() == Some("1") {
            println!("MAINT_DIAG post_table_commit url={}", url);
        }

        let mut req = self.http_client.post(url).json(request);
        if let Some(token) = &self.catalog_token {
            req = req.bearer_auth(token);
        }

        let response = req.send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            if std::env::var("MAINT_DIAG").ok().as_deref() == Some("1") {
                let preview = if body.len() > 800 {
                    &body[..800]
                } else {
                    body.as_str()
                };
                println!(
                    "MAINT_DIAG post_table_commit_failed status={} body_preview={}",
                    status, preview
                );
            }
            return Err(anyhow!(
                "Table commit failed for {}: {} - {}",
                table_ident,
                status,
                body
            ));
        }

        Ok(())
    }

    async fn rewrite_manifests(&self, table_ident: &TableIdent) -> Result<ActionResult> {
        let request = serde_json::json!({ "options": {} });
        self.post_table_action(table_ident, "rewrite-manifests", &request)
            .await
    }

    async fn remove_orphan_files(&self, table_ident: &TableIdent) -> Result<ActionResult> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow!("time error: {}", e))?
            .as_millis() as i64;
        let older_than_ms = now_ms.saturating_sub(
            (self
                .config
                .compaction
                .metadata_remove_orphan_older_than_seconds as i64)
                * 1000,
        );
        let request = serde_json::json!({
            "options": { "older-than": older_than_ms }
        });

        self.post_table_action(table_ident, "remove-orphan-files", &request)
            .await
    }

    async fn post_table_action(
        &self,
        table_ident: &TableIdent,
        action: &str,
        payload: &serde_json::Value,
    ) -> Result<ActionResult> {
        let url = format!(
            "{}/namespaces/{}/tables/{}/actions/{}",
            self.catalog_api_base_url,
            table_ident.namespace().to_url_string(),
            table_ident.name(),
            action
        );

        let mut req = self.http_client.post(url).json(payload);
        if let Some(token) = &self.catalog_token {
            req = req.bearer_auth(token);
        }

        let response = req.send().await?;
        if response.status().is_success() {
            return Ok(ActionResult {
                status: ActionStatus::Completed,
            });
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status.as_u16() == 404
            || status.as_u16() == 501
            || (status.as_u16() == 400 && body.contains("No route for request"))
        {
            warn!(
                "Action {} not supported by catalog for {}",
                action, table_ident
            );
            return Ok(ActionResult {
                status: ActionStatus::Unsupported,
            });
        }

        Err(anyhow!(
            "Action {} failed for {}: {} - {}",
            action,
            table_ident,
            status,
            body
        ))
    }
}
