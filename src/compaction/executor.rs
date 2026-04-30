use crate::config::Config;
use crate::storage::iceberg::IcebergCatalog;
use anyhow::{anyhow, Result};
use duckdb::{Connection, ToSql};
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
    catalog: Option<Arc<iceberg_catalog_rest::RestCatalog>>,
    http_client: reqwest::Client,
    catalog_api_base_url: Option<String>,
    catalog_token: Option<String>,
    ducklake: Option<crate::config::DuckLakeConfig>,
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
        let http_client = if std::env::var("ICEBERG_DISABLE_TLS_VALIDATION").is_ok() {
            reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .build()?
        } else {
            reqwest::Client::builder().build()?
        };

        if config.ducklake.is_some() {
            return Ok(Self {
                config: config.clone(),
                catalog: None,
                http_client,
                catalog_api_base_url: None,
                catalog_token: None,
                ducklake: Some(config.ducklake_or_default()),
            });
        }

        let catalog = IcebergCatalog::new(config).await?;
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
            catalog: Some(catalog.catalog().clone()),
            http_client,
            catalog_api_base_url: Some(catalog_api_base_url),
            catalog_token,
            ducklake: None,
        })
    }

    pub async fn run_once(&self) -> Result<MaintenanceSummary> {
        if self.ducklake.is_some() {
            return self.run_once_ducklake().await;
        }
        let Some(catalog) = self.catalog.as_ref() else {
            return Err(anyhow!("Iceberg catalog is not initialized"));
        };
        let namespace = iceberg::NamespaceIdent::from_strs(["default"])?;
        let tables = catalog.list_tables(&namespace).await?;
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
        let Some(catalog) = self.catalog.as_ref() else {
            return Err(anyhow!("Iceberg catalog is not initialized"));
        };
        let table = catalog.load_table(table_ident).await?;

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

    async fn run_once_ducklake(&self) -> Result<MaintenanceSummary> {
        let ducklake = self
            .ducklake
            .as_ref()
            .ok_or_else(|| anyhow!("DuckLake config not initialized"))?;
        let conn = self.open_ducklake_connection(ducklake)?;
        self.attach_ducklake(&conn, ducklake)?;

        let tables = vec!["traces", "logs", "metrics"];
        let mut results = Vec::new();
        for table in tables {
            let table_ident = TableIdent::from_strs([ducklake.metadata_schema.as_str(), table])?;
            let compaction = if self.config.compaction.enabled {
                CompactionResult {
                    status: self.ducklake_compact_table(&conn, ducklake, table)?,
                }
            } else {
                CompactionResult {
                    status: CompactionStatus::Skipped,
                }
            };

            let metadata = if self.config.compaction.metadata_maintenance_enabled {
                let expired = self.ducklake_expire_snapshots(&conn, ducklake)?;
                self.ducklake_cleanup_files(&conn, ducklake)?;
                MetadataMaintenanceResult {
                    expired_snapshots: expired,
                    skipped: false,
                }
            } else {
                MetadataMaintenanceResult {
                    expired_snapshots: 0,
                    skipped: true,
                }
            };

            results.push(TableMaintenanceResult {
                table: table_ident,
                metadata,
                compaction,
                rewrite_manifests: ActionResult {
                    status: ActionStatus::Unsupported,
                },
                remove_orphan_files: ActionResult {
                    status: ActionStatus::Unsupported,
                },
            });
        }
        Ok(MaintenanceSummary { tables: results })
    }

    fn open_ducklake_connection(&self, ducklake: &crate::config::DuckLakeConfig) -> Result<Connection> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
        if ducklake.catalog_type == "postgres" {
            conn.execute_batch("INSTALL postgres; LOAD postgres;")?;
        }
        if ducklake.catalog_type == "sqlite" {
            conn.execute_batch("INSTALL sqlite; LOAD sqlite;")?;
        }
        if let Some(endpoint) = self.config.s3.endpoint.as_ref() {
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
        if let Some(access_key) = self.config.s3.access_key_id.as_ref() {
            conn.execute("SET s3_access_key_id = ?;", [access_key as &dyn ToSql])?;
        }
        if let Some(secret_key) = self.config.s3.secret_access_key.as_ref() {
            conn.execute("SET s3_secret_access_key = ?;", [secret_key as &dyn ToSql])?;
        }
        conn.execute(
            "SET s3_region = ?;",
            [&self.config.storage.s3_region as &dyn ToSql],
        )?;
        Ok(conn)
    }

    fn attach_ducklake(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<()> {
        let attach_target = match ducklake.catalog_type.as_str() {
            "postgres" => {
                if ducklake.metadata_path.starts_with("postgres:") {
                    ducklake.metadata_path.clone()
                } else {
                    format!("postgres:{}", ducklake.metadata_path)
                }
            }
            "sqlite" => {
                if ducklake.metadata_path.starts_with("sqlite:") {
                    ducklake.metadata_path.clone()
                } else {
                    format!("sqlite:{}", ducklake.metadata_path)
                }
            }
            _ => ducklake.metadata_path.clone(),
        };
        self.prepare_local_ducklake_paths(ducklake, &attach_target)?;
        let attach_sql = format!(
            "ATTACH 'ducklake:{}' AS {} (DATA_PATH '{}');",
            attach_target.replace('\'', "''"),
            ducklake.catalog_alias,
            ducklake.data_path.replace('\'', "''")
        );
        conn.execute_batch(&attach_sql)?;
        Ok(())
    }

    fn prepare_local_ducklake_paths(
        &self,
        ducklake: &crate::config::DuckLakeConfig,
        attach_target: &str,
    ) -> Result<()> {
        if ducklake.catalog_type == "duckdb" || ducklake.catalog_type == "sqlite" {
            let raw = attach_target
                .strip_prefix("sqlite:")
                .unwrap_or(attach_target)
                .strip_prefix("duckdb:")
                .unwrap_or(attach_target);
            let metadata_path = std::path::PathBuf::from(raw);
            if let Some(parent) = metadata_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if !ducklake.data_path.contains("://") {
                std::fs::create_dir_all(&ducklake.data_path)?;
            }
        }
        Ok(())
    }

    fn ducklake_compact_table(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
        table: &str,
    ) -> Result<CompactionStatus> {
        // DuckLake expects size literals with units for this option.
        let target_file_size = size_literal(self.config.compaction.target_file_size_bytes);
        let set_target = format!(
            "CALL {}.set_option('target_file_size', '{}', table_name => '{}');",
            ducklake.catalog_alias,
            target_file_size,
            table
        );
        conn.execute_batch(&set_target)?;
        let sql = format!(
            "CALL ducklake_merge_adjacent_files('{}', '{}', schema => '{}');",
            ducklake.catalog_alias, table, ducklake.metadata_schema
        );
        match conn.execute_batch(&sql) {
            Ok(_) => Ok(CompactionStatus::Completed),
            Err(err) if is_ducklake_unsupported(&err) => Ok(CompactionStatus::Unsupported),
            Err(err) => Err(anyhow!(
                "DuckLake compaction failed for {}.{}: {}",
                ducklake.metadata_schema,
                table,
                err
            )),
        }
    }

    fn ducklake_expire_snapshots(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<usize> {
        let days = std::cmp::max(
            1,
            self.config.compaction.metadata_max_snapshot_age_seconds / (24 * 3600),
        );
        let dry_run_sql = format!(
            "CALL ducklake_expire_snapshots('{}', dry_run => true, older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{} days');",
            ducklake.catalog_alias, days
        );
        let planned = count_returned_rows(conn, &dry_run_sql)?;
        let sql = format!(
            "CALL ducklake_expire_snapshots('{}', older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{} days');",
            ducklake.catalog_alias, days
        );
        conn.execute_batch(&sql)?;
        Ok(planned)
    }

    fn ducklake_cleanup_files(
        &self,
        conn: &Connection,
        ducklake: &crate::config::DuckLakeConfig,
    ) -> Result<()> {
        let older_than_seconds = self
            .config
            .compaction
            .metadata_remove_orphan_older_than_seconds;
        let sql = if older_than_seconds == 0 {
            format!(
                "CALL ducklake_cleanup_old_files('{}', cleanup_all => true);",
                ducklake.catalog_alias
            )
        } else {
            let days = std::cmp::max(1, older_than_seconds / (24 * 3600));
            format!(
                "CALL ducklake_cleanup_old_files('{}', older_than => CAST(now() AS TIMESTAMP) - INTERVAL '{} days');",
                ducklake.catalog_alias, days
            )
        };
        conn.execute_batch(&sql)?;
        Ok(())
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

        let table_after = self
            .catalog
            .as_ref()
            .ok_or_else(|| anyhow!("Iceberg catalog is not initialized"))?
            .load_table(table_ident)
            .await?;
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
        let api_base = self
            .catalog_api_base_url
            .as_ref()
            .ok_or_else(|| anyhow!("Iceberg API base URL is not initialized"))?;
        let url = format!(
            "{}/namespaces/{}/tables/{}/actions/rewrite-data-files",
            api_base,
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
        let api_base = self
            .catalog_api_base_url
            .as_ref()
            .ok_or_else(|| anyhow!("Iceberg API base URL is not initialized"))?;
        let url = format!(
            "{}/namespaces/{}/tables/{}",
            api_base,
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
        let api_base = self
            .catalog_api_base_url
            .as_ref()
            .ok_or_else(|| anyhow!("Iceberg API base URL is not initialized"))?;
        let url = format!(
            "{}/namespaces/{}/tables/{}/actions/{}",
            api_base,
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

fn is_ducklake_unsupported(err: &duckdb::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("catalog error")
        || msg.contains("function")
            && (msg.contains("does not exist") || msg.contains("not found"))
        || msg.contains("no function matches")
        || msg.contains("not implemented")
}

fn count_returned_rows(conn: &Connection, sql: &str) -> Result<usize> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let mut count = 0usize;
    while let Some(_row) = rows.next()? {
        count += 1;
    }
    Ok(count)
}

fn size_literal(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    const GB: usize = 1024 * MB;
    if bytes >= GB && bytes.is_multiple_of(GB) {
        format!("{}GB", bytes / GB)
    } else if bytes >= MB && bytes.is_multiple_of(MB) {
        format!("{}MB", bytes / MB)
    } else if bytes >= KB && bytes.is_multiple_of(KB) {
        format!("{}KB", bytes / KB)
    } else {
        format!("{}B", bytes)
    }
}
