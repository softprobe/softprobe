use crate::models::{Score, ScoreConfig};
use crate::storage::schema::arrow;
use crate::storage::schema::tables::{ScoreConfigTable, ScoreTable};
use anyhow::{anyhow, Result};
use std::collections::HashMap;

use super::DuckLakeWriter;

/// Projection for score_configs reads via the writer DuckDB pool.
/// `to_json(metadata)` keeps MAP metadata round-trippable as a JSON object string.
const SCORE_CONFIG_SELECT: &str = "SELECT config_id::VARCHAR, strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ'), name::VARCHAR, data_type::VARCHAR, \
 description::VARCHAR, min_value, max_value, categories::VARCHAR, author_id::VARCHAR, \
 CAST(to_json(metadata) AS VARCHAR), strftime(record_date, '%Y-%m-%d')";

fn score_config_from_sql_row(row: &duckdb::Row<'_>) -> Result<Option<ScoreConfig>> {
    let config_id: String = row.get(0)?;
    let timestamp_raw: String = row.get(1)?;
    let name: String = row.get(2)?;
    let data_type_raw: String = row.get(3)?;
    let description: Option<String> = row.get(4)?;
    let min_value: Option<f64> = row.get(5)?;
    let max_value: Option<f64> = row.get(6)?;
    let categories_raw: Option<String> = row.get(7)?;
    let author_id: Option<String> = row.get(8)?;
    let metadata_raw: Option<String> = row.get(9)?;
    let record_date_raw: String = row.get(10)?;
    let data_type = match data_type_raw.as_str() {
        "numeric" => crate::models::ScoreDataType::Numeric,
        "categorical" => crate::models::ScoreDataType::Categorical,
        "boolean" => crate::models::ScoreDataType::Boolean,
        "text" => crate::models::ScoreDataType::Text,
        _ => return Ok(None),
    };
    let timestamp = chrono::DateTime::parse_from_rfc3339(&timestamp_raw)
        .or_else(|_| chrono::DateTime::parse_from_str(&timestamp_raw, "%Y-%m-%dT%H:%M:%S%.fZ"))
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(|_| chrono::Utc::now());
    let record_date = chrono::NaiveDate::parse_from_str(&record_date_raw, "%Y-%m-%d")
        .unwrap_or_else(|_| timestamp.date_naive());
    let categories = categories_raw
        .as_deref()
        .filter(|raw| !raw.is_empty())
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default();
    let metadata = metadata_raw
        .as_deref()
        .filter(|raw| !raw.is_empty() && *raw != "null")
        .and_then(|raw| serde_json::from_str::<HashMap<String, String>>(raw).ok())
        .unwrap_or_default();
    Ok(Some(ScoreConfig {
        config_id,
        timestamp,
        name,
        data_type,
        description,
        min_value,
        max_value,
        categories,
        author_id,
        metadata,
        record_date,
    }))
}

impl DuckLakeWriter {
    pub async fn write_score_batches(&self, batches: Vec<Vec<Score>>) -> Result<()> {
        let scores: Vec<Score> = batches.into_iter().flatten().collect();
        if scores.is_empty() {
            return Ok(());
        }
        for score in &scores {
            score
                .validate()
                .map_err(|message| anyhow!("invalid score: {message}"))?;
        }

        let schema = ScoreTable::schema();
        let record_batch = arrow::scores_to_record_batch(&scores, &schema)?;
        if self.use_tenant_scoped_ducklake() {
            let scope = self
                .tenant_bound_scope()
                .ok_or_else(|| anyhow!("score writes require a tenant-bound DuckLake writer"))?;
            let dk = self.effective_ducklake(&scope);
            return self
                .write_record_batches_internal_with_ducklake(
                    &dk,
                    ScoreTable::table_name(),
                    vec![record_batch],
                )
                .await;
        }

        self.write_record_batches_internal(ScoreTable::table_name(), vec![record_batch])
            .await
    }

    pub async fn score_exists(&self, score_id: &str) -> Result<bool> {
        let dk = if self.use_tenant_scoped_ducklake() {
            let scope = self
                .tenant_bound_scope()
                .ok_or_else(|| anyhow!("score lookup requires a tenant-bound DuckLake writer"))?;
            self.effective_ducklake(&scope)
        } else {
            self.ducklake.clone()
        };
        let candidates = self.table_name_candidates_for(ScoreTable::table_name(), &dk);
        let pool = self.get_or_create_pool(&dk)?;
        let score_id = score_id.to_string();
        tokio::task::spawn_blocking(move || {
            pool.with_conn(|conn| {
                for table in candidates {
                    let sql =
                        format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE score_id = ? LIMIT 1)");
                    match conn.query_row(&sql, [&score_id], |row| row.get::<_, bool>(0)) {
                        Ok(exists) => return Ok(exists),
                        Err(error) if error.to_string().contains("does not exist") => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
                Ok(false)
            })
        })
        .await
        .map_err(|error| anyhow!("DuckLake score lookup task failed: {error}"))?
    }

    pub async fn write_score_config_batches(&self, batches: Vec<Vec<ScoreConfig>>) -> Result<()> {
        let configs: Vec<ScoreConfig> = batches.into_iter().flatten().collect();
        if configs.is_empty() {
            return Ok(());
        }
        for config in &configs {
            config
                .validate()
                .map_err(|message| anyhow!("invalid score config: {message}"))?;
        }

        let schema = ScoreConfigTable::schema();
        let record_batch = arrow::score_configs_to_record_batch(&configs, &schema)?;
        if self.use_tenant_scoped_ducklake() {
            let scope = self.tenant_bound_scope().ok_or_else(|| {
                anyhow!("score config writes require a tenant-bound DuckLake writer")
            })?;
            let dk = self.effective_ducklake(&scope);
            return self
                .write_record_batches_internal_with_ducklake(
                    &dk,
                    ScoreConfigTable::table_name(),
                    vec![record_batch],
                )
                .await;
        }

        self.write_record_batches_internal(ScoreConfigTable::table_name(), vec![record_batch])
            .await
    }

    pub async fn score_config_exists(&self, config_id: &str) -> Result<bool> {
        let dk = if self.use_tenant_scoped_ducklake() {
            let scope = self.tenant_bound_scope().ok_or_else(|| {
                anyhow!("score config lookup requires a tenant-bound DuckLake writer")
            })?;
            self.effective_ducklake(&scope)
        } else {
            self.ducklake.clone()
        };
        let candidates = self.table_name_candidates_for(ScoreConfigTable::table_name(), &dk);
        let pool = self.get_or_create_pool(&dk)?;
        let config_id = config_id.to_string();
        tokio::task::spawn_blocking(move || {
            pool.with_conn(|conn| {
                for table in candidates {
                    let sql =
                        format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE config_id = ? LIMIT 1)");
                    match conn.query_row(&sql, [&config_id], |row| row.get::<_, bool>(0)) {
                        Ok(exists) => return Ok(exists),
                        Err(error) if error.to_string().contains("does not exist") => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
                Ok(false)
            })
        })
        .await
        .map_err(|error| anyhow!("DuckLake score config lookup task failed: {error}"))?
    }

    pub async fn list_score_configs(&self) -> Result<Vec<ScoreConfig>> {
        let dk = if self.use_tenant_scoped_ducklake() {
            let scope = self.tenant_bound_scope().ok_or_else(|| {
                anyhow!("score config list requires a tenant-bound DuckLake writer")
            })?;
            self.effective_ducklake(&scope)
        } else {
            self.ducklake.clone()
        };
        let candidates = self.table_name_candidates_for(ScoreConfigTable::table_name(), &dk);
        let pool = self.get_or_create_pool(&dk)?;
        tokio::task::spawn_blocking(move || {
            pool.with_conn(|conn| {
                for table in candidates {
                    let sql = format!(
                        "{} FROM {table} ORDER BY timestamp DESC, config_id DESC",
                        SCORE_CONFIG_SELECT
                    );
                    let mut stmt = match conn.prepare(&sql) {
                        Ok(stmt) => stmt,
                        Err(error) if error.to_string().contains("does not exist") => continue,
                        Err(error) => return Err(error.into()),
                    };
                    let mut rows = stmt.query([])?;
                    let mut configs = Vec::new();
                    while let Some(row) = rows.next()? {
                        if let Some(config) = score_config_from_sql_row(row)? {
                            configs.push(config);
                        }
                    }
                    return Ok(configs);
                }
                Ok(Vec::new())
            })
        })
        .await
        .map_err(|error| anyhow!("DuckLake score config list task failed: {error}"))?
    }

    pub async fn get_score_config(&self, config_id: &str) -> Result<Option<ScoreConfig>> {
        let dk = if self.use_tenant_scoped_ducklake() {
            let scope = self.tenant_bound_scope().ok_or_else(|| {
                anyhow!("score config lookup requires a tenant-bound DuckLake writer")
            })?;
            self.effective_ducklake(&scope)
        } else {
            self.ducklake.clone()
        };
        let candidates = self.table_name_candidates_for(ScoreConfigTable::table_name(), &dk);
        let pool = self.get_or_create_pool(&dk)?;
        let config_id = config_id.to_string();
        tokio::task::spawn_blocking(move || {
            pool.with_conn(|conn| {
                for table in candidates {
                    let sql = format!(
                        "{} FROM {table} WHERE config_id = ? LIMIT 1",
                        SCORE_CONFIG_SELECT
                    );
                    let mut stmt = match conn.prepare(&sql) {
                        Ok(stmt) => stmt,
                        Err(error) if error.to_string().contains("does not exist") => continue,
                        Err(error) => return Err(error.into()),
                    };
                    let mut rows = stmt.query([&config_id])?;
                    if let Some(row) = rows.next()? {
                        return Ok(score_config_from_sql_row(row)?);
                    }
                }
                Ok(None)
            })
        })
        .await
        .map_err(|error| anyhow!("DuckLake score config get task failed: {error}"))?
    }
}
