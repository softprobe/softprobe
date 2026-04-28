use crate::config::{Config, DuckLakeConfig};
use crate::models::{Log, Metric, Span};
use crate::storage::iceberg::arrow;
use crate::storage::iceberg::tables::{OtlpLogsTable, OtlpMetricsTable, TraceTable};
use ::arrow::record_batch::RecordBatch;
use anyhow::{anyhow, Result};
use duckdb::{Connection, ToSql};
use iceberg::spec::Schema as IcebergSchema;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

pub struct DuckLakeWriter {
    config: Config,
    ducklake: DuckLakeConfig,
    cache_dir: Option<PathBuf>,
}

impl DuckLakeWriter {
    pub async fn new(config: &Config) -> Result<Self> {
        let ducklake = config.ducklake_or_default();
        let writer = Self {
            config: config.clone(),
            ducklake,
            cache_dir: config.ingest_engine.cache_dir.as_ref().map(PathBuf::from),
        };
        writer.initialize_catalog().await?;
        info!("DuckLake writer initialized");
        Ok(writer)
    }

    async fn initialize_catalog(&self) -> Result<()> {
        let conn = self.open_connection()?;
        self.attach_ducklake(&conn)?;
        self.ensure_schema(&conn)?;
        Ok(())
    }

    pub async fn write_span_batches(&self, batches: Vec<Vec<Span>>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let schema = self.spans_schema().await?;
        let mut record_batches = Vec::new();
        for batch in batches {
            if !batch.is_empty() {
                record_batches.push(Span::to_record_batch(&batch, schema.as_ref())?);
            }
        }
        self.write_record_batches_internal("traces", record_batches).await
    }

    pub async fn write_log_batches(&self, batches: Vec<Vec<Log>>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let schema = self.logs_schema().await?;
        let mut record_batches = Vec::new();
        for batch in batches {
            if !batch.is_empty() {
                record_batches.push(arrow::logs_to_record_batch(&batch, schema.as_ref())?);
            }
        }
        self.write_record_batches_internal("logs", record_batches).await
    }

    pub async fn write_metric_batches(&self, batches: Vec<Vec<Metric>>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let schema = self.metrics_schema().await?;
        let mut record_batches = Vec::new();
        for batch in batches {
            if !batch.is_empty() {
                record_batches.push(arrow::metrics_to_record_batch(&batch, schema.as_ref())?);
            }
        }
        self.write_record_batches_internal("metrics", record_batches).await
    }

    pub async fn write_span_record_batches(&self, record_batches: Vec<RecordBatch>) -> Result<()> {
        self.write_record_batches_internal("traces", record_batches).await
    }

    pub async fn write_log_record_batches(&self, record_batches: Vec<RecordBatch>) -> Result<()> {
        self.write_record_batches_internal("logs", record_batches).await
    }

    pub async fn write_metric_record_batches(
        &self,
        record_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        self.write_record_batches_internal("metrics", record_batches).await
    }

    pub async fn spans_schema(&self) -> Result<Arc<IcebergSchema>> {
        let promotion = self
            .config
            .schema_promotion
            .as_ref()
            .and_then(|sp| sp.traces.as_ref());
        Ok(Arc::new(TraceTable::schema(promotion)))
    }

    pub async fn logs_schema(&self) -> Result<Arc<IcebergSchema>> {
        let promotion = self
            .config
            .schema_promotion
            .as_ref()
            .and_then(|sp| sp.logs.as_ref());
        Ok(Arc::new(OtlpLogsTable::schema(promotion)))
    }

    pub async fn metrics_schema(&self) -> Result<Arc<IcebergSchema>> {
        let promotion = self
            .config
            .schema_promotion
            .as_ref()
            .and_then(|sp| sp.metrics.as_ref());
        Ok(Arc::new(OtlpMetricsTable::schema(promotion)))
    }

    async fn write_record_batches_internal(
        &self,
        table_name: &str,
        record_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        if record_batches.is_empty() {
            return Ok(());
        }

        let temp_path = self.write_temp_parquet(table_name, &record_batches)?;
        let conn = self.open_connection()?;
        self.attach_ducklake(&conn)?;
        self.ensure_schema(&conn)?;

        let escaped_path = escape_sql_literal(temp_path.to_string_lossy().as_ref());
        let candidates = self.table_name_candidates(table_name);
        let mut last_err: Option<anyhow::Error> = None;
        let mut wrote = false;
        for qualified_table in candidates {
            let ddl = format!(
                "CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM read_parquet('{path}') LIMIT 0;",
                table = qualified_table,
                path = escaped_path
            );
            let insert = format!(
                "INSERT INTO {table} SELECT * FROM read_parquet('{path}');",
                table = qualified_table,
                path = escaped_path
            );
            conn.execute_batch("BEGIN TRANSACTION;")?;
            match conn.execute_batch(&ddl).and_then(|_| conn.execute_batch(&insert)) {
                Ok(_) => {
                    conn.execute_batch("COMMIT;")?;
                    wrote = true;
                    break;
                }
                Err(err) => {
                    let _ = conn.execute_batch("ROLLBACK;");
                    last_err = Some(anyhow!("DuckLake write failed for {}: {}", qualified_table, err));
                }
            }
        }
        if !wrote {
            let _ = std::fs::remove_file(&temp_path);
            return Err(last_err.unwrap_or_else(|| anyhow!("DuckLake write failed")));
        }
        self.update_metadata_pointer(table_name)?;
        let _ = std::fs::remove_file(&temp_path);
        Ok(())
    }

    fn write_temp_parquet(&self, table_name: &str, batches: &[RecordBatch]) -> Result<PathBuf> {
        let base_dir = std::env::temp_dir().join("splake-ducklake");
        std::fs::create_dir_all(&base_dir)?;
        let temp_path = base_dir.join(format!(
            "{}-{}.parquet",
            table_name,
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let file = std::fs::File::create(&temp_path)?;
        let mut writer =
            ArrowWriter::try_new(file, batches[0].schema(), Some(WriterProperties::builder().build()))?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.close()?;
        Ok(temp_path)
    }

    fn open_connection(&self) -> Result<Connection> {
        let conn = Connection::open_in_memory().map_err(|e| anyhow!("DuckDB open failed: {}", e))?;
        conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
        if self.ducklake.catalog_type == "postgres" {
            conn.execute_batch("INSTALL postgres; LOAD postgres;")?;
        }
        if self.ducklake.catalog_type == "sqlite" {
            conn.execute_batch("INSTALL sqlite; LOAD sqlite;")?;
        }
        self.apply_s3_settings(&conn)?;
        Ok(conn)
    }

    fn apply_s3_settings(&self, conn: &Connection) -> Result<()> {
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
        if let Some(secret) = self.config.s3.secret_access_key.as_ref() {
            conn.execute("SET s3_secret_access_key = ?;", [secret as &dyn ToSql])?;
        }
        conn.execute(
            "SET s3_region = ?;",
            [&self.config.storage.s3_region as &dyn ToSql],
        )?;
        Ok(())
    }

    fn attach_ducklake(&self, conn: &Connection) -> Result<()> {
        let attach_target = match self.ducklake.catalog_type.as_str() {
            "postgres" => {
                if self.ducklake.metadata_path.starts_with("postgres:") {
                    self.ducklake.metadata_path.clone()
                } else {
                    format!("postgres:{}", self.ducklake.metadata_path)
                }
            }
            "sqlite" => {
                if self.ducklake.metadata_path.starts_with("sqlite:") {
                    self.ducklake.metadata_path.clone()
                } else {
                    format!("sqlite:{}", self.ducklake.metadata_path)
                }
            }
            _ => self.ducklake.metadata_path.clone(),
        };
        self.prepare_local_ducklake_paths(&attach_target)?;

        let mut options = vec![format!(
            "DATA_PATH '{}'",
            escape_sql_literal(&self.ducklake.data_path)
        )];
        if let Some(limit) = self.ducklake.data_inlining_row_limit {
            options.push(format!("DATA_INLINING_ROW_LIMIT {}", limit));
        }
        let sql = format!(
            "ATTACH 'ducklake:{target}' AS {alias} ({opts});",
            target = escape_sql_literal(&attach_target),
            alias = self.ducklake.catalog_alias,
            opts = options.join(", ")
        );
        match conn.execute_batch(&sql) {
            Ok(()) => Ok(()),
            Err(err) => {
                if err.to_string().contains("already exists") {
                    Ok(())
                } else {
                    Err(anyhow!("DuckLake attach failed: {}", err))
                }
            }
        }
    }

    fn prepare_local_ducklake_paths(&self, attach_target: &str) -> Result<()> {
        if self.ducklake.catalog_type == "duckdb" || self.ducklake.catalog_type == "sqlite" {
            let raw = attach_target
                .strip_prefix("sqlite:")
                .unwrap_or(attach_target)
                .strip_prefix("duckdb:")
                .unwrap_or(attach_target);
            let metadata_path = PathBuf::from(raw);
            if let Some(parent) = metadata_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if !self.ducklake.data_path.contains("://") {
                std::fs::create_dir_all(&self.ducklake.data_path)?;
            }
        }
        Ok(())
    }

    fn ensure_schema(&self, conn: &Connection) -> Result<()> {
        if self.ducklake.metadata_schema == "main" {
            return Ok(());
        }
        conn.execute_batch(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{};",
            self.ducklake.catalog_alias, self.ducklake.metadata_schema
        ))?;
        Ok(())
    }

    fn qualified_table_name(&self, table_name: &str) -> String {
        format!(
            "{}.{}.{}",
            self.ducklake.catalog_alias, self.ducklake.metadata_schema, table_name
        )
    }

    fn table_name_candidates(&self, table_name: &str) -> Vec<String> {
        vec![
            self.qualified_table_name(table_name),
            format!("{}.{}", self.ducklake.catalog_alias, table_name),
        ]
    }

    fn update_metadata_pointer(&self, table_name: &str) -> Result<()> {
        let Some(cache_dir) = self.cache_dir.as_ref() else {
            return Ok(());
        };
        let metadata_dir = cache_dir.join("iceberg_metadata");
        std::fs::create_dir_all(&metadata_dir)?;
        let pointer_path = metadata_dir.join(format!("{table_name}.json"));
        let mut next_snapshot = chrono::Utc::now().timestamp_millis();
        if let Ok(existing) = std::fs::read_to_string(&pointer_path) {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&existing) {
                if let Some(snapshot) = value.get("snapshot_id").and_then(|v| v.as_i64()) {
                    next_snapshot = std::cmp::max(next_snapshot, snapshot + 1);
                }
            }
        }
        let payload = serde_json::json!({
            "table_location": self.ducklake.data_path,
            "metadata_file": format!("{table_name}-ducklake-metadata.json"),
            "metadata_location": format!("ducklake://{}/{}/{}", self.ducklake.catalog_alias, self.ducklake.metadata_schema, table_name),
            "snapshot_id": next_snapshot,
            "data_files_path": serde_json::Value::Null,
        });
        std::fs::write(pointer_path, payload.to_string())?;
        Ok(())
    }
}

fn escape_sql_literal(input: &str) -> String {
    input.replace('\'', "''")
}
