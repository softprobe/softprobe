use crate::models::{Log, Metric, Span};
use crate::promotion::{
    extract_telemetry_promoted_value, PromotionColumn, TelemetryColumnsManifest,
    TelemetryPromotionEvent, TelemetryPromotionRow, TelemetryTable,
};
use crate::runtime_engine::DuckLakeScope;
use crate::storage::schema::arrow;
use crate::storage::schema::tables::{OtlpLogsTable, OtlpMetricsTable, TraceTable};
use ::arrow::record_batch::RecordBatch;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

use super::DuckLakeWriter;

impl DuckLakeWriter {
    pub(super) fn flatten_spans(batches: Vec<Vec<Span>>) -> Vec<Span> {
        batches.into_iter().flatten().collect()
    }

    pub(super) fn flatten_logs(batches: Vec<Vec<Log>>) -> Vec<Log> {
        batches.into_iter().flatten().collect()
    }

    pub(super) fn flatten_metrics(batches: Vec<Vec<Metric>>) -> Vec<Metric> {
        batches.into_iter().flatten().collect()
    }

    pub(super) fn telemetry_columns_for_table(
        manifests: &[TelemetryColumnsManifest],
        table: TelemetryTable,
    ) -> Vec<PromotionColumn> {
        manifests
            .iter()
            .filter(|manifest| manifest.target.tables.contains(&table))
            .flat_map(|manifest| manifest.columns.iter().cloned())
            .collect()
    }

    pub(super) fn apply_span_promotions(
        spans: &mut [Span],
        columns: &[PromotionColumn],
    ) -> Result<()> {
        for span in spans {
            let events = span
                .events
                .iter()
                .map(|event| TelemetryPromotionEvent {
                    name: event.name.clone(),
                    attributes: event.attributes.clone(),
                })
                .collect::<Vec<_>>();
            let row = TelemetryPromotionRow {
                resource_attributes: &span.resource_attributes,
                attributes: &span.attributes,
                events: &events,
                http_request_body: span.http_request_body.as_deref(),
                http_response_body: span.http_response_body.as_deref(),
                metric_value: None,
            };
            let mut promoted = Vec::new();
            for column in columns {
                if let Some(value) = extract_telemetry_promoted_value(&row, column)? {
                    promoted.push((column.name.clone(), value));
                }
            }
            span.attributes.extend(promoted);
        }
        Ok(())
    }

    pub(super) fn apply_log_promotions(
        logs: &mut [Log],
        columns: &[PromotionColumn],
    ) -> Result<()> {
        for log in logs {
            let row = TelemetryPromotionRow {
                resource_attributes: &log.resource_attributes,
                attributes: &log.attributes,
                events: &[],
                http_request_body: None,
                http_response_body: None,
                metric_value: None,
            };
            let mut promoted = Vec::new();
            for column in columns {
                if let Some(value) = extract_telemetry_promoted_value(&row, column)? {
                    promoted.push((column.name.clone(), value));
                }
            }
            log.attributes.extend(promoted);
        }
        Ok(())
    }

    pub(super) fn apply_metric_promotions(
        metrics: &mut [Metric],
        columns: &[PromotionColumn],
    ) -> Result<()> {
        for metric in metrics {
            let row = TelemetryPromotionRow {
                resource_attributes: &metric.resource_attributes,
                attributes: &metric.attributes,
                events: &[],
                http_request_body: None,
                http_response_body: None,
                metric_value: Some(metric.value),
            };
            let mut promoted = Vec::new();
            for column in columns {
                if let Some(value) = extract_telemetry_promoted_value(&row, column)? {
                    promoted.push((column.name.clone(), value));
                }
            }
            metric.attributes.extend(promoted);
        }
        Ok(())
    }

    pub async fn write_span_batches(&self, batches: Vec<Vec<Span>>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        if self.use_tenant_scoped_ducklake() {
            let resolver = self.tenant_ducklake.as_ref().unwrap();
            if let Some(scope) = self.tenant_bound_scope() {
                let mut spans = Self::flatten_spans(batches);
                if spans.is_empty() {
                    return Ok(());
                }
                let manifests = resolver
                    .load_active_telemetry_columns_manifests_for_scope(&scope)
                    .await?;
                let columns = Self::telemetry_columns_for_table(&manifests, TelemetryTable::Traces);
                Self::apply_span_promotions(&mut spans, &columns)?;
                let schema = Arc::new(TraceTable::schema_with_promoted_columns(&columns));
                let dk = self.effective_ducklake(&scope);
                let record_batches = vec![Span::to_record_batch(&spans, schema.as_ref())?];
                self.write_record_batches_internal_with_ducklake(&dk, "traces", record_batches)
                    .await?;
                return Ok(());
            }
            let spans = Self::flatten_spans(batches);
            if spans.is_empty() {
                return Ok(());
            }
            let mut by_tenant: HashMap<String, Vec<Span>> = HashMap::new();
            for span in spans {
                let tid = span
                    .tenant_id
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "tenant-scoped ingest requires span.tenant_id from authenticated tenant"
                        )
                    })?
                    .clone();
                by_tenant.entry(tid).or_default().push(span);
            }
            for (tenant_id, mut tenant_spans) in by_tenant {
                let (scope, manifests) = resolver
                    .load_active_telemetry_columns_manifests(&tenant_id)
                    .await?;
                let columns = Self::telemetry_columns_for_table(&manifests, TelemetryTable::Traces);
                Self::apply_span_promotions(&mut tenant_spans, &columns)?;
                let schema = Arc::new(TraceTable::schema_with_promoted_columns(&columns));
                let dk = self.effective_ducklake(&scope);
                let record_batches = vec![Span::to_record_batch(&tenant_spans, schema.as_ref())?];
                self.write_record_batches_internal_with_ducklake(&dk, "traces", record_batches)
                    .await?;
            }
            Ok(())
        } else if self.ducklake.catalog_type == "sqlite" {
            let mut spans = Self::flatten_spans(batches);
            if spans.is_empty() {
                return Ok(());
            }
            let manifests = self.load_active_telemetry_manifests_local()?;
            let columns = Self::telemetry_columns_for_table(&manifests, TelemetryTable::Traces);
            Self::apply_span_promotions(&mut spans, &columns)?;
            let schema = Arc::new(TraceTable::schema_with_promoted_columns(&columns));
            let record_batches = vec![Span::to_record_batch(&spans, schema.as_ref())?];
            self.write_record_batches_internal("traces", record_batches)
                .await
        } else {
            let schema = self.spans_schema().await?;
            let mut record_batches = Vec::new();
            for batch in batches {
                if !batch.is_empty() {
                    record_batches.push(Span::to_record_batch(&batch, schema.as_ref())?);
                }
            }
            self.write_record_batches_internal("traces", record_batches)
                .await
        }
    }

    pub(super) async fn write_tenant_log_batches(
        &self,
        scope: &DuckLakeScope,
        manifests: &[TelemetryColumnsManifest],
        batches: Vec<Vec<Log>>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let mut logs = Self::flatten_logs(batches);
        if logs.is_empty() {
            return Ok(());
        }
        let columns = Self::telemetry_columns_for_table(manifests, TelemetryTable::Logs);
        Self::apply_log_promotions(&mut logs, &columns)?;
        let schema = Arc::new(OtlpLogsTable::schema_with_promoted_columns(&columns));
        let dk = self.effective_ducklake(scope);
        let record_batches = vec![arrow::logs_to_record_batch(&logs, schema.as_ref())?];
        self.write_record_batches_internal_with_ducklake(&dk, "logs", record_batches)
            .await?;
        Ok(())
    }

    pub async fn write_log_batches(&self, batches: Vec<Vec<Log>>) -> Result<()> {
        if self.use_tenant_scoped_ducklake() {
            let resolver = self.tenant_ducklake.as_ref().unwrap();
            // Non-scope-bound writers (single-tenant / tests) use the configured DuckLake scope.
            let scope = self.tenant_bound_scope().unwrap_or_else(|| DuckLakeScope {
                metadata_schema: self.ducklake.metadata_schema.clone(),
                data_path: self.ducklake.data_path.clone(),
            });
            let manifests = if self.scope_bound {
                resolver
                    .load_active_telemetry_columns_manifests_for_scope(&scope)
                    .await?
            } else {
                resolver
                    .load_active_telemetry_columns_manifests("")
                    .await?
                    .1
            };
            return self
                .write_tenant_log_batches(&scope, &manifests, batches)
                .await;
        }
        if self.ducklake.catalog_type == "sqlite" {
            let scope = DuckLakeScope {
                metadata_schema: self.ducklake.metadata_schema.clone(),
                data_path: self.ducklake.data_path.clone(),
            };
            let manifests = self.load_active_telemetry_manifests_local()?;
            return self
                .write_tenant_log_batches(&scope, &manifests, batches)
                .await;
        }
        let schema = self.logs_schema().await?;
        let mut record_batches = Vec::new();
        for batch in batches {
            if !batch.is_empty() {
                record_batches.push(arrow::logs_to_record_batch(&batch, schema.as_ref())?);
            }
        }
        self.write_record_batches_internal("logs", record_batches)
            .await
    }

    pub(super) async fn write_tenant_metric_batches(
        &self,
        scope: &DuckLakeScope,
        manifests: &[TelemetryColumnsManifest],
        batches: Vec<Vec<Metric>>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let mut metrics = Self::flatten_metrics(batches);
        if metrics.is_empty() {
            return Ok(());
        }
        let columns = Self::telemetry_columns_for_table(manifests, TelemetryTable::Metrics);
        Self::apply_metric_promotions(&mut metrics, &columns)?;
        let schema = Arc::new(OtlpMetricsTable::schema_with_promoted_columns(&columns));
        let dk = self.effective_ducklake(scope);
        let record_batches = vec![arrow::metrics_to_record_batch(&metrics, schema.as_ref())?];
        self.write_record_batches_internal_with_ducklake(&dk, "metrics", record_batches)
            .await?;
        Ok(())
    }

    pub async fn write_metric_batches(&self, batches: Vec<Vec<Metric>>) -> Result<()> {
        if self.use_tenant_scoped_ducklake() {
            let resolver = self.tenant_ducklake.as_ref().unwrap();
            // Non-scope-bound writers (single-tenant / tests) use the configured DuckLake scope.
            let scope = self.tenant_bound_scope().unwrap_or_else(|| DuckLakeScope {
                metadata_schema: self.ducklake.metadata_schema.clone(),
                data_path: self.ducklake.data_path.clone(),
            });
            let manifests = if self.scope_bound {
                resolver
                    .load_active_telemetry_columns_manifests_for_scope(&scope)
                    .await?
            } else {
                resolver
                    .load_active_telemetry_columns_manifests("")
                    .await?
                    .1
            };
            return self
                .write_tenant_metric_batches(&scope, &manifests, batches)
                .await;
        }
        if self.ducklake.catalog_type == "sqlite" {
            let scope = DuckLakeScope {
                metadata_schema: self.ducklake.metadata_schema.clone(),
                data_path: self.ducklake.data_path.clone(),
            };
            let manifests = self.load_active_telemetry_manifests_local()?;
            return self
                .write_tenant_metric_batches(&scope, &manifests, batches)
                .await;
        }
        let schema = self.metrics_schema().await?;
        let mut record_batches = Vec::new();
        for batch in batches {
            if !batch.is_empty() {
                record_batches.push(arrow::metrics_to_record_batch(&batch, schema.as_ref())?);
            }
        }
        self.write_record_batches_internal("metrics", record_batches)
            .await
    }

    pub async fn write_span_record_batches(&self, record_batches: Vec<RecordBatch>) -> Result<()> {
        if self.use_tenant_scoped_ducklake() {
            let scope = self
                .tenant_ducklake
                .as_ref()
                .unwrap()
                .resolve_or_create("")
                .await?;
            let dk = self.effective_ducklake(&scope);
            self.write_record_batches_internal_with_ducklake(&dk, "traces", record_batches)
                .await
        } else {
            self.write_record_batches_internal("traces", record_batches)
                .await
        }
    }

    pub async fn write_log_record_batches(&self, record_batches: Vec<RecordBatch>) -> Result<()> {
        if self.use_tenant_scoped_ducklake() {
            let scope = self
                .tenant_ducklake
                .as_ref()
                .unwrap()
                .resolve_or_create("")
                .await?;
            let dk = self.effective_ducklake(&scope);
            return self
                .write_record_batches_internal_with_ducklake(&dk, "logs", record_batches)
                .await;
        }
        self.write_record_batches_internal("logs", record_batches)
            .await
    }

    pub async fn write_metric_record_batches(
        &self,
        record_batches: Vec<RecordBatch>,
    ) -> Result<()> {
        if self.use_tenant_scoped_ducklake() {
            let scope = self
                .tenant_ducklake
                .as_ref()
                .unwrap()
                .resolve_or_create("")
                .await?;
            let dk = self.effective_ducklake(&scope);
            return self
                .write_record_batches_internal_with_ducklake(&dk, "metrics", record_batches)
                .await;
        }
        self.write_record_batches_internal("metrics", record_batches)
            .await
    }
}
