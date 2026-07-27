use crate::promotion::{PromotionColumn, PromotionDataType};
use crate::storage::schema::variant::hot_variant_columns;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use std::sync::Arc;

fn utf8() -> DataType {
    DataType::Utf8
}

fn ts_utc() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
}

fn string_map() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", utf8(), false),
                Field::new("value", utf8(), true),
            ])),
            false,
        )),
        false,
    )
}

/// Staging type for DuckLake VARIANT columns (JSON text → `::JSON::VARIANT` on write).
fn variant_json() -> DataType {
    utf8()
}

/// Nullable hot VARIANT field; must be registered in [`hot_variant_columns`].
fn opt_hot_variant(table: &str, name: &'static str) -> Field {
    assert!(
        hot_variant_columns(table).contains(&name),
        "column '{name}' must be listed in hot_variant_columns(\"{table}\")"
    );
    opt(name, variant_json())
}

fn promoted_fields(columns: &[PromotionColumn]) -> Vec<Field> {
    columns
        .iter()
        .map(|column| {
            let data_type = match column.data_type {
                PromotionDataType::String | PromotionDataType::Json => utf8(),
                PromotionDataType::Bool => DataType::Boolean,
                PromotionDataType::Int64 => DataType::Int64,
                PromotionDataType::Double | PromotionDataType::Decimal => DataType::Float64,
                PromotionDataType::Timestamp => ts_utc(),
            };
            Field::new(&column.name, data_type, true)
        })
        .collect()
}

fn req(name: &str, dt: DataType) -> Field {
    Field::new(name, dt, false)
}

fn opt(name: &str, dt: DataType) -> Field {
    Field::new(name, dt, true)
}

/// Raw sessions table - stores OTLP spans
pub struct TraceTable;

impl TraceTable {
    pub fn table_name() -> &'static str {
        "traces"
    }

    pub fn schema() -> Schema {
        Self::schema_with_promoted_columns(&[])
    }

    pub fn schema_with_promoted_columns(columns: &[PromotionColumn]) -> Schema {
        let events_element = DataType::Struct(Fields::from(vec![
            req("name", utf8()),
            req("timestamp", ts_utc()),
            opt("attributes", string_map()),
        ]));
        let mut fields = vec![
            req("session_id", utf8()),
            req("trace_id", utf8()),
            req("span_id", utf8()),
            opt("parent_span_id", utf8()),
            req("app_id", utf8()),
            opt("organization_id", utf8()),
            opt("tenant_id", utf8()),
            req("message_type", utf8()),
            opt("span_kind", utf8()),
            req("timestamp", ts_utc()),
            opt("end_timestamp", ts_utc()),
            opt_hot_variant("traces", "attributes"),
            opt(
                "events",
                DataType::List(Arc::new(Field::new("item", events_element, true))),
            ),
            opt("status_code", utf8()),
            opt("status_message", utf8()),
            opt("http_request_method", utf8()),
            opt("http_request_path", utf8()),
            opt("http_request_headers", utf8()),
            opt("http_request_body", utf8()),
            opt("http_response_status_code", DataType::Int32),
            opt("http_response_headers", utf8()),
            opt("http_response_body", utf8()),
            req("record_date", DataType::Date32),
        ];
        fields.extend(promoted_fields(columns));
        Schema::new(fields)
    }
}

/// Immutable LLM evaluation scores attached to traces, spans, or sessions.
pub struct ScoreTable;

impl ScoreTable {
    pub fn table_name() -> &'static str {
        "scores"
    }

    pub fn schema() -> Schema {
        Schema::new(vec![
            req("score_id", utf8()),
            req("timestamp", ts_utc()),
            opt("trace_id", utf8()),
            opt("span_id", utf8()),
            opt("session_id", utf8()),
            req("name", utf8()),
            req("data_type", utf8()),
            opt("numeric_value", DataType::Float64),
            opt("string_value", utf8()),
            opt("boolean_value", DataType::Boolean),
            req("source", utf8()),
            opt("comment", utf8()),
            opt("config_id", utf8()),
            opt("author_id", utf8()),
            opt("metadata", string_map()),
            req("record_date", DataType::Date32),
        ])
    }
}

/// OTLP logs table
pub struct OtlpLogsTable;

impl OtlpLogsTable {
    pub fn table_name() -> &'static str {
        "logs"
    }

    pub fn schema() -> Schema {
        Self::schema_with_promoted_columns(&[])
    }

    pub fn schema_with_promoted_columns(columns: &[PromotionColumn]) -> Schema {
        let mut fields = vec![
            opt("session_id", utf8()),
            req("timestamp", ts_utc()),
            opt("observed_timestamp", ts_utc()),
            req("severity_number", DataType::Int32),
            req("severity_text", utf8()),
            req("body", utf8()),
            opt_hot_variant("logs", "attributes"),
            opt_hot_variant("logs", "resource_attributes"),
            opt("trace_id", utf8()),
            opt("span_id", utf8()),
            req("record_date", DataType::Date32),
        ];
        fields.extend(promoted_fields(columns));
        Schema::new(fields)
    }
}

/// OTLP metrics table
pub struct OtlpMetricsTable;

impl OtlpMetricsTable {
    pub fn table_name() -> &'static str {
        "metrics"
    }

    pub fn schema() -> Schema {
        Self::schema_with_promoted_columns(&[])
    }

    pub fn schema_with_promoted_columns(columns: &[PromotionColumn]) -> Schema {
        let mut fields = vec![
            req("metric_name", utf8()),
            req("description", utf8()),
            req("unit", utf8()),
            req("metric_type", utf8()),
            req("timestamp", ts_utc()),
            req("value", DataType::Float64),
            opt_hot_variant("metrics", "attributes"),
            opt_hot_variant("metrics", "resource_attributes"),
            req("record_date", DataType::Date32),
        ];
        fields.extend(promoted_fields(columns));
        Schema::new(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn hot_attribute_columns_use_utf8_json_staging() {
        let traces = TraceTable::schema();
        assert!(matches!(
            traces.field_with_name("attributes").unwrap().data_type(),
            DataType::Utf8
        ));
        // Nested event attributes remain MAP for non-hot nested maps.
        let events = traces.field_with_name("events").unwrap().data_type();
        let DataType::List(item) = events else {
            panic!("expected list");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("expected struct");
        };
        let attrs = fields.iter().find(|f| f.name() == "attributes").unwrap();
        assert!(matches!(attrs.data_type(), DataType::Map(_, _)));

        let logs = OtlpLogsTable::schema();
        assert!(matches!(
            logs.field_with_name("attributes").unwrap().data_type(),
            DataType::Utf8
        ));
        assert!(matches!(
            logs.field_with_name("resource_attributes")
                .unwrap()
                .data_type(),
            DataType::Utf8
        ));

        let metrics = OtlpMetricsTable::schema();
        assert!(matches!(
            metrics.field_with_name("attributes").unwrap().data_type(),
            DataType::Utf8
        ));
        assert!(matches!(
            metrics
                .field_with_name("resource_attributes")
                .unwrap()
                .data_type(),
            DataType::Utf8
        ));

        // Scores metadata stays MAP (out of hot-column scope).
        let scores = ScoreTable::schema();
        assert!(matches!(
            scores.field_with_name("metadata").unwrap().data_type(),
            DataType::Map(_, _)
        ));
    }

    #[test]
    fn hot_variant_registry_covers_schema_columns() {
        use crate::storage::schema::variant::hot_variant_columns;

        for (table, schema) in [
            ("traces", TraceTable::schema()),
            ("logs", OtlpLogsTable::schema()),
            ("metrics", OtlpMetricsTable::schema()),
        ] {
            for col in hot_variant_columns(table) {
                let field = schema
                    .field_with_name(col)
                    .unwrap_or_else(|_| panic!("{table}.{col} missing from schema"));
                assert!(
                    matches!(field.data_type(), DataType::Utf8),
                    "{table}.{col} must stage as Utf8 JSON for VARIANT cast"
                );
            }
        }
    }
}
