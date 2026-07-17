use crate::promotion::{PromotionColumn, PromotionDataType};
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
            opt("attributes", string_map()),
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
            opt("attributes", string_map()),
            opt("resource_attributes", string_map()),
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
            opt("attributes", string_map()),
            opt("resource_attributes", string_map()),
            req("record_date", DataType::Date32),
        ];
        fields.extend(promoted_fields(columns));
        Schema::new(fields)
    }
}
