use iceberg::spec::{
    Schema as IcebergSchema, NestedField, Type, PrimitiveType,
    PartitionSpec, UnboundPartitionField, SortOrder,
    MapType, ListType, StructType,
};
use std::collections::HashMap;

/// Raw sessions table - stores OTLP spans
pub struct TraceTable;

impl TraceTable {
    pub fn table_name() -> &'static str {
        "otlp_traces"
    }

    pub fn schema() -> IcebergSchema {
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                // Primary Identifiers
                NestedField::required(1, "session_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "trace_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "span_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(4, "parent_span_id", Type::Primitive(PrimitiveType::String)).into(),

                // Application Context
                NestedField::required(5, "app_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(6, "organization_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(7, "tenant_id", Type::Primitive(PrimitiveType::String)).into(),

                // Span Metadata
                NestedField::required(8, "message_type", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(9, "span_kind", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(10, "timestamp", Type::Primitive(PrimitiveType::Timestamptz)).into(),
                NestedField::optional(11, "end_timestamp", Type::Primitive(PrimitiveType::Timestamptz)).into(),

                // Attributes MAP<STRING, STRING>
                NestedField::optional(
                    12,
                    "attributes",
                    Type::Map(
                        MapType::new(
                            NestedField::required(17, "key", Type::Primitive(PrimitiveType::String)).into(),
                            NestedField::optional(18, "value", Type::Primitive(PrimitiveType::String)).into(),
                        )
                    ),
                ).into(),

                // Events ARRAY<STRUCT<name, timestamp, attributes>>
                NestedField::optional(
                    13,
                    "events",
                    Type::List(
                        ListType::new(
                            NestedField::optional(
                                19,
                                "element",
                                Type::Struct(
                                    StructType::new(vec![
                                        NestedField::required(20, "name", Type::Primitive(PrimitiveType::String)).into(),
                                        NestedField::required(21, "timestamp", Type::Primitive(PrimitiveType::Timestamptz)).into(),
                                        NestedField::optional(
                                            22,
                                            "attributes",
                                            Type::Map(
                                                MapType::new(
                                                    NestedField::required(23, "key", Type::Primitive(PrimitiveType::String)).into(),
                                                    NestedField::optional(24, "value", Type::Primitive(PrimitiveType::String)).into(),
                                                )
                                            ),
                                        ).into(),
                                    ])
                                ),
                            ).into()
                        )
                    ),
                ).into(),

                // Status
                NestedField::optional(14, "status_code", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(15, "status_message", Type::Primitive(PrimitiveType::String)).into(),

                // Partition Key
                NestedField::required(16, "record_date", Type::Primitive(PrimitiveType::Date)).into(),
            ])
            .build()
            .unwrap()
    }

    pub fn partition_spec(schema: &IcebergSchema) -> anyhow::Result<PartitionSpec> {
        let partition_field = UnboundPartitionField {
            source_id: 16, // record_date field ID
            field_id: Some(1000),
            transform: iceberg::spec::Transform::Identity,
            name: "record_date".to_string(),
        };
        PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_unbound_field(partition_field)?
            .build()
            .map_err(Into::into)
    }

    pub fn sort_order(schema: &IcebergSchema) -> anyhow::Result<SortOrder> {
        SortOrder::builder()
            .with_order_id(1)
            .with_fields(vec![
                iceberg::spec::SortField::builder()
                    .source_id(1) // session_id
                    .direction(iceberg::spec::SortDirection::Ascending)
                    .null_order(iceberg::spec::NullOrder::First)
                    .transform(iceberg::spec::Transform::Identity)
                    .build(),
                iceberg::spec::SortField::builder()
                    .source_id(2) // trace_id
                    .direction(iceberg::spec::SortDirection::Ascending)
                    .null_order(iceberg::spec::NullOrder::First)
                    .transform(iceberg::spec::Transform::Identity)
                    .build(),
                iceberg::spec::SortField::builder()
                    .source_id(10) // timestamp
                    .direction(iceberg::spec::SortDirection::Ascending)
                    .null_order(iceberg::spec::NullOrder::First)
                    .transform(iceberg::spec::Transform::Identity)
                    .build(),
            ])
            .build(schema)
            .map_err(Into::into)
    }

    pub fn table_properties() -> HashMap<String, String> {
        let mut properties = HashMap::new();
        properties.insert("write.format.default".to_string(), "parquet".to_string());
        properties.insert("format-version".to_string(), "2".to_string());
        properties.insert("write.target-file-size-bytes".to_string(), "134217728".to_string());
        properties.insert("write.parquet.compression-codec".to_string(), "zstd".to_string());
        properties.insert("write.parquet.compression-level".to_string(), "3".to_string());

        // Bloom filters for ID columns
        for id_col in ["session_id", "trace_id", "span_id", "parent_span_id"] {
            properties.insert(
                format!("write.parquet.bloom-filter-enabled.{}", id_col),
                "true".to_string(),
            );
            properties.insert(
                format!("write.parquet.bloom-filter-fpp.{}", id_col),
                "0.01".to_string(),
            );
        }

        properties
    }
}

/// OTLP logs table - stores OTLP log records
pub struct OtlpLogsTable;

impl OtlpLogsTable {
    pub fn table_name() -> &'static str {
        "otlp_logs"
    }

    pub fn schema() -> IcebergSchema {
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                // Session context
                NestedField::optional(1, "session_id", Type::Primitive(PrimitiveType::String)).into(),

                // Timestamps
                NestedField::required(2, "timestamp", Type::Primitive(PrimitiveType::Timestamptz)).into(),
                NestedField::optional(3, "observed_timestamp", Type::Primitive(PrimitiveType::Timestamptz)).into(),

                // Severity
                NestedField::required(4, "severity_number", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(5, "severity_text", Type::Primitive(PrimitiveType::String)).into(),

                // Log content
                NestedField::required(6, "body", Type::Primitive(PrimitiveType::String)).into(),

                // Attributes MAP<STRING, STRING>
                NestedField::optional(
                    7,
                    "attributes",
                    Type::Map(
                        MapType::new(
                            NestedField::required(11, "key", Type::Primitive(PrimitiveType::String)).into(),
                            NestedField::optional(12, "value", Type::Primitive(PrimitiveType::String)).into(),
                        )
                    ),
                ).into(),

                // Resource attributes MAP<STRING, STRING>
                NestedField::optional(
                    8,
                    "resource_attributes",
                    Type::Map(
                        MapType::new(
                            NestedField::required(13, "key", Type::Primitive(PrimitiveType::String)).into(),
                            NestedField::optional(14, "value", Type::Primitive(PrimitiveType::String)).into(),
                        )
                    ),
                ).into(),

                // Trace correlation
                NestedField::optional(9, "trace_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(10, "span_id", Type::Primitive(PrimitiveType::String)).into(),

                // Partition Key
                NestedField::required(15, "record_date", Type::Primitive(PrimitiveType::Date)).into(),
            ])
            .build()
            .unwrap()
    }

    pub fn partition_spec(schema: &IcebergSchema) -> anyhow::Result<PartitionSpec> {
        let partition_field = UnboundPartitionField {
            source_id: 15, // record_date field ID
            field_id: Some(1000),
            transform: iceberg::spec::Transform::Identity,
            name: "record_date".to_string(),
        };
        PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_unbound_field(partition_field)?
            .build()
            .map_err(Into::into)
    }

    pub fn sort_order(schema: &IcebergSchema) -> anyhow::Result<SortOrder> {
        SortOrder::builder()
            .with_order_id(1)
            .with_fields(vec![
                iceberg::spec::SortField::builder()
                    .source_id(1) // session_id
                    .direction(iceberg::spec::SortDirection::Ascending)
                    .null_order(iceberg::spec::NullOrder::First)
                    .transform(iceberg::spec::Transform::Identity)
                    .build(),
                iceberg::spec::SortField::builder()
                    .source_id(2) // timestamp
                    .direction(iceberg::spec::SortDirection::Ascending)
                    .null_order(iceberg::spec::NullOrder::First)
                    .transform(iceberg::spec::Transform::Identity)
                    .build(),
            ])
            .build(schema)
            .map_err(Into::into)
    }

    pub fn table_properties() -> HashMap<String, String> {
        let mut properties = HashMap::new();
        properties.insert("write.format.default".to_string(), "parquet".to_string());
        properties.insert("format-version".to_string(), "2".to_string());
        properties.insert("write.target-file-size-bytes".to_string(), "134217728".to_string());
        properties.insert("write.parquet.compression-codec".to_string(), "zstd".to_string());
        properties.insert("write.parquet.compression-level".to_string(), "3".to_string());

        // Bloom filters for correlation IDs
        for id_col in ["session_id", "trace_id", "span_id"] {
            properties.insert(
                format!("write.parquet.bloom-filter-enabled.{}", id_col),
                "true".to_string(),
            );
            properties.insert(
                format!("write.parquet.bloom-filter-fpp.{}", id_col),
                "0.01".to_string(),
            );
        }

        properties
    }
}
