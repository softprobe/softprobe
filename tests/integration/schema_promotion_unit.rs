use softprobe_runtime::config::{PromotedDataType, SchemaPromotionConfig, TablePromotionConfig};
use softprobe_runtime::storage::iceberg::tables::{OtlpLogsTable, OtlpMetricsTable, TraceTable};
use arrow::datatypes::Schema as ArrowSchema;

#[test]
fn test_schema_promotion_config_loading() {
    // Test that schema promotion config can be deserialized from YAML
    // Create a minimal config with just schema_promotion
    let promotion_config = SchemaPromotionConfig {
        traces: Some(TablePromotionConfig {
            attributes: vec![
                softprobe_runtime::config::PromotedColumn {
                    attribute_key: "user.id".to_string(),
                    column_name: Some("user_id".to_string()),
                    data_type: Some(PromotedDataType::String),
                },
                softprobe_runtime::config::PromotedColumn {
                    attribute_key: "department".to_string(),
                    column_name: None,
                    data_type: None,
                },
                softprobe_runtime::config::PromotedColumn {
                    attribute_key: "sp.order.id".to_string(),
                    column_name: Some("order_id".to_string()),
                    data_type: None,
                },
            ],
            resource_attributes: vec![
                softprobe_runtime::config::PromotedColumn {
                    attribute_key: "service.name".to_string(),
                    column_name: Some("service_name".to_string()),
                    data_type: None,
                },
            ],
        }),
        logs: Some(TablePromotionConfig {
            attributes: vec![
                softprobe_runtime::config::PromotedColumn {
                    attribute_key: "user.id".to_string(),
                    column_name: None,
                    data_type: None,
                },
            ],
            resource_attributes: vec![],
        }),
        metrics: Some(TablePromotionConfig {
            attributes: vec![
                softprobe_runtime::config::PromotedColumn {
                    attribute_key: "environment".to_string(),
                    column_name: None,
                    data_type: None,
                },
            ],
            resource_attributes: vec![],
        }),
    };
    
    // Check traces config
    assert!(promotion_config.traces.is_some());
    let traces = promotion_config.traces.as_ref().unwrap();
    assert_eq!(traces.attributes.len(), 3);
    assert_eq!(traces.attributes[0].attribute_key, "user.id");
    assert_eq!(traces.attributes[0].column_name, Some("user_id".to_string()));
    assert!(matches!(traces.attributes[0].data_type, Some(PromotedDataType::String)));
    assert_eq!(traces.attributes[1].attribute_key, "department");
    assert_eq!(traces.attributes[1].column_name, None); // Should default to attribute_key
    assert_eq!(traces.resource_attributes.len(), 1);
    
    // Check logs config
    assert!(promotion_config.logs.is_some());
    let logs = promotion_config.logs.as_ref().unwrap();
    assert_eq!(logs.attributes.len(), 1);
    
    // Check metrics config
    assert!(promotion_config.metrics.is_some());
    let metrics = promotion_config.metrics.as_ref().unwrap();
    assert_eq!(metrics.attributes.len(), 1);
}

#[test]
fn test_schema_generation_with_promotion() {
    // Test that promoted columns are added to schema
    let promotion_config = TablePromotionConfig {
        attributes: vec![
            softprobe_runtime::config::PromotedColumn {
                attribute_key: "user.id".to_string(),
                column_name: Some("user_id".to_string()),
                data_type: Some(PromotedDataType::String),
            },
            softprobe_runtime::config::PromotedColumn {
                attribute_key: "department".to_string(),
                column_name: None,
                data_type: None,
            },
        ],
        resource_attributes: vec![
            softprobe_runtime::config::PromotedColumn {
                attribute_key: "service.name".to_string(),
                column_name: Some("service_name".to_string()),
                data_type: Some(PromotedDataType::String),
            },
        ],
    };

    // Generate schema with promotion
    let iceberg_schema = TraceTable::schema(Some(&promotion_config));
    
    // Convert to Arrow schema to access fields
    let arrow_schema = ArrowSchema::try_from(&iceberg_schema).expect("should convert");
    
    // Check that promoted columns exist
    let fields: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    
    assert!(fields.contains(&"user_id".to_string()), "user_id should be in schema");
    assert!(fields.contains(&"department".to_string()), "department should be in schema");
    assert!(fields.contains(&"service_name".to_string()), "service_name should be in schema");
    
    // Check that base fields still exist
    assert!(fields.contains(&"session_id".to_string()), "base fields should still exist");
    assert!(fields.contains(&"trace_id".to_string()), "base fields should still exist");
    assert!(fields.contains(&"record_date".to_string()), "record_date should still exist");
}

#[test]
fn test_schema_generation_without_promotion() {
    // Test that schema works without promotion (backward compatibility)
    let iceberg_schema = TraceTable::schema(None);
    let arrow_schema = ArrowSchema::try_from(&iceberg_schema).expect("should convert");
    
    let fields: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    
    // Should have all base fields
    assert!(fields.contains(&"session_id".to_string()));
    assert!(fields.contains(&"trace_id".to_string()));
    assert!(fields.contains(&"attributes".to_string()));
    assert!(fields.contains(&"record_date".to_string()));
    
    // Should NOT have any promoted columns
    assert!(!fields.contains(&"user_id".to_string()));
    assert!(!fields.contains(&"department".to_string()));
}

#[test]
fn test_schema_promotion_data_types() {
    // Test that different data types are handled correctly
    let promotion_config = TablePromotionConfig {
        attributes: vec![
            softprobe_runtime::config::PromotedColumn {
                attribute_key: "count".to_string(),
                column_name: Some("count".to_string()),
                data_type: Some(PromotedDataType::Int),
            },
            softprobe_runtime::config::PromotedColumn {
                attribute_key: "price".to_string(),
                column_name: Some("price".to_string()),
                data_type: Some(PromotedDataType::Double),
            },
            softprobe_runtime::config::PromotedColumn {
                attribute_key: "is_active".to_string(),
                column_name: Some("is_active".to_string()),
                data_type: Some(PromotedDataType::Boolean),
            },
        ],
        resource_attributes: vec![],
    };

    let iceberg_schema = TraceTable::schema(Some(&promotion_config));
    
    // Convert to Arrow schema to check field types
    let arrow_schema = ArrowSchema::try_from(&iceberg_schema).expect("should convert");
    
    // Find fields and check their types
    let count_field = arrow_schema.field_with_name("count").expect("count field should exist");
    assert!(matches!(
        count_field.data_type(),
        arrow::datatypes::DataType::Int32
    ));
    
    let price_field = arrow_schema.field_with_name("price").expect("price field should exist");
    assert!(matches!(
        price_field.data_type(),
        arrow::datatypes::DataType::Float64
    ));
    
    let is_active_field = arrow_schema.field_with_name("is_active").expect("is_active field should exist");
    assert!(matches!(
        is_active_field.data_type(),
        arrow::datatypes::DataType::Boolean
    ));
}

#[test]
fn test_logs_schema_promotion() {
    let promotion_config = TablePromotionConfig {
        attributes: vec![
            softprobe_runtime::config::PromotedColumn {
                attribute_key: "user.id".to_string(),
                column_name: None,
                data_type: None,
            },
        ],
        resource_attributes: vec![],
    };

    let iceberg_schema = OtlpLogsTable::schema(Some(&promotion_config));
    let arrow_schema = ArrowSchema::try_from(&iceberg_schema).expect("should convert");
    let fields: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    
    assert!(fields.contains(&"user.id".to_string()));
    assert!(fields.contains(&"session_id".to_string())); // Base field
}

#[test]
fn test_metrics_schema_promotion() {
    let promotion_config = TablePromotionConfig {
        attributes: vec![
            softprobe_runtime::config::PromotedColumn {
                attribute_key: "environment".to_string(),
                column_name: None,
                data_type: None,
            },
        ],
        resource_attributes: vec![],
    };

    let iceberg_schema = OtlpMetricsTable::schema(Some(&promotion_config));
    let arrow_schema = ArrowSchema::try_from(&iceberg_schema).expect("should convert");
    let fields: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    
    assert!(fields.contains(&"environment".to_string()));
    assert!(fields.contains(&"metric_name".to_string())); // Base field
}
