use chrono::Utc;
use softprobe_runtime::config::{PromotedDataType, SchemaPromotionConfig, TablePromotionConfig};
use softprobe_runtime::models::Span as SpanData;
use softprobe_runtime::storage::iceberg::arrow::spans_to_record_batch_with_promotion;
use softprobe_runtime::storage::iceberg::tables::TraceTable;
use std::collections::HashMap;
use iceberg::Catalog;

use crate::util::iceberg::load_test_config;

#[tokio::test]
async fn test_arrow_conversion_with_promoted_columns() {
    // Create promotion config
    let promotion_config = TablePromotionConfig {
        attributes: vec![
            softprobe_runtime::config::PromotedColumn {
                attribute_key: "user.id".to_string(),
                column_name: Some("user_id".to_string()),
                data_type: Some(PromotedDataType::String),
            },
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

    // Generate schema with promotion
    let schema = TraceTable::schema(Some(&promotion_config));

    // Create spans with promoted attributes
    let mut spans = Vec::new();
    for i in 0..5 {
        let mut attributes = HashMap::new();
        attributes.insert("user.id".to_string(), format!("user-{}", i));
        attributes.insert("count".to_string(), (i * 10).to_string());
        attributes.insert("price".to_string(), format!("{}.99", i));
        attributes.insert("is_active".to_string(), (i % 2 == 0).to_string());
        attributes.insert("other_attr".to_string(), "should_not_be_promoted".to_string());

        spans.push(SpanData {
            session_id: format!("session-{}", i),
            trace_id: format!("trace-{}", i),
            span_id: format!("span-{}", i),
            parent_span_id: None,
            app_id: "test-app".to_string(),
            organization_id: None,
            tenant_id: None,
            message_type: "test".to_string(),
            span_kind: None,
            timestamp: Utc::now(),
            end_timestamp: None,
            attributes,
            events: Vec::new(),
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
            status_code: None,
            status_message: None,
        });
    }

    // Convert to RecordBatch with promotion config
    let record_batch = spans_to_record_batch_with_promotion(&spans, &schema, Some(&promotion_config))
        .expect("should convert");

    // Verify schema matches (convert Iceberg to Arrow for comparison)
    let arrow_schema = arrow::datatypes::Schema::try_from(&schema).expect("should convert");
    assert_eq!(
        record_batch.schema().fields().len(),
        arrow_schema.fields().len(),
        "RecordBatch schema should match Iceberg schema"
    );

    // Find promoted column indices
    let user_id_idx = record_batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "user_id")
        .expect("user_id column should exist");
    let count_idx = record_batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "count")
        .expect("count column should exist");
    let price_idx = record_batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "price")
        .expect("price column should exist");
    let is_active_idx = record_batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "is_active")
        .expect("is_active column should exist");

    // Verify data in promoted columns
    // Check user_id (String)
    let user_id_array = record_batch.column(user_id_idx);
    let user_id_strings: &arrow::array::StringArray = user_id_array
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("should be StringArray");
    for (i, val) in user_id_strings.iter().enumerate() {
        assert_eq!(val, Some(format!("user-{}", i).as_str()));
    }

    // Check count (Int)
    let count_array = record_batch.column(count_idx);
    let count_ints: &arrow::array::Int32Array = count_array
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .expect("should be Int32Array");
    for (i, val) in count_ints.iter().enumerate() {
        assert_eq!(val, Some((i * 10) as i32));
    }

    // Check price (Double)
    let price_array = record_batch.column(price_idx);
    let price_doubles: &arrow::array::Float64Array = price_array
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("should be Float64Array");
    for (i, val) in price_doubles.iter().enumerate() {
        assert_eq!(val, Some(i as f64 + 0.99));
    }

    // Check is_active (Boolean)
    let is_active_array = record_batch.column(is_active_idx);
    let is_active_bools: &arrow::array::BooleanArray = is_active_array
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .expect("should be BooleanArray");
    for (i, val) in is_active_bools.iter().enumerate() {
        assert_eq!(val, Some(i % 2 == 0));
    }
}

#[tokio::test]
async fn test_arrow_conversion_with_missing_promoted_attributes() {
    // Test that missing attributes result in NULL values
    let promotion_config = TablePromotionConfig {
        attributes: vec![
            softprobe_runtime::config::PromotedColumn {
                attribute_key: "user.id".to_string(),
                column_name: Some("user_id".to_string()),
                data_type: Some(PromotedDataType::String),
            },
        ],
        resource_attributes: vec![],
    };

    let schema = TraceTable::schema(Some(&promotion_config));

    // Create spans - some with user.id, some without
    let mut spans = Vec::new();
    for i in 0..5 {
        let mut attributes = HashMap::new();
        // Only add user.id for even indices
        if i % 2 == 0 {
            attributes.insert("user.id".to_string(), format!("user-{}", i));
        }

        spans.push(SpanData {
            session_id: format!("session-{}", i),
            trace_id: format!("trace-{}", i),
            span_id: format!("span-{}", i),
            parent_span_id: None,
            app_id: "test-app".to_string(),
            organization_id: None,
            tenant_id: None,
            message_type: "test".to_string(),
            span_kind: None,
            timestamp: Utc::now(),
            end_timestamp: None,
            attributes,
            events: Vec::new(),
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
            status_code: None,
            status_message: None,
        });
    }

    let record_batch = spans_to_record_batch_with_promotion(&spans, &schema, Some(&promotion_config))
        .expect("should convert");

    // Find user_id column
    let user_id_idx = record_batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "user_id")
        .expect("user_id column should exist");

    // Verify NULL values for missing attributes
    let user_id_array = record_batch.column(user_id_idx);
    let user_id_strings: &arrow::array::StringArray = user_id_array
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("should be StringArray");
    for (i, val) in user_id_strings.iter().enumerate() {
        if i % 2 == 0 {
            assert_eq!(val, Some(format!("user-{}", i).as_str()));
        } else {
            assert_eq!(val, None, "Missing attribute should result in NULL");
        }
    }
}

#[tokio::test]
async fn test_table_creation_with_promotion() {
    let mut config = load_test_config();
    
    // Add schema promotion config
    config.schema_promotion = Some(SchemaPromotionConfig {
        traces: Some(TablePromotionConfig {
            attributes: vec![
                softprobe_runtime::config::PromotedColumn {
                    attribute_key: "user.id".to_string(),
                    column_name: Some("user_id".to_string()),
                    data_type: Some(PromotedDataType::String),
                },
            ],
            resource_attributes: vec![],
        }),
        logs: None,
        metrics: None,
    });

    // Drop the table first if it exists (to test schema creation with promotion)
    let catalog = softprobe_runtime::storage::iceberg::IcebergCatalog::new(&config)
        .await
        .expect("should create catalog");
    let namespace_name = config.iceberg.namespace.as_str();
    let table_name = softprobe_runtime::storage::iceberg::tables::TraceTable::table_name();
    let table_ident = iceberg::TableIdent::from_strs([namespace_name, table_name])
        .expect("table ident");
    
    // Try to drop the table if it exists (ignore errors if it doesn't exist)
    let _ = catalog.catalog().drop_table(&table_ident).await;

    // Create Iceberg writer (this will create tables with promoted columns)
    let writer = softprobe_runtime::storage::iceberg::IcebergWriter::new(&config)
        .await
        .expect("should create writer");

    // Get the schema and verify promoted column exists
    let iceberg_schema = writer.spans_schema().await.expect("should get schema");
    let arrow_schema = arrow::datatypes::Schema::try_from(iceberg_schema.as_ref()).expect("should convert");
    let fields: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    assert!(
        fields.contains(&"user_id".to_string()),
        "Promoted column user_id should exist in table schema"
    );
    assert!(
        fields.contains(&"session_id".to_string()),
        "Base field session_id should still exist"
    );
}
