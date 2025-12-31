use crate::models::{Span, Log, Metric};
use anyhow::Result;
use arrow::array::{
    ArrayRef, StringArray, Int32Array, TimestampMicrosecondArray, Date32Array,
    MapArray, ListArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::{debug, trace, info};
use chrono::NaiveDate;
use iceberg::spec::Schema as IcebergSchema;

/// Convert Span batch to Arrow RecordBatch using Iceberg table schema
pub fn spans_to_record_batch(spans: &[Span], iceberg_schema: &IcebergSchema) -> Result<RecordBatch> {
    // Convert Iceberg schema to Arrow schema
    let arrow_schema = Arc::new(Schema::try_from(iceberg_schema)?);

    let num_spans = spans.len();
    debug!("Converting {} spans to Arrow RecordBatch", num_spans);
    trace!("Arrow schema field count: {}", arrow_schema.fields().len());
    for (i, f) in arrow_schema.fields().iter().enumerate() {
        trace!("Field[{}]: {} : {:?}", i, f.name(), f.data_type());
    }

    // Extract field definitions from schema to preserve field IDs
    let attributes_field = Arc::new(arrow_schema.field_with_name("attributes")
        .map_err(|e| anyhow::anyhow!("attributes field not found in schema: {}", e))?.clone());

    let events_field = Arc::new(arrow_schema.field_with_name("events")
        .map_err(|e| anyhow::anyhow!("events field not found in schema: {}", e))?.clone());

    // Build arrays for each column
    let session_ids: ArrayRef = Arc::new(StringArray::from(
        spans.iter()
            .map(|s| s.attributes.get("sp.session.id")
                .map(|id| id.as_str())
                .unwrap_or(s.trace_id.as_str()))
            .collect::<Vec<_>>()
    ));

    let trace_ids: ArrayRef = Arc::new(StringArray::from(
        spans.iter().map(|s| s.trace_id.as_str()).collect::<Vec<_>>()
    ));

    let span_ids: ArrayRef = Arc::new(StringArray::from(
        spans.iter().map(|s| s.span_id.as_str()).collect::<Vec<_>>()
    ));

    let parent_span_ids: ArrayRef = Arc::new(StringArray::from(
        spans.iter()
            .map(|s| s.parent_span_id.as_deref())
            .collect::<Vec<_>>()
    ));

    let app_ids: ArrayRef = Arc::new(StringArray::from(
        spans.iter().map(|s| s.app_id.as_str()).collect::<Vec<_>>()
    ));

    let organization_ids: ArrayRef = Arc::new(StringArray::from(
        spans.iter()
            .map(|s| s.organization_id.as_deref())
            .collect::<Vec<_>>()
    ));

    let tenant_ids: ArrayRef = Arc::new(StringArray::from(
        spans.iter()
            .map(|s| s.tenant_id.as_deref())
            .collect::<Vec<_>>()
    ));

    let message_types: ArrayRef = Arc::new(StringArray::from(
        spans.iter().map(|s| s.message_type.as_str()).collect::<Vec<_>>()
    ));

    let span_kinds: ArrayRef = Arc::new(StringArray::from(
        spans.iter()
            .map(|s| s.span_kind.as_deref())
            .collect::<Vec<_>>()
    ));

    // Convert timestamps to microseconds since epoch (TIMESTAMPTZ)
    let timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            spans.iter()
                .map(|s| s.timestamp.timestamp_micros())
                .collect::<Vec<_>>()
        ).with_timezone_utc()
    );

    let end_timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            spans.iter()
                .map(|s| s.end_timestamp.map(|t| t.timestamp_micros()))
                .collect::<Vec<_>>()
        ).with_timezone_utc()
    );

    // Build attributes MAP<STRING, STRING> for each span
    let attributes_array = build_span_attributes_array(spans, &attributes_field)?;

    // Build events LIST<STRUCT> for each span
    let events_array = build_events_array(spans, &events_field)?;

    let status_codes: ArrayRef = Arc::new(StringArray::from(
        spans.iter()
            .map(|s| s.status_code.as_deref())
            .collect::<Vec<_>>()
    ));

    let status_messages: ArrayRef = Arc::new(StringArray::from(
        spans.iter()
            .map(|s| s.status_message.as_deref())
            .collect::<Vec<_>>()
    ));

    // record_date: derive from each span's timestamp for proper partition assignment
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let record_date_values: Vec<i32> = spans.iter()
        .map(|s| {
            let span_date = s.timestamp.date_naive();
            (span_date - epoch).num_days() as i32
        })
        .collect();

    // Verify all spans have the same record_date (required for partition compatibility)
    if let Some(first_date) = record_date_values.first() {
        if record_date_values.iter().any(|&d| d != *first_date) {
            let unique_dates: std::collections::HashSet<i32> = record_date_values.iter().copied().collect();
            return Err(anyhow::anyhow!(
                "All spans in a batch must have the same record_date for partition compatibility. Found {} unique dates: {:?}",
                unique_dates.len(),
                unique_dates
            ));
        }
    }

    let record_dates: ArrayRef = Arc::new(Date32Array::from(record_date_values));

    let record_batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            session_ids,
            trace_ids,
            span_ids,
            parent_span_ids,
            app_ids,
            organization_ids,
            tenant_ids,
            message_types,
            span_kinds,
            timestamps,
            end_timestamps,
            attributes_array,
            events_array,
            status_codes,
            status_messages,
            record_dates,
        ],
    )?;

    debug!("Created Arrow RecordBatch with {} rows for spans", record_batch.num_rows());
    Ok(record_batch)
}

/// Build attributes MAP<STRING, STRING> array for spans
fn build_span_attributes_array(spans: &[Span], attributes_field: &arrow::datatypes::FieldRef) -> Result<ArrayRef> {
    use arrow::datatypes::DataType;

    let mut all_keys = Vec::new();
    let mut all_values = Vec::new();
    let mut offsets = vec![0i32];
    let mut current_offset = 0i32;

    for span in spans {
        for (key, value) in &span.attributes {
            all_keys.push(key.as_str());
            all_values.push(value.as_str());
            current_offset += 1;
        }
        offsets.push(current_offset);
    }

    let keys_array: ArrayRef = Arc::new(StringArray::from(all_keys));
    let values_array: ArrayRef = Arc::new(StringArray::from(all_values));

    let map_data_type = attributes_field.data_type();
    let (entries_field, _) = if let DataType::Map(f, _) = map_data_type {
        (f.clone(), true)
    } else {
        return Err(anyhow::anyhow!("Expected Map type for attributes field"));
    };

    let struct_fields = if let DataType::Struct(fields) = entries_field.data_type() {
        fields.clone()
    } else {
        return Err(anyhow::anyhow!("Expected Struct type in Map entries"));
    };

    let entries_struct = StructArray::new(
        struct_fields,
        vec![keys_array, values_array],
        None,
    );

    let offsets_buffer = OffsetBuffer::new(offsets.into());

    let map_array = MapArray::try_new(
        entries_field,
        offsets_buffer,
        entries_struct,
        None,
        false,
    )?;

    Ok(Arc::new(map_array))
}

/// Build events LIST<STRUCT> array for spans
fn build_events_array(spans: &[Span], events_field: &arrow::datatypes::FieldRef) -> Result<ArrayRef> {
    use arrow::datatypes::DataType;

    let element_field = if let DataType::List(f) = events_field.data_type() {
        f.clone()
    } else {
        return Err(anyhow::anyhow!("Expected List type for events field"));
    };

    let struct_fields = if let DataType::Struct(fields) = element_field.data_type() {
        fields.clone()
    } else {
        return Err(anyhow::anyhow!("Expected Struct type in List element"));
    };

    let event_attr_field = struct_fields.iter()
        .find(|f| f.name() == "attributes")
        .ok_or_else(|| anyhow::anyhow!("attributes field not found in event struct"))?;

    let event_attr_entries_field = if let DataType::Map(f, _) = event_attr_field.data_type() {
        f.clone()
    } else {
        return Err(anyhow::anyhow!("Expected Map type for event attributes field"));
    };

    let event_attr_struct_fields = if let DataType::Struct(fields) = event_attr_entries_field.data_type() {
        fields.clone()
    } else {
        return Err(anyhow::anyhow!("Expected Struct type in Map entries"));
    };

    let mut all_event_names = Vec::new();
    let mut all_event_timestamps = Vec::new();
    let mut all_event_attr_keys = Vec::new();
    let mut all_event_attr_values = Vec::new();
    let mut event_attr_offsets = vec![0i32];
    let mut list_offsets = vec![0i32];

    let mut current_event_offset = 0i32;
    let mut current_attr_offset = 0i32;

    for span in spans {
        for event in &span.events {
            all_event_names.push(event.name.as_str());
            all_event_timestamps.push(event.timestamp.timestamp_micros());

            for (key, value) in &event.attributes {
                all_event_attr_keys.push(key.as_str());
                all_event_attr_values.push(value.as_str());
                current_attr_offset += 1;
            }
            event_attr_offsets.push(current_attr_offset);
            current_event_offset += 1;
        }
        list_offsets.push(current_event_offset);
    }

    let names_array: ArrayRef = Arc::new(StringArray::from(all_event_names));
    let timestamps_array: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(all_event_timestamps).with_timezone_utc()
    );

    let event_attr_keys_array: ArrayRef = Arc::new(StringArray::from(all_event_attr_keys));
    let event_attr_values_array: ArrayRef = Arc::new(StringArray::from(all_event_attr_values));

    let event_attr_entries = StructArray::new(
        event_attr_struct_fields,
        vec![event_attr_keys_array, event_attr_values_array],
        None,
    );

    let event_attr_offsets_buffer = OffsetBuffer::new(event_attr_offsets.into());

    let event_attr_map = MapArray::try_new(
        event_attr_entries_field,
        event_attr_offsets_buffer,
        event_attr_entries,
        None,
        false,
    )?;

    let struct_arrays: Vec<ArrayRef> = vec![
        names_array,
        timestamps_array,
        Arc::new(event_attr_map),
    ];

    let struct_array = StructArray::new(
        struct_fields,
        struct_arrays,
        None,
    );

    let list_offsets_buffer = OffsetBuffer::new(list_offsets.into());

    let list_array = ListArray::try_new(
        element_field,
        list_offsets_buffer,
        Arc::new(struct_array),
        None,
    )?;

    Ok(Arc::new(list_array))
}

/// Convert Log batch to Arrow RecordBatch using Iceberg table schema
pub fn logs_to_record_batch(logs: &[Log], iceberg_schema: &IcebergSchema) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(Schema::try_from(iceberg_schema)?);

    debug!("Converting {} logs to Arrow RecordBatch", logs.len());
    trace!("Arrow schema field count: {}", arrow_schema.fields().len());

    // Extract field definitions from schema to preserve field IDs
    let attributes_field = Arc::new(arrow_schema.field_with_name("attributes")
        .map_err(|e| anyhow::anyhow!("attributes field not found in schema: {}", e))?.clone());

    let resource_attributes_field = Arc::new(arrow_schema.field_with_name("resource_attributes")
        .map_err(|e| anyhow::anyhow!("resource_attributes field not found in schema: {}", e))?.clone());

    // Build arrays for each column
    let session_ids: ArrayRef = Arc::new(StringArray::from(
        logs.iter()
            .map(|l| l.session_id.as_deref())
            .collect::<Vec<_>>()
    ));

    let timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            logs.iter()
                .map(|l| l.timestamp.timestamp_micros())
                .collect::<Vec<_>>()
        ).with_timezone_utc()
    );

    let observed_timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            logs.iter()
                .map(|l| l.observed_timestamp.map(|t| t.timestamp_micros()))
                .collect::<Vec<_>>()
        ).with_timezone_utc()
    );

    let severity_numbers: ArrayRef = Arc::new(Int32Array::from(
        logs.iter().map(|l| l.severity_number).collect::<Vec<_>>()
    ));

    let severity_texts: ArrayRef = Arc::new(StringArray::from(
        logs.iter().map(|l| l.severity_text.as_str()).collect::<Vec<_>>()
    ));

    let bodies: ArrayRef = Arc::new(StringArray::from(
        logs.iter().map(|l| l.body.as_str()).collect::<Vec<_>>()
    ));

    // Build attributes MAP<STRING, STRING>
    let attributes_array = build_log_map_array(
        logs.iter().map(|l| &l.attributes).collect::<Vec<_>>().as_slice(),
        &attributes_field
    )?;

    // Build resource_attributes MAP<STRING, STRING>
    let resource_attributes_array = build_log_map_array(
        logs.iter().map(|l| &l.resource_attributes).collect::<Vec<_>>().as_slice(),
        &resource_attributes_field
    )?;

    let trace_ids: ArrayRef = Arc::new(StringArray::from(
        logs.iter()
            .map(|l| l.trace_id.as_deref())
            .collect::<Vec<_>>()
    ));

    let span_ids: ArrayRef = Arc::new(StringArray::from(
        logs.iter()
            .map(|l| l.span_id.as_deref())
            .collect::<Vec<_>>()
    ));

    // record_date: derive from each log's timestamp
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let record_date_values: Vec<i32> = logs.iter()
        .map(|l| {
            let log_date = l.timestamp.date_naive();
            (log_date - epoch).num_days() as i32
        })
        .collect();

    // Verify all logs have the same record_date
    if let Some(first_date) = record_date_values.first() {
        if record_date_values.iter().any(|&d| d != *first_date) {
            let unique_dates: std::collections::HashSet<i32> = record_date_values.iter().copied().collect();
            return Err(anyhow::anyhow!(
                "All logs in a batch must have the same record_date for partition compatibility. Found {} unique dates: {:?}",
                unique_dates.len(),
                unique_dates
            ));
        }
    }

    let record_dates: ArrayRef = Arc::new(Date32Array::from(record_date_values));

    let record_batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            session_ids,
            timestamps,
            observed_timestamps,
            severity_numbers,
            severity_texts,
            bodies,
            attributes_array,
            resource_attributes_array,
            trace_ids,
            span_ids,
            record_dates,
        ],
    )?;

    debug!("Created Arrow RecordBatch with {} rows for logs", record_batch.num_rows());
    Ok(record_batch)
}

/// Build a MAP<STRING, STRING> array for log attributes
fn build_log_map_array(
    maps: &[&std::collections::HashMap<String, String>],
    map_field: &arrow::datatypes::FieldRef
) -> Result<ArrayRef> {
    use arrow::datatypes::DataType;

    let mut all_keys = Vec::new();
    let mut all_values = Vec::new();
    let mut offsets = vec![0i32];
    let mut current_offset = 0i32;

    for map in maps {
        for (key, value) in map.iter() {
            all_keys.push(key.as_str());
            all_values.push(value.as_str());
            current_offset += 1;
        }
        offsets.push(current_offset);
    }

    let keys_array: ArrayRef = Arc::new(StringArray::from(all_keys));
    let values_array: ArrayRef = Arc::new(StringArray::from(all_values));

    let map_data_type = map_field.data_type();
    let (entries_field, _) = if let DataType::Map(f, _) = map_data_type {
        (f.clone(), true)
    } else {
        return Err(anyhow::anyhow!("Expected Map type for map field"));
    };

    let struct_fields = if let DataType::Struct(fields) = entries_field.data_type() {
        fields.clone()
    } else {
        return Err(anyhow::anyhow!("Expected Struct type in Map entries"));
    };

    let entries_struct = StructArray::new(
        struct_fields,
        vec![keys_array, values_array],
        None,
    );

    let offsets_buffer = OffsetBuffer::new(offsets.into());

    let map_array = MapArray::try_new(
        entries_field,
        offsets_buffer,
        entries_struct,
        None,
        false,
    )?;

    Ok(Arc::new(map_array))
}

/// Convert Metric batch to Arrow RecordBatch using Iceberg table schema
pub fn metrics_to_record_batch(metrics: &[Metric], iceberg_schema: &IcebergSchema) -> Result<RecordBatch> {
    // Convert Iceberg schema to Arrow schema
    let arrow_schema = Arc::new(Schema::try_from(iceberg_schema)?);

    let num_metrics = metrics.len();
    debug!("Converting {} metrics to Arrow RecordBatch", num_metrics);

    // Validate all metrics have the same partition key (date)
    if num_metrics > 0 {
        let first_date = metrics[0].timestamp.date_naive();
        for metric in metrics.iter().skip(1) {
            let metric_date = metric.timestamp.date_naive();
            if metric_date != first_date {
                return Err(anyhow::anyhow!(
                    "All metrics in batch must have same record_date. Found {} and {}",
                    first_date, metric_date
                ));
            }
        }
    }

    // Extract field definitions from schema to preserve field IDs
    let attributes_field = Arc::new(arrow_schema.field_with_name("attributes")
        .map_err(|e| anyhow::anyhow!("attributes field not found in schema: {}", e))?.clone());

    let resource_attributes_field = Arc::new(arrow_schema.field_with_name("resource_attributes")
        .map_err(|e| anyhow::anyhow!("resource_attributes field not found in schema: {}", e))?.clone());

    // Build arrays for each column
    let metric_names: ArrayRef = Arc::new(StringArray::from(
        metrics.iter().map(|m| m.metric_name.as_str()).collect::<Vec<_>>()
    ));

    let descriptions: ArrayRef = Arc::new(StringArray::from(
        metrics.iter().map(|m| m.description.as_str()).collect::<Vec<_>>()
    ));

    let units: ArrayRef = Arc::new(StringArray::from(
        metrics.iter().map(|m| m.unit.as_str()).collect::<Vec<_>>()
    ));

    let metric_types: ArrayRef = Arc::new(StringArray::from(
        metrics.iter().map(|m| m.metric_type.as_str()).collect::<Vec<_>>()
    ));

    // Convert timestamps to microseconds since epoch (TIMESTAMPTZ)
    let timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            metrics.iter()
                .map(|m| m.timestamp.timestamp_micros())
                .collect::<Vec<_>>()
        ).with_timezone_utc()
    );

    // Convert values to Float64Array
    use arrow::array::Float64Array;
    let values: ArrayRef = Arc::new(Float64Array::from(
        metrics.iter().map(|m| m.value).collect::<Vec<_>>()
    ));

    // Build attributes MAP<STRING, STRING>
    let attributes_maps: Vec<&std::collections::HashMap<String, String>> =
        metrics.iter().map(|m| &m.attributes).collect();
    let attributes_array = build_metric_map_array(&attributes_maps, &attributes_field)?;

    // Build resource_attributes MAP<STRING, STRING>
    let resource_attributes_maps: Vec<&std::collections::HashMap<String, String>> =
        metrics.iter().map(|m| &m.resource_attributes).collect();
    let resource_attributes_array = build_metric_map_array(&resource_attributes_maps, &resource_attributes_field)?;

    // Convert dates to days since epoch (Date32)
    let record_dates: ArrayRef = Arc::new(Date32Array::from(
        metrics.iter()
            .map(|m| {
                let date = m.timestamp.date_naive();
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                date.signed_duration_since(epoch).num_days() as i32
            })
            .collect::<Vec<_>>()
    ));

    // Create RecordBatch with columns matching Iceberg schema order
    let record_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            metric_names,
            descriptions,
            units,
            metric_types,
            timestamps,
            values,
            attributes_array,
            resource_attributes_array,
            record_dates,
        ],
    )?;

    Ok(record_batch)
}

/// Build a MAP<STRING, STRING> array for metric attributes
fn build_metric_map_array(
    maps: &[&std::collections::HashMap<String, String>],
    map_field: &arrow::datatypes::FieldRef
) -> Result<ArrayRef> {
    use arrow::datatypes::DataType;

    let mut all_keys = Vec::new();
    let mut all_values = Vec::new();
    let mut offsets = vec![0i32];
    let mut current_offset = 0i32;

    for map in maps {
        for (key, value) in map.iter() {
            all_keys.push(key.as_str());
            all_values.push(value.as_str());
            current_offset += 1;
        }
        offsets.push(current_offset);
    }

    let keys_array: ArrayRef = Arc::new(StringArray::from(all_keys));
    let values_array: ArrayRef = Arc::new(StringArray::from(all_values));

    let map_data_type = map_field.data_type();
    let (entries_field, _) = if let DataType::Map(f, _) = map_data_type {
        (f.clone(), true)
    } else {
        return Err(anyhow::anyhow!("Expected Map type for map field"));
    };

    let struct_fields = if let DataType::Struct(fields) = entries_field.data_type() {
        fields.clone()
    } else {
        return Err(anyhow::anyhow!("Expected Struct type in Map entries"));
    };

    let entries_struct = StructArray::new(
        struct_fields,
        vec![keys_array, values_array],
        None,
    );

    let offsets_buffer = OffsetBuffer::new(offsets.into());

    let map_array = MapArray::try_new(
        entries_field,
        offsets_buffer,
        entries_struct,
        None,
        false,
    )?;

    Ok(Arc::new(map_array))
}
