use crate::models::{Log, Metric, Score, ScoreConfig, ScoreDataType, ScoreSource, Span};
use crate::storage::schema::variant::encode_attributes_json;
use anyhow::Result;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, ListArray, MapArray,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::sync::Arc;
use tracing::{debug, trace};

pub fn scores_to_record_batch(scores: &[Score], schema: &Schema) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(schema.clone());
    let metadata_field = Arc::new(
        schema
            .field_with_name("metadata")
            .map_err(|e| anyhow::anyhow!("metadata field not found in score schema: {}", e))?
            .clone(),
    );

    let score_ids: ArrayRef = Arc::new(StringArray::from(
        scores
            .iter()
            .map(|score| score.score_id.as_str())
            .collect::<Vec<_>>(),
    ));
    let timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            scores
                .iter()
                .map(|score| score.timestamp.timestamp_micros())
                .collect::<Vec<_>>(),
        )
        .with_timezone_utc(),
    );
    let trace_ids: ArrayRef = Arc::new(StringArray::from(
        scores
            .iter()
            .map(|score| score.trace_id.as_deref())
            .collect::<Vec<_>>(),
    ));
    let span_ids: ArrayRef = Arc::new(StringArray::from(
        scores
            .iter()
            .map(|score| score.span_id.as_deref())
            .collect::<Vec<_>>(),
    ));
    let session_ids: ArrayRef = Arc::new(StringArray::from(
        scores
            .iter()
            .map(|score| score.session_id.as_deref())
            .collect::<Vec<_>>(),
    ));
    let names: ArrayRef = Arc::new(StringArray::from(
        scores
            .iter()
            .map(|score| score.name.as_str())
            .collect::<Vec<_>>(),
    ));
    let data_types: ArrayRef = Arc::new(StringArray::from(
        scores
            .iter()
            .map(|score| match score.data_type {
                ScoreDataType::Numeric => "numeric",
                ScoreDataType::Categorical => "categorical",
                ScoreDataType::Boolean => "boolean",
                ScoreDataType::Text => "text",
            })
            .collect::<Vec<_>>(),
    ));
    let numeric_values: ArrayRef = Arc::new(Float64Array::from(
        scores
            .iter()
            .map(|score| score.numeric_value)
            .collect::<Vec<_>>(),
    ));
    let string_values: ArrayRef = Arc::new(StringArray::from(
        scores
            .iter()
            .map(|score| score.string_value.as_deref())
            .collect::<Vec<_>>(),
    ));
    let boolean_values: ArrayRef = Arc::new(BooleanArray::from(
        scores
            .iter()
            .map(|score| score.boolean_value)
            .collect::<Vec<_>>(),
    ));
    let sources: ArrayRef = Arc::new(StringArray::from(
        scores
            .iter()
            .map(|score| match score.source {
                ScoreSource::Api => "api",
                ScoreSource::User => "user",
                ScoreSource::Evaluator => "evaluator",
                ScoreSource::Annotation => "annotation",
            })
            .collect::<Vec<_>>(),
    ));
    let comments: ArrayRef = Arc::new(StringArray::from(
        scores
            .iter()
            .map(|score| score.comment.as_deref())
            .collect::<Vec<_>>(),
    ));
    let config_ids: ArrayRef = Arc::new(StringArray::from(
        scores
            .iter()
            .map(|score| score.config_id.as_deref())
            .collect::<Vec<_>>(),
    ));
    let author_ids: ArrayRef = Arc::new(StringArray::from(
        scores
            .iter()
            .map(|score| score.author_id.as_deref())
            .collect::<Vec<_>>(),
    ));
    let metadata = build_score_metadata_array(scores, &metadata_field)?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let record_dates: ArrayRef = Arc::new(Date32Array::from(
        scores
            .iter()
            .map(|score| (score.record_date - epoch).num_days() as i32)
            .collect::<Vec<_>>(),
    ));

    Ok(RecordBatch::try_new(
        arrow_schema,
        vec![
            score_ids,
            timestamps,
            trace_ids,
            span_ids,
            session_ids,
            names,
            data_types,
            numeric_values,
            string_values,
            boolean_values,
            sources,
            comments,
            config_ids,
            author_ids,
            metadata,
            record_dates,
        ],
    )?)
}

pub fn score_configs_to_record_batch(
    configs: &[ScoreConfig],
    schema: &Schema,
) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(schema.clone());
    let metadata_field = Arc::new(
        schema
            .field_with_name("metadata")
            .map_err(|e| anyhow::anyhow!("metadata field not found in score config schema: {}", e))?
            .clone(),
    );

    let config_ids: ArrayRef = Arc::new(StringArray::from(
        configs
            .iter()
            .map(|c| c.config_id.as_str())
            .collect::<Vec<_>>(),
    ));
    let timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            configs
                .iter()
                .map(|c| c.timestamp.timestamp_micros())
                .collect::<Vec<_>>(),
        )
        .with_timezone_utc(),
    );
    let names: ArrayRef = Arc::new(StringArray::from(
        configs.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
    ));
    let data_types: ArrayRef = Arc::new(StringArray::from(
        configs
            .iter()
            .map(|c| match c.data_type {
                ScoreDataType::Numeric => "numeric",
                ScoreDataType::Categorical => "categorical",
                ScoreDataType::Boolean => "boolean",
                ScoreDataType::Text => "text",
            })
            .collect::<Vec<_>>(),
    ));
    let descriptions: ArrayRef = Arc::new(StringArray::from(
        configs
            .iter()
            .map(|c| c.description.as_deref())
            .collect::<Vec<_>>(),
    ));
    let min_values: ArrayRef = Arc::new(Float64Array::from(
        configs.iter().map(|c| c.min_value).collect::<Vec<_>>(),
    ));
    let max_values: ArrayRef = Arc::new(Float64Array::from(
        configs.iter().map(|c| c.max_value).collect::<Vec<_>>(),
    ));
    let category_strings: Vec<Option<String>> = configs
        .iter()
        .map(|c| {
            if c.categories.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&c.categories).unwrap_or_else(|_| "[]".to_string()))
            }
        })
        .collect();
    let categories: ArrayRef = Arc::new(StringArray::from(
        category_strings
            .iter()
            .map(|value| value.as_deref())
            .collect::<Vec<_>>(),
    ));
    let author_ids: ArrayRef = Arc::new(StringArray::from(
        configs
            .iter()
            .map(|c| c.author_id.as_deref())
            .collect::<Vec<_>>(),
    ));
    let metadata =
        build_string_metadata_array(configs.iter().map(|c| &c.metadata), &metadata_field)?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let record_dates: ArrayRef = Arc::new(Date32Array::from(
        configs
            .iter()
            .map(|c| (c.record_date - epoch).num_days() as i32)
            .collect::<Vec<_>>(),
    ));

    Ok(RecordBatch::try_new(
        arrow_schema,
        vec![
            config_ids,
            timestamps,
            names,
            data_types,
            descriptions,
            min_values,
            max_values,
            categories,
            author_ids,
            metadata,
            record_dates,
        ],
    )?)
}

fn build_score_metadata_array(
    scores: &[Score],
    metadata_field: &arrow::datatypes::FieldRef,
) -> Result<ArrayRef> {
    build_string_metadata_array(scores.iter().map(|score| &score.metadata), metadata_field)
}

fn build_string_metadata_array<'a, I>(
    maps: I,
    metadata_field: &arrow::datatypes::FieldRef,
) -> Result<ArrayRef>
where
    I: IntoIterator<Item = &'a std::collections::HashMap<String, String>>,
{
    use arrow::datatypes::DataType;

    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut offsets = vec![0i32];
    let mut offset = 0i32;
    for map in maps {
        for (key, value) in map {
            keys.push(key.as_str());
            values.push(value.as_str());
            offset += 1;
        }
        offsets.push(offset);
    }

    let entries_field = match metadata_field.data_type() {
        DataType::Map(field, _) => field.clone(),
        _ => return Err(anyhow::anyhow!("Expected Map type for score metadata")),
    };
    let struct_fields = match entries_field.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => {
            return Err(anyhow::anyhow!(
                "Expected Struct type in score metadata map"
            ))
        }
    };
    let entries = StructArray::new(
        struct_fields,
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(StringArray::from(values)),
        ],
        None,
    );
    Ok(Arc::new(MapArray::try_new(
        entries_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        None,
        false,
    )?))
}

/// Base field names for traces table (used to identify promoted columns)
const TRACES_BASE_FIELDS: &[&str] = &[
    "session_id",
    "trace_id",
    "span_id",
    "parent_span_id",
    "app_id",
    "organization_id",
    "tenant_id",
    "message_type",
    "span_kind",
    "timestamp",
    "end_timestamp",
    "attributes",
    "events",
    "status_code",
    "status_message",
    "http_request_method",
    "http_request_path",
    "http_request_headers",
    "http_request_body",
    "http_response_status_code",
    "http_response_headers",
    "http_response_body",
    "record_date",
];

const LOGS_BASE_FIELDS: &[&str] = &[
    "session_id",
    "timestamp",
    "observed_timestamp",
    "severity_number",
    "severity_text",
    "body",
    "attributes",
    "resource_attributes",
    "trace_id",
    "span_id",
    "record_date",
];

const METRICS_BASE_FIELDS: &[&str] = &[
    "metric_name",
    "description",
    "unit",
    "metric_type",
    "timestamp",
    "value",
    "attributes",
    "resource_attributes",
    "record_date",
];

fn promoted_array_from_values(
    field: &arrow::datatypes::Field,
    values: Vec<Option<String>>,
) -> ArrayRef {
    match field.data_type() {
        arrow::datatypes::DataType::Utf8 => Arc::new(StringArray::from(values)),
        arrow::datatypes::DataType::Int32 => Arc::new(Int32Array::from(
            values
                .iter()
                .map(|v| v.as_ref().and_then(|s| s.parse::<i32>().ok()))
                .collect::<Vec<_>>(),
        )),
        arrow::datatypes::DataType::Int64 => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|v| v.as_ref().and_then(|s| s.parse::<i64>().ok()))
                .collect::<Vec<_>>(),
        )),
        arrow::datatypes::DataType::Float64 => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|v| v.as_ref().and_then(|s| s.parse::<f64>().ok()))
                .collect::<Vec<_>>(),
        )),
        arrow::datatypes::DataType::Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|v| v.as_ref().and_then(|s| s.parse::<bool>().ok()))
                .collect::<Vec<_>>(),
        )),
        arrow::datatypes::DataType::Timestamp(_, _) => Arc::new(
            TimestampMicrosecondArray::from(
                values
                    .iter()
                    .map(|v| {
                        v.as_ref().and_then(|s| {
                            chrono::DateTime::parse_from_rfc3339(s)
                                .ok()
                                .map(|t| t.timestamp_micros())
                        })
                    })
                    .collect::<Vec<_>>(),
            )
            .with_timezone_utc(),
        ),
        _ => Arc::new(StringArray::from(values)),
    }
}

/// Build arrays for extra promoted fields (e.g. tenant-applied promoted columns) in schema order.
/// Attribute keys default to the Arrow column name.
fn build_promoted_columns_for_spans(
    spans: &[Span],
    arrow_schema: &Schema,
) -> Result<Vec<ArrayRef>> {
    let mut promoted_arrays = Vec::new();

    for field in arrow_schema.fields() {
        let field_name = field.name();
        // Skip base fields
        if TRACES_BASE_FIELDS.contains(&field_name.as_str()) {
            continue;
        }

        let values: Vec<Option<String>> = spans
            .iter()
            .map(|span| span.attributes.get(field_name.as_str()).cloned())
            .collect();

        promoted_arrays.push(promoted_array_from_values(field, values));
    }

    Ok(promoted_arrays)
}

fn build_promoted_columns_from_attribute_maps(
    maps: &[&std::collections::HashMap<String, String>],
    arrow_schema: &Schema,
    base_fields: &[&str],
) -> Vec<ArrayRef> {
    let mut promoted_arrays = Vec::new();
    for field in arrow_schema.fields() {
        let field_name = field.name();
        if base_fields.contains(&field_name.as_str()) {
            continue;
        }
        let values = maps
            .iter()
            .map(|attrs| attrs.get(field_name.as_str()).cloned())
            .collect::<Vec<_>>();
        promoted_arrays.push(promoted_array_from_values(field, values));
    }
    promoted_arrays
}

/// Convert Span batch to Arrow RecordBatch using telemetry Arrow schema
pub fn spans_to_record_batch(spans: &[Span], schema: &Schema) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(schema.clone());

    let num_spans = spans.len();
    debug!("Converting {} spans to Arrow RecordBatch", num_spans);
    trace!("Arrow schema field count: {}", arrow_schema.fields().len());
    for (i, f) in arrow_schema.fields().iter().enumerate() {
        trace!("Field[{}]: {} : {:?}", i, f.name(), f.data_type());
    }

    // Extract field definitions from schema to preserve field IDs
    let attributes_field = Arc::new(
        arrow_schema
            .field_with_name("attributes")
            .map_err(|e| anyhow::anyhow!("attributes field not found in schema: {}", e))?
            .clone(),
    );

    let events_field = Arc::new(
        arrow_schema
            .field_with_name("events")
            .map_err(|e| anyhow::anyhow!("events field not found in schema: {}", e))?
            .clone(),
    );

    // Build arrays for each column
    // Use explicit session_id field (already populated in Span model)
    let session_ids: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.session_id.as_str())
            .collect::<Vec<_>>(),
    ));

    let trace_ids: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.trace_id.as_str())
            .collect::<Vec<_>>(),
    ));

    let span_ids: ArrayRef = Arc::new(StringArray::from(
        spans.iter().map(|s| s.span_id.as_str()).collect::<Vec<_>>(),
    ));

    let parent_span_ids: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.parent_span_id.as_deref())
            .collect::<Vec<_>>(),
    ));

    let app_ids: ArrayRef = Arc::new(StringArray::from(
        spans.iter().map(|s| s.app_id.as_str()).collect::<Vec<_>>(),
    ));

    let organization_ids: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.organization_id.as_deref())
            .collect::<Vec<_>>(),
    ));

    let tenant_ids: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.tenant_id.as_deref())
            .collect::<Vec<_>>(),
    ));

    let message_types: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.message_type.as_str())
            .collect::<Vec<_>>(),
    ));

    let span_kinds: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.span_kind.as_deref())
            .collect::<Vec<_>>(),
    ));

    // Convert timestamps to microseconds since epoch (TIMESTAMPTZ)
    let timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            spans
                .iter()
                .map(|s| s.timestamp.timestamp_micros())
                .collect::<Vec<_>>(),
        )
        .with_timezone_utc(),
    );

    let end_timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            spans
                .iter()
                .map(|s| s.end_timestamp.map(|t| t.timestamp_micros()))
                .collect::<Vec<_>>(),
        )
        .with_timezone_utc(),
    );

    // Build attributes JSON (Utf8) for DuckLake VARIANT cast on write
    let attributes_array = build_span_attributes_array(spans, &attributes_field)?;

    // Build events LIST<STRUCT> for each span
    let events_array = build_events_array(spans, &events_field)?;

    // HTTP body fields (extracted from span events by extract_http_data_from_events())
    let http_request_methods: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.http_request_method.as_deref())
            .collect::<Vec<_>>(),
    ));

    let http_request_paths: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.http_request_path.as_deref())
            .collect::<Vec<_>>(),
    ));

    let http_request_headers: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.http_request_headers.as_deref())
            .collect::<Vec<_>>(),
    ));

    let http_request_bodies: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.http_request_body.as_deref())
            .collect::<Vec<_>>(),
    ));

    let http_response_status_codes: ArrayRef = Arc::new(Int32Array::from(
        spans
            .iter()
            .map(|s| s.http_response_status_code)
            .collect::<Vec<_>>(),
    ));

    let http_response_headers: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.http_response_headers.as_deref())
            .collect::<Vec<_>>(),
    ));

    let http_response_bodies: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.http_response_body.as_deref())
            .collect::<Vec<_>>(),
    ));

    let status_codes: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.status_code.as_deref())
            .collect::<Vec<_>>(),
    ));

    let status_messages: ArrayRef = Arc::new(StringArray::from(
        spans
            .iter()
            .map(|s| s.status_message.as_deref())
            .collect::<Vec<_>>(),
    ));

    // record_date: derive from each span's timestamp for proper partition assignment
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let record_date_values: Vec<i32> = spans
        .iter()
        .map(|s| {
            let span_date = s.timestamp.date_naive();
            (span_date - epoch).num_days() as i32
        })
        .collect();

    // Verify all spans have the same record_date (required for partition compatibility)
    if let Some(first_date) = record_date_values.first() {
        if record_date_values.iter().any(|&d| d != *first_date) {
            let unique_dates: std::collections::HashSet<i32> =
                record_date_values.iter().copied().collect();
            return Err(anyhow::anyhow!(
                "All spans in a batch must have the same record_date for partition compatibility. Found {} unique dates: {:?}",
                unique_dates.len(),
                unique_dates
            ));
        }
    }

    let record_dates: ArrayRef = Arc::new(Date32Array::from(record_date_values));

    // Build promoted columns (in schema order, after base fields)
    let promoted_arrays = build_promoted_columns_for_spans(spans, &arrow_schema)?;

    // Assemble all arrays in schema order
    let mut all_arrays: Vec<ArrayRef> = vec![
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
        http_request_methods,
        http_request_paths,
        http_request_headers,
        http_request_bodies,
        http_response_status_codes,
        http_response_headers,
        http_response_bodies,
        record_dates,
    ];
    all_arrays.extend(promoted_arrays);

    let record_batch = RecordBatch::try_new(arrow_schema, all_arrays)?;

    debug!(
        "Created Arrow RecordBatch with {} rows for spans",
        record_batch.num_rows()
    );
    Ok(record_batch)
}

/// Build attributes JSON (Utf8) array for spans — staged for DuckLake VARIANT.
fn build_span_attributes_array(
    spans: &[Span],
    attributes_field: &arrow::datatypes::FieldRef,
) -> Result<ArrayRef> {
    build_variant_json_array(
        &spans.iter().map(|s| &s.attributes).collect::<Vec<_>>(),
        attributes_field,
        "attributes",
    )
}

fn build_variant_json_array(
    maps: &[&std::collections::HashMap<String, String>],
    field: &arrow::datatypes::FieldRef,
    field_name: &str,
) -> Result<ArrayRef> {
    use arrow::datatypes::DataType;

    match field.data_type() {
        DataType::Utf8 => {
            let values: Vec<String> = maps.iter().map(|m| encode_attributes_json(m)).collect();
            Ok(Arc::new(StringArray::from(
                values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )))
        }
        other => Err(anyhow::anyhow!(
            "Expected Utf8 JSON staging type for {field_name}, got {other:?}"
        )),
    }
}

/// Build events LIST<STRUCT> array for spans
fn build_events_array(
    spans: &[Span],
    events_field: &arrow::datatypes::FieldRef,
) -> Result<ArrayRef> {
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

    let event_attr_field = struct_fields
        .iter()
        .find(|f| f.name() == "attributes")
        .ok_or_else(|| anyhow::anyhow!("attributes field not found in event struct"))?;

    let event_attr_entries_field = if let DataType::Map(f, _) = event_attr_field.data_type() {
        f.clone()
    } else {
        return Err(anyhow::anyhow!(
            "Expected Map type for event attributes field"
        ));
    };

    let event_attr_struct_fields =
        if let DataType::Struct(fields) = event_attr_entries_field.data_type() {
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
    let timestamps_array: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(all_event_timestamps).with_timezone_utc());

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

    let struct_arrays: Vec<ArrayRef> =
        vec![names_array, timestamps_array, Arc::new(event_attr_map)];

    let struct_array = StructArray::new(struct_fields, struct_arrays, None);

    let list_offsets_buffer = OffsetBuffer::new(list_offsets.into());

    let list_array = ListArray::try_new(
        element_field,
        list_offsets_buffer,
        Arc::new(struct_array),
        None,
    )?;

    Ok(Arc::new(list_array))
}

/// Convert Log batch to Arrow RecordBatch using telemetry Arrow schema
pub fn logs_to_record_batch(logs: &[Log], schema: &Schema) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(schema.clone());

    debug!("Converting {} logs to Arrow RecordBatch", logs.len());
    trace!("Arrow schema field count: {}", arrow_schema.fields().len());

    // Extract field definitions from schema to preserve field IDs
    let attributes_field = Arc::new(
        arrow_schema
            .field_with_name("attributes")
            .map_err(|e| anyhow::anyhow!("attributes field not found in schema: {}", e))?
            .clone(),
    );

    let resource_attributes_field = Arc::new(
        arrow_schema
            .field_with_name("resource_attributes")
            .map_err(|e| anyhow::anyhow!("resource_attributes field not found in schema: {}", e))?
            .clone(),
    );

    // Build arrays for each column
    let session_ids: ArrayRef = Arc::new(StringArray::from(
        logs.iter()
            .map(|l| l.session_id.as_deref())
            .collect::<Vec<_>>(),
    ));

    let timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            logs.iter()
                .map(|l| l.timestamp.timestamp_micros())
                .collect::<Vec<_>>(),
        )
        .with_timezone_utc(),
    );

    let observed_timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            logs.iter()
                .map(|l| l.observed_timestamp.map(|t| t.timestamp_micros()))
                .collect::<Vec<_>>(),
        )
        .with_timezone_utc(),
    );

    let severity_numbers: ArrayRef = Arc::new(Int32Array::from(
        logs.iter().map(|l| l.severity_number).collect::<Vec<_>>(),
    ));

    let severity_texts: ArrayRef = Arc::new(StringArray::from(
        logs.iter()
            .map(|l| l.severity_text.as_str())
            .collect::<Vec<_>>(),
    ));

    let bodies: ArrayRef = Arc::new(StringArray::from(
        logs.iter().map(|l| l.body.as_str()).collect::<Vec<_>>(),
    ));

    // Build attributes JSON (Utf8) for DuckLake VARIANT cast on write
    let attributes_array = build_log_map_array(
        logs.iter()
            .map(|l| &l.attributes)
            .collect::<Vec<_>>()
            .as_slice(),
        &attributes_field,
    )?;

    // Build resource_attributes JSON (Utf8) for DuckLake VARIANT cast on write
    let resource_attributes_array = build_log_map_array(
        logs.iter()
            .map(|l| &l.resource_attributes)
            .collect::<Vec<_>>()
            .as_slice(),
        &resource_attributes_field,
    )?;

    let trace_ids: ArrayRef = Arc::new(StringArray::from(
        logs.iter()
            .map(|l| l.trace_id.as_deref())
            .collect::<Vec<_>>(),
    ));

    let span_ids: ArrayRef = Arc::new(StringArray::from(
        logs.iter()
            .map(|l| l.span_id.as_deref())
            .collect::<Vec<_>>(),
    ));

    // record_date: derive from each log's timestamp
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let record_date_values: Vec<i32> = logs
        .iter()
        .map(|l| {
            let log_date = l.timestamp.date_naive();
            (log_date - epoch).num_days() as i32
        })
        .collect();

    // Verify all logs have the same record_date
    if let Some(first_date) = record_date_values.first() {
        if record_date_values.iter().any(|&d| d != *first_date) {
            let unique_dates: std::collections::HashSet<i32> =
                record_date_values.iter().copied().collect();
            return Err(anyhow::anyhow!(
                "All logs in a batch must have the same record_date for partition compatibility. Found {} unique dates: {:?}",
                unique_dates.len(),
                unique_dates
            ));
        }
    }

    let record_dates: ArrayRef = Arc::new(Date32Array::from(record_date_values));
    let promoted_arrays = build_promoted_columns_from_attribute_maps(
        logs.iter()
            .map(|l| &l.attributes)
            .collect::<Vec<_>>()
            .as_slice(),
        &arrow_schema,
        LOGS_BASE_FIELDS,
    );

    let mut arrays = vec![
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
    ];
    arrays.extend(promoted_arrays);

    let record_batch = RecordBatch::try_new(arrow_schema, arrays)?;

    debug!(
        "Created Arrow RecordBatch with {} rows for logs",
        record_batch.num_rows()
    );
    Ok(record_batch)
}

/// Build a JSON (Utf8) array for log/metric VARIANT attribute columns.
fn build_log_map_array(
    maps: &[&std::collections::HashMap<String, String>],
    map_field: &arrow::datatypes::FieldRef,
) -> Result<ArrayRef> {
    build_variant_json_array(maps, map_field, map_field.name())
}

/// Convert Metric batch to Arrow RecordBatch using telemetry Arrow schema
pub fn metrics_to_record_batch(metrics: &[Metric], schema: &Schema) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(schema.clone());

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
                    first_date,
                    metric_date
                ));
            }
        }
    }

    // Extract field definitions from schema to preserve field IDs
    let attributes_field = Arc::new(
        arrow_schema
            .field_with_name("attributes")
            .map_err(|e| anyhow::anyhow!("attributes field not found in schema: {}", e))?
            .clone(),
    );

    let resource_attributes_field = Arc::new(
        arrow_schema
            .field_with_name("resource_attributes")
            .map_err(|e| anyhow::anyhow!("resource_attributes field not found in schema: {}", e))?
            .clone(),
    );

    // Build arrays for each column
    let metric_names: ArrayRef = Arc::new(StringArray::from(
        metrics
            .iter()
            .map(|m| m.metric_name.as_str())
            .collect::<Vec<_>>(),
    ));

    let descriptions: ArrayRef = Arc::new(StringArray::from(
        metrics
            .iter()
            .map(|m| m.description.as_str())
            .collect::<Vec<_>>(),
    ));

    let units: ArrayRef = Arc::new(StringArray::from(
        metrics.iter().map(|m| m.unit.as_str()).collect::<Vec<_>>(),
    ));

    let metric_types: ArrayRef = Arc::new(StringArray::from(
        metrics
            .iter()
            .map(|m| m.metric_type.as_str())
            .collect::<Vec<_>>(),
    ));

    // Convert timestamps to microseconds since epoch (TIMESTAMPTZ)
    let timestamps: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(
            metrics
                .iter()
                .map(|m| m.timestamp.timestamp_micros())
                .collect::<Vec<_>>(),
        )
        .with_timezone_utc(),
    );

    // Convert values to Float64Array
    use arrow::array::Float64Array;
    let values: ArrayRef = Arc::new(Float64Array::from(
        metrics.iter().map(|m| m.value).collect::<Vec<_>>(),
    ));

    // Build attributes JSON (Utf8) for DuckLake VARIANT cast on write
    let attributes_maps: Vec<&std::collections::HashMap<String, String>> =
        metrics.iter().map(|m| &m.attributes).collect();
    let attributes_array = build_metric_map_array(&attributes_maps, &attributes_field)?;

    // Build resource_attributes JSON (Utf8) for DuckLake VARIANT cast on write
    let resource_attributes_maps: Vec<&std::collections::HashMap<String, String>> =
        metrics.iter().map(|m| &m.resource_attributes).collect();
    let resource_attributes_array =
        build_metric_map_array(&resource_attributes_maps, &resource_attributes_field)?;

    // Convert dates to days since epoch (Date32)
    let record_dates: ArrayRef = Arc::new(Date32Array::from(
        metrics
            .iter()
            .map(|m| {
                let date = m.timestamp.date_naive();
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                date.signed_duration_since(epoch).num_days() as i32
            })
            .collect::<Vec<_>>(),
    ));
    let promoted_arrays = build_promoted_columns_from_attribute_maps(
        metrics
            .iter()
            .map(|m| &m.attributes)
            .collect::<Vec<_>>()
            .as_slice(),
        &arrow_schema,
        METRICS_BASE_FIELDS,
    );

    // Create RecordBatch with columns matching schema field order
    let mut arrays = vec![
        metric_names,
        descriptions,
        units,
        metric_types,
        timestamps,
        values,
        attributes_array,
        resource_attributes_array,
        record_dates,
    ];
    arrays.extend(promoted_arrays);
    let record_batch = RecordBatch::try_new(arrow_schema.clone(), arrays)?;

    Ok(record_batch)
}

/// Build a JSON (Utf8) array for metric VARIANT attribute columns.
fn build_metric_map_array(
    maps: &[&std::collections::HashMap<String, String>],
    map_field: &arrow::datatypes::FieldRef,
) -> Result<ArrayRef> {
    build_variant_json_array(maps, map_field, map_field.name())
}
