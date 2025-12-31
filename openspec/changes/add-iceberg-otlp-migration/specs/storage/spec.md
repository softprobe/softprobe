## ADDED Requirements

### Requirement: Iceberg Tables for Each Signal Type
The system SHALL persist telemetry data to separate Iceberg tables: `otlp_traces`, `otlp_logs`, `otlp_metrics`, all in Parquet format with day-based partitioning.

#### Scenario: Write traces to dedicated table
- **WHEN** buffered spans are flushed
- **THEN** they are written to the `otlp_traces` table with partition by date and sort by session_id, trace_id, timestamp

#### Scenario: Write logs to dedicated table
- **WHEN** buffered log records are flushed
- **THEN** they are written to the `otlp_logs` table with partition by date and sort by session_id, timestamp

#### Scenario: Write metrics to dedicated table
- **WHEN** buffered metrics are flushed
- **THEN** they are written to the `otlp_metrics` table with partition by date and metric_name, sorted by metric_name, timestamp (NOT session-based)

### Requirement: Session-Based Row Grouping for Traces and Logs
The system SHALL write one row group per session_id for traces and logs; metrics use different organization since they are NOT session-correlated.

#### Scenario: Row group per session for traces and logs
- **WHEN** traces and logs for the same session_id are flushed
- **THEN** they form dedicated row groups in their respective tables with aligned session boundaries

#### Scenario: Multiple sessions in one Parquet file
- **WHEN** multiple sessions from different applications are flushed together
- **THEN** they are written to the same Parquet file as separate row groups

#### Scenario: Metrics organized by metric name, not session
- **WHEN** metrics are flushed
- **THEN** they are organized and grouped by metric_name and time period, NOT by session_id

### Requirement: Flush Triggers
The system SHALL flush on size (~128MB per file), age (~60s), or session end/timeout (configurable, default 30m).

#### Scenario: Size-based flush
- **WHEN** current buffer size approaches 128MB
- **THEN** the writer finalizes and commits the file to Iceberg

#### Scenario: Time-based flush
- **WHEN** buffered data age exceeds 60 seconds
- **THEN** the writer flushes regardless of buffer size

#### Scenario: Session timeout flush
- **WHEN** a session has no new telemetry for 30 minutes (default)
- **THEN** the session buffer is flushed and closed

### Requirement: Parquet Schema Preservation
The system SHALL preserve complete OTLP structure in Parquet schema including all resource attributes, span/log/metric attributes, and nested structures.

#### Scenario: Store complete span structure
- **WHEN** writing OTLP spans to Parquet
- **THEN** the schema includes trace_id, span_id, parent_span_id, name, kind, timestamps, status, events, links, and all attributes

#### Scenario: Store complete log structure
- **WHEN** writing OTLP log records to Parquet
- **THEN** the schema includes timestamp, observed_timestamp, severity_number, severity_text, body, and all attributes

#### Scenario: Store complete metric structure
- **WHEN** writing OTLP metrics to Parquet
- **THEN** the schema includes metric name, description, unit, and type-specific data (gauge/sum/histogram with all fields)

### Requirement: Coordinated Writes for Traces and Logs
The system SHALL coordinate writes to traces and logs tables to maintain session alignment.

#### Scenario: Same sessions in both tables
- **WHEN** flushing a session with both traces and logs
- **THEN** the same session appears in both `otlp_traces` and `otlp_logs` with aligned row groups

#### Scenario: Atomic session commit
- **WHEN** writing session data
- **THEN** commits to both tables complete successfully or both roll back to maintain consistency

### Requirement: Experimental Metrics Storage
The system SHALL note that Iceberg storage for metrics is experimental - time-series aggregations may require alternative storage.

#### Scenario: Metrics storage caveat
- **WHEN** storing metrics in Iceberg
- **THEN** acknowledge this is experimental and may be replaced with specialized time-series storage (e.g., Prometheus, InfluxDB)

