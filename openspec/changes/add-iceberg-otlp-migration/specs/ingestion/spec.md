## ADDED Requirements

### Requirement: OTLP Traces Ingestion Endpoint
The system SHALL expose an OTLP HTTP endpoint `/v1/traces` accepting protobuf and JSON payloads per OpenTelemetry Protocol specification.

#### Scenario: Accept OTLP traces over HTTP
- **WHEN** a client sends OTLP traces to `/v1/traces`
- **THEN** the service responds 200 on valid payloads and enqueues spans for processing

#### Scenario: Accept OTLP traces with compression
- **WHEN** a client sends gzip/deflate/brotli/zstd compressed OTLP traces
- **THEN** the service decompresses and processes the payload successfully

### Requirement: OTLP Logs Ingestion Endpoint
The system SHALL expose an OTLP HTTP endpoint `/v1/logs` accepting protobuf and JSON payloads per OpenTelemetry Protocol specification.

#### Scenario: Accept OTLP logs over HTTP
- **WHEN** a client sends OTLP log records to `/v1/logs`
- **THEN** the service responds 200 on valid payloads and enqueues log records for processing

#### Scenario: Accept OTLP logs with various severity levels
- **WHEN** a client sends log records with different SeverityNumber values
- **THEN** the service preserves severity information in storage

### Requirement: OTLP Metrics Ingestion Endpoint
The system SHALL expose an OTLP HTTP endpoint `/v1/metrics` accepting protobuf and JSON payloads per OpenTelemetry Protocol specification.

#### Scenario: Accept OTLP metrics over HTTP
- **WHEN** a client sends OTLP metrics to `/v1/metrics`
- **THEN** the service responds 200 on valid payloads and enqueues metric data points for processing

#### Scenario: Accept different metric types
- **WHEN** a client sends Gauge, Sum, Histogram, or ExponentialHistogram metrics
- **THEN** the service stores each metric type with its specific data structure preserved

### Requirement: Unified Session Buffering for Traces and Logs
The system SHALL buffer traces and logs together by `session_id`, flushing as coordinated session-scoped batches to both tables.

#### Scenario: Group traces and logs by session
- **WHEN** spans and log records arrive with the same `session_id` attribute
- **THEN** they are buffered together and flushed simultaneously to `traces` and `logs` tables

#### Scenario: Coordinated flush for session
- **WHEN** a session buffer is flushed
- **THEN** traces are written to `traces` and logs to `logs` in the same commit operation

### Requirement: Separate Metrics Buffering
The system SHALL buffer metrics separately from traces/logs since metrics are aggregations, NOT session-correlated.

#### Scenario: Metrics buffered independently
- **WHEN** metric data points arrive
- **THEN** they are buffered separately and flushed independently of session boundaries

#### Scenario: Metrics NOT correlated by session
- **WHEN** processing metrics
- **THEN** session_id is NOT used for buffering or grouping - metrics are organized by metric_name and timestamp

### Requirement: Session ID Extraction
The system SHALL extract session_id from OTLP resource attributes or span/log/metric attributes using a configurable attribute name (default: "session.id").

#### Scenario: Extract session_id from resource attributes
- **WHEN** OTLP data arrives with `resource.attributes["session.id"]`
- **THEN** the session_id is extracted and used for buffering and storage

