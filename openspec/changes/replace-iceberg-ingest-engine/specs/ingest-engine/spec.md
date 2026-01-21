## ADDED Requirements
### Requirement: Local WAL
The system SHALL write all incoming telemetry mutations to a local WAL and treat them as durable only on the local node until an Iceberg commit succeeds.

#### Scenario: Local WAL durability
- **WHEN** a write request is accepted
- **THEN** the WAL entry is persisted locally before acknowledging durability

#### Scenario: Commit uploads
- **WHEN** an Iceberg commit completes
- **THEN** the committed Parquet data is uploaded to object storage as part of the commit path

### Requirement: Buffered Ingestion
The system SHALL buffer incoming telemetry in an in-memory or NVMe-backed buffer prior to Iceberg commits.

#### Scenario: Buffering before commit
- **WHEN** telemetry arrives
- **THEN** records are appended to the buffer and are queryable via union-read

### Requirement: Schema-Consistent WAL Segments
The system SHALL write WAL segments using Arrow or Parquet with the same schema as the Iceberg tables to avoid JSON schema duplication.

#### Scenario: WAL matches Iceberg schema
- **WHEN** WAL segments are persisted
- **THEN** their column schema matches the Iceberg table schema for that telemetry type

### Requirement: Session-Grouped Row Groups
The system SHALL preserve session-based row-grouping when flushing traces/logs to Parquet.

#### Scenario: Row group clustering
- **WHEN** traces or logs are flushed to Parquet
- **THEN** row groups are clustered by session id to preserve query performance

### Requirement: Flush Policy
The system SHALL flush buffered data to Parquet based on size and time thresholds.

#### Scenario: Flush on size or age
- **WHEN** the buffer exceeds the configured size or age threshold
- **THEN** the system flushes buffered data into Parquet files for later commit

### Requirement: Append-Only OTLP Ingestion
The system SHALL treat OpenTelemetry telemetry ingestion as append-only by default.

#### Scenario: Append-only writes
- **WHEN** OTLP traces/logs/metrics are ingested
- **THEN** the system appends records without requiring update/delete semantics
