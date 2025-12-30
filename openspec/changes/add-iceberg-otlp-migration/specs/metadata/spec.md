## ADDED Requirements

### Requirement: Real-Time Metadata ETL
The system SHALL run a near-real-time ETL that reads new Iceberg Parquet files and extracts business metadata only.

#### Scenario: Extract metadata on new data
- **WHEN** a new Iceberg snapshot with Parquet files is available
- **THEN** the ETL reads the files via catalog and emits per-span business metadata without payload bodies or file pointers

### Requirement: ClickHouse Metadata Tables
The system SHALL populate `session_metadata`, `recording_events`, `recording_ops_minute`, and `recording_tag_counts` tables for fast metadata queries.

#### Scenario: Populate recording_events with business metadata only
- **WHEN** metadata is extracted for a span
- **THEN** a row is written containing ONLY business fields (trace_id, session_id, app_id, timestamp, status_code) and NO Iceberg internal paths

### Requirement: Session Metadata in ClickHouse
The system SHALL maintain a ClickHouse `session_metadata` table with session-level summaries.

#### Scenario: Session summary recorded
- **WHEN** a session is processed
- **THEN** session_metadata records session_id, app_id, start/end timestamps, message_count, and size_bytes (NO file pointers)

### Requirement: No Storage of Iceberg Internal Paths
The system SHALL NOT store Iceberg file paths, row group indices, or row indices in ClickHouse.

#### Scenario: Query uses Iceberg scan API
- **WHEN** full span data is needed
- **THEN** the query client uses Iceberg scan API with predicates (session_id, trace_id, timestamp) to retrieve data, letting Iceberg manage file discovery

