## ADDED Requirements

### Requirement: Iceberg Raw Sessions Table
The system SHALL persist raw spans to an Iceberg table `raw_sessions` in Parquet format, partitioned by record_date only (no app-based bucketing), with sort order by session_id, trace_id, timestamp to maximize co-location for session reads while keeping each trace contiguous within a session.

#### Scenario: Write Parquet with configured partitions
- **WHEN** buffered spans are flushed
- **THEN** they are written to Parquet files under Iceberg with the defined partitions and sort order

### Requirement: Multi-App Session Row Grouping
The system SHALL write one row group per session_id; all traces within the same session are co-located. Multiple sessions (from different apps) may share the same Parquet file.

#### Scenario: Row group per session
- **WHEN** spans for the same session_id are flushed within a window
- **THEN** they form a dedicated row group containing all traces for that session, and multiple sessions from different apps share a single Parquet file

### Requirement: Flush Triggers
The system SHALL flush on size (~128MB), age (~60s), or session end/timeout (default 30m).

#### Scenario: Size-based flush
- **WHEN** current file size approaches 128MB
- **THEN** the writer finalizes and commits the file to Iceberg

