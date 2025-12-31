## ADDED Requirements

### Requirement: Query API Endpoints
The system SHALL expose HTTP query endpoints for retrieving telemetry data by session, time range, and attributes.

#### Scenario: Query session by ID
- **WHEN** a client sends GET `/v1/query/session/{session_id}`
- **THEN** the system returns all traces, logs, and metrics for that session from Iceberg

#### Scenario: Query by time range and filters
- **WHEN** a client sends POST `/query` with time range and attribute filters
- **THEN** the system performs direct Iceberg scan with predicates to retrieve matching telemetry

### Requirement: Session-Based Retrieval
The system SHALL support retrieving complete sessions including all signal types (traces, logs, metrics) in a single query.

#### Scenario: Retrieve complete session
- **WHEN** a client queries by `session_id`
- **THEN** the system returns traces from `traces`, logs from `logs`, and metrics from `metrics` for that session

#### Scenario: Session existence check
- **WHEN** a client queries by `session_id`
- **THEN** the system uses Iceberg predicate scan to verify session exists before full retrieval

### Requirement: Query Optimization via Iceberg Metadata
The system SHALL leverage Iceberg's built-in metadata for efficient query execution.

#### Scenario: Partition elimination
- **WHEN** querying with time range filters
- **THEN** Iceberg eliminates entire date partitions outside the range

#### Scenario: Manifest-level pruning
- **WHEN** querying Iceberg with session_id or attribute predicates
- **THEN** Iceberg uses manifest files to eliminate data files that don't match predicates

#### Scenario: Row group statistics
- **WHEN** scanning Parquet files
- **THEN** row group min/max statistics filter out irrelevant row groups before reading data

### Requirement: Attribute-Based Filtering
The system SHALL support filtering by resource attributes and span/log/metric attributes using Iceberg predicate pushdown.

#### Scenario: Filter by application ID
- **WHEN** a client filters by `resource.attributes["service.name"]`
- **THEN** the system uses Iceberg predicates to scan only matching data

#### Scenario: Filter by custom attributes
- **WHEN** a client filters by custom span/log/metric attributes
- **THEN** the system applies predicates at Iceberg scan level with Parquet column filtering

### Requirement: Time-Range Queries
The system SHALL support efficient time-range queries using partition elimination and metadata indexes.

#### Scenario: Query by time range
- **WHEN** a client queries telemetry between start_time and end_time
- **THEN** the system uses Iceberg day partitions to eliminate irrelevant files

#### Scenario: Recent data access
- **WHEN** a client queries recent data (last 24 hours)
- **THEN** the query accesses minimal partitions (1-2 days) via partition pruning

### Requirement: Pagination Support
The system SHALL support cursor-based pagination for large result sets.

#### Scenario: Paginated results
- **WHEN** a client requests paginated results with a page size
- **THEN** the system returns results with a cursor for fetching the next page

#### Scenario: Stable pagination
- **WHEN** a client uses a cursor to fetch the next page
- **THEN** no items are skipped or duplicated across pages

### Requirement: Multi-Signal Type Queries
The system SHALL support querying specific signal types or all signal types for a session.

#### Scenario: Query traces only
- **WHEN** a client requests only traces for a session
- **THEN** the system retrieves data only from `traces` table

#### Scenario: Query all signal types
- **WHEN** a client requests all telemetry for a session
- **THEN** the system retrieves and correlates data from all three tables (traces, logs, metrics)

### Requirement: Payload Retrieval
The system SHALL provide an endpoint for retrieving raw payloads by specific identifiers.

#### Scenario: Retrieve by trace_id
- **WHEN** a client requests traces by `trace_id`
- **THEN** the system scans Iceberg with trace_id predicate and returns matching spans

#### Scenario: Retrieve by session_id list
- **WHEN** a client provides multiple session_ids
- **THEN** the system batches Iceberg queries and returns telemetry for all sessions
