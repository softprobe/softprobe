## ADDED Requirements

### Requirement: Direct Metadata & Payload Access
Java applications SHALL query ClickHouse for metadata filtering and use Iceberg scan API with predicates for full data retrieval (read-only) using a shared client module.

#### Scenario: Query by record_id (trace_id)
- **WHEN** a client queries by `trace_id` and `app_id`
- **THEN** the Java client verifies existence in `recording_events`, then uses Iceberg scan API with predicates (trace_id, app_id) to retrieve full span data

#### Scenario: Session retrieval
- **WHEN** a client queries by `session_id`
- **THEN** the Java client loads session summary from ClickHouse `session_metadata`, then uses Iceberg scan API with predicates (session_id, timestamp range) to retrieve all spans

### Requirement: Performance Targets
The system SHALL meet P95 <100ms for metadata lookups and <1s for full span retrieval via Iceberg under nominal load.

#### Scenario: Metadata lookup within targets
- **WHEN** querying `recording_events` by `trace_id`
- **THEN** P95 latency is <100ms for ClickHouse metadata query

#### Scenario: Iceberg scan efficiency
- **WHEN** querying Iceberg with predicates (session_id or trace_id)
- **THEN** Iceberg prunes 90%+ of files using manifest statistics and P95 latency for full data retrieval is <1s

### Requirement: Caching Strategy
The Java query client SHALL cache metadata query results and optionally cache hot span data.

#### Scenario: Metadata cache hit
- **WHEN** a recently queried trace_id or session is requested again
- **THEN** metadata is served from ClickHouse query cache (5 min TTL) without re-querying

#### Scenario: Span data cache (optional)
- **WHEN** hot span data is cached client-side
- **THEN** full data can be served without Iceberg scan for recently accessed spans (<1 hour)

### Requirement: Query Guarantees (Essential)
The system SHALL provide deterministic ordering, stable cursor-based pagination, and a consistent session read.

#### Scenario: Deterministic ordering
- **WHEN** results are returned for any list query
- **THEN** they are ordered by `timestamp ASC, trace_id ASC, span_id ASC` deterministically

#### Scenario: Stable cursor pagination
- **WHEN** a client paginates through results with a cursor
- **THEN** no item is skipped or duplicated across pages and the cursor encodes the last `(timestamp, trace_id, span_id)`

#### Scenario: Consistent session read
- **WHEN** reading by `session_id`
- **THEN** the system returns a consistent snapshot of the session row group without partial/interleaved reads

### Requirement: Query by session_id
The solution SHALL support reading all spans for a given `session_id` via Iceberg scan API with session_id predicate.

#### Scenario: Fetch by session_id
- **WHEN** a client queries by `session_id` (and optional `app_id`)
- **THEN** the Java client returns all spans using Iceberg scan with predicates; Iceberg finds files efficiently via manifest statistics

### Requirement: Core filters (appId, time range, operationName, recordEnvironment, categoryType)
The system SHALL support core filters via ClickHouse metadata with Iceberg scan API for full data retrieval.

#### Scenario: Filter by appId + time range + operationName
- **WHEN** a client filters by `appId`, `beginTime/endTime`, and `operationName`
- **THEN** the system filters in ClickHouse `recording_events`, then uses Iceberg scan with predicates to retrieve matching full spans

#### Scenario: Filter by environment and categoryType
- **WHEN** a client includes `recordEnvironment` and `categoryType`
- **THEN** the system applies these filters in ClickHouse metadata, then retrieves full data via Iceberg scan

### Requirement: Tag filtering (equals)
The system SHALL support equality filters on tags (e.g., `tags.key = value`) using ClickHouse tag metadata and Iceberg scan.

#### Scenario: Filter by tag equality
- **WHEN** a client filters by `tags.geo = 'us-east'`
- **THEN** the system finds candidate trace_ids via ClickHouse tag metadata, then uses Iceberg scan with trace_id predicates to retrieve full spans

### Requirement: Version filtering
The system SHALL support filtering by `recordVersion` for compatibility and analysis.

#### Scenario: Filter by recordVersion
- **WHEN** a client filters by `recordVersion`
- **THEN** results are constrained to spans with the specified version via metadata filters

### Requirement: App/time-range browsing
The system SHALL support browsing by `appId` and time range with stable ordering and pagination.

#### Scenario: Paginated, time-ordered results
- **WHEN** a client lists spans for an `appId` between `beginTime` and `endTime`
- **THEN** results are ordered by timestamp and paginated; the system uses Iceberg scan with app_id and timestamp predicates for efficient retrieval

### Requirement: Aggregated operation counts
The system SHALL provide operation count analytics using ClickHouse `recording_ops_minute` without reading Iceberg raw spans.

#### Scenario: Count by appId and operationName
- **WHEN** a client requests counts by `appId`, `operationName`, and time buckets
- **THEN** the system queries ClickHouse `recording_ops_minute` and returns pre-aggregated counts (no Iceberg access needed)

### Requirement: Error-only filtering
The system SHALL support filtering spans by error status via ClickHouse metadata and Iceberg scan.

#### Scenario: Filter errors in a time window
- **WHEN** a client filters for `status_code = 'ERROR'` within a time range
- **THEN** the system filters in ClickHouse `recording_events`, then uses Iceberg scan with predicates to retrieve full error span data

### Requirement: Full scan with pruning
The system SHALL support full scans for analytical workloads with partition and row-group pruning.

#### Scenario: Analytical scan
- **WHEN** a client runs a full-scan query with predicates
- **THEN** the system prunes by `record_date` and uses row-group min/max statistics to limit I/O

### Requirement: Multi-tenancy filters
The system SHALL support optional filters by `organization_id` and `tenant_id` where present in metadata.

#### Scenario: Filter by tenant
- **WHEN** a client includes `tenant_id`
- **THEN** the system restricts results accordingly using metadata filters
