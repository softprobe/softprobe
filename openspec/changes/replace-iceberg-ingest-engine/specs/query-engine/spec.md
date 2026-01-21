## ADDED Requirements
### Requirement: DuckDB SQL Interface
The system SHALL expose DuckDB SQL as the primary query interface and avoid custom REST query APIs for analytics.

#### Scenario: DuckDB drives analytics
- **WHEN** analytics queries are executed
- **THEN** they run through DuckDB against Iceberg/catalog-compatible sources

### Requirement: Iceberg Catalog Compatibility
The system SHALL keep query paths compatible with Iceberg catalogs (REST/Glue/etc.) so DuckDB can use `iceberg_scan` without bespoke adapters.

#### Scenario: DuckDB iceberg_scan works
- **WHEN** DuckDB executes `iceberg_scan` against the configured catalog
- **THEN** results are returned without requiring non-standard API layers

### Requirement: Union-Read Freshness
The system SHALL provide a union-read query path in DuckDB that merges buffered/WAL data with Iceberg snapshots.

#### Scenario: Query sees uncommitted data
- **WHEN** a query is executed
- **THEN** results include buffered data not yet committed to Iceberg

### Requirement: Snapshot Consistency
The system SHALL provide snapshot-consistent reads by pairing a specific Iceberg snapshot with the relevant buffer state.

#### Scenario: Consistent union-read
- **WHEN** a query starts
- **THEN** it uses a consistent Iceberg snapshot and a matching buffer watermark

### Requirement: Near Real-Time Query Performance
The system SHALL return DuckDB SQL queries that include WAL + staged + Iceberg data in under 1 second for warm-cache queries over recent partitions.

#### Scenario: Warm query under 1s
- **WHEN** a query against the last N days is executed after cache warm-up
- **THEN** the end-to-end latency is under 1 second

### Requirement: Concurrent Query Throughput
The system SHALL sustain sub-second warm-query latency under parallel query load for union-read queries.

#### Scenario: Parallel load stays under 1s
- **WHEN** multiple concurrent DuckDB queries run against union-read views
- **THEN** p95 latency remains under 1 second

### Requirement: WAL View Stability
The system SHALL avoid rebuilding DuckDB WAL views on each WAL write, and only refresh views when schemas change.

#### Scenario: WAL writes do not trigger view rebuilds
- **WHEN** new WAL files are written
- **THEN** DuckDB queries continue to include WAL data without recreating views
