## ADDED Requirements
### Requirement: DuckLake Committed Views
The system SHALL query committed telemetry data through DuckDB views over attached DuckLake tables.

#### Scenario: Committed traces are queried
- **WHEN** a query references the `spans` union view
- **THEN** committed rows are read from the DuckLake `traces` table without using `iceberg_scan`

#### Scenario: Committed logs and metrics are queried
- **WHEN** a query references the `logs` or `metrics` union view
- **THEN** committed rows are read from the corresponding DuckLake table without pinned Iceberg metadata files

### Requirement: DuckLake Union-Read Freshness
The system SHALL preserve union-read freshness by combining buffered data, staged Parquet data, and committed DuckLake data in DuckDB.

#### Scenario: Query sees recent uncommitted data
- **WHEN** telemetry has been accepted but not yet committed to DuckLake
- **THEN** DuckDB query results include matching buffered or staged records

#### Scenario: Query sees committed data
- **WHEN** telemetry has been committed to DuckLake
- **THEN** DuckDB query results include matching committed records from the attached DuckLake catalog

### Requirement: DuckLake Snapshot Reads
The system SHALL rely on DuckLake snapshot isolation for committed data while pairing it with a consistent buffer and staged snapshot for each query.

#### Scenario: Query uses a consistent committed snapshot
- **WHEN** a query starts
- **THEN** committed rows are read from a consistent DuckLake snapshot for that query
