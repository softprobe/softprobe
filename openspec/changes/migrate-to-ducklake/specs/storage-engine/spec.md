## ADDED Requirements
### Requirement: DuckLake Committed Storage
The system SHALL store committed OTLP traces, logs, and metrics in DuckLake tables backed by immutable Parquet files.

#### Scenario: Tables are initialized
- **WHEN** the service starts with DuckLake storage enabled
- **THEN** the configured DuckLake catalog is attached and the `traces`, `logs`, and `metrics` tables exist with the configured schema

#### Scenario: Append data is committed
- **WHEN** the background optimizer commits staged telemetry data
- **THEN** the records are appended to the corresponding DuckLake table in a committed snapshot

### Requirement: DuckLake Catalog Configuration
The system SHALL support DuckLake catalog configuration for PostgreSQL in production and local DuckDB or SQLite catalogs in development and tests.

#### Scenario: Production catalog attaches
- **WHEN** `ducklake.catalog_type` is `postgres`
- **THEN** the service attaches DuckLake with the configured PostgreSQL metadata connection and object-storage data path

#### Scenario: Local catalog attaches
- **WHEN** `ducklake.catalog_type` is `duckdb` or `sqlite`
- **THEN** the service attaches DuckLake with a local metadata catalog and local or object-storage data path

### Requirement: DuckLake Maintenance
The system SHALL perform DuckLake-native maintenance instead of Iceberg manifest and orphan-file maintenance.

#### Scenario: Small files are compacted
- **WHEN** the compaction schedule runs
- **THEN** the system invokes DuckLake file merge operations according to the configured target file size

#### Scenario: Old snapshots are expired
- **WHEN** the snapshot retention policy is reached
- **THEN** the system expires old DuckLake snapshots and schedules unreferenced files for cleanup
