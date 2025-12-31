## ADDED Requirements

### Requirement: Iceberg Manifest-Based Query Optimization
The system SHALL leverage Iceberg manifest files for efficient query planning and file pruning.

#### Scenario: Manifest-level file pruning
- **WHEN** a query specifies predicates (session_id, time range, attributes)
- **THEN** the Iceberg query engine uses manifest files to eliminate data files that don't match predicates

#### Scenario: Partition-level elimination
- **WHEN** a query filters by date range
- **THEN** Iceberg eliminates entire partitions (days) that fall outside the range using partition metadata

### Requirement: Partition Statistics for Efficient Filtering
The system SHALL use Iceberg partition statistics to minimize data scanning.

#### Scenario: Date partition pruning
- **WHEN** querying for sessions within a specific date range
- **THEN** only Parquet files within matching date partitions are scanned

#### Scenario: Partition metadata caching
- **WHEN** repeatedly querying the same time range
- **THEN** partition metadata is cached to avoid redundant manifest reads

### Requirement: Row Group Statistics for Predicate Pushdown
The system SHALL use Parquet row group min/max statistics for efficient data filtering.

#### Scenario: Session ID row group filtering
- **WHEN** querying by specific session_id
- **THEN** only row groups with matching session_id min/max ranges are read from Parquet files

#### Scenario: Timestamp-based row group pruning
- **WHEN** querying with fine-grained time filters
- **THEN** row group statistics eliminate groups outside the time range

### Requirement: Snapshot Isolation for Consistent Reads
The system SHALL use Iceberg snapshot isolation to provide consistent query results.

#### Scenario: Consistent session view
- **WHEN** reading a session across multiple Iceberg tables (traces, logs)
- **THEN** the same snapshot ID is used for both tables to ensure time-consistent data

#### Scenario: Snapshot-based time travel
- **WHEN** querying historical data
- **THEN** Iceberg snapshots allow querying data as it existed at a specific point in time

### Requirement: Manifest File Structure
The system SHALL organize Iceberg metadata using manifest files for scalability.

#### Scenario: Manifest list organization
- **WHEN** Iceberg commits new data
- **THEN** a manifest list references multiple manifest files, each containing metadata for a set of data files

#### Scenario: Incremental manifest updates
- **WHEN** new sessions are written
- **THEN** only new manifest files are created (not rewriting entire metadata)

### Requirement: No External Metadata Store
The system SHALL NOT use external databases or indexes for metadata - all metadata is managed by Iceberg.

#### Scenario: Query without external dependency
- **WHEN** executing a query
- **THEN** all metadata (partition stats, file locations, row group statistics) comes from Iceberg catalog and manifest files

#### Scenario: Self-contained metadata
- **WHEN** moving or replicating the Iceberg catalog
- **THEN** all query capabilities are preserved without external synchronization

