## ADDED Requirements
### Requirement: Background Optimizer
The system SHALL run a background optimizer that commits buffered data into Iceberg snapshots.

#### Scenario: Optimizer commits
- **WHEN** optimizer criteria are met
- **THEN** buffered data is committed as a new Iceberg snapshot

### Requirement: Local Cache
The system SHALL maintain a local NVMe cache for hot Parquet, index files, and deletion vectors.

#### Scenario: Cache read-through
- **WHEN** a query targets recently flushed data
- **THEN** the system serves data from the local cache when available
