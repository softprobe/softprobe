## ADDED Requirements
### Requirement: DuckLake Optimizer Commits
The system SHALL commit staged telemetry data to DuckLake from the background optimizer.

#### Scenario: Staged spans are committed
- **WHEN** the optimizer processes staged span batches
- **THEN** the batches are inserted into the DuckLake `traces` table in a transaction

#### Scenario: Staged logs and metrics are committed
- **WHEN** the optimizer processes staged log or metric batches
- **THEN** the batches are inserted into the corresponding DuckLake table in a transaction

### Requirement: WAL and Staged Path Preservation
The system SHALL keep the local WAL and staged Parquet path as the source of recovery and near-real-time visibility before DuckLake commit.

#### Scenario: Data is flushed before commit
- **WHEN** buffered telemetry is flushed
- **THEN** the system writes local WAL files and staged Parquet files before the data is committed to DuckLake

#### Scenario: Startup replay restores uncommitted data
- **WHEN** the service starts with replay enabled
- **THEN** uncommitted WAL data is replayed into staged Parquet files before query serving

### Requirement: Append-Only DuckLake Ingestion
The system SHALL treat OTLP ingestion as append-only when writing to DuckLake.

#### Scenario: OTLP records are accepted
- **WHEN** traces, logs, or metrics are ingested
- **THEN** the system appends records and does not require update or delete semantics
