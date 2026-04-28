## Context

The current storage path is built around Apache Iceberg tables managed through a REST catalog. The ingest engine already moved the hot path away from commit-per-request behavior by writing incoming telemetry to in-memory buffers, local Parquet WAL files, and staged Parquet files that DuckDB can query immediately. A background optimizer later commits staged record batches into Iceberg snapshots.

Recent catalog work is focused on making frequent Iceberg commits and metadata reads fast enough for telemetry workloads. DuckLake changes the shape of that problem: metadata is stored in a SQL catalog database, data remains in immutable Parquet files, and DuckDB can read and write the lake through a native `ducklake` extension. This removes the need to maintain Iceberg manifests, pinned metadata pointers, manifest downloads, and `iceberg_scan` fallback logic in the query path.

DuckLake is still a lakehouse format, not a streaming database. Each committed write creates a snapshot, and small writes can still create many files unless ingestion batches, inlining, and compaction are controlled. The migration should therefore keep the existing buffer/WAL/staged freshness model and replace only the committed storage layer first.

## Goals

- Replace the committed Iceberg layer with DuckLake while preserving OTLP ingestion semantics, table names, and user-facing query behavior for `traces`, `logs`, and `metrics`.
- Use DuckDB as the single read/write integration point for DuckLake instead of adding a new Rust catalog client.
- Keep the existing buffer + staged + committed union-read model so queries still see recent uncommitted data.
- Use PostgreSQL as the production DuckLake catalog backend, with local DuckDB or SQLite catalog support for development and tests.
- Simplify committed reads by querying attached DuckLake tables directly instead of maintaining local Iceberg metadata pointers and `iceberg_scan` views.
- Preserve append-only ingestion as the primary path; updates/deletes remain out of scope for OTLP ingestion.

## Non-Goals

- Migrating already deployed Iceberg data in the first implementation pass.
- Supporting MySQL as a DuckLake catalog backend.
- Introducing secondary indexes or an external query service.
- Reworking OTLP models, API routes, session extraction, or schema-promotion semantics beyond the storage type changes needed for DuckLake.
- Relying on DuckLake data inlining for high-volume telemetry payloads.

## Proposed Architecture

### Storage Model

Add a DuckLake storage module that owns committed table lifecycle and batch commits:

- `DuckLakeStore` replaces `IcebergWriter` as the committed storage facade.
- `DuckLakeStore::new(config)` installs and loads DuckDB extensions, attaches the configured DuckLake, creates schemas/tables if needed, and applies DuckLake options.
- The table contract remains `traces`, `logs`, and `metrics`; schemas are translated from the current Arrow/Iceberg table definitions into DuckDB DDL.
- Production uses `ATTACH 'ducklake:postgres:...' AS softprobe (DATA_PATH 's3://...')`.
- Local development can use `ATTACH 'ducklake:metadata.ducklake' AS softprobe (DATA_PATH './warehouse/ducklake/')` or SQLite when multi-process local reads are useful.

The existing `iceberg` config block should be replaced by a `ducklake` block:

```yaml
ducklake:
  catalog_type: "postgres"
  metadata_path: "dbname=ducklake_catalog host=db user=postgres password=postgres"
  data_path: "s3://warehouse/ducklake/"
  catalog_alias: "softprobe"
  metadata_schema: "main"
  data_inlining_row_limit: 0
  target_file_size: "128MB"
  parquet_compression: "zstd"
```

`data_inlining_row_limit` should default to `0` for this project. DuckLake defaults can inline very small inserts into the metadata catalog, which is useful for small analytical apps but is a poor default for telemetry with large payload columns and a PostgreSQL catalog. If a future low-volume deployment wants inlining, it can be enabled explicitly.

### Write Path

Keep the existing hot path:

```text
OTLP request -> in-memory buffer -> flush -> local WAL + staged Parquet -> queryable immediately
```

Change only the optimizer commit target:

```text
staged Parquet groups -> DuckDB transaction -> INSERT INTO DuckLake tables -> DuckLake snapshot
```

The first implementation should commit with `INSERT INTO softprobe.traces SELECT ... FROM read_parquet([...])` rather than registering staged files with `ducklake_add_data_files`. This lets DuckLake write its own Parquet files with the correct table metadata, field identifiers, partition information, compression, and file layout. It does rewrite staged data, but it is simpler and safer for the initial migration.

After correctness and performance are established, we can evaluate a faster commit mode that uploads staged Parquet files into the DuckLake data path and registers them with `ducklake_add_data_files`. That path transfers file ownership to DuckLake and must prove that our staged Parquet schemas, field identifiers, partition values, and cleanup rules match DuckLake expectations.

Commits should use one DuckDB transaction per telemetry kind and date group initially. Each transaction maps to one DuckLake snapshot. If snapshot volume remains high, the optimizer can combine multiple date groups or telemetry kinds per transaction, but that should be driven by measurements.

### Query Path

The query engine should continue exposing stable DuckDB views:

```text
spans   = buffer_spans  UNION ALL staged_spans  UNION ALL committed_traces
logs    = buffer_logs   UNION ALL staged_logs   UNION ALL committed_logs
metrics = buffer_metrics UNION ALL staged_metrics UNION ALL committed_metrics
```

`committed_*` becomes a direct view over attached DuckLake tables:

```sql
CREATE OR REPLACE TEMP VIEW committed_traces AS
SELECT * FROM softprobe.traces;
```

This removes:

- `INSTALL iceberg`
- Iceberg REST catalog attachment
- pinned metadata pointer files
- manifest-list downloads and rewrites
- `iceberg_scan(... mode := 'metadata')`
- fallback paths that read committed data from hand-maintained data file lists

DuckLake provides snapshot isolation for committed reads. For a single query, the query worker should attach DuckLake once per worker connection and run the union query against that connection's current committed snapshot plus the buffer/staged snapshot taken during view preparation. If stronger repeatability across multiple statements is needed later, the query API can wrap a request in an explicit read transaction.

### Schema, Partitioning, and Sorting

Initial DuckLake table schemas should mirror the current table columns and promoted columns:

- `traces` partitioned by `record_date`, sorted by `session_id`, `trace_id`, `timestamp`.
- `logs` partitioned by `record_date`, sorted by `session_id`, `timestamp`.
- `metrics` partitioned by `record_date`, sorted by `metric_name`, `timestamp`.

DuckLake supports partition evolution and sorted tables through SQL. Table creation should set these properties with `ALTER TABLE ... SET PARTITIONED BY (...)` and `SET SORTED BY (...)` where supported by the extension version. If sorted table syntax is not available in the bundled DuckDB version, keep row grouping in staged files and rely on DuckLake compaction in the first pass.

Current promoted-column configuration should remain the source of truth for additional physical columns. The migration should centralize schema definitions around Arrow/DuckDB types and stop using Iceberg field IDs as the primary schema representation.

### Maintenance

DuckLake maintenance replaces Iceberg metadata maintenance:

- Run `ducklake_merge_adjacent_files` on a schedule for small-file compaction.
- Use tiered compaction by changing `target_file_size` before calls when needed.
- Run `ducklake_flush_inlined_data` only if data inlining is explicitly enabled.
- Run `ducklake_expire_snapshots` according to retention policy.
- Run `ducklake_cleanup_old_files` after the safe deletion delay.
- Let PostgreSQL handle catalog maintenance with normal `VACUUM` and backups.

The existing `compaction` config can be retained but reinterpreted for DuckLake. Iceberg-specific options such as manifest rewrite and orphan removal should be removed or renamed.

### Deployment

Local Docker should replace Lakekeeper with a DuckLake catalog database:

- Keep MinIO for object storage.
- Reuse PostgreSQL as the DuckLake metadata catalog in docker-compose.
- Remove Lakekeeper migrate/server services once Iceberg is no longer needed.
- Configure the app to attach DuckLake with PostgreSQL metadata and MinIO-backed S3 data path.

The DuckDB bundled crate must support DuckDB 1.5.2 or newer for DuckLake v1.0. The current crate is pinned to DuckDB 1.4.3, so dependency validation is an early migration task. If the Rust `duckdb` crate cannot bundle a new enough DuckDB release yet, the migration should either wait, build against a system DuckDB, or isolate DuckLake writes in a sidecar/CLI until Rust support catches up.

## Migration Plan

1. Add the `ducklake` configuration model and keep the existing `iceberg` block only behind a temporary compatibility path while the migration branch is in progress.
2. Introduce `src/storage/ducklake` with table DDL, attach/configuration, and `write_*_record_batches` APIs that match the current `IcebergWriter` call sites.
3. Update the ingest optimizer to depend on a committed-storage trait instead of directly on `IcebergWriter`, then implement the trait with `DuckLakeStore`.
4. Change DuckDB query initialization to install/load `ducklake` and attach the configured DuckLake on each worker connection.
5. Replace committed `iceberg_*` views with committed DuckLake table views while preserving buffer and staged views.
6. Replace Iceberg maintenance with DuckLake merge/expire/cleanup jobs.
7. Update docker-compose and test configs to use PostgreSQL metadata plus MinIO data storage.
8. Add migration tests for table creation, append commits, query union freshness, restart WAL replay, schema promotion, compaction, and snapshot/time-travel behavior.
9. Remove Iceberg dependencies, Lakekeeper config, metadata pointer code, and Iceberg-specific docs after DuckLake tests pass.

## Rollback Strategy

Until production migration is proven, keep DuckLake behind a storage backend config flag on the migration branch. Rollback means starting the service with the Iceberg backend and the previous Iceberg catalog configuration. Do not write the same telemetry stream to both backends in production unless a separate dual-write validation plan is approved, because split-brain retention and query results would be hard to reason about.

For local and CI testing, dual-run comparison is useful: ingest the same fixture into Iceberg and DuckLake, query both with the same SQL, and compare row counts and selected payload fields.

## Risks and Mitigations

- DuckDB/DuckLake version availability in Rust: validate extension support before broad refactoring; do a small attach/create/insert/query spike first.
- Concurrent writers: use PostgreSQL catalog in production and rely on DuckLake retry settings; keep commit batches append-only and avoid concurrent schema changes.
- Metadata catalog bloat from tiny inserts or inlining: keep optimizer batching, default inlining to `0`, and monitor DuckLake catalog table growth.
- File ownership and cleanup: use `INSERT SELECT` for MVP so DuckLake owns committed files; defer `ducklake_add_data_files` until ownership semantics are tested.
- Query regressions from removing local Iceberg metadata cache: preserve local staged/cache views for freshness and measure warm-query latency against DuckLake attached reads.
- Schema mismatch between Arrow, staged Parquet, and DuckLake DDL: centralize schema generation and add round-trip tests for each telemetry table.
- S3 path behavior with MinIO/R2: test DuckDB `httpfs` credentials and endpoint settings in docker-compose and CI before removing Lakekeeper.

## Open Questions

- Can the Rust `duckdb` crate used by this project support DuckDB 1.5.2+ and the DuckLake extension without a system dependency?
- Should production use one DuckLake catalog/schema per tenant, one schema per tenant, or shared tables with tenant columns?
- What snapshot retention should be used for troubleshooting and ETL workloads?
- Do we need a one-time Iceberg-to-DuckLake data migration tool, or can existing Iceberg deployments be treated as non-production during this change?
- Is `SET SORTED BY` available and stable enough for the initial table creation path in the selected DuckLake extension version?
