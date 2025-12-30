# Recording Query Client Implementation Plan

## Context
- Java services must replace MongoDB-centric queries with a shared client that reads ClickHouse metadata and Iceberg payloads directly, meeting latency and ordering guarantees (`docs/design.md:501`).
- Query flows must cover trace lookups, session streaming, caching, pagination, and metrics as spelled out in the OpenSpec change `add-iceberg-otlp-migration` (`openspec/changes/add-iceberg-otlp-migration/specs/query/spec.md:3`).
- Existing Mongo providers rely on `appId`, `creationTime`, `operationName`, environment, category, and record version filters, with hybrid tag handling (`docs/mongodb-query-parameters-analysis.md:31` and `docs/mongodb-query-parameters-analysis.md:46`).
- Current repositories such as `SpMockerMongoRepositoryProvider` implement pagination, TTL updates, and record version selection that the new client must emulate against ClickHouse/Iceberg (`sp-storage/sp-storage-web-api/src/main/java/ai/softprobe/storage/repository/impl/mongo/SpMockerMongoRepositoryProvider.java:127`).
- Time-series optimizations and count aggregation patterns live in `OperationMetricsService`, informing the analytics parity we need on the new stack (`sp-storage/sp-storage-web-api/src/main/java/ai/softprobe/storage/service/OperationMetricsService.java:91`).

## Requirements Snapshot
| Capability | Acceptance Criteria | Source |
|------------|--------------------|--------|
| Direct metadata + payload access | Fetch spans by trace or session via ClickHouse pointers and Iceberg row-groups | `openspec/changes/add-iceberg-otlp-migration/specs/query/spec.md:3` |
| Deterministic ordering & cursors | Always sort by `(timestamp, trace_id, span_id)` with stable pagination tokens | `openspec/changes/add-iceberg-otlp-migration/specs/query/spec.md:28` |
| Filtering coverage | Support core filters (app, time, op, env, category), recordVersion, tags, status, tenant | `openspec/changes/add-iceberg-otlp-migration/specs/query/spec.md:50` |
| Performance | Metadata lookups <100 ms P95, session queries <500 ms P95 with caching | `openspec/changes/add-iceberg-otlp-migration/specs/query/spec.md:14` |
| Analytics counts | Serve operation counts from ClickHouse without Iceberg scans | `openspec/changes/add-iceberg-otlp-migration/specs/query/spec.md:82` |
| Caching & telemetry | Provide row-group/span caches plus Micrometer metrics | `openspec/changes/add-iceberg-otlp-migration/specs/query/spec.md:21` and `docs/design.md:509` |

## Legacy Query Observations
- Mongo repositories consistently co-filter `appId`, `creationTime`, `operationName`, `recordEnvironment`, and `categoryType`, implying compound indexes we must mirror in ClickHouse SQL predicates (`docs/mongodb-query-parameters-analysis.md:31`).
- Record-level lookups fetch the freshest record version, with per-request TTL updates (`sp-storage/sp-storage-web-api/src/main/java/ai/softprobe/storage/repository/impl/mongo/SpMockerMongoRepositoryProvider.java:131`).
- Tag filters currently allow equality matches on arbitrary keys; complex analytics are offloaded, motivating ClickHouse tag pivot tables (`docs/mongodb-query-parameters-analysis.md:64`).
- Operation metrics already aggregate by minute for dashboards, so our client should hit the `recording_ops_minute` table for analogous queries (`sp-storage/sp-storage-web-api/src/main/java/ai/softprobe/storage/service/OperationMetricsService.java:95`).

## Workstreams & Progress
- [x] **Module scaffolding** — Create `recording-query-client` Maven module, define `RecordingQueryClient`, configuration objects, and dependency set (ClickHouse JDBC, DuckDB, Caffeine, Micrometer).
- [x] **Metadata repository** — Implement ClickHouse DAO operations for trace/session lookup, filtered listings, tag/status/version filters, and aggregated counts with deterministic ordering.
- [x] **Iceberg reader** — Integrate DuckDB `iceberg_scan`, handle grouped row-group reads, JSON decoding, and enforce consistent snapshots.
- [x] **Caching & metrics** — Add Caffeine caches (row-group + span), expose Micrometer timers/counters, structured logging for slow queries.
- [x] **Pagination & cursors** — Encode/decode `(timestamp, trace_id, span_id)` cursors, ensure stable page traversal across metadata + Iceberg reads.
- [ ] **Testing & validation** — Build unit tests for SQL builders/cursors, DuckDB-backed integration tests, Micrometer assertions; document verification strategy and update OpenSpec task checklist.

## Immediate Next Steps
1. Broaden automated coverage (metadata SQL builder, Iceberg reader smoke tests) and document validation steps.
2. Update OpenSpec task checklist once verification story is complete.

## Metadata Repository Design Notes
- **Tables & Columns**  
  - `recording_events` (ClickHouse) must expose `app_id`, `session_id`, `trace_id`, `span_id`, `record_environment`, `category_type`, `record_version`, `status_code`, `organization_id`, `tenant_id`, `timestamp`, `end_timestamp`, `iceberg_file_uri`, `row_group_index`, `row_index_in_group`, `tags` (Map), plus partition key `record_date`. Mirrors hot Mongo filters (`docs/mongodb-query-parameters-analysis.md:31`).  
  - `recording_ops_minute` provides aggregated counts keyed by `(app_id, operation_name, record_version, env, category)` with metric columns `count`, `error_count`.  
  - `session_metadata` surfaces `file_segments` as array-of-struct `(iceberg_file_uri, row_group_start, row_group_end)` for contiguous session fetches.
- **Queries to Support**  
  1. **Trace lookup**: `SELECT iceberg_file_uri, row_group_index, row_index_in_group, span_id FROM recording_events WHERE app_id=? AND trace_id=? LIMIT 1`.  
  2. **Session segments**: `SELECT file_segments FROM session_metadata WHERE app_id=? AND session_id=?` → decode JSON/Map and order by `row_group_start`.  
  3. **Span listing**: Build dynamic SQL with WHERE clauses for time window, operations, env, category, version, status/error, tenant/org, tag equality (using `mapContains` or `mapElement`), plus cursor predicate `(timestamp, trace_id, span_id)` for forward pagination. ORDER BY `timestamp, trace_id, span_id` LIMIT `limit`.  
  4. **Tag filtering**: For each `tags.key = value`, append `AND mapContains(tags, (?, ?))`.  
  5. **Error-only**: derive from `status_code = 'ERROR' OR errorOnly=true`.  
  6. **Aggregation**: `SELECT window_start, operation_name, count, error_count FROM recording_ops_minute WHERE app_id=? AND window_start BETWEEN ? AND ?` for dashboards.
- **Timeouts & Connection Management**  
  - Use HikariCP configured from `RecordingQueryConfig.ClickHouse` (pool size, socket timeout).  
  - Apply per-query timeout via `PreparedStatement.setQueryTimeout()` in seconds.  
  - Ensure read-only transactions and fetch size tuned for pagination (`setFetchSize(limit)`).
- **Result Mapping**  
  - Convert ClickHouse timestamps to `Instant` via `ResultSet::getTimestamp` with UTC zone.  
  - Derive `RecordingPointer` alongside cursor fields (`timestamp`, `trace_id`, `span_id`, `row_index_in_group`).  
  - Jackson `ObjectMapper` decodes `file_segments` JSON into `SessionSegments`.
- **Failure Handling**  
  - Wrap SQLExceptions in `MetadataAccessException` with contextual info (query label, parameters).  
  - Use structured logging for slow queries with elapsed duration > config threshold.
