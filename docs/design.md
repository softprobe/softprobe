# OpenTelemetry Collector with Apache Iceberg Storage

**Version**: 2.0
**Date**: 2025-01-31
**Status**: Active
**Project Type**: Standalone OTLP Collector

> **See Also**: [Softprobe Business Vision & Strategic Goals](goals.md) - Understand the broader business context and long-term vision for this project.

---

## 1. Executive Summary

### Overview

This is a standalone OpenTelemetry-compatible collector that **extends OTLP to capture and store full HTTP request/response bodies** in an efficient, open-source data lake. Unlike traditional observability backends, this system treats telemetry data as a comprehensive data warehouse that serves two critical purposes:

1. **Business Case Troubleshooting**: AI agents and engineers can access complete session context (full HTTP payloads) to debug complex business logic failures
2. **ETL Pipeline Source**: Full payloads serve as the source of truth for data analytics, business intelligence, and ad-hoc data exploration

The system provides:

- **Extended OTLP Protocol**: Captures full HTTP request/response bodies alongside standard telemetry
- **Separated Storage Architecture**: Metadata (spans, attributes) and bodies stored separately for efficient access patterns
- **Business Attribute Indexing**: Search sessions by business identifiers (user ID, order ID, confirmation code) without scanning payloads
- **Iceberg Data Lake**: Open-source, vendor-neutral storage enabling SQL queries and analytics
- **Session Management**: Session-aware buffering for traces and logs (NOT metrics)

### Primary Goal

**THE SINGLE MOST IMPORTANT GOAL**: Store complete HTTP request/response bodies in an efficient, open-source data lake that can be:
- Queried for troubleshooting via business metadata (user ID, order ID, etc.) WITHOUT reading bodies
- Accessed by AI agents to analyze full session context when troubleshooting hard business cases
- Used as the source for ETL pipelines and ad-hoc data analytics

Bodies are stored separately from metadata to avoid expensive read/write operations during routine queries.

### Key Design Principles

1. **Complete Data Capture**: Store full HTTP payloads, not just metadata
2. **Separated Storage**: Metadata and bodies stored separately for efficient access
3. **Business-Centric Search**: Index by business attributes (order ID, user ID) for fast session lookup
4. **Open Source & Vendor Neutral**: Iceberg-based data lake, no proprietary storage
5. **OTLP Extension**: Build on OpenTelemetry standards, add body capture
6. **Dual Purpose**: Troubleshooting tool AND analytics data warehouse

### Goals and Non-Goals

**Goals**:
- **PRIMARY**: Capture and store full HTTP request/response bodies in efficient, separated storage
- **PRIMARY**: Enable search by business attributes (user ID, order ID, PNR, etc.) without reading bodies
- **PRIMARY**: Provide complete session context for AI-powered troubleshooting
- **PRIMARY**: Serve as ETL source for business analytics and ad-hoc data exploration
- Complete OTLP v1 API implementation with body capture extensions
- Apache Iceberg storage with metadata/body separation
- Business attribute extraction and indexing from HTTP payloads
- Session management for traces and logs (coordinated buffering and storage)
- Horizontal scalability with consistent hashing

**Non-Goals**:
- OTLP exporter functionality (collector is ingestion-only)
- Real-time streaming queries (batch-oriented query model)
- Java integration (standalone Rust service)
- Sampling or data reduction (stores EVERYTHING)
- External metadata indexing layer (using Iceberg's built-in capabilities)

---

## 2. Architecture

### 2.1 Data Flow

```
┌──────────────────┐
│ OTel SDK Client  │ (Standard OTLP)
└────────┬─────────┘
         │ POST /v1/traces, /v1/logs, /v1/metrics
         ▼
┌────────────────────────────┐
│ OTLP Collector (Rust/Axum) │
│ - Trace/Log Buffer (unified)│
│ - Metrics Buffer (separate) │
└────────┬───────────────────┘
         │
         │ Coordinated Flush
         ▼
┌───────────────────────────────────┐
│ Apache Iceberg Tables             │
│ ├─ traces (session-based)    │
│ ├─ logs (session-based)      │
│ └─ metrics (metric_name-based)│
└────────┬──────────────────────────┘
         │
         │ Direct Query with Predicates
         ▼
┌────────────────────────────┐
│ Query API                  │
│ - Manifest Pruning         │
│ - Partition Elimination    │
│ - Row Group Statistics     │
└────────────────────────────┘
```

### 2.2 Component Architecture

| Component | Responsibility | Technology |
|-----------|---------------|------------|
| **OTLP Endpoints** | Accept standard OTLP traces/logs/metrics | Rust (Axum), Tonic |
| **Session Buffer** | Unified buffer for traces+logs by session_id | Rust (DashMap) |
| **Metrics Buffer** | Separate buffer organized by metric_name | Rust (DashMap) |
| **Iceberg Writer** | Coordinated writes to separate tables | iceberg-rust 0.7 |
| **Query Engine** | Direct Iceberg scans with predicates | iceberg-rust, DuckDB |
| **Object Storage** | S3-compatible backend | MinIO/S3/R2 |

---

## 3. Buffering Strategy

### 3.1 Separate Buffers for Traces and Logs

**Rationale**: While traces and logs are used together for session investigation, they must be buffered separately to avoid Iceberg file fragmentation. Writing to separate tables from a unified buffer would create many small Parquet files.

**Traces Buffer**:
- **Buffer Organization**: HashMap<session_id, Vec<Span>> OR global Vec<Span> with session tracking
- **Flush Triggers**:
  - Size: ~128MB buffer size
  - Age: 60 seconds
  - Session timeout: 30 minutes (configurable)
- **Storage**: Writes to `traces` table with session-based row groups

**Logs Buffer**:
- **Buffer Organization**: HashMap<session_id, Vec<LogRecord>> OR global Vec<LogRecord> with session tracking
- **Flush Triggers**:
  - Size: ~128MB buffer size (independent tracking)
  - Age: 60 seconds
  - Session timeout: 30 minutes (same timeout as traces for consistency)
- **Storage**: Writes to `logs` table with session-based row groups

**Session Coordination**: While buffers are separate, session timeouts can be coordinated to ensure traces and logs for a session are queryable around the same time. This is a query optimization, not a storage requirement.

### 3.2 Separate Metrics Buffering

**Rationale**: Metrics are aggregations, NOT session-correlated.

- **Buffer Organization**: HashMap<metric_name, Vec<MetricDataPoint>> OR global Vec<MetricDataPoint>
- **Flush Triggers**:
  - Size: ~128MB buffer size
  - Age: 60 seconds (independent of session timeouts)
- **Storage Pattern**: Organized by metric_name + timestamp, NOT session_id

**Note**: Iceberg may not be ideal for time-series metrics. Consider Prometheus/InfluxDB later.

---

## 4. Storage Platform Decision

### 4.0 Table Format: Iceberg vs Delta Lake vs Hudi

**Decision**: ✅ **Apache Iceberg**

**Rationale** (See [Storage Design Analysis](storage_design.md#is-iceberg-the-right-foundation)):

| Criterion | Iceberg | Delta Lake | Winner |
|-----------|---------|------------|--------|
| Vendor Neutrality | ✅ Apache Foundation | ⚠️ Databricks-centric | **Iceberg** |
| Multi-Engine Support | ✅ Spark, Trino, Dremio, Flink, DuckDB | ⚠️ Databricks-first | **Iceberg** |
| Rust Ecosystem | ✅ iceberg-rust 0.7+ | ❌ No official support | **Iceberg** |
| Query Performance (OLAP) | ⚠️ 1.7x slower (TPC-DS) | ✅ Fastest | **Delta** |
| Session Lookups | ✅ Partition pruning + row groups | ✅ Good | **Tie** |
| Open Specification | ✅ Fully open | ⚠️ Databricks influence | **Iceberg** |

**Performance Consideration**: Iceberg is 1.7x slower than Delta Lake in TPC-DS OLAP benchmarks, but this doesn't reflect our workload:
- Our queries: Session lookups (`WHERE session_id = 'xxx'`) + batch ETL
- Iceberg's optimizations (partition pruning, row group stats, bloom filters) excel at session-based queries
- Network I/O from object storage dominates ETL queries, not Parquet decode time

**Trade-off Accepted**: Vendor neutrality and multi-engine support outweigh 1.7x OLAP performance tax.

**When We'd Reconsider**:
- Session lookup latency >1s AND all optimization attempts fail
- Customer demand for Databricks-native integration
- iceberg-rust library becomes unmaintained

### 4.0.1 Object Storage: Cloud-Neutral S3-Compatible

**Decision**: ✅ **S3-Compatible Storage (Customer Choice)**

**Supported Backends**:
- **AWS S3** - Industry standard, existing infrastructure
- **Cloudflare R2** - Zero egress, cost-optimized
- **Google Cloud Storage** - GCP customers
- **MinIO** - Self-hosted, compliance/on-prem
- **Any S3-compatible provider** - Wasabi, etc.

**Cost Analysis** (Example: 100TB data + 50TB monthly egress):

| Provider | Storage/GB | Egress/GB | Monthly Cost |
|----------|------------|-----------|--------------|
| AWS S3 | $0.023 | $0.09 | **$6,800** |
| Cloudflare R2 | $0.015 | **$0** | **$1,500** |
| Google Cloud Storage | $0.020 | $0.12 | **$8,300** |
| MinIO (self-hosted) | Variable | $0 | Infrastructure cost |

**Strategic Rationale**:
- **Cloud neutrality**: Works with customer's existing infrastructure
- **Customer choice**: "Bring your own storage" reduces lock-in
- **Cost awareness**: Document cost implications, let customers decide
- **Self-hosted option**: MinIO for compliance/sovereignty requirements
- **S3-compatible**: Single implementation, multiple deployment options

**Recommendation** (not requirement):
- High egress workloads benefit from zero-egress providers (R2, Wasabi) or self-hosted (MinIO)

---

## 5. Iceberg Storage Schema

### 5.1 Table: traces

**Partition**: `date` (day-based)
**Sort Order**: `session_id, trace_id, timestamp`
**Row Groups**: One row group per session_id

**Design Decision** (See [Storage Design](storage_design.md#executive-decision--analysis)):
- ✅ **HTTP bodies stored as STRING columns** in traces table (columnar separation via Parquet)
- ✅ **Business attributes** via user-provided `sp.*` convention in attributes MAP
- ✅ **Cloudflare R2 storage** for zero-egress cost advantage

```
CREATE TABLE traces (
  -- Identifiers
  trace_id STRING,
  span_id STRING,
  parent_span_id STRING,
  session_id STRING,

  -- Application context
  app_id STRING,
  organization_id STRING,
  tenant_id STRING,

  -- Span metadata
  message_type STRING,
  span_kind STRING,
  timestamp TIMESTAMPTZ,
  end_timestamp TIMESTAMPTZ,

  -- Attributes (includes user-provided sp.* business identifiers)
  attributes MAP<STRING, STRING>,

  -- Events (metadata only, NOT full bodies)
  events ARRAY<STRUCT<
    name STRING,
    timestamp TIMESTAMPTZ,
    attributes MAP<STRING, STRING>
  >>,

  -- HTTP Bodies (stored as STRING, Parquet ZSTD compressed)
  -- Columnar format ensures these are NOT read during metadata-only queries
  http_request_method STRING,
  http_request_path STRING,
  http_request_headers STRING,    -- JSON string
  http_request_body STRING,        -- Full body (Parquet compressed)
  http_response_status_code INT,
  http_response_headers STRING,    -- JSON string
  http_response_body STRING,       -- Full body (Parquet compressed)

  -- Status
  status_code STRING,
  status_message STRING,

  -- Partition key
  record_date DATE
)
PARTITIONED BY (record_date)
SORT BY (session_id, trace_id, timestamp)
```

**Key Design Rationale**:

1. **Columnar Separation**: Parquet's columnar format provides automatic I/O separation
   - Query: `SELECT session_id FROM traces WHERE attributes['sp.user.id'] = 'user-123'`
   - Only reads: session_id, attributes columns (NOT http_request_body, http_response_body)

2. **User-Provided Business Attributes**: `sp.*` convention in attributes MAP
   - Example: `attributes['sp.user.id'] = 'user-123'`
   - Example: `attributes['sp.order.id'] = 'ORD-456'`
   - Users instrument their code to add business context

3. **Bodies as STRING**: Let Parquet ZSTD compression handle it
   - No application-level compression needed
   - DuckDB can query JSON directly: `json_extract(http_request_body, '$.orderId')`

4. **Future Migration Path**: If columnar separation proves insufficient
   - Phase 2: Move bodies to separate `http_payloads` table
   - But start simple and validate performance first

### 5.2 Table: logs

**Partition**: `date` (day-based)
**Sort Order**: `session_id, timestamp`
**Row Groups**: One row group per session_id (aligned with traces)

```
CREATE TABLE logs (
  session_id STRING,
  timestamp TIMESTAMP,
  observed_timestamp TIMESTAMP,
  severity_number INT,
  severity_text STRING,
  body STRING,
  attributes MAP<STRING, STRING>,
  resource_attributes MAP<STRING, STRING>,
  date DATE  -- partition key
)
PARTITIONED BY (date)
```

### 5.3 Table: metrics (Experimental)

**Partition**: `date, metric_name` (NOT session-based)
**Sort Order**: `metric_name, timestamp`
**Row Groups**: Organized by metric_name, NOT session_id

```
CREATE TABLE metrics (
  metric_name STRING,
  description STRING,
  unit STRING,
  metric_type STRING,  -- gauge, sum, histogram
  timestamp TIMESTAMP,
  value DOUBLE,
  attributes MAP<STRING, STRING>,
  resource_attributes MAP<STRING, STRING>,
  date DATE,  -- partition key
  -- partitioned by (date, metric_name)
)
PARTITIONED BY (date, metric_name)
```

---

## 6. Query Strategy

### 6.1 Grafana via DuckDB Plugin (Primary Query Interface)

**Architecture**: Grafana DuckDB Plugin → DuckDB → Iceberg Extension → S3/R2

**Query Path**:
1. User creates dashboard in Grafana with SQL queries
2. DuckDB plugin executes `iceberg_scan('s3://warehouse/traces')` queries
3. DuckDB Iceberg extension leverages:
   - **Manifest pruning**: Eliminate irrelevant data files
   - **Partition pruning**: Date-based partition elimination
   - **Row group statistics**: Fine-grained filtering via Parquet metadata
4. Results streamed back to Grafana for visualization

**Example Grafana Query**:
```sql
SELECT
  time_bucket($__interval, timestamp) AS time,
  COUNT(*) as request_count
FROM iceberg_scan('s3://warehouse/traces')
WHERE $__timeFilter(timestamp)
  AND record_date >= DATE $__timeFrom
  AND record_date <= DATE $__timeTo
GROUP BY time
ORDER BY time
```

**Benefits**:
- **No custom query API needed** - DuckDB plugin handles everything
- **Grafana native features** - Time macros, variables, query inspector
- **Direct Iceberg access** - No HTTP overhead
- **Official maintained plugin** - Updates and bug fixes from MotherDuck

**Documentation**: See [docs/grafana.md](grafana.md)

### 6.2 Programmatic Access (Future)

For programmatic access outside Grafana (e.g., Slack bots, CI/CD analytics):

**Option 1: DuckDB CLI/SDK**
```bash
duckdb -c "SELECT COUNT(*) FROM iceberg_scan('s3://warehouse/traces') WHERE session_id = 'xxx'"
```

**Option 2: Custom REST API** (if needed)
- Implement `/api/query` endpoint
- Add authentication, rate limiting
- Use DuckDB Rust bindings

**Current Status**: Not implemented - Grafana plugin satisfies all current query needs

---

## 6. Performance Considerations

### 6.1 Targets

- **Ingestion**: 10k+ spans/sec per collector instance
- **Session Queries**: Best-effort using Iceberg optimizations
- **Time-Range Queries**: Leverages partition elimination

**Note**: No specific latency targets (<100ms, <500ms) - optimize later if needed.

### 6.2 Scalability

- **Horizontal Scaling**: Multiple collector instances with consistent hashing by session_id
- **Storage**: Iceberg scales to petabytes with S3-compatible object storage
- **Query Fan-out**: Mitigated by partition pruning and row group statistics

### 6.3 Storage Maintenance (Metadata + Data)

**Problem**: Frequent commits generate many snapshots and manifest files (metadata bloat), and small Parquet files hurt read performance.

**Approach**:
- **Metadata maintenance**: Run scheduled Iceberg procedures to keep snapshot/manifest metadata compact.
  - Expire old snapshots (keep recent N hours/days for rollback and time travel)
  - Rewrite manifests to consolidate small manifest files
  - Remove orphan files to clean up unreferenced data/metadata
- **Data compaction**: Rewrite small Parquet files into target-sized files to reduce file counts and improve scan efficiency.

**Notes**:
- Maintenance runs asynchronously and does NOT change commit frequency or ingestion latency.
- Maintenance cadence and retention policy are configurable per table (traces/logs/metrics).

---

## 7. Implementation Status

See [tasks.md](../openspec/changes/add-iceberg-otlp-migration/tasks.md) for current status.

**Completed**:
- ✅ OTLP `/v1/traces` endpoint
- ✅ Session buffering for traces
- ✅ Iceberg table schema for traces
- ✅ Multi-app row-group batching

**In Progress**:
- 🔄 OTLP `/v1/logs` endpoint
- 🔄 Unified traces+logs buffering
- 🔄 OTLP `/v1/metrics` endpoint
- 🔄 Query APIs

---

## 8. Design Decisions

### Decision 1: Custom Rust OTLP Collector vs. Standard OTel Collector

**Choice**: Build a custom Rust-based OTLP collector instead of using the standard OpenTelemetry Collector
**Rationale**: Enables session-aware buffering, multi-app row-group batching, and direct Iceberg writes with fine-grained control over the storage layer
**Alternatives Considered**:
- Standard OTel Collector (lacks session buffering capability, requires custom exporters)
- OpenTelemetry Collector Contrib (harder to customize storage layer for session-aware batching)
**Trade-off**: Maintenance burden of custom collector vs. full control over buffering and storage optimization

### Decision 2: Separate Iceberg Tables per Signal Type

**Choice**: Use separate tables (`traces`, `logs`, `metrics`) instead of a unified table
**Rationale**: Optimizes schema and queries for each signal type - traces, logs, and metrics have fundamentally different access patterns and structure
**Alternatives Considered**:
- Single unified table (complex schema, harder to optimize for different query patterns)
- Separate databases (operational overhead, harder to maintain consistency)
**Implementation**: Coordinated writes to maintain session alignment across traces and logs tables

### Decision 3: Direct Iceberg Queries vs. External Metadata Index

**Choice**: Direct Iceberg queries using built-in metadata (manifests, partition stats, row group statistics) - no external index
**Rationale**: Simplicity over complexity - defer metadata optimization, avoid ETL pipeline and operational overhead
**Alternatives Considered**:
- ClickHouse/PostgreSQL metadata layer (adds complexity, operational overhead, ETL lag between raw and indexed data)
- Elasticsearch (expensive at scale, complex operational requirements)
- Embedded secondary indexes (not yet mature in Iceberg ecosystem)
**Trade-off**: Accept potentially slower queries vs. dedicated index in exchange for simpler architecture and no ETL pipeline

### Decision 4: Separate Buffers for Traces, Logs, and Metrics

**Choice**: Separate buffers for traces, logs, and metrics - each signal type buffered independently
**Rationale**:
- **Traces**: Buffered by session_id, written to `traces` table with session-based row groups
- **Logs**: Buffered independently by session_id, written to `logs` table with session-based row groups
- **Metrics**: Buffered by metric_name (NOT session), written to `metrics` table
- **Why separate?**: Unified buffer would cause Iceberg fragmentation - writing to separate tables from one buffer creates many small Parquet files
**Alternatives Considered**:
- Unified buffer for traces+logs (would cause file fragmentation and poor Iceberg compaction)
- Single buffer for all three signal types (metrics are fundamentally different - aggregations, not session-correlated)
**Implementation**:
- Independent buffers with separate flush triggers based on size/time
- Session timeout coordination (30 min) ensures traces and logs for a session are queryable around the same time
- Metrics flushed independently organized by metric_name + timestamp
**Note**: Iceberg may not be ideal for time-series metrics - consider Prometheus/InfluxDB later

### Decision 5: Multi-App Row Groups

**Choice**: One Parquet file can contain multiple sessions from different applications
**Rationale**: Reduces S3 PUT/GET operations while maintaining query efficiency via row group pruning
**Alternatives Considered**:
- One file per session (too many small files, high S3 costs, poor compaction)
- One file per app (inefficient for cross-application queries)
**Implementation**: One row group per session_id within each Parquet file, enables efficient session-level filtering

### Decision 6: Scheduled Iceberg Maintenance (Metadata + Data)

**Choice**: Add scheduled maintenance jobs for Iceberg metadata (snapshots/manifests) and data file compaction.
**Rationale**: Commit frequency cannot be reduced without adding ingestion delay, so metadata bloat must be handled by maintenance. Small Parquet files degrade query performance without compaction.
**Alternatives Considered**:
- Reduce commit frequency (adds ingestion delay)
- External metadata index (adds operational complexity)
**Implementation**:
- Metadata maintenance: snapshot expiration, manifest rewrite, orphan file cleanup
- Data maintenance: compaction to target file sizes (e.g., 64MB)

---

## 9. Open Questions

1. **Session Timeout**: Default 30 minutes inactivity - is this appropriate for all workloads?
   - Short-lived services may need shorter timeouts (5-10 minutes)
   - Long-running batch jobs may need longer timeouts (hours)
   - Consider making this configurable per application/service

2. **Large Payloads**: How to handle >10MB telemetry payloads?
   - Options: compression (gzip/zstd), chunking (split large traces), external reference (store in blob storage, reference in Iceberg)
   - Current limit: no enforced limit, but large payloads may impact buffer memory

3. **Metrics Storage**: Should we replace Iceberg with Prometheus for metrics?
   - Iceberg storage for metrics is experimental
   - Metrics are aggregations with different access patterns than traces/logs
   - Consider dedicated time-series database (Prometheus, InfluxDB, VictoriaMetrics)

4. **Metrics Aggregation Strategy**: Raw vs. pre-aggregated storage?
   - Store raw metric data points (high cardinality, high storage cost)
   - Pre-aggregate at collection time (lower storage, potential data loss)
   - Hybrid approach (raw for short retention, aggregated for long-term)

5. **Query Performance**: If direct Iceberg queries are too slow, add external index layer?
   - Current approach: rely on Iceberg's built-in metadata (manifests, partition stats, row group statistics)
   - If insufficient: consider ClickHouse/PostgreSQL metadata index or Elasticsearch
   - Monitor query performance before committing to added complexity

---

## 10. References

- **Business Vision**: [Softprobe Strategic Goals](goals.md) - Long-term vision and business context
- **Storage Design**: [HTTP Body Storage & Business Attribute Indexing](storage_design.md) - Detailed design options
- **OpenTelemetry Protocol**: https://opentelemetry.io/docs/specs/otlp/
- **Apache Iceberg**: https://iceberg.apache.org/docs/latest/
- **Iceberg Rust**: https://github.com/apache/iceberg-rust
- **OpenSpec Proposal**: [openspec/changes/add-iceberg-otlp-migration/proposal.md](../openspec/changes/add-iceberg-otlp-migration/proposal.md)
- **OpenSpec Tasks**: [openspec/changes/add-iceberg-otlp-migration/tasks.md](../openspec/changes/add-iceberg-otlp-migration/tasks.md)

---

## 11. Glossary

- **OTLP**: OpenTelemetry Protocol
- **Session**: Logical grouping of related traces and logs (NOT metrics)
- **Manifest File**: Iceberg metadata file tracking data file statistics
- **Row Group**: Unit of data organization within Parquet files
- **Predicate Pushdown**: Applying filters at storage layer for efficiency
