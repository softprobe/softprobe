# OpenTelemetry Collector with Apache Iceberg Storage

**Version**: 2.0
**Date**: 2025-01-31
**Status**: Active
**Project Type**: Standalone OTLP Collector

---

## 1. Executive Summary

### Overview

This is a standalone OpenTelemetry-compatible collector that stores traces, logs, and metrics in Apache Iceberg tables. The system provides:

- **OTLP Protocol Compliance**: Standard `/v1/traces`, `/v1/logs`, `/v1/metrics` endpoints
- **Iceberg Storage**: Scalable data lake with built-in metadata for efficient queries
- **Session Management**: Session-aware buffering for traces and logs (NOT metrics)
- **Direct Queries**: No external metadata index - leverages Iceberg's built-in capabilities

### Key Design Principles

1. **Simplicity over Complexity**: Direct Iceberg queries, no ETL pipeline
2. **OTLP Standard Compliance**: Interoperable with standard OpenTelemetry SDKs
3. **Session-Based Organization**: Traces and logs grouped by session for investigation
4. **Metrics are Different**: Metrics are aggregations, NOT session-correlated

### Goals and Non-Goals

**Goals**:
- Complete OTLP v1 API implementation (traces, logs, metrics)
- Apache Iceberg storage with efficient Parquet organization
- Direct Iceberg queries leveraging built-in metadata
- Session management for traces and logs (coordinated buffering and storage)
- Horizontal scalability with consistent hashing

**Non-Goals**:
- OTLP exporter functionality (collector is ingestion-only)
- Data transformation or sampling (stores raw telemetry as received)
- Java integration (standalone Rust service)
- External metadata indexing layer (using Iceberg's built-in capabilities)
- Real-time streaming queries (batch-oriented query model)

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
│ ├─ otlp_traces (session-based)    │
│ ├─ otlp_logs (session-based)      │
│ └─ otlp_metrics (metric_name-based)│
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
- **Storage**: Writes to `otlp_traces` table with session-based row groups

**Logs Buffer**:
- **Buffer Organization**: HashMap<session_id, Vec<LogRecord>> OR global Vec<LogRecord> with session tracking
- **Flush Triggers**:
  - Size: ~128MB buffer size (independent tracking)
  - Age: 60 seconds
  - Session timeout: 30 minutes (same timeout as traces for consistency)
- **Storage**: Writes to `otlp_logs` table with session-based row groups

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

## 4. Iceberg Storage Schema

### 4.1 Table: otlp_traces

**Partition**: `date` (day-based)
**Sort Order**: `session_id, trace_id, timestamp`
**Row Groups**: One row group per session_id

```
CREATE TABLE otlp_traces (
  trace_id STRING,
  span_id STRING,
  parent_span_id STRING,
  session_id STRING,
  name STRING,
  kind STRING,
  start_timestamp TIMESTAMP,
  end_timestamp TIMESTAMP,
  status_code STRING,
  status_message STRING,
  attributes MAP<STRING, STRING>,
  events ARRAY<STRUCT<...>>,
  links ARRAY<STRUCT<...>>,
  resource_attributes MAP<STRING, STRING>,
  date DATE  -- partition key
)
PARTITIONED BY (date)
```

### 4.2 Table: otlp_logs

**Partition**: `date` (day-based)
**Sort Order**: `session_id, timestamp`
**Row Groups**: One row group per session_id (aligned with traces)

```
CREATE TABLE otlp_logs (
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

### 4.3 Table: otlp_metrics (Experimental)

**Partition**: `date, metric_name` (NOT session-based)
**Sort Order**: `metric_name, timestamp`
**Row Groups**: Organized by metric_name, NOT session_id

```
CREATE TABLE otlp_metrics (
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

## 5. Query Strategy

### 5.1 Direct Iceberg Queries

**No External Metadata Index**: Queries leverage Iceberg's built-in metadata.

**Query Path**:
1. Client sends query with predicates (session_id, time_range, attributes)
2. Iceberg uses **manifest files** to eliminate irrelevant data files
3. **Partition pruning** eliminates entire date partitions
4. **Row group statistics** (min/max) filter out irrelevant row groups
5. Read matching Parquet data

### 5.2 Session Retrieval

```rust
GET /v1/query/session/{session_id}

// Parallel Iceberg scans:
let traces = iceberg_table("otlp_traces")
    .scan()
    .filter(col("session_id").eq(session_id))
    .collect();

let logs = iceberg_table("otlp_logs")
    .scan()
    .filter(col("session_id").eq(session_id))
    .collect();

// Merge by timestamp and return unified view
```

### 5.3 Time-Range Queries

```rust
POST /query with {
  start_time: "2025-01-30T00:00:00Z",
  end_time: "2025-01-31T00:00:00Z",
  service_name: "api-gateway"
}

// Iceberg automatically:
// 1. Eliminates partitions outside date range (Jan 29, Feb 1+)
// 2. Uses manifest files to find relevant data files
// 3. Applies predicate pushdown for service_name
```

### 5.4 Query Optimization

**Iceberg Built-in Optimizations**:
- **Manifest Pruning**: Manifest files track column min/max per data file
- **Partition Elimination**: Date partitioning eliminates entire days
- **Row Group Statistics**: Parquet row group min/max for fine-grained filtering
- **Predicate Pushdown**: Filters applied at storage layer

**Trade-off**: Potentially slower queries vs. ClickHouse, but simpler architecture.

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

**Choice**: Use separate tables (`otlp_traces`, `otlp_logs`, `otlp_metrics`) instead of a unified table
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
- **Traces**: Buffered by session_id, written to `otlp_traces` table with session-based row groups
- **Logs**: Buffered independently by session_id, written to `otlp_logs` table with session-based row groups
- **Metrics**: Buffered by metric_name (NOT session), written to `otlp_metrics` table
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
