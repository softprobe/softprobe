# Design Document for MongoDB to Iceberg Migration with OpenTelemetry Integration

**Version**: 1.0  
**Date**: 2025-01-31  
**Status**: Draft - Pending Review  
**Authors**: Architecture Team

---

## 1. Executive Summary

### Problem Statement

MongoDB is experiencing scalability issues with high-volume recording/replay data. Current architecture stores all SpMocker data in MongoDB collections, leading to:

- High storage costs (50KB per document with payloads)
- Query performance degradation at scale
- Limited support for analytical workloads
- Inability to efficiently query by session_id and trace_id across large datasets

### Solution Overview

Migrate recording/replay data to Apache Iceberg while maintaining OpenTelemetry compatibility. Architecture follows a phased approach:

- **Short-term**: Forward to OpenTelemetry Collector API (standard OTLP endpoints)
- **Long-term**: Full OpenTelemetry API migration
- **Storage**: Iceberg raw data lake with multi-app row groups
- **Metadata**: Real-time ETL extracts lightweight metadata for high-frequency queries
- **Query Routing**: Metadata points to Iceberg for actual message retrieval

### Key Benefits

- 90%+ storage cost reduction (object storage vs MongoDB)
- Sub-second query latency for metadata lookups
- Scalable to tens of billions of messages
- OpenTelemetry-compatible for future extensibility
- Efficient session and trace-level queries via row group co-location

---

## 2. Architecture Overview

### 2.1 Data Flow Diagram

```
┌──────────────┐
│ Java Agent   │ (SpMocker data)
│ (sp-agent)  │
└──────┬───────┘
       │ HTTP POST /api/storage/record/batchSaveMockers
       │ (Zstd-compressed JSON)
       ▼
┌──────────────┐
│ sp-storage   │ ──► Reads: MongoDB (config only)
│ (Java Proxy) │ ──► Writes: OpenTelemetry Collector API (short-term)
└──────┬───────┘     └─► OTLP API (long-term)
       │
       │ HTTP POST /v1/traces (OTLP protobuf/JSON)
       │
       ▼
┌──────────────────────────────┐
│ Rust OTLP Collector          │ ──► Buffers by session_id, groups by trace_id
└──────┬───────────────────────┘
       │
       │ Batched Parquet writes
       │ (multi-app row groups)
       ▼
┌──────────────────────┐
│ Iceberg Raw Table    │ ──► Parquet files in object storage
│ (raw_sessions)       │ ──► Partitioned by date only (no app bucketing)
└──────┬───────────────┘
       │
       │ Real-time ETL (Python/PyArrow)
       └─► ClickHouse metadata tables (ALL metadata in ClickHouse)
           ├─ session_metadata (session summaries)
           ├─ recording_events (per-span index)
           ├─ recording_ops_minute (time-series aggregation)
           └─ recording_tag_counts (tag analytics)
```

### 2.2 Component Architecture

| Component | Responsibility | Technology |
|-----------|---------------|------------|
| **Rust OTLP Collector** | Receive OTLP spans, buffer by session_id, flush to Iceberg | Rust (Axum/Tonic), Tokio |
| **Iceberg Raw Table** | Store all raw spans/messages in Parquet | Apache Iceberg on S3/OSS |
| **Real-time ETL** | Extract business metadata, populate ClickHouse tables | Python (PyArrow/Polars) |
| **ClickHouse Metadata** | ALL metadata queries (sessions, traces, aggregations) | ClickHouse MergeTree |
| **Java Query Client** | ClickHouse metadata filtering + Iceberg scan API for full data | Java (JDBC + Iceberg Java SDK) |

---

## 3. OpenTelemetry Integration Strategy

### 3.1 Short-Term: Rust OTLP Collector

**Current State**: Java agent sends SpMocker data to `sp-storage` service

**Migration Path**:

1. `sp-storage` forwards SpMocker data to the Rust OTLP Collector
2. Rust Collector exposes OTLP `/v1/traces` (protobuf and/or JSON)
3. Collector performs session-aware buffering and writes directly to Iceberg

**OTLP Endpoint Mapping**:

- SpMocker → OTLP Span
- `recordId` → `trace_id` (preserves original trace_id from OTLP)
- `operationName` → `span.name`
- `appId` → `resource.attributes["sp.app.id"]`
- `sessionId` → `span.attributes["sp.session.id"]` (extracted from OTLP span attributes, used for buffering/grouping)
- Payloads → `span.events[]` with `name="sp.body"`

**Key Design Clarification**:
- `trace_id` remains the standard OTLP trace identifier
- `session_id` is extracted from span attributes and used as the primary key for session-aware buffering and row group organization
- If `session_id` is missing from span attributes, we can use `trace_id` as the session grouping key
- Sort order in Iceberg: `session_id ASC, trace_id ASC, timestamp ASC` to keep sessions contiguous

Implementation notes:
- Supports OTLP HTTP `/v1/traces` (protobuf and JSON)
- Session-aware buffering keyed by `session_id` (extracted from span attributes)
- Multi-app row-group formation; direct Parquet writes to Iceberg

### 3.2 Long-Term: Direct OTLP API

**Future State**: Java agent sends directly to OTLP-compatible backend

- Agent uses OpenTelemetry SDK
- Direct HTTP/gRPC to OTLP ingestion service
- No intermediate collector required

---

## 4. Iceberg Raw Data Storage

### 4.1 Table Schema Design

**Table Name**: `raw_sessions`

**Partitioning Strategy**: Multi-level partitioning for query efficiency
 
- Primary: `date_trunc('day', timestamp)` (daily partitions)

**Sort Order**: Optimized for session/trace lookups
 
- Sort by: `session_id ASC, trace_id ASC, timestamp ASC`
- Ensures session data is co-located in row groups

**Schema Definition**:

```sql
CREATE TABLE raw_sessions (
  -- Primary Identifiers
  session_id        STRING NOT NULL,      -- Session identifier (grouping key)
  trace_id          STRING NOT NULL,      -- OTLP trace ID (record ID)
  span_id           STRING NOT NULL,       -- OTLP span ID
  parent_span_id    STRING,               -- OTLP parent span ID
  
  -- Application Context
  app_id            STRING NOT NULL,      -- Application identifier
  organization_id   STRING,                -- Organization identifier
  tenant_id         STRING,                -- Multi-tenancy support
  
  -- Span Metadata
  message_type      STRING NOT NULL,      -- Operation name (span.name)
  span_kind         STRING,                -- OTLP span kind (SERVER, CLIENT, etc.)
  timestamp         TIMESTAMP NOT NULL,    -- Span start time
  end_timestamp     TIMESTAMP,            -- Span end time
  
  -- Attributes (Metadata only, no large payloads)
  attributes        MAP<STRING, STRING>,   -- OTLP attributes (tags, headers, etc.)
  
  -- Events (Payloads stored here)
  events            ARRAY<STRUCT<
    name            STRING,
    timestamp       TIMESTAMP,
    attributes      MAP<STRING, STRING>    -- Contains payloads in attributes["sp.body"]
  >>,
  
  -- Status
  status_code       STRING,                -- OK, ERROR, UNSET
  status_message    STRING,                -- Optional error message
  
  -- Derived Partition Key
  record_date       DATE NOT NULL          -- Derived from timestamp
)
USING iceberg
PARTITIONED BY (
  record_date                               -- Daily partitions
)
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'format-version' = '2',
  'write.target-file-size-bytes' = '134217728',  -- 128MB target
  'write.sort-order' = 'session_id ASC NULLS FIRST, trace_id ASC NULLS FIRST, timestamp ASC NULLS FIRST',
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.compression-level' = '3'
);
```

### 4.2 Multi-App Row Group Strategy

**Goal**: Store messages from multiple apps in same Parquet file, grouped into row groups by session_id for fast reads

**Row Group Structure**:

- Each row group contains messages from one session
- Multiple sessions (from different apps) can share same Parquet file
- Sort order ensures session data is contiguous within row groups

**File Layout** (pseudo-example):

```
File: part-00001.parquet
  Row Group 0: session_id='s1', app_id='app1', [span1, span2, span3]
  Row Group 1: session_id='s2', app_id='app2', [span4, span5]
  Row Group 2: session_id='s3', app_id='app1', [span6, span7, span8]
```

**Benefits**:

- Reduce S3 PUT/GET operations (batch writes)
- Fast session replay (read single row group)
- Efficient trace lookup (min/max stats enable row group pruning)
- Messages from multiple apps stored together (cost-efficient)

### 4.3 Buffering and Flush Logic

**Session Buffering**:

- Gateway/ingestion service buffers spans by `session_id`
- Consistent hashing ensures same session hits same worker
- Session boundaries determined by timeout (default: 30 minutes) or explicit end flag

**Flush Triggers** (per file writer):

- Writer open time ≈ 60 seconds (rolling window)
- File size ≈ 128 MB
- Session explicitly ends or times out

**Buffering Implementation** (pseudo-code):

```rust
async fn ingest(span: Span) -> Result<()> {
    let key = SessionKey::new(&span.app_id, &span.session_id);
    let buf = buffers.entry(key.clone()).or_insert_with(SessionBuffer::new);
    buf.push(span);

    if buf.should_flush() {
        // Sort spans by timestamp within session
        buf.spans.sort_by_key(|s| s.timestamp);
        
        // Write as row group to current Parquet file
        let row_group = parquet::write_row_group(&buf.spans)?;
        current_file.add_row_group(row_group);
        
        // Flush file if needed
        if current_file.size() >= 128MB || current_file.age() >= 60s {
            let file_path = current_file.flush_to_iceberg()?;
            metadata_tx.send(SessionMetadata {
                session_id: key.session_id,
                file_uri: file_path,
                row_group_index: current_file.row_group_count() - 1,
                row_count: buf.spans.len(),
            })?;
            current_file = new_file();
        }
        
        buf.clear();
    }
    Ok(())
}
```

---

## 5. Real-Time ETL for Metadata Extraction

### 5.1 ETL Pipeline Architecture

**Trigger**: New Parquet files written to Iceberg

- Watch for Iceberg snapshot updates using `IncrementalAppendScan` API
- Trigger Python ETL worker per new snapshot

**Processing Flow**:

1. Discover new Parquet files via Iceberg catalog (not filesystem scanning)
2. Read files via PyArrow with S3 support
3. Extract business metadata per span (NO payload bodies, NO file pointers)
4. Aggregate for time-series and session summary tables
5. Write ALL metadata to ClickHouse

**Key Principles**:
- Metadata-only extraction, payloads remain in Iceberg raw table
- NO storage of Iceberg internal paths (file URIs, row group indices) in ClickHouse
- Use Iceberg scan API with predicates for data retrieval

### 5.2 ClickHouse Metadata Tables

#### 5.2.1 Time-Series Aggregation Table

**Table**: `recording_ops_minute`

**Purpose**: Fast operation count queries by app_id, message_type, time bucket

```sql
CREATE TABLE recording_ops_minute ON CLUSTER default
(
  bucket_minute   DateTime,
  app_id          String,
  message_type    LowCardinality(String),
  record_environment Int32,              -- 0=unknown, 1=dev, 2=prod
  message_count   UInt64,
  error_count     UInt64                -- Count of spans with status_code='ERROR'
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMMDD(bucket_minute)
ORDER BY (app_id, message_type, record_environment, bucket_minute);
```

**ETL Job** (Flink pseudo-code):

```scala
rawSessions
  .flatMap(explodeSpans)                    // One span per row
  .assignTimestampsAndWatermarks(...)        // Event time
  .keyBy(span => (span.appId, span.messageType, span.recordEnvironment))
  .window(TumblingEventTimeWindows.of(Time.minutes(1)))
  .aggregate(new CountAggregator)            // Count per bucket
  .sinkTo(clickhouseSink("recording_ops_minute"))
```

#### 5.2.2 Session/Trace Index Table

**Table**: `recording_events`

**Purpose**: Fast metadata filtering and existence checks for traces

```sql
CREATE TABLE recording_events ON CLUSTER default
(
  trace_id        String,                   -- Record ID (primary lookup key)
  session_id      String,                   -- Session ID (grouping key)
  app_id          String,
  message_type    LowCardinality(String),
  timestamp       DateTime64(3),
  end_timestamp   DateTime64(3),
  status_code     String,
  record_date     Date                      -- For partition pruning
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (trace_id, timestamp);             -- Optimized for trace_id lookup

-- Secondary index for session_id queries
ALTER TABLE recording_events ADD INDEX session_idx session_id TYPE bloom_filter GRANULARITY 1;
```

**ETL Job**: Extract per-span business metadata only (no file pointers)

**Purpose**:
- Fast existence checks: "Does this trace_id exist?"
- Metadata filtering: "Find traces by app_id, time range, status"
- NOT for storing file locations (Iceberg manages its own files)

#### 5.2.3 Tag Analytics Table

**Table**: `recording_tag_counts`

**Purpose**: Tag-based filtering and analytics

```sql
CREATE TABLE recording_tag_counts ON CLUSTER default
(
  bucket_hour     DateTime,
  app_id          String,
  tag_key         String,                   -- e.g., "geo", "user_type"
  tag_value       String,                   -- e.g., "us-east", "premium"
  total_count     UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMMDD(bucket_hour)
ORDER BY (app_id, tag_key, tag_value, bucket_hour);
```

#### 5.2.4 Session Metadata Table

**Table**: `session_metadata`

**Purpose**: Session-level summaries for efficient session queries

```sql
CREATE TABLE session_metadata ON CLUSTER default
(
  session_id      String,
  app_id          String,
  start_timestamp DateTime64(3),
  end_timestamp   DateTime64(3),
  message_count   UInt64,
  size_bytes      UInt64,
  record_date     Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(record_date)
ORDER BY (session_id, app_id);
```

**ETL Job**: Aggregate per-session statistics from span metadata

**Purpose**:
- Fast session existence and metadata checks
- Session-level aggregations (count, size, time range)
- Filter sessions before querying Iceberg for full data
- NO file segment tracking (Iceberg catalog handles file discovery)

---

## 6. Query Strategy

### 6.1 High-Frequency Query Patterns

#### 6.1.1 Find by Record ID (trace_id)

**Query Pattern**: `SELECT * WHERE trace_id = ? AND app_id = ?`

**Implementation**:

1. Query `recording_events` in ClickHouse to verify existence and get metadata (sub-100ms)
2. Use Iceberg scan API with predicates to retrieve full span data
3. Iceberg uses manifest files + statistics for efficient file/row group pruning
4. Return full span data with payloads

**Example**:

```java
// Step 1: Fast metadata check (ClickHouse)
RecordingEvent event = clickhouse.query(
    "SELECT trace_id, timestamp, record_date FROM recording_events " +
    "WHERE trace_id = ? AND app_id = ?", traceId, appId
).orElseThrow(() -> new RecordingNotFound());

// Step 2: Query Iceberg with predicates (Iceberg finds files internally)
Table table = catalog.loadTable("raw_sessions");
TableScan scan = table.newScan()
    .filter(Expressions.equal("trace_id", traceId))
    .filter(Expressions.equal("app_id", appId));

// Iceberg internally:
// - Reads manifest list to find relevant manifests
// - Filters data files using min/max statistics
// - Prunes row groups using Parquet metadata
// - Returns only matching rows

try (CloseableIterable<Record> records = scan.planFiles()) {
    for (Record record : records) {
        // Process full span with payloads
    }
}
```

#### 6.1.2 Find Session by session_id

**Query Pattern**: `SELECT * WHERE session_id = ? AND app_id = ?`

**Implementation**:

1. Query `session_metadata` in ClickHouse for session summary and time bounds
2. Use Iceberg scan API with session_id predicate and timestamp range
3. Iceberg efficiently finds all spans for the session across files
4. Return all spans sorted by timestamp

**Example**:

```java
// Step 1: Get session summary from ClickHouse
SessionMetadata session = clickhouse.query(
    "SELECT session_id, start_timestamp, end_timestamp, message_count " +
    "FROM session_metadata WHERE session_id = ? AND app_id = ?",
    sessionId, appId
).orElseThrow(() -> new SessionNotFound());

// Step 2: Query Iceberg with predicates
Table table = catalog.loadTable("raw_sessions");
TableScan scan = table.newScan()
    .filter(Expressions.equal("session_id", sessionId))
    .filter(Expressions.equal("app_id", appId))
    // Optional: add timestamp range for better pruning
    .filter(Expressions.greaterThanOrEqual("timestamp", session.startTimestamp))
    .filter(Expressions.lessThanOrEqual("timestamp", session.endTimestamp));

// Iceberg handles file discovery and row group pruning automatically
List<Span> spans = new ArrayList<>();
try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(scan.filter()).build()) {
    for (Record record : records) {
        spans.add(convertToSpan(record));
    }
}

// Sort by timestamp (sessions are already sorted within row groups)
spans.sort(Comparator.comparing(Span::getTimestamp));
return spans;
```

#### 6.1.3 Operation Count Analytics

**Query Pattern**: Time-series aggregation by app_id, message_type

**Implementation**:

- Query `recording_ops_minute` in ClickHouse (pre-aggregated)
- No Iceberg access required for counts
- Returns counts only, not actual spans

#### 6.1.4 Find by App ID and Time Range

**Query Pattern**: `SELECT * WHERE app_id = ? AND timestamp BETWEEN ? AND ?`

**Implementation**:

1. Optionally query ClickHouse `recording_events` for count estimation
2. Use Iceberg scan API with app_id and timestamp predicates
3. Leverage Iceberg partition pruning (by record_date) and statistics
4. Return full span data

**Example**:

```java
// Optional: Get count estimate from ClickHouse
long estimatedCount = clickhouse.queryCount(
    "SELECT count(*) FROM recording_events " +
    "WHERE app_id = ? AND timestamp BETWEEN ? AND ?",
    appId, startTime, endTime
);

// Query Iceberg directly with predicates
Table table = catalog.loadTable("raw_sessions");
TableScan scan = table.newScan()
    .filter(Expressions.equal("app_id", appId))
    .filter(Expressions.greaterThanOrEqual("timestamp", startTime))
    .filter(Expressions.lessThanOrEqual("timestamp", endTime));

// Iceberg automatically:
// - Prunes partitions by record_date
// - Filters files by app_id and timestamp statistics
// - Reads only relevant row groups

try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(scan.filter()).build()) {
    for (Record record : records) {
        // Process spans
    }
}
```

### 6.2 Java Query Client Architecture

Instead of a centralized query router, single-tenant Java services link against a shared `recording-query-client` library. The module owns:

- JDBC connection pools for ClickHouse metadata queries (session/trace filters, aggregations)
- Iceberg Java SDK client for catalog-based data retrieval
- NO direct file path manipulation - all Iceberg access via catalog scan API
- Deterministic ordering and cursor-based pagination utilities following `(timestamp ASC, trace_id ASC, span_id ASC)`
- Micrometer metrics and structured logs measuring metadata latency, Iceberg scan latency, and bytes scanned

**Key Architecture Principles**:
- ClickHouse stores ONLY business metadata (no file pointers)
- Iceberg catalog manages all file locations and metadata
- Use Iceberg scan API with predicates for efficient data retrieval
- Loose coupling between ClickHouse and Iceberg

```java
public final class RecordingQueryClient implements AutoCloseable {
    private final ClickHouseMetadataRepository metadata;
    private final IcebergCatalog icebergCatalog;
    private final Table rawSessionsTable;

    public RecordingQueryClient(DataSource clickhouse, String catalogUri) {
        this.metadata = new ClickHouseMetadataRepository(clickhouse);
        this.icebergCatalog = loadCatalog(catalogUri);
        this.rawSessionsTable = icebergCatalog.loadTable("raw_sessions");
    }

    public Recording findByTraceId(String appId, String traceId) {
        // Step 1: Verify existence in ClickHouse
        RecordingEvent event = metadata
            .findByTraceId(appId, traceId)
            .orElseThrow(() -> new RecordingNotFound(traceId));

        // Step 2: Query Iceberg with predicates (NO file pointers!)
        TableScan scan = rawSessionsTable.newScan()
            .filter(Expressions.equal("trace_id", traceId))
            .filter(Expressions.equal("app_id", appId));

        return readFirstSpan(scan);
    }

    public List<Span> fetchSession(String appId, String sessionId) {
        // Step 1: Get session metadata from ClickHouse
        SessionMetadata session = metadata
            .findSession(appId, sessionId)
            .orElseThrow(() -> new SessionNotFound(sessionId));

        // Step 2: Query Iceberg with predicates
        TableScan scan = rawSessionsTable.newScan()
            .filter(Expressions.equal("session_id", sessionId))
            .filter(Expressions.equal("app_id", appId))
            .filter(Expressions.greaterThanOrEqual("timestamp", session.startTimestamp))
            .filter(Expressions.lessThanOrEqual("timestamp", session.endTimestamp));

        return readAllSpans(scan);
    }
}
```

### 6.3 Why NOT Store File Pointers in ClickHouse

**Design Decision**: ClickHouse stores ONLY business metadata, NOT Iceberg internal paths.

**Rationale**:

1. **Compaction Safety**: Iceberg's compaction/optimization operations rewrite files. Storing file paths in ClickHouse would create stale references that break on every compaction.

2. **Iceberg's Metadata System**: Iceberg maintains a sophisticated metadata hierarchy (snapshots → manifest lists → manifests → data files). Bypassing this by storing file paths loses:
   - Partition pruning
   - File-level statistics (min/max, null counts)
   - Row group statistics
   - Schema evolution support
   - Time travel capabilities

3. **Loose Coupling**: ClickHouse and Iceberg operate independently. ClickHouse doesn't need to know about Iceberg's file layout changes.

4. **Maintenance Burden**: Storing file pointers would require complex synchronization logic to update ClickHouse every time Iceberg rewrites files.

**How Iceberg Scan API is Efficient**:

When querying with predicates like `session_id = 's123'`:
1. **Snapshot metadata** (in-memory): O(1) lookup of current snapshot
2. **Manifest list** (single S3 GET ~KB): Lists which manifests to check
3. **Manifest files** (parallel S3 GETs ~MB): Filter data files by column statistics
4. **Data file reads** (only matching files): Read specific row groups using Parquet metadata

Result: 90%+ of files skipped without reading them.

### 6.4 Query Performance Optimization

**ClickHouse Optimizations**:
- Partition pruning by record_date
- Bloom filter indexes on session_id
- Pre-aggregated time-series data in `recording_ops_minute`

**Iceberg Optimizations**:
- Partition pruning (record_date)
- File-level statistics (min/max per column)
- Row group statistics (Parquet metadata)
- Z-ordering/sorting by session_id for better co-location

**Client-side Caching**:
- Metadata query results cached briefly (5 min TTL)
- Optional span-level cache for hot data (<1 hour old)
- Iceberg connection pool for reuse

---

### 6.5 Compatibility with Existing MongoDB Queries (Design)

This migration must preserve current query capabilities while improving performance. Without changing any table DDL at this stage, we define how existing query parameters are served by the new query path.

- Core recording filters
  - appId, beginTime/endTime, operationName, recordEnvironment, categoryType
  - Served by: metadata index (recording_events) with fast filters; pointers to Iceberg rows for payload retrieval. ETL extracts low-cardinality fields (categoryType, recordEnvironment) into explicit metadata fields for speed.

- Direct record lookup
  - recordId (trace_id)
  - Served by: recording_events primary lookup → single-row pointer → direct Iceberg row access.

- Tag filtering
  - tags.key = value
  - Served by: tag analytics path. ETL explodes attributes into tag counts (for aggregates) and a tag index (for filter-by-tag to raw). Counts via recording_tag_counts; pointer retrieval via tag index to batch-read Iceberg rows. No DDL change required here; extraction defines the surfaced columns.

- Secondary filters
  - recordVersion, recordEnvironment, categoryType
  - Served by: fields extracted into the metadata index for fast filters (no schema changes required now; extraction config will surface them).

- Configuration queries
  - appId, appName, key; serviceId, operationId
  - Remain in MongoDB (unchanged), cached where appropriate.

- Historical/analytical
  - Trend, cross-application, audit-like queries
  - Served via Iceberg scans with partition pruning by date (and app), avoiding MongoDB pressure. Latency acceptable for low-frequency workloads.

Notes
- This section specifies behavior and ETL extraction only; concrete DDL and new columns are deferred. The implementation will ensure the metadata index exposes the above fields for fast filters without altering raw storage.

Out of Scope (moved to separate design)
- Replay/Execution query flows (planId, planItemId, contextIdentifier, sendStatus, compareStatus) and compare-result drill-down are documented in a separate design: see `docs/replay-design.md`.

## 7. Migration Strategy

### 7.1 Phase 1: Rust OTLP Collector Implementation (Weeks 1-2)

**Goal**: Build and deploy a Rust OTLP Collector and forward SpMocker data to it

**Tasks**:

1. Implement Rust OTLP Collector `/v1/traces` endpoint (protobuf/JSON)
2. Add session-aware buffering and multi-app row-group batching
3. Implement direct Iceberg Parquet writes with required sort order
4. Modify `sp-storage` to forward to Rust Collector `/v1/traces`

**Validation**:

- Dual-write: Continue writing to MongoDB (fallback)
- Validate OTLP format correctness
- Monitor ingestion rate and latency

**Deliverables**:

- Rust OTLP Collector operational
- `sp-storage` forwarding to Rust Collector
- Dual-write validation successful

### 7.2 Phase 2: Iceberg Ingestion Service (Weeks 3-6)

**Goal**: Build service to receive OTLP spans and write to Iceberg

**Tasks**:

1. Build Rust/Java service accepting OTLP `/v1/traces` endpoint
2. Implement session buffering (group by session_id)
3. Implement multi-app row group batching
4. Write Parquet files to Iceberg with proper sort order
5. Implement flush triggers (time, size, session end)

**Deliverables**:

- OTLP ingestion service operational
- Iceberg raw_sessions table created
- Data flowing from Rust Collector → Iceberg
- Multi-app row groups working correctly

### 7.3 Phase 3: PyArrow ETL Service (Weeks 7-10)

**Goal**: Extract metadata and populate ClickHouse tables using lightweight PyArrow service

**Tasks**:

1. Implement PyArrow ETL service with file watcher for new Parquet files
2. Build metadata extraction using Polars for fast vectorized processing
3. Extract metadata per span (no payloads) with precise file coordinates
4. Populate `recording_ops_minute`, `recording_events`, `recording_tag_counts` via ClickHouse batch writes
5. Populate Iceberg `session_metadata` table with session-level aggregations
6. Add state tracking (SQLite) for processed files and error recovery

**Technology Stack**:
- **PyArrow/Polars**: Fast Parquet reading and data transformations
- **ClickHouse-Connect**: Efficient batch writes to ClickHouse
- **File System Monitoring**: Timestamp-based discovery of new Parquet files
- **SQLite**: Simple state persistence for processed files tracking

**Deliverables**:

- Lightweight ETL service operational (~200MB memory footprint)
- ClickHouse tables populated with exact Iceberg file coordinates
- Metadata queries working with sub-100ms latency
- ETL lag <2 minutes (30s polling + processing time)
- Docker containerized deployment

### 7.4 Phase 4: Java Direct-Query Integration (Weeks 11-12)

**Goal**: Deliver the shared Java query client and wire it into existing services.

**Tasks**:

1. Implement `recording-query-client` library (ClickHouse DAO, DuckDB/Trino Iceberg reader, caching, telemetry).
2. Integrate library with `sp-storage` and replay services for record/session lookups.
3. Roll out configuration and credentials for ClickHouse/Iceberg across on-prem environments.
4. Validate deterministic ordering, cursor pagination, and caching behavior under load.

**Deliverables**:

- Java query client published and documented.
- Core services using direct ClickHouse/Iceberg access.
- All query patterns validated end-to-end.
- Performance benchmarks met with cache hit metrics instrumented.

---

## 8. Performance Targets

| Metric | Target | Measurement |
|--------|--------|--------------|
| **Ingestion Rate** | 10,000+ spans/sec | Throughput at Rust OTLP Collector |
| **Metadata Query Latency** | <100ms P95 | ClickHouse queries (by record_id, app_id) |
| **Session Query Latency** | <500ms P95 | Iceberg read with row group pruning |
| **Storage Cost** | 90% reduction vs MongoDB | Object storage vs MongoDB costs |
| **Availability** | 99.95% | Uptime of ingestion pipeline and Java query clients (core services) |
| **ETL Lag** | <5 minutes | Time from Iceberg write to metadata availability |

---

## 9. Risk Assessment & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| OTLP Collector bottleneck | High | Horizontal scaling, load balancing, consistent hashing |
| Iceberg write latency | Medium | Batching, async writes, background flush, size-based triggers |
| ClickHouse metadata lag | Low | Real-time ETL with low latency, eventual consistency acceptable |
| Row group size too large | Medium | Tune row group size (128MB), split long sessions across multiple row groups |
| Query performance degradation | High | Partition pruning, metadata caching, query optimization, row group statistics |
| Data loss during migration | Critical | Dual-write validation, transaction safety, rollback procedures |
| Session boundary detection | Medium | Timeout-based (30 min default) + explicit end flag, configurable timeout |

---

## 10. Monitoring & Observability

### 10.1 Key Metrics

**Ingestion Metrics**:

- Rust OTLP Collector: request rate, error rate, buffer size, latency
- Iceberg Writer: write latency, file size, row group count, flush frequency
- PyArrow ETL Service: lag, processing rate, errors, throughput, file discovery rate

**Query Metrics**:

- ClickHouse: query latency, cache hit rate, query rate
- Iceberg: scan latency, row groups read, partition pruning efficiency
- Java query client: end-to-end latency, cache hit ratio, query type distribution

**Storage Metrics**:

- Iceberg: total size, file count, partition distribution, snapshot count
- S3: storage size, PUT/GET request counts, data transfer costs
- ClickHouse: table size, partition count, merge operations

### 10.2 Dashboards

- **Ingestion Dashboard**: Throughput, latency, errors, buffer sizes
- **Query Dashboard**: Query latency by type, cache hit rate, error rate
- **Storage Dashboard**: Total size, file count, partition distribution, cost trends
- **ETL Dashboard**: ETL lag, processing rate, error rate, throughput

### 10.3 Alerting

- Ingestion rate drops below 80% of baseline
- Query latency P95 exceeds 2x target
- ETL lag exceeds 10 minutes
- Error rate exceeds 1% for any component
- Storage costs exceed budget threshold

---

## 11. Cost Analysis

### 11.1 Storage Costs (Estimated for 10TB/year)

| Component | Size (1 year) | Cost |
|-----------|---------------|------|
| Iceberg Raw Data (S3) | 10TB | $230/year (S3 Standard) |
| Iceberg Metadata | 100GB | $2.30/year |
| ClickHouse Metadata | 500GB | $11.50/year |
| **Total** | **10.6TB** | **~$244/year** |

**MongoDB Equivalent**: ~$2,400/year (10TB at $0.24/GB/month)

**Savings**: 90% reduction

### 11.2 Query Costs

- S3 GET requests: ~$0.0004 per 1,000 requests
- ClickHouse compute: Included in infrastructure costs
- Iceberg metadata queries: Minimal (metadata small)
- Estimated query cost: ~$50/year for 100M queries

### 11.3 Infrastructure Costs

- ETL Cluster (Flink): ~$500/month (scalable based on load)
- ClickHouse Cluster: ~$300/month (depends on query volume)
- Rust OTLP Collector: ~$200/month (stateless, scales horizontally)

**Total Infrastructure**: ~$1,000/month (~$12,000/year)

**Total Annual Cost**: ~$12,244/year (vs $2,400/year for MongoDB storage only, excluding compute)

**Note**: Infrastructure costs are higher but provide better scalability and performance. Cost savings primarily in storage.

---

## 12. Open Questions

1. **Session timeout**: Default timeout for session boundaries? (Recommended: 30 minutes inactivity)
2. **Large payload handling**: Store payloads >10MB separately? (Recommend: Yes, use external S3 object reference in events attributes)
3. **ETL latency**: Acceptable delay for metadata extraction? (Recommended: <5 minutes)
4. **Historical data migration**: Migrate existing MongoDB data? (Recommend: Yes, phased backfill starting with oldest data)
5. **Schema evolution**: How to handle OTLP schema changes? (Recommend: Iceberg schema evolution capabilities, version metadata in ClickHouse)
6. **Multi-region support**: How to handle cross-region data replication? (Recommend: S3 cross-region replication for raw data, ClickHouse replication for metadata)

---

## 13. Success Criteria

### Phase 1 (OTLP Collector)

- [ ] OTLP Collector receiving SpMocker data
- [ ] Dual-write validation successful
- [ ] Zero data loss during migration
- [ ] OTLP format transformation correct

### Phase 2 (Iceberg Ingestion)

- [ ] Iceberg raw_sessions table operational
- [ ] 10,000+ spans/sec ingestion rate achieved
- [ ] Multi-app row groups working correctly
- [ ] Session buffering and flush logic working

### Phase 3 (ETL Pipeline)

- [ ] ClickHouse tables populated
- [ ] Metadata queries <100ms P95
- [ ] ETL lag <5 minutes
- [ ] All metadata tables updated correctly

### Phase 4 (Query Integration)

- [ ] Query service routing logic working
- [ ] Java services integrated
- [ ] All query patterns validated
- [ ] Caching layer operational

### Phase 5 (Traffic Migration)

- [ ] 100% traffic migrated to Iceberg
- [ ] Storage cost reduction >85%
- [ ] Query latency targets met
- [ ] Zero production incidents during migration

---

## 14. Appendix

### 14.1 Glossary

- **OTLP**: OpenTelemetry Protocol
- **Span**: OTLP unit of work (maps to SpMocker record)
- **Trace**: Collection of spans with same trace_id
- **Session**: Collection of spans grouped by session_id
- **Row Group**: Parquet file segment containing rows
- **Metadata**: Lightweight summary information (no payloads)
- **ETL**: Extract, Transform, Load pipeline

### 14.2 References

- OpenTelemetry Specification: https://opentelemetry.io/docs/specs/otel/
- Apache Iceberg Documentation: https://iceberg.apache.org/docs/
- ClickHouse Documentation: https://clickhouse.com/docs/
- Parquet Format Specification: https://parquet.apache.org/docs/

### 14.3 Related Documents

- `mongodb-query-parameters-analysis.md`: Query parameter analysis
- `migration-to-iceberg-design.md`: Previous design iterations (v2.1)
- `plan.md`: Implementation plan with phases

### 14.4 Design Decisions Log

**Decision 1**: Build a custom Rust OTLP Collector instead of using the standard Collector
- **Rationale**: Enables session-aware buffering, multi-app row-group batching, and direct Iceberg writes with tuned flush triggers
- **Alternatives Considered**: Standard OTel Collector (lacks session-aware buffering and row-group control), direct agent-to-backend (larger change for agents initially)

**Decision 2**: Store payloads inline in Iceberg events array
- **Rationale**: Simplifies query patterns, reduces S3 object count, enables efficient batch reads
- **Alternatives Considered**: Separate payload storage (more complex, more S3 objects)

**Decision 3**: Multi-app row groups in single Parquet file
- **Rationale**: Reduces S3 PUT/GET operations, maintains query efficiency via row group pruning
- **Alternatives Considered**: One file per session (too many files), one file per app (inefficient for multi-app queries)

**Decision 4**: ClickHouse for metadata queries
- **Rationale**: Fast OLAP queries, optimized for time-series, excellent performance for high-frequency lookups
- **Alternatives Considered**: MongoDB time-series (scalability concerns), Elasticsearch (higher cost)

**Decision 5**: Real-time ETL with <5 minute lag
- **Rationale**: Balance between freshness and performance, acceptable for most use cases
- **Alternatives Considered**: Batch ETL (higher lag, lower cost), streaming ETL (lower lag, higher complexity)

---

**Document Status**: This design document will be updated as implementation progresses and new requirements are discovered.

**Next Steps**: 
1. Review this document with architecture team
2. Get stakeholder approval
3. Begin Phase 1 implementation (OTLP Collector Integration)
