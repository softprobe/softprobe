# Migration to Session-Centric Data Lake Architecture

**Version**: 2.1  
**Date**: 2025-01-31  
**Status**: Revised - Pending Review  
**Authors**: Architecture Team

**Revision History**:
- v1.0–1.8: Initial Iceberg/DuckDB design, ingestion refinements, OTLP mapping
- v1.9: Session-centric ClickHouse metadata, redundant Kafka references removed
- v2.0 (2025-01-30): Reframed around raw OTLP session files + Flink ETL for metadata/analytics, no Kafka dependency
- v2.1 (2025-01-31): Batched multi-session Parquet files (Iceberg raw table) + ClickHouse time-series stats

---

## 1. Executive Summary

Softprobe’s long-term goal is to capture runtime messages into a semi-structured data lake, enabling multiple downstream applications (testing/replay, analytics, ML). This document defines a session-centric architecture with the following properties:

- **Ingest** raw OTLP spans/messages via a lightweight gateway (no Kafka/Flink in hot path)
- **Persist** spans/messages of each session are grouped together as much as possible so that we can read the messages of a session
efficiently for troubleshooting and replay. Data are stored in Parquet file in object storage (S3/OSS/MinIO). In order to reduce the number of S3
reads/writes, we store mulitple sessions in one single Parquet file.
- **Leave metadata & analytic tables in MongoDB** we still leave the metadata and query 
- **Expose** low-latency queries through ClickHouse (session metadata, replay indices, operation counts, tag analytics)
- **Lifecycle**: raw files 180 days, analytics tables 365 days (configurable)

Primary benefits:
- Minimal operational footprint (stateless gateway/workers, shared Flink for ETL)
- Replay-friendly (one Parquet per session)
- Analytics-friendly (Parquet columnar, ClickHouse summaries)
- Scales to tens of billions of messages with OSS cost (~$20/TB/year)

---

## 2. Architecture Overview

### 2.1 Logical Flow

```
Agents ──OTLP──► Gateway/Workers ──► Object Storage (raw session files)
                                      │
                                      ▼
                            Flink ETL (batch or streaming)
                                      │
                                      ├─► Iceberg metadata table
                                      └─► ClickHouse analytic tables
                                      
Downstream Apps: Replay UI, Analytics Dashboards, ML Pipelines
```

### 2.2 Components

| Component | Responsibility |
|-----------|----------------|
| OTLP Gateway | Receive spans/messages, route by `session_id`, buffer in memory, flush to Parquet |
| Session Worker | Performs buffering/flush logic (can be the same process as gateway) |
| Object Storage (OSS) | Durable storage for raw session files (`app/session.parquet`) |
| Flink ETL Jobs | Read raw session files, emit metadata rows + analytic tables |
| Iceberg Metadata Table | Session-level index (start/end, counts, URIs) for lookups |
| ClickHouse Tables | Hot aggregations (operation counts, tag counts) and replay lists |
| Retention Jobs | Enforce TTL (delete raw + metadata beyond retention window) |

---

## 3. Raw Data Format

### 3.1 OTLP Event Schema

We standardise on OTLP spans/events, encoded via HTTP/JSON, Protobuf, or Arrow. Each span carries:

Please refer to OpenTelemetry trace proto schema: https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto

| Attribute | OTLP Field | Required | Notes |
|-----------|-----------|----------|-------|
| Session ID | `span.attributes["sp.session.id"]` | ✅ | Primary grouping key |
| Trace ID | `trace_id` | ✅ | OTEL Trace ID (record id) |
| Span ID | `span_id` | ✅ | OTEL Span ID |
| Parent Span ID | `parent_span_id` | ✅ | OTEL Parent Span ID |
| App ID | `resource.attributes["sp.app.id"]` | ✅ | Existing app concept |
| Message Type | `span.name` map to Softprobe's `operation name` | ✅ | e.g., `OrderCreated` |
| Timestamp | `span.start_time_unix_nano` | ✅ | Source timestamp |
| Kind | `kind` | ✅ | OTEL Span Kind |
| Start Time | `start_time_unix_nano`  | ✅ | OTEL Span start_time_unix_nano |
| End Time | `end_time_unix_nano`  | ✅ | OTEL Span end_time_unix_nano |
| Metadata | `span.attributes` | ✅ | Arbitrary key/values |
| Body | `span.events[]` with `name = "sp.body"` and `bytesValue` | Optional | Payload (binary or base64) |
| Session End Flag | `span.attributes["sp.session.end"] = true` | Optional | Explicit closure |

The existing Arex/Softprobe (Java implementation) may not have `session id`, In order to be backwards compatible,
we should make it the same value as trace id if it is not provided.

All additional tags go under Metadata `span.attributes[sp.tags.*]` to keep ingestion schema-free.

### 3.2 Raw Table in Iceberg

Rather than writing arbitrary files, we treat the raw dataset itself as an **Iceberg table**. Each flush appends a Parquet data file containing rows with the schema below:

| Column | Type | Description |
|--------|------|-------------|
| `app_id` | STRING | Application identifier |
| `session_id` | STRING | Session identifier |
| `span_id` | STRING | OTLP span ID |
| `parent_span_id` | STRING | OTLP parent span ID |
| `trace_id` | STRING | OTLP trace ID (record ID) |
| `timestamp` | TIMESTAMP | Span start time |
| `end_timestamp` | TIMESTAMP | Span end time |
| `message_type` | STRING | Span/operation name |
| `span_kind` | STRING | OTLP span kind |
| `attributes` | MAP<STRING, STRING> | Span attributes (metadata) |
| `events` | ARRAY<STRUCT<name STRING, timestamp TIMESTAMP, attributes MAP<STRING, STRING>>> | OTLP events (including payload/headers) |
| `status_code` | STRING | OTLP status code |
| `status_message` | STRING | Optional status message |

**Partition spec**: `PARTITION BY (date_trunc('day', timestamp))`

**Sort order**: table property `TBLPROPERTIES ('write.sort-order'='session_id ASC NULLS FIRST, timestamp ASC NULLS FIRST, trace_id ASC NULLS FIRST')`. Writers sort rows by session and time before closing a file so Iceberg min/max stats remain selective for UUID IDs.

### 3.3 Iceberg Table DDL

```sql
CREATE TABLE raw_sessions (
  app_id           STRING,
  session_id       STRING,
  span_id          STRING,
  parent_span_id   STRING,
  trace_id         STRING,
  timestamp        TIMESTAMP,
  end_timestamp    TIMESTAMP,
  message_type     STRING,
  span_kind        STRING,
  attributes       MAP<STRING, STRING>,
  events           ARRAY<STRUCT<name STRING, timestamp TIMESTAMP, attributes MAP<STRING, STRING>>>,
  status_code      STRING,
  status_message   STRING
)
USING iceberg
PARTITIONED BY (date_trunc('day', timestamp))
TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'format-version' = '2',
  'write.target-file-size-bytes' = '134217728',
  'write.sort-order' = 'session_id ASC NULLS FIRST, timestamp ASC NULLS FIRST, trace_id ASC NULLS FIRST'
);
```

The ingestion service opens an Iceberg append transaction for each flush, adds the Parquet file (containing one or many session row groups), and commits. Sorting is handled automatically according to the table property above.

Each Iceberg data file contains rows from one or more sessions. To keep session data local we align row groups and sort the rows:

- Before closing a file the writer sorts buffered rows by `session_id`, then `timestamp` (optionally `trace_id`).
- Each session occupies a contiguous row group (one row per span/message).
- Multiple sessions may share a file, but sort order + Iceberg statistics let queries skip unrelated row groups even when IDs are UUIDs.
- Long sessions that span multiple flush windows produce multiple segments; the metadata table stores one entry per segment.

To reduce OSS PUT/GET volume we allow a single Iceberg data file to carry multiple sessions. A writer process maintains an in-memory buffer of completed sessions and writes them sequentially, keeping one row group per session. Flush conditions:

- Writer has been open ~60 seconds (rolling window),
- File size reaches ~128 MB, or
- Session explicitly requests flush (timeout or end flag).

For each session segment we persist `file_path`, `row_offset`, and `row_count` (derived from Iceberg row-group stats) in the 
metadata table so replay can fetch only the relevant rows. Long-lived sessions simply generate multiple segments across files.

This approach lets us leverage Iceberg features (schema evolution, snapshotting, partition pruning) while minimising the number of OSS PUT/GET operations.


### 3.2 Session Buffers and File Writers

For replay we keep each session’s messages contiguous **without** allocating a file per session. The gateway load balance/partiton 
messages to the ingestion service by `session_id` so that all messages of the same session will be send to the same ingestion 
service instance. The ingestion service buffers per `session_id`; when a session closes, its spans are sorted and appended as a 
dedicated row group to the current Iceberg Parquet file.

Flush triggers (per file writer):
- Writer open time ≈ 60 seconds (rolling window)
- File size ≈ 128 MB
- Session explicitly ends or times out

Data files live under `oss://lake/raw/dt={YYYY-MM-DD}/part-<uuid>.parquet`. Iceberg table properties enforce sorting (`session_id ASC`, `timestamp ASC`, `trace_id ASC`) so manifests capture tight min/max ranges, even for UUIDs.

### 3.3 Buffering Logic (Pseudo-code)

```rust
async fn ingest(span: Span) -> Result<()> {
    let key = SessionKey::new(&span.app_id, &span.session_id);
    let buf = buffers.entry(key.clone()).or_insert_with(SessionBuffer::new);
    buf.push(span);

    if buf.should_flush() {
        let tmp_path = parquet::write_temp(&buf.spans)?;
        let oss_uri = oss::upload(tmp_path, &key)?;
        metadata_tx.send(buf.to_metadata_row(&oss_uri))?;
        buf.clear();
    }
    Ok(())
}
```

The gateway holds an LRU of active sessions. If memory pressure occurs, the oldest inactive session is flushed early. Optionally, we can persist a small WAL to guarantee at-least-once semantics.

---

## 5. Analytics via Flink ETL

Flink (or Spark/DuckDB in batch mode) powers downstream tables.

### 5.1 Session Metadata ETL

Job reads new Parquet session files, extracts summary info, and appends to the Iceberg `metadata` table if the gateway is running in lightweight mode (e.g., no direct metadata writes). Key steps:

1. List new files from OSS or consume from an object notification queue (e.g., S3 event/topic).
2. Read Parquet, compute `start_ts`, `end_ts`, `message_count`, `size_bytes`.
3. Upsert into Iceberg using Flink Iceberg sink.

### 5.2 Operation Counts (ClickHouse `recording_ops_minute`)

For fast dashboards and alerts we maintain a ClickHouse SummingMergeTree table, populated via Flink:

```sql
CREATE TABLE recording_ops_minute ON CLUSTER default
(
  bucket_minute   DateTime,
  app_id          String,
  message_type    LowCardinality(String),
  message_count   UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMMDD(bucket_minute)
ORDER BY (app_id, message_type, bucket_minute);
```

Flink job (pseudo):

```scala
rawSessions
  .flatMap(explodeMessages)
  .assignTimestampsAndWatermarks(...)
  .keyBy(msg -> (msg.appId, msg.messageType))
  .window(TumblingEventTimeWindows.of(Time.minutes(1)))
  .aggregate(countAggregator)
  .sinkTo(clickhouseSink("recording_ops_minute"))
```

### 5.3 Tag Analytics (`recording_tag_counts` in ClickHouse)

To support tag-based filters we explode the OTLP attribute map and store hourly counts in ClickHouse:

```sql
CREATE TABLE recording_tag_counts ON CLUSTER default
(
  bucket_hour   DateTime,
  app_id        String,
  tag_key       String,
  tag_value     String,
  total_count   UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMMDD(bucket_hour)
ORDER BY (app_id, tag_key, tag_value, bucket_hour);
```

Flink job emits `(tag_key, tag_value)` pairs per message and aggregates over tumbling 1-hour windows before writing to ClickHouse.

### 5.4 Replay Index Table (`recording_events`)

For fast replay queries we maintain a ClickHouse table populated by ETL:

```sql
CREATE TABLE recording_events ON CLUSTER default
(
  session_id         String,
  record_id          String,
  app_id             String,
  message_type       LowCardinality(String),
  timestamp          DateTime64(3),
  oss_uri            String,
  row_index          UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (session_id, timestamp);
```

ETL extracts per-message metadata, storing the Parquet row index for direct payload retrieval.

---

## 6. Replay & Query Workloads

### 6.1 Find Session

```sql
SELECT * FROM metadata
WHERE session_id = 's123';
```

When querying the raw Iceberg table directly, always include a time range (e.g., `timestamp BETWEEN ...`) so partition pruning keeps scans tight. Replay APIs derive these bounds from `start_ts`/`end_ts`.

### 6.2 Replay Messages

1. Query `recording_events` for the desired session, optionally filtering by `app_id` or `message_type`.
2. Load the Parquet file (`oss_uri`) and seek to `row_index` to retrieve the message bodies.

### 6.3 Operation Analytics

```sql
SELECT bucket_minute, message_count
FROM recording_ops_minute
WHERE app_id = 'a1'
  AND message_type = 'OrderCreated'
  AND bucket_minute BETWEEN now() - INTERVAL 7 DAY AND now();
```

### 6.4 Tag Filtering

```sql
SELECT *
FROM recording_tag_counts
WHERE tag_key = 'geo'
  AND tag_value = 'us-east'
  AND bucket_hour >= date_trunc('hour', now() - INTERVAL 1 DAY);
```

---

## 7. Retention & Lifecycle

| Tier | Contents | Retention | Enforced By |
|------|----------|-----------|-------------|
| Raw | Session Parquet (`oss://raw/...`) | 180 days (configurable) | OSS lifecycle rule |
| Analytics | Operation/tag tables | 365 days | OSS lifecycle + Iceberg snapshot expiration |
| Metadata | Iceberg `metadata` | 365 days | Iceberg snapshot expiration |

Retention job outline:

```python
def retention_job():
    sessions = metadata.query("end_ts < now() - interval '180 days'")
    for row in sessions:
        oss.delete(row.oss_uri)
    iceberg.expire_snapshots('metadata', older_than=...)
    iceberg.expire_snapshots('recording_ops_minute', ...)
```

---

## 8. Operational Considerations

### 8.1 Gateway / Worker Scaling

- Stateless; scale deployments horizontally behind a load balancer.
- Consistent hashing ensures the same session hits the same worker. Rendezvous hashing handles scaling events.
- LRU eviction protects against memory overload; long sessions can flush mid-stream.

### 8.2 Flink Cluster

- Shared infrastructure for all ETL jobs (metadata, ops counts, tag counts, additional analytics).
- Configure checkpoints and savepoints for fault tolerance.
- Autoscale TaskManagers based on lag (new session files) or time-to-process.

### 8.3 Monitoring

- Gateway metrics: request rate/error rate, buffer size, flush latency.
- Worker metrics: active sessions, flush count, Parquet write failures.
- Flink metrics: checkpoint duration, lag in reading new files.
- Storage: number of session files, total bytes in raw/analytics buckets.

### 8.4 Security & Multi-Tenancy

- OSS buckets partitioned by `app_id`; IAM policies enforce isolation.
- Optional client-side encryption for sensitive payloads.

---

## 9. Implementation Roadmap

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| **Phase 1 – MVP Ingestion** | Weeks 1-4 | OTLP gateway + session buffering, Parquet flush, raw OSS storage, DuckDB metadata prototype |
| **Phase 2 – Iceberg Metadata** | Weeks 5-6 | Move metadata to Iceberg, retention scripts, replay CLI |
| **Phase 3 – Flink ETL** | Weeks 7-10 | Flink jobs for `metadata`, `recording_ops_minute`, `recording_tag_counts` |
| **Phase 4 – ClickHouse Serving** | Weeks 11-12 | Populate `recording_events`, integrate Replay UI / API |
| **Phase 5 – Hardening** | Weeks 13-16 | Autoscaling, monitoring dashboards, security reviews, documentation |

---

## 10. Open Questions

1. **Session boundaries**: Rely solely on timeouts or require clients to send `session.end`? (Hybrid recommended.)
2. **Large payload handling**: Inline vs. external object (e.g., >10 MB body stored separately).
3. **Analytics latency**: How fresh must the ClickHouse summaries be? (Hourly ETL vs. streaming Flink job.)
4. **Schema evolution**: Need a registry for `softprobe.meta.*` keys? (Probably not initially.)

---

## 11. Summary

This architecture positions Softprobe’s data layer for long-term growth:

- **Simple ingestion** (stateless OTLP gateway + session buffers)
- **Durable raw data** (Parquet per session in OSS)
- **Flexible analytics** (Flink ETL + ClickHouse tables)
- **Cost-effective** (storage-tiered, OSS lifecycle)

Replay, testing, analytics, and future data applications all build on the same foundational dataset, keeping the platform maintainable and extensible.

