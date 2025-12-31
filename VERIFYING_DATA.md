# Verifying Iceberg Data with DuckDB

This guide shows how to verify data ingested into Iceberg tables using DuckDB and the Iceberg extension.

## Prerequisites

1. **DuckDB CLI installed**:
   ```bash
   # macOS
   brew install duckdb

   # Or download from https://duckdb.org/docs/installation/
   ```

2. **Services running**:
   - MinIO (S3-compatible storage) on `localhost:9002`
   - Iceberg REST catalog on `localhost:8181`
   - OTLP collector (this service) on `localhost:8090`

## Quick Verification

### Option 1: Automated Script (Recommended)

Run the automated verification script:

```bash
./scripts/verify_quick.sh
```

This will:
- Check if DuckDB is installed
- Check if MinIO and Iceberg REST catalog are running
- Run all verification queries
- Show counts, recent data, and statistics

### Option 2: Interactive DuckDB Session

Launch an interactive session with Iceberg pre-configured:

```bash
./scripts/interactive_query.sh
```

Then you can run queries like:

```sql
-- Count traces
SELECT COUNT(*) FROM traces;

-- Show recent logs
SELECT * FROM logs ORDER BY timestamp DESC LIMIT 10;

-- Find sessions with both traces and logs
SELECT
    session_id,
    COUNT(*) as item_count
FROM (
    SELECT session_id FROM traces
    UNION ALL
    SELECT session_id FROM logs
)
GROUP BY session_id
ORDER BY item_count DESC;
```

### Option 3: Manual SQL File

Run the comprehensive verification queries:

```bash
duckdb < scripts/verify_iceberg.sql
```

## Verification Queries

### Check Tables Exist

```sql
INSTALL iceberg;
LOAD iceberg;

SET s3_endpoint='localhost:9002';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';
SET unsafe_enable_version_guessing=true;

-- Create views for Iceberg tables using S3 paths
CREATE OR REPLACE VIEW otlp_traces AS
SELECT * FROM iceberg_scan('s3://warehouse/default/otlp_traces', allow_moved_paths := true);

CREATE OR REPLACE VIEW otlp_logs AS
SELECT * FROM iceberg_scan('s3://warehouse/default/otlp_logs', allow_moved_paths := true);

-- Now you can query using simple view names
SELECT COUNT(*) FROM otlp_traces;
SELECT COUNT(*) FROM otlp_logs;
```

### Verify Traces Table

```sql
-- First, create the view (if not already done)
CREATE OR REPLACE VIEW otlp_traces AS
SELECT * FROM iceberg_scan('s3://warehouse/default/otlp_traces', allow_moved_paths := true);

-- Table schema
DESCRIBE otlp_traces;

-- Count total spans
SELECT COUNT(*) FROM otlp_traces;

-- Recent spans
SELECT
    session_id,
    trace_id,
    message_type,
    timestamp
FROM otlp_traces
ORDER BY timestamp DESC
LIMIT 10;

-- Spans by session
SELECT
    session_id,
    COUNT(*) as span_count
FROM otlp_traces
GROUP BY session_id
ORDER BY span_count DESC;
```

### Verify Logs Table

```sql
-- First, create the view (if not already done)
CREATE OR REPLACE VIEW otlp_logs AS
SELECT * FROM iceberg_scan('s3://warehouse/default/otlp_logs', allow_moved_paths := true);

-- Table schema
DESCRIBE otlp_logs;

-- Count total logs
SELECT COUNT(*) FROM otlp_logs;

-- Recent logs
SELECT
    session_id,
    severity_text,
    body,
    timestamp
FROM otlp_logs
ORDER BY timestamp DESC
LIMIT 10;

-- Logs by severity
SELECT
    severity_text,
    COUNT(*) as log_count
FROM otlp_logs
GROUP BY severity_text
ORDER BY log_count DESC;
```

### Verify Session Correlation

```sql
-- Find sessions with both traces and logs
SELECT
    t.session_id,
    COUNT(DISTINCT t.span_id) as span_count,
    COUNT(DISTINCT l.body) as log_count
FROM otlp_traces t
LEFT JOIN otlp_logs l
    ON t.session_id = l.session_id
GROUP BY t.session_id
HAVING log_count > 0
ORDER BY span_count + log_count DESC
LIMIT 10;
```

### Verify Specific Session

```sql
-- Load session verification macro
.read scripts/verify_session.sql

-- Query specific session (replace with actual session_id)
SELECT * FROM verify_session('your-session-id-here');
```

### Verify Trace-Log Correlation

```sql
-- Count logs with trace correlation
SELECT
    COUNT(*) as correlated_logs,
    COUNT(DISTINCT trace_id) as unique_traces,
    COUNT(DISTINCT span_id) as unique_spans
FROM otlp_logs
WHERE trace_id IS NOT NULL;

-- Find logs for a specific trace
SELECT
    l.session_id,
    l.timestamp,
    l.severity_text,
    l.body,
    t.message_type as related_span
FROM otlp_logs l
LEFT JOIN otlp_traces t
    ON l.trace_id = t.trace_id
WHERE l.trace_id = 'your-trace-id-here'
ORDER BY l.timestamp;
```

## Partition and Storage Details

### Check Partition Distribution

```sql
-- Traces by date partition
SELECT
    record_date,
    COUNT(*) as span_count
FROM otlp_traces
GROUP BY record_date
ORDER BY record_date DESC;

-- Logs by date partition
SELECT
    record_date,
    COUNT(*) as log_count
FROM otlp_logs
GROUP BY record_date
ORDER BY record_date DESC;
```

### Storage Metadata

```sql
-- Show table snapshots (using S3 paths)
SELECT
    sequence_number,
    snapshot_id,
    timestamp_ms,
    manifest_list
FROM iceberg_snapshots('s3://warehouse/default/otlp_traces');

SELECT
    sequence_number,
    snapshot_id,
    timestamp_ms,
    manifest_list
FROM iceberg_snapshots('s3://warehouse/default/otlp_logs');

-- Show detailed table metadata
SELECT * FROM iceberg_metadata('s3://warehouse/default/otlp_traces');
```

## Testing Data Ingestion

### Send Test Traces

```bash
curl -X POST http://localhost:8090/v1/traces \
  -H "Content-Type: application/json" \
  -d '{
    "resourceSpans": [{
      "resource": {
        "attributes": [{
          "key": "service.name",
          "value": {"stringValue": "test-service"}
        }, {
          "key": "sp.session.id",
          "value": {"stringValue": "test-session-123"}
        }]
      },
      "scopeSpans": [{
        "spans": [{
          "traceId": "1234567890abcdef1234567890abcdef",
          "spanId": "1234567890abcdef",
          "name": "test-span",
          "kind": 1,
          "startTimeUnixNano": "1704067200000000000",
          "endTimeUnixNano": "1704067201000000000"
        }]
      }]
    }]
  }'
```

### Send Test Logs

```bash
curl -X POST http://localhost:8090/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [{
      "resource": {
        "attributes": [{
          "key": "service.name",
          "value": {"stringValue": "test-service"}
        }]
      },
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "1704067200000000000",
          "severityNumber": 9,
          "severityText": "INFO",
          "body": {"stringValue": "Test log message"},
          "attributes": [{
            "key": "session.id",
            "value": {"stringValue": "test-session-123"}
          }]
        }]
      }]
    }]
  }'
```

### Verify Test Data

After sending test data, wait ~60 seconds for buffer flush (or restart collector for immediate flush), then:

```bash
./scripts/verify_quick.sh
```

Or query interactively:

```bash
./scripts/interactive_query.sh
# Then: SELECT * FROM traces WHERE session_id = 'test-session-123';
```

## Troubleshooting

### "Table not found" error

**Problem**: `iceberg_catalog.default.otlp_traces` doesn't exist

**Solutions**:
1. Check if Iceberg REST catalog is running: `curl http://localhost:8181/v1/config`
2. Start the OTLP collector at least once (it creates tables on startup)
3. Verify MinIO is accessible: `curl http://localhost:9002/minio/health/live`

### "Connection refused" to MinIO

**Problem**: Cannot connect to S3 endpoint

**Solutions**:
1. Check MinIO is running: `docker ps | grep minio`
2. Verify endpoint in config: `s3_endpoint='localhost:9002'`
3. Check credentials match: `minioadmin` / `minioadmin` (default)

### Empty tables

**Problem**: Tables exist but have no data

**Reasons**:
1. No data has been ingested yet
2. Data is still in buffer (not yet flushed)
3. Buffer flush failed (check collector logs)

**Solutions**:
1. Send test data (see "Testing Data Ingestion" above)
2. Wait 60 seconds for time-based flush
3. Restart collector to force flush on shutdown
4. Check collector logs: `docker logs <collector-container>`

## Advanced Queries

### Session Timeline (Traces + Logs Merged)

```sql
WITH session_data AS (
    SELECT
        'trace' as type,
        session_id,
        trace_id,
        span_id,
        message_type as content,
        timestamp
    FROM otlp_traces
    WHERE session_id = 'your-session-id'

    UNION ALL

    SELECT
        'log' as type,
        session_id,
        trace_id,
        span_id,
        CONCAT(severity_text, ': ', body) as content,
        timestamp
    FROM otlp_logs
    WHERE session_id = 'your-session-id'
)
SELECT * FROM session_data
ORDER BY timestamp;
```

### Error Analysis

```sql
-- Find spans with errors
SELECT
    session_id,
    trace_id,
    message_type,
    status_code,
    status_message
FROM otlp_traces
WHERE status_code = 'ERROR'
ORDER BY timestamp DESC
LIMIT 20;

-- Find error/warning logs
SELECT
    session_id,
    severity_text,
    body,
    timestamp
FROM otlp_logs
WHERE severity_number >= 17  -- ERROR or higher
ORDER BY timestamp DESC
LIMIT 20;
```

### Performance Statistics

```sql
-- Span duration statistics
SELECT
    message_type,
    COUNT(*) as count,
    AVG(EXTRACT(EPOCH FROM (end_timestamp - timestamp))) as avg_duration_secs,
    MAX(EXTRACT(EPOCH FROM (end_timestamp - timestamp))) as max_duration_secs
FROM otlp_traces
WHERE end_timestamp IS NOT NULL
GROUP BY message_type
ORDER BY avg_duration_secs DESC;
```

## References

- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [OpenTelemetry Protocol Spec](https://opentelemetry.io/docs/specs/otlp/)
