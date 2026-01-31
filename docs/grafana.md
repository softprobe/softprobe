# Grafana Integration Guide

This guide shows how to visualize your OTLP telemetry data in Grafana using the **official DuckDB Data Source Plugin**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Grafana UI                             │
│  Dashboards for monitoring, debugging, analytics            │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ DuckDB Plugin (Go backend)
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              DuckDB Process                                 │
│  - Loads Iceberg extension                                  │
│  - Executes iceberg_scan() queries                          │
│  - Direct S3/R2 access via httpfs extension                │
└─────────────────────┼───────────────────────────────────────┘
                      │
                      │ S3-compatible protocol
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              Iceberg Tables (S3/R2/MinIO)                   │
│  - traces table                                             │
│  - logs table                                               │
│  - metrics table                                            │
└─────────────────────────────────────────────────────────────┘
```

**Key Benefit:** DuckDB queries Iceberg directly - no OTLP Backend involvement needed for queries.

---

## Installation

### Prerequisites

- Grafana 10.4.0 or later
- **Ubuntu 22.04+ (glibc 2.35+)** - Alpine Linux NOT supported
- Docker users: Use `grafana/grafana:latest-ubuntu` (not default Alpine image)

### Step 1: Download Plugin

```bash
# Download latest release
wget https://github.com/motherduckdb/grafana-duckdb-datasource/releases/latest/download/motherduck-duckdb-datasource.zip

# Extract to Grafana plugins directory
unzip motherduck-duckdb-datasource.zip -d /var/lib/grafana/plugins/

# Or for Docker:
# Mount as volume: -v ./plugins:/var/lib/grafana/plugins
```

### Step 2: Allow Unsigned Plugin

The plugin is currently unsigned. Edit `grafana.ini`:

```ini
[plugins]
allow_loading_unsigned_plugins = motherduck-duckdb-datasource
```

**Docker:**
```bash
docker run -d \
  -p 3000:3000 \
  -e "GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=motherduck-duckdb-datasource" \
  -v ./plugins:/var/lib/grafana/plugins \
  grafana/grafana:latest-ubuntu
```

### Step 3: Restart Grafana

```bash
# Linux
sudo systemctl restart grafana-server

# Docker
docker restart grafana

# macOS
brew services restart grafana
```

### Step 4: Verify Installation

1. Navigate: **☰ Menu** → **Administration** → **Plugins**
2. Search: `DuckDB`
3. Should see: **DuckDB** by MotherDuck

---

## Configuration for Iceberg Tables

### Create Data Source

1. **Navigate:** Configuration → Data Sources → Add data source
2. **Search:** `DuckDB`
3. **Select:** DuckDB by MotherDuck

### Configuration Options

#### Option A: In-Memory Database (Simplest)

| Field | Value |
|-------|-------|
| **Name** | `OTLP Iceberg Data Lake` |
| **Path** | *(leave empty for in-memory)* |
| **Init SQL** | See below |

**Init SQL (Load Extensions):**
```sql
-- Install required extensions
INSTALL iceberg;
INSTALL httpfs;

-- Load extensions
LOAD iceberg;
LOAD httpfs;

-- Configure S3/R2 credentials
SET s3_endpoint='https://your-endpoint.r2.cloudflarestorage.com';
SET s3_access_key_id='your_access_key';
SET s3_secret_access_key='your_secret_key';
SET s3_region='auto';
```

#### Option B: Persistent Database File

| Field | Value |
|-------|-------|
| **Path** | `/var/lib/grafana/datalake.db` |
| **Init SQL** | Same as Option A |

**Benefits:**
- Query plans cached
- Extension state persists
- Better performance for repeated queries

---

## Configuration via HTTP API (Recommended for Real-Time Queries)

### Why Use HTTP API?

The HTTP API datasource provides access to **union views** that combine:
- **Buffer** (in-memory): Most recent data, not yet flushed
- **Staged** (local parquet): Flushed but not yet committed to Iceberg
- **Committed** (Iceberg): Durably committed to object store

This enables **sub-second read freshness** for real-time monitoring and alerting, which direct DuckDB file access cannot provide.

### Architecture with HTTP API

```
┌─────────────────────────────────────────────────────────────┐
│                      Grafana UI                             │
│  Dashboards for monitoring, debugging, analytics            │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ HTTP API (JSON)
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              OTLP Backend Service                            │
│  - /v1/query/sql endpoint                                   │
│  - DuckDB with union views (buffer ∪ staged ∪ committed)  │
│  - Real-time query execution                                │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ Union Views
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              Data Sources                                    │
│  - Buffer (in-memory)                                       │
│  - Staged (local parquet)                                   │
│  - Committed (Iceberg S3/R2)                                │
└─────────────────────────────────────────────────────────────┘
```

### Setup HTTP API Datasource

#### Option 1: Using JSON API Datasource Plugin

1. **Install JSON API Plugin:**
   ```bash
   grafana-cli plugins install marcusolsson-json-datasource
   ```

2. **Configure Data Source:**
   - Navigate: Configuration → Data Sources → Add data source
   - Search: `JSON API`
   - Select: JSON API by Marcus Olsson

3. **Data Source Settings:**
   | Field | Value |
   |-------|-------|
   | **Name** | `OTLP Backend API` |
   | **URL** | `http://localhost:8090` |
   | **Method** | `POST` |
   | **Headers** | `Content-Type: application/json` |

4. **Query Configuration:**
   - **Query Type:** `JSONPath`
   - **Path:** `$.rows[*]`
   - **Body:**
   ```json
   {
     "sql": "SELECT * FROM union_spans WHERE timestamp >= NOW() - INTERVAL '5 minutes' LIMIT 100"
   }
   ```

#### Option 2: Using HTTP API Datasource (Community Plugin)

1. **Install HTTP API Plugin:**
   ```bash
   grafana-cli plugins install yesoreyeram-grafana-json-datasource
   ```

2. **Configure Data Source:**
   - Navigate: Configuration → Data Sources → Add data source
   - Search: `JSON`
   - Select: JSON API by Yesoreyeram

3. **Data Source Settings:**
   | Field | Value |
   |-------|-------|
   | **Name** | `OTLP Backend API` |
   | **URL** | `http://localhost:8090/v1/query/sql` |
   | **Method** | `POST` |
   | **Headers** | `Content-Type: application/json` |

4. **Query Editor:**
   ```json
   {
     "sql": "${query}"
   }
   ```
   Where `${query}` is a Grafana variable containing your SQL.

### Query Examples with HTTP API

#### 1. Real-Time Error Rate (5 minutes)

**Query Body:**
```json
{
  "sql": "SELECT COUNT(*) AS errors FROM union_spans WHERE timestamp >= (CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL '5 minutes') AND (http_response_status_code >= 500 OR status_code = 'ERROR')"
}
```

**Grafana Panel:**
- **Type:** Stat
- **Value:** `errors`
- **Refresh:** 10s

#### 2. Recent Spans by Session

**Query Body:**
```json
{
  "sql": "SELECT trace_id, span_id, timestamp, http_request_path, http_response_status_code FROM union_spans WHERE record_date >= DATE '2025-01-01' AND session_id = '${session_id}' ORDER BY timestamp DESC LIMIT 50"
}
```

**Grafana Panel:**
- **Type:** Table
- **Variables:** `session_id` (text input)

#### 3. Time Series - Request Rate with Union Views

**Query Body:**
```json
{
  "sql": "SELECT date_trunc('minute', timestamp) AS time, COUNT(*) AS requests FROM union_spans WHERE timestamp >= (CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL '1 hour') GROUP BY 1 ORDER BY 1"
}
```

**Grafana Panel:**
- **Type:** Time series
- **X-Axis:** `time`
- **Y-Axis:** `requests`

### Benefits of HTTP API Approach

✅ **Real-time data access** - Queries include buffered data not yet committed  
✅ **Unified query interface** - Single endpoint for all data sources  
✅ **Sub-second freshness** - No waiting for Iceberg commits  
✅ **Service integration** - Queries go through the same path as other APIs  
✅ **Error handling** - Service-level error responses and validation  

### When to Use Each Approach

| Use Case | Recommended Approach |
|----------|---------------------|
| **Real-time monitoring/alerts** | HTTP API (union views) |
| **Historical analysis** | Direct DuckDB (Iceberg only) |
| **Ad-hoc exploration** | HTTP API (union views) |
| **Batch reporting** | Direct DuckDB (Iceberg only) |

---

## Query Examples

### 1. Time Series - Request Rate

**Query:**
```sql
SELECT
  time_bucket($__interval, timestamp) AS time,
  COUNT(*) / EXTRACT(EPOCH FROM INTERVAL $__interval) as requests_per_sec
FROM iceberg_scan('s3://warehouse/traces')
WHERE $__timeFilter(timestamp)
  AND record_date >= DATE $__timeFrom
  AND record_date <= DATE $__timeTo
GROUP BY time
ORDER BY time
```

**Grafana Macros Used:**
- `$__interval` → `'1 minute'`, `'5 minutes'` (auto-calculated)
- `$__timeFilter(timestamp)` → `timestamp BETWEEN ... AND ...`
- `$__timeFrom` → `'2025-01-02'`
- `$__timeTo` → `'2025-01-02'`

**Panel Configuration:**
- **Type:** Time series
- **Format:** Time series

---

### 2. Error Rate Percentage

**Query:**
```sql
SELECT
  time_bucket($__interval, timestamp) AS time,
  (COUNT(*) FILTER (WHERE status_code = 'ERROR') * 100.0 / COUNT(*)) as error_rate
FROM iceberg_scan('s3://warehouse/traces')
WHERE $__timeFilter(timestamp)
  AND record_date >= DATE $__timeFrom
GROUP BY time
ORDER BY time
```

**Panel Configuration:**
- **Type:** Time series
- **Unit:** Percent (0-100)
- **Thresholds:**
  - Green: 0-1%
  - Yellow: 1-5%
  - Red: >5%

---

### 3. P95 Latency

**Query:**
```sql
SELECT
  time_bucket($__interval, timestamp) AS time,
  approx_quantile(
    EXTRACT(EPOCH FROM (end_timestamp - timestamp)) * 1000,
    0.95
  ) as p95_latency_ms
FROM iceberg_scan('s3://warehouse/traces')
WHERE $__timeFilter(timestamp)
  AND record_date >= DATE $__timeFrom
  AND end_timestamp IS NOT NULL
GROUP BY time
ORDER BY time
```

**Panel Configuration:**
- **Type:** Time series
- **Unit:** milliseconds (ms)

---

### 4. Active Sessions (Stat)

**Query:**
```sql
SELECT COUNT(DISTINCT session_id) as active_sessions
FROM iceberg_scan('s3://warehouse/traces')
WHERE timestamp >= NOW() - INTERVAL '5 minutes'
```

**Panel Configuration:**
- **Type:** Stat
- **Show:** Value

---

### 5. Top Endpoints (Table)

**Query:**
```sql
SELECT
  http_request_path as endpoint,
  COUNT(*) as request_count,
  COUNT(*) FILTER (WHERE status_code = 'ERROR') as error_count,
  approx_quantile(
    EXTRACT(EPOCH FROM (end_timestamp - timestamp)) * 1000,
    0.95
  ) as p95_ms
FROM iceberg_scan('s3://warehouse/traces')
WHERE $__timeFilter(timestamp)
  AND record_date >= DATE $__timeFrom
  AND http_request_path IS NOT NULL
GROUP BY endpoint
ORDER BY request_count DESC
LIMIT 20
```

**Panel Configuration:**
- **Type:** Table
- **Columns:** Auto-detected

---

### 6. Session Explorer

**Query with Variable:**
```sql
SELECT
  timestamp,
  trace_id,
  span_id,
  message_type,
  http_request_path,
  http_response_status_code,
  status_code
FROM iceberg_scan('s3://warehouse/traces')
WHERE session_id = '$session_id'
  AND record_date >= CURRENT_DATE - 30
ORDER BY timestamp
```

**Create Variable:**
- **Name:** `session_id`
- **Type:** Text box
- **Label:** Session ID

---

### 7. Logs Correlated with Traces

**Query:**
```sql
SELECT
  l.timestamp,
  l.severity_text,
  l.body,
  t.http_request_path
FROM iceberg_scan('s3://warehouse/logs') l
JOIN iceberg_scan('s3://warehouse/traces') t
  ON l.trace_id = t.trace_id
  AND l.span_id = t.span_id
WHERE $__timeFilter(l.timestamp)
  AND l.record_date >= DATE $__timeFrom
  AND l.severity_number >= 17  -- ERROR level
ORDER BY l.timestamp DESC
LIMIT 100
```

---

## Template Variables

### Service Selector

**Variable Configuration:**

| Field | Value |
|-------|-------|
| **Name** | `app_id` |
| **Type** | Query |
| **Data source** | OTLP Iceberg Data Lake |
| **Query** | See below |

**Query:**
```sql
SELECT DISTINCT app_id
FROM iceberg_scan('s3://warehouse/traces')
WHERE record_date >= CURRENT_DATE - 7
ORDER BY app_id
```

**Use in queries:**
```sql
WHERE app_id = '$app_id'
```

---

### Dynamic Time Range Variable

**Variable Configuration:**

| Field | Value |
|-------|-------|
| **Name** | `max_days_back` |
| **Type** | Custom |
| **Custom options** | `1,7,30,90` |
| **Label** | Max Days |

**Use in queries:**
```sql
WHERE record_date >= CURRENT_DATE - $max_days_back
```

---

## Advanced Features

### 1. Query Multiple Tables

```sql
-- Union traces and logs for unified timeline
SELECT timestamp, 'trace' as type, message_type as event
FROM iceberg_scan('s3://warehouse/traces')
WHERE session_id = '$session_id'

UNION ALL

SELECT timestamp, 'log' as type, body as event
FROM iceberg_scan('s3://warehouse/logs')
WHERE session_id = '$session_id'

ORDER BY timestamp
```

### 2. Use Iceberg Time Travel

```sql
-- Query historical snapshot
SELECT COUNT(*) as count
FROM iceberg_scan(
  's3://warehouse/traces',
  snapshot_id => 1234567890
)
WHERE record_date = CURRENT_DATE
```

### 3. View Iceberg Metadata

```sql
-- Check available snapshots
SELECT * FROM iceberg_snapshots('s3://warehouse/traces')
ORDER BY committed_at DESC
```

```sql
-- View data file details
SELECT * FROM iceberg_metadata('s3://warehouse/traces')
WHERE record_date >= CURRENT_DATE - 7
```

---

## Performance Optimization

### 1. Use Partition Filters (Critical!)

```sql
-- ✅ Good: Partition pruning enabled
WHERE record_date >= DATE $__timeFrom
  AND record_date <= DATE $__timeTo

-- ❌ Bad: Full table scan
WHERE timestamp >= $__timeFrom
```

### 2. Limit Result Sets

```sql
-- Always add LIMIT for large queries
LIMIT 1000
```

### 3. Use Approximate Aggregations

```sql
-- Fast approximate count
SELECT approx_count_distinct(session_id) FROM ...

-- Fast approximate percentiles
SELECT approx_quantile(latency, 0.95) FROM ...
```

### 4. Enable Query Result Caching

In data source settings:
- **Cache TTL:** 60 seconds (for dashboard auto-refresh)

---

## Troubleshooting

### "Failed to initialize plugin"

**Issue:** Alpine Linux / musl libc incompatibility

**Solution:**
```bash
# Use Ubuntu-based Grafana image
docker pull grafana/grafana:latest-ubuntu
```

### "Extension not found: iceberg"

**Issue:** Extensions not installed

**Solution:** Add to Init SQL:
```sql
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;
```

### "S3 access denied"

**Issue:** Missing S3 credentials

**Solution:** Configure in Init SQL:
```sql
SET s3_access_key_id='your_key';
SET s3_secret_access_key='your_secret';
```

### "Query timeout"

**Issue:** Large time range without partition filter

**Solution:**
1. Add partition filter: `record_date >= DATE $__timeFrom`
2. Reduce time range in dashboard
3. Add `LIMIT` clause

---

## Comparison: DuckDB Plugin vs HTTP SQL API

### When to Use DuckDB Plugin ✅

- **Standard deployments** - Most use cases
- **Quick setup** - Minimal configuration
- **Grafana-native features** - Macros, variables work out of the box
- **Direct Iceberg access** - No backend needed
- **Multi-tenant** - Grafana handles auth

### When to Use HTTP SQL API

- **Custom authentication** - Complex auth requirements
- **Rate limiting** - Need query throttling
- **Query preprocessing** - Validate/transform queries
- **Audit logging** - Track all queries
- **Backend integration** - Query alongside other APIs

---

## Next Steps

1. ✅ **Install plugin** following steps above
2. ✅ **Configure data source** with S3/R2 credentials
3. ✅ **Create first dashboard** with time series panels
4. ✅ **Add template variables** for dynamic filtering
5. ✅ **Set up alerts** on error rates, latency
6. ✅ **Share dashboards** with team

---

## References

- [MotherDuck Grafana DuckDB Plugin](https://github.com/motherduckdb/grafana-duckdb-datasource) - Official plugin repository
- [DuckDB Iceberg Extension](https://duckdb.org/docs/stable/core_extensions/iceberg/overview) - Iceberg extension docs
- [DuckDB S3 Iceberg Import](https://duckdb.org/docs/stable/guides/network_cloud_storage/s3_iceberg_import) - S3 integration guide
- [Grafana Data Sources](https://grafana.com/docs/grafana/latest/datasources/) - Official Grafana documentation
- [DuckDB-Iceberg GitHub](https://github.com/duckdb/duckdb-iceberg) - Extension source code

---

## Production Checklist

Before deploying to production:

- [ ] Plugin installed and verified
- [ ] S3/R2 credentials configured securely (use secrets management)
- [ ] Init SQL script tested
- [ ] Partition filters on all queries
- [ ] Template variables for common filters
- [ ] Dashboard auto-refresh configured (30s-60s)
- [ ] Alerts configured for critical metrics
- [ ] Query performance tested (<2s for most queries)
- [ ] Backup Grafana dashboards (export JSON)
- [ ] Documentation for team
