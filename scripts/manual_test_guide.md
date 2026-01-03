# Manual End-to-End Test Guide

This guide helps you manually test the OTLP Data Lake with Grafana integration.

## Prerequisites

- Docker and Docker Compose installed
- Python 3 with pip

## Step 1: Start Services

```bash
# Build and start all services
docker-compose up -d --build

# Wait for services to be ready (this may take 5-10 minutes for first build)
docker-compose logs -f otlp-backend
```

Wait until you see logs indicating the backend is running.

## Step 2: Install Python Dependencies

```bash
pip3 install -r scripts/requirements.txt
```

## Step 3: Generate Telemetry Data

```bash
python3 scripts/generate_telemetry.py
```

This will generate:
- 5 sessions with unique `sp.session.id` values
- 10 requests per session (50 total traces)
- HTTP methods: GET, POST, PUT, DELETE
- Various endpoints: /api/users, /api/products, /api/orders, etc.
- Status codes: ~85% OK, ~10% ERROR, ~5% UNSET

Expected output:
```
🚀 Starting Telemetry Generator
   OTLP Endpoint: localhost:4317
   Service: demo-service
   Sessions: 5
   Requests per session: 10

============================================================
Session 1/5: session-<uuid>
============================================================
  [ 1/10] GET    /api/users           status=OK    duration=123ms
  ...
```

## Step 4: Wait for Data to Flush

The backend buffers data before writing to Iceberg. Wait ~15 seconds after telemetry generation completes.

```bash
# Check if data was flushed
docker-compose logs otlp-backend | grep -i flush
```

## Step 5: Verify Data in MinIO

```bash
# Access MinIO Console
open http://localhost:9001

# Login credentials:
# Username: minioadmin
# Password: minioadmin

# Navigate to: Buckets → warehouse → traces
# You should see Parquet files
```

## Step 6: Access Grafana

```bash
open http://localhost:3000
```

Grafana is configured with:
- Anonymous access enabled (no login required)
- DuckDB data source pre-configured
- "OTLP Overview" dashboard pre-loaded

## Step 7: View Dashboard

1. Navigate to **Dashboards** → **OTLP Overview**
2. Set time range to "Last 1 hour" (top right)
3. Verify panels show data:
   - **Request Rate**: Line chart showing requests/sec
   - **Error Rate**: Line chart showing error percentage
   - **Top Endpoints**: Table with endpoint statistics
   - **Sessions**: Table with `sp.session.id` values

## Step 8: Explore Data

### Query Examples

Click on any panel → **Edit** to see the SQL query.

Example queries you can try in **Explore** view:

**1. All sessions:**
```sql
SELECT DISTINCT "sp.session.id" as session_id
FROM iceberg_scan('s3://warehouse/traces')
WHERE record_date >= CURRENT_DATE - 1
```

**2. Requests by endpoint:**
```sql
SELECT
  http_request_path,
  COUNT(*) as count
FROM iceberg_scan('s3://warehouse/traces')
WHERE record_date >= CURRENT_DATE - 1
GROUP BY http_request_path
ORDER BY count DESC
```

**3. Session timeline:**
```sql
SELECT
  timestamp,
  http_method,
  http_request_path,
  status_code
FROM iceberg_scan('s3://warehouse/traces')
WHERE "sp.session.id" = '<paste-session-id-here>'
  AND record_date >= CURRENT_DATE - 1
ORDER BY timestamp
```

## Troubleshooting

### No data in dashboard

**Check 1: Is data in Iceberg?**
```bash
docker-compose exec -T minio mc ls minio/warehouse/traces/
```

**Check 2: Backend logs**
```bash
docker-compose logs otlp-backend | tail -50
```

**Check 3: Grafana data source**
- Go to **Configuration** → **Data Sources** → **OTLP Data Lake**
- Click **Save & Test**
- Should see "Connection successful"

**Check 4: Query manually**
```bash
docker-compose exec -T otlp-backend duckdb -c "
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;
SET s3_endpoint='http://minio:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SELECT COUNT(*) FROM iceberg_scan('s3://warehouse/traces');
"
```

### Grafana plugin not loading

**Check plugin installation:**
```bash
docker-compose exec grafana ls -la /var/lib/grafana/plugins/
```

Should see `motherduck-duckdb-datasource` directory.

**Check Grafana logs:**
```bash
docker-compose logs grafana | grep -i duckdb
```

### Permission errors

**Reset MinIO data:**
```bash
docker-compose down
rm -rf warehouse/minio/*
docker-compose up -d
```

## Clean Up

```bash
# Stop all services
docker-compose down

# Remove all data (optional)
docker-compose down -v
rm -rf warehouse/minio/*
```

## Next Steps

Once the system is working:

1. **Create custom dashboards** - Use the query examples as templates
2. **Add template variables** - For dynamic filtering by session, endpoint, etc.
3. **Set up alerts** - Configure Grafana alerts for error rates, latency, etc.
4. **Scale testing** - Generate more data to test partition pruning performance
5. **Production deployment** - Configure authentication, HTTPS, resource limits

## Performance Tips

- **Always include partition filters** in queries:
  ```sql
  WHERE record_date >= DATE '$__timeFrom'
    AND record_date <= DATE '$__timeTo'
  ```

- **Use time bucketing** for aggregations:
  ```sql
  SELECT time_bucket('1m', timestamp) AS time, COUNT(*)
  FROM iceberg_scan('s3://warehouse/traces')
  WHERE record_date >= CURRENT_DATE - 7
  GROUP BY time
  ```

- **Limit result sets** for tables:
  ```sql
  SELECT * FROM iceberg_scan('s3://warehouse/traces')
  WHERE record_date >= CURRENT_DATE - 1
  LIMIT 1000
  ```
