# Scripts

Most workflows are exposed via Makefile targets; scripts are thin helpers.

## Quick Start

```bash
# Automated verification (recommended)
make verify-e2e

# Interactive session
make duckdb-shell

# Run SQL file
duckdb < scripts/verify_iceberg.sql
```

## DuckDB Iceberg Syntax (v1.4.3+)

### Basic Setup

```sql
INSTALL iceberg;
LOAD iceberg;

-- S3 configuration for MinIO
SET s3_endpoint='localhost:9002';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- Required for REST catalog
SET unsafe_enable_version_guessing=true;

-- Create views using iceberg_scan function with S3 paths
CREATE OR REPLACE VIEW traces AS
SELECT * FROM iceberg_scan('s3://warehouse/default/traces', allow_moved_paths := true);

CREATE OR REPLACE VIEW logs AS
SELECT * FROM iceberg_scan('s3://warehouse/default/logs', allow_moved_paths := true);

-- Now query using simple view names
SELECT COUNT(*) FROM traces;
SELECT COUNT(*) FROM logs;
```

### Key Points

1. **Use `iceberg_scan()` function** with REST catalog URL
2. **Create views** for easier querying (avoid long URLs in every query)
3. **Enable `unsafe_enable_version_guessing`** for REST catalogs without explicit versions
4. **Set `allow_moved_paths := true`** to handle Iceberg table evolution

## Files

- **verify_iceberg.sql** - Comprehensive verification queries
- **verify_session.sql** - Session-specific queries with macro
- **interactive_query.sh** - Interactive DuckDB session launcher (used by `make duckdb-shell`)
- **demo_session_queries.sh** - Sample session queries (used by `make demo-session`)
- **verify_e2e.sh** - End-to-end verification (used by `make verify-e2e`)
- **drop_all_tables.sh** - Reset Iceberg tables (used by `make drop-tables`)
- **README.md** - This file

## Example Queries

### Count Data
```sql
SELECT COUNT(*) FROM traces;
SELECT COUNT(*) FROM logs;
```

### Recent Data
```sql
SELECT * FROM traces ORDER BY timestamp DESC LIMIT 10;
SELECT * FROM logs ORDER BY timestamp DESC LIMIT 10;
```

### Session Analysis
```sql
-- Sessions with most spans
SELECT session_id, COUNT(*) as span_count
FROM traces
GROUP BY session_id
ORDER BY span_count DESC
LIMIT 10;

-- Sessions with both traces and logs
SELECT
    t.session_id,
    COUNT(DISTINCT t.span_id) as spans,
    COUNT(DISTINCT l.body) as logs
FROM traces t
LEFT JOIN logs l ON t.session_id = l.session_id
GROUP BY t.session_id
HAVING logs > 0
ORDER BY spans + logs DESC;
```

### Specific Session
```sql
-- Load macro first
.read scripts/verify_session.sql

-- Query session
SELECT * FROM verify_session('your-session-id');
```

## Troubleshooting

### "Unknown parameter 'uri'" or "Catalog does not exist"
**Fix**: Use `iceberg_scan()` function with S3 paths instead of `CREATE SECRET` or `ATTACH` syntax:
```sql
CREATE VIEW traces AS
SELECT * FROM iceberg_scan('s3://warehouse/default/traces', allow_moved_paths := true);
```

### "No version was provided"
**Fix**: Enable unsafe version guessing:
```sql
SET unsafe_enable_version_guessing=true;
```

### Connection refused to MinIO/REST catalog
**Fix**: Check services are running:
```bash
# MinIO
curl http://localhost:9002/minio/health/live

# Iceberg REST catalog
curl http://localhost:8181/v1/config
```

### Empty tables
**Reasons**:
1. No data ingested yet - send test data
2. Data still in buffer - wait 60s or restart collector
3. Check collector logs for errors

## References

- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg)
- [Full Documentation](../VERIFYING_DATA.md)
