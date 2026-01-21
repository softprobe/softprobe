#!/bin/bash
# Interactive DuckDB session with Iceberg tables pre-configured
# Usage: ./scripts/interactive_query.sh

cat << 'EOF' > /tmp/duckdb_init.sql
-- Auto-load Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- Configure S3 for MinIO
SET s3_endpoint='localhost:9002';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- Enable unsafe version guessing
SET unsafe_enable_version_guessing=true;

-- Welcome message
.print '================================================'
.print 'DuckDB Interactive Session - Iceberg Tables'
.print '================================================'
.print ''
.print 'Loading Iceberg tables...'

-- Create views for Iceberg tables (using S3 paths)
CREATE OR REPLACE VIEW traces AS
SELECT * FROM iceberg_scan('s3://warehouse/default/traces', allow_moved_paths := true);

CREATE OR REPLACE VIEW logs AS
SELECT * FROM iceberg_scan('s3://warehouse/default/logs', allow_moved_paths := true);

CREATE OR REPLACE VIEW metrics AS
SELECT * FROM iceberg_scan('s3://warehouse/default/metrics', allow_moved_paths := true);

-- 1. Create a helper to find the latest metadata file from S3
CREATE OR REPLACE TEMP TABLE latest_meta AS 
FROM glob('s3://warehouse/default/traces/metadata/*.metadata.json') 
SELECT file_name ORDER BY file_name DESC LIMIT 1;

-- 2. Create a macro so you can just type 'checkpoint_traces()'
CREATE OR REPLACE MACRO checkpoint_traces() AS TABLE 
SELECT * FROM iceberg_snapshots((SELECT file_name FROM latest_meta));

.print ''
.print 'Available views:'
.print '  - traces'
.print '  - logs'
.print '  - metrics'
.print ''
.print 'Useful commands:'
.print '  DESCRIBE traces;'
.print '  DESCRIBE logs;'
.print '  DESCRIBE metrics;'
.print ''
.print 'Example queries:'
.print '  SELECT COUNT(*) FROM traces;'
.print '  SELECT * FROM logs ORDER BY timestamp DESC LIMIT 10;'
.print '  SELECT session_id, COUNT(*) FROM traces GROUP BY session_id;'
.print '  SELECT metric_name, COUNT(*) FROM metrics GROUP BY metric_name;'
.print '  SELECT * FROM metrics WHERE metric_name = '\''cpu.usage'\'' ORDER BY timestamp DESC LIMIT 10;'
.print ''
.print '================================================'
.print ''
EOF

# Launch DuckDB with init script
duckdb -init /tmp/duckdb_init.sql
