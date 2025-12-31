-- DuckDB Iceberg Verification Scripts
-- This file provides queries to verify data ingested into Iceberg tables
-- Usage: duckdb < scripts/verify_iceberg.sql

-- Load Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- Configure S3 credentials for MinIO
SET s3_endpoint='localhost:9002';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- Enable unsafe version guessing (required for REST catalog without explicit version)
SET unsafe_enable_version_guessing=true;

-- ============================================================================
-- TRACES TABLE VERIFICATION
-- ============================================================================

.print '=== OTLP TRACES TABLE ==='
.print ''

-- Create views for easier querying (using iceberg_scan function with S3 paths)
-- Note: The metadata location comes from the Iceberg REST catalog
CREATE OR REPLACE VIEW otlp_traces AS
SELECT * FROM iceberg_scan('s3://warehouse/default/otlp_traces', allow_moved_paths := true);

CREATE OR REPLACE VIEW otlp_logs AS
SELECT * FROM iceberg_scan('s3://warehouse/default/otlp_logs', allow_moved_paths := true);

-- Show table metadata
.print '--- Table Info ---'
DESCRIBE otlp_traces;

-- Count total spans
.print ''
.print '--- Total Spans ---'
SELECT COUNT(*) as total_spans FROM otlp_traces;

-- Count by session
.print ''
.print '--- Spans by Session ---'
SELECT
    session_id,
    COUNT(*) as span_count,
    MIN(timestamp) as first_span,
    MAX(timestamp) as last_span
FROM otlp_traces
GROUP BY session_id
ORDER BY span_count DESC
LIMIT 10;

-- Count by app_id
.print ''
.print '--- Spans by Application ---'
SELECT
    app_id,
    COUNT(*) as span_count
FROM otlp_traces
GROUP BY app_id
ORDER BY span_count DESC;

-- Recent spans
.print ''
.print '--- Recent Spans (Last 10) ---'
SELECT
    session_id,
    trace_id,
    span_id,
    message_type,
    timestamp,
    record_date
FROM otlp_traces
ORDER BY timestamp DESC
LIMIT 10;

-- Partition distribution
.print ''
.print '--- Partition Distribution ---'
SELECT
    record_date,
    COUNT(*) as span_count
FROM otlp_traces
GROUP BY record_date
ORDER BY record_date DESC;

-- ============================================================================
-- LOGS TABLE VERIFICATION
-- ============================================================================

.print ''
.print '=== OTLP LOGS TABLE ==='
.print ''

-- Show table metadata
.print '--- Table Info ---'
DESCRIBE otlp_logs;

-- Count total logs
.print ''
.print '--- Total Logs ---'
SELECT COUNT(*) as total_logs FROM otlp_logs;

-- Count by session
.print ''
.print '--- Logs by Session ---'
SELECT
    session_id,
    COUNT(*) as log_count,
    MIN(timestamp) as first_log,
    MAX(timestamp) as last_log
FROM otlp_logs
GROUP BY session_id
ORDER BY log_count DESC
LIMIT 10;

-- Count by severity
.print ''
.print '--- Logs by Severity ---'
SELECT
    severity_text,
    COUNT(*) as log_count
FROM otlp_logs
GROUP BY severity_text
ORDER BY log_count DESC;

-- Recent logs
.print ''
.print '--- Recent Logs (Last 10) ---'
SELECT
    session_id,
    severity_text,
    body,
    timestamp,
    trace_id,
    span_id
FROM otlp_logs
ORDER BY timestamp DESC
LIMIT 10;

-- Logs with trace correlation
.print ''
.print '--- Logs with Trace Correlation ---'
SELECT
    COUNT(*) as correlated_logs,
    COUNT(DISTINCT trace_id) as unique_traces
FROM otlp_logs
WHERE trace_id IS NOT NULL;

-- ============================================================================
-- SESSION CORRELATION (Traces + Logs)
-- ============================================================================

.print ''
.print '=== SESSION CORRELATION ==='
.print ''

-- Sessions with both traces and logs
.print '--- Sessions with Both Traces and Logs ---'
SELECT
    COALESCE(t.session_id, l.session_id) as session_id,
    COUNT(DISTINCT t.span_id) as span_count,
    COUNT(DISTINCT l.body) as log_count
FROM otlp_traces t
FULL OUTER JOIN otlp_logs l
    ON t.session_id = l.session_id
WHERE COALESCE(t.session_id, l.session_id) IS NOT NULL
GROUP BY COALESCE(t.session_id, l.session_id)
ORDER BY span_count + log_count DESC
LIMIT 10;

.print ''
.print 'Verification complete!'
