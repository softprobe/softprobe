-- Verify a specific session across traces and logs
-- Usage: duckdb -c ".read scripts/verify_session.sql" -c "CALL verify_session('your-session-id');"

-- Load Iceberg extension
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

-- Create views for Iceberg tables (using S3 paths)
CREATE OR REPLACE VIEW traces AS
SELECT * FROM iceberg_scan('s3://warehouse/default/traces', allow_moved_paths := true);

CREATE OR REPLACE VIEW logs AS
SELECT * FROM iceberg_scan('s3://warehouse/default/logs', allow_moved_paths := true);

-- Create a macro to verify a session
CREATE OR REPLACE MACRO verify_session(sid) AS TABLE
WITH traces AS (
    SELECT
        'trace' as type,
        session_id,
        trace_id,
        span_id,
        message_type as content,
        timestamp,
        record_date
    FROM traces
    WHERE session_id = sid
),
logs AS (
    SELECT
        'log' as type,
        session_id,
        trace_id,
        span_id,
        body as content,
        timestamp,
        record_date
    FROM logs
    WHERE session_id = sid
)
SELECT * FROM traces
UNION ALL
SELECT * FROM logs
ORDER BY timestamp;

-- Example usage (uncomment and modify):
-- SELECT * FROM verify_session('example-session-id');
