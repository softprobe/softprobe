-- Optional: `.read` after tables exist. Qualified names must match CONFIG_FILE (default here: softprobe + schema softprobe).
-- Prefer `make duckdb-shell` — it creates only views for tables that exist.

CREATE OR REPLACE VIEW traces AS SELECT * FROM softprobe.softprobe.traces;
CREATE OR REPLACE VIEW logs AS SELECT * FROM softprobe.softprobe.logs;
CREATE OR REPLACE VIEW metric_samples AS SELECT * FROM softprobe.softprobe.metric_samples;
