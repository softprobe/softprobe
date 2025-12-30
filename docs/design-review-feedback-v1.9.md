# Design Review Feedback v1.9 - Session-Centric Architecture

**Date**: 2025-01-29  
**Document**: `docs/migration-to-iceberg-design.md`  
**Version**: 1.8 → 1.9

---

## Summary

Pivoted the design to a session-centric architecture backed by Kafka + ClickHouse, with session payloads stored per session in S3/MinIO. Removed the Iceberg/DuckDB dependency and documented the new ingestion, storage, and query strategy that supports 100K QPS and 60B recordings.

---

## Key Changes

1. **Architecture & Data Flow**  
   - Kafka-based ingestion pipeline with session workers.  
   - ClickHouse becomes the authoritative metadata store; session payloads live in per-session Parquet files.  
   - Updated diagrams, technology stack, and storage strategy.

2. **Schema & Query Model**  
   - Documented ClickHouse `recordings` table and `session_summary` materialised view.  
   - Replaced Iceberg/DuckDB query sections with ClickHouse SQL examples and payload retrieval flow.

3. **Operational Model**  
   - New migration phases covering Kafka, ClickHouse, and retention.  
   - Revised monitoring, cost analysis, and risk assessment for the new stack.  
   - Simplified retention (30-day payload cleanup job).

---

## Follow-up Actions

- Provision Kafka and ClickHouse clusters in staging.  
- Build session worker prototype and measure 100K QPS ingest.  
- Update Java clients to consume ClickHouse metadata + session payload offsets.  
- Implement nightly retention/deletion job.

---

**Status**: Architecture updated; ready for next review ✅
