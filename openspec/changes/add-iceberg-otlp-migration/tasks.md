## 0. Prep (Completed)
- [x] 0.1 Add minimal OTLP JSON endpoint `/v1/traces` (accepts resourceSpans)
- [x] 0.2 Author Iceberg and ClickHouse DDL files under `schemas/`
- [x] 0.3 Add documentation headers explaining table goals and schema rationale

## 1. Implementation
- [x] 1.1 Implement Rust OTLP `/v1/traces` ingestion with session buffering
- [x] 1.2 Add multi-app row-group batching and Parquet writer to Iceberg
- [x] 1.3 Create Iceberg `raw_sessions` table with partitions and sort order
- [x] 1.4 Stand up ClickHouse and create metadata tables
- [x] 1.5 Build ETL job to populate metadata tables and `session_metadata`
- [ ] 1.6 Connecting ETL jobs to the Iceberg `raw_sessions` table for developement evironment
- [ ] 1.7 Build Java direct-query client (ClickHouse metadata + Iceberg payload reader)
- [ ] 1.8 (If applicable) Forward `sp-storage` writes to OTLP endpoint
- [ ] 1.9 Integrate Java services with direct-query client and caching
- [ ] 1.10 Performance validation against targets; tune flush, batching, caching
- [ ] 1.11 Implement deterministic ordering and cursor-based pagination; ensure consistent session reads

## 2. Validation
- [ ] 2.1 Strict spec validation and internal review
- [ ] 2.2 Benchmarks: ingestion throughput, query latency, ETL lag
- [ ] 2.3 Observability dashboards and alerts
