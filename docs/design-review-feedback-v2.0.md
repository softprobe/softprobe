# Design Review Feedback v2.0 - Session Data Lake Architecture

**Date**: 2025-01-30  
**Document**: `docs/migration-to-iceberg-design.md`  
**Version**: 1.10 → 2.0

---

## Summary

Replaced the ClickHouse-ingestion design with a simplified data-lake approach: raw OTLP sessions stored as Parquet, Flink ETL builds metadata and analytic tables, ClickHouse serves downstream queries. No Kafka dependency; gateway/worker remains lightweight.

---

## Key Changes

1. **Architecture reset** – New end-to-end flow (gateway → OSS → Flink → Iceberg/ClickHouse) with updated explanations.
2. **Raw session format** – Defined OTLP attribute conventions, buffering thresholds, Parquet layout, and retention strategy.
3. **ETL layer** – Added sections describing Flink jobs for metadata, operation counts, tag counts, and replay index tables.
4. **Roadmap & risks** – Updated implementation phases, monitoring, and open questions for the new approach.

---

## Follow-up Actions

- Build OTLP gateway/worker MVP and confirm buffering behaviour.
- Provision Flink cluster and implement metadata + analytic ETL pipelines.
- Populate ClickHouse tables and adapt replay/testing services to use them.
- Automate retention (OSS lifecycle + Iceberg snapshot expiration).

---

**Status**: Architecture updated; ready for review ✅
