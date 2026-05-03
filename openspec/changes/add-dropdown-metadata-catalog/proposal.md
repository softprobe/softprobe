## Why

Hosted UIs need tenant-scoped filter dropdowns (service names, status codes, etc.) over DuckLake-backed telemetry. Distinct values must be discoverable without unconstrained SQL and must reflect data whether DuckLake inlines small batches into Postgres metadata or writes Parquet to object storage.

## What Changes

- **ADDED** Postgres EAV table `ui_dropdown_catalog` in the same database/schema as DuckLake metadata (application table, not DuckLake internals).
- **ADDED** Ingest-time upsert from trace `RecordBatch` columns **before** each DuckLake `INSERT` (order: materialize batch → catalog upsert → lake write).
- **ADDED** Config `dropdown_catalog` (enable, TTL days, maintenance prune, optional column skip list).
- **ADDED** Hosted `GET /v1/catalog/entity-types` and `GET /v1/catalog/values` (bearer auth, tenant-scoped).
- **ADDED** Optional TTL prune in the existing maintenance loop.
- **Public contract:** `spec/protocol/http-control-api.md` and JSON Schemas for responses.

## Impact

- Affected code: `softprobe-runtime` (`catalog/`, `storage/ducklake/`, `ingest_engine/`, `compaction/`, `hosted.rs`, `config.rs`).
- Affected docs: `docs-site/public/ai-context.md`, `e2e/softprobe-runtime.yaml` sample.
- **Out of scope:** OSS-only deployments without hosted auth and without Postgres DuckLake metadata (catalog remains disabled or unwired).
