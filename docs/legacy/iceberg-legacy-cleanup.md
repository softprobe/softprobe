# Iceberg Legacy Cleanup

> **Legacy migration record.** This inventory captured the tree before the
> cleanup phases completed. Present-tense statements below describe that
> historical snapshot and are not current implementation guidance. See
> [`../design.md`](../design.md) for the current architecture.

**Date**: 2026-07-16  
**Status**: Done (Phases A–E implemented)  
**Historical context**: At the time of this inventory, the runtime write and
query path had moved to **DuckLake**, while Apache Iceberg REST catalog,
Lakekeeper, and `iceberg-rust` remnants still existed in the tree.

This file preserves the inventory and proposed sequence used during the
completed cleanup. It is historical evidence only; do not use it to open new
cleanup work or infer the current tree.

---

## Historical pre-cleanup inventory (not current truth)

| Layer | Active path | Legacy path still in tree |
| --- | --- | --- |
| Ingest flush / durable commit | `DuckLakeWriter` (`src/storage/ducklake/`) | `IcebergWriter` (`src/storage/iceberg/`) — **tests only** |
| Schema + Arrow conversion | Still defined as `iceberg::spec::Schema` in `tables.rs` / `arrow.rs`, then reused by DuckLake | Iceberg partition/sort/catalog APIs unused by DuckLake writes |
| Query (`union_*` / committed tiers) | DuckLake `ATTACH` + qualified tables when `config.ducklake` is set | REST Iceberg `ATTACH`, `iceberg_scan()`, pinned `iceberg_metadata` cache |
| Maintenance | DuckLake branch in `MaintenanceExecutor` (`ducklake_merge_adjacent_files`, expire, cleanup) | Full Iceberg REST rewrite / snapshot expire path when `ducklake` is absent |
| Local stack | `ducklake-postgres` in `docker-compose.yml` | Lakekeeper + `lakekeeper-db` still defined |

**Rule of thumb**: if code mentions Iceberg but runs only when `config.ducklake.is_none()`, or only constructs `IcebergWriter`, it is cleanup candidate. If DuckLake still imports it for schemas/Arrow, treat it as **shared schema debt**, not dead code.

---

## Footprint summary (approx.)

| Area | Size / signal |
| --- | --- |
| `src/storage/iceberg/` | ~2.9k LOC (`mod`, `writer`, `catalog`, `tables`, `arrow`) |
| `src/query/duckdb.rs` | ~1.7k LOC total; **100+** Iceberg-named symbols / fallback branches |
| `src/compaction/executor.rs` | ~1k LOC; DuckLake + Iceberg dual implementation |
| `tests/integration/iceberg.rs` | ~2.0k LOC (largest single test file) |
| Cargo | `default = ["iceberg_catalog"]`; crates `iceberg` / `iceberg-catalog-rest` optional but on by default |
| Compose | Lakekeeper services still present alongside DuckLake Postgres |

---

## 1. Runtime / library code

### 1.1 Dead or test-only Iceberg writer stack — **high priority delete**

| Path | Role | Notes |
| --- | --- | --- |
| `src/storage/iceberg/mod.rs` | `IcebergWriter` | Production `Storage` holds `Arc<DuckLakeWriter>` only. `IcebergWriter::new` is referenced from `tests/integration/iceberg.rs` (and skipped when DuckLake is configured). |
| `src/storage/iceberg/writer.rs` | Generic Iceberg table writer | Only used by `IcebergWriter`. |
| `src/storage/iceberg/catalog.rs` | REST catalog bootstrap | Used by `IcebergWriter` and Iceberg branch of compaction. |

**Cleanup**: Remove `IcebergWriter` + writer + catalog once compaction Iceberg branch and Iceberg-only tests are gone (or gated behind a non-default feature that CI does not build).

### 1.2 Shared schema / Arrow — **keep logic, drop Iceberg types**

DuckLake still depends on Iceberg-shaped modules:

```text
src/storage/ducklake/mod.rs
  → storage::iceberg::tables::{TraceTable, OtlpLogsTable, OtlpMetricsTable}
  → storage::iceberg::arrow::{spans,logs,metrics}_to_record_batch
  → iceberg::spec::Schema (return type of spans_schema / logs_schema / metrics_schema)
```

Also consumers:

- `src/models/{span,log,metric}.rs` — comments and `to_record_batch` APIs take `iceberg::spec::Schema`
- `src/query/duckdb.rs` — buffer→Parquet uses the same Arrow helpers
- `src/catalog/dropdown.rs` (tests) — TraceTable + arrow conversion
- `src/promotion.rs` — comments still say “Iceberg / DuckLake column names”

**Cleanup** (recommended order):

1. Move `tables.rs` + `arrow.rs` to a neutral module (e.g. `src/storage/schema/` or `src/storage/telemetry_schema/`).
2. Represent canonical columns as Arrow `Schema` (or a thin local struct), not `iceberg::spec::Schema`.
3. Drop Iceberg partition/sort helpers if DuckLake DDL comes from empty Parquet `CREATE TABLE … AS SELECT … LIMIT 0` (current DuckLake bootstrap path).
4. Update model / DuckLake / query imports; delete the `iceberg` crate dependency if nothing else needs it.

Until that extraction lands, **do not delete** `tables.rs` / `arrow.rs`.

### 1.3 Query engine dual path — **large simplification**

File: `src/query/duckdb.rs`

Legacy surfaces still present:

- Constant `CATALOG_ALIAS = "iceberg_catalog"`
- Public SQL aliases `iceberg_spans` / `iceberg_logs` / `iceberg_metrics` (rewritten to `tm_icb_*`)
- `IcebergSource` enum: `Pinned`, `Catalog`, `ScanUri`, `Stub`
- `resolve_iceberg_source`, `create_iceberg_view`, `iceberg_pinned_metadata`, `iceberg_scan(...)` SQL
- Session init still `INSTALL iceberg` “for backward-compatible fallback”
- Non-DuckLake `ATTACH … TYPE ICEBERG` using `config.iceberg.*`
- View counters `iceberg_recreates` / env `DUCKDB_TEST_ICEBERG_FALLBACK_PATH`

When `config.ducklake.is_some()` (normal deployments and `TestPipeline`), most of this is short-circuited to attached catalog tables — but the code and complexity remain.

**Cleanup**:

1. Make DuckLake attach the only catalog path; delete Iceberg ATTACH / `iceberg_scan` / pinned-metadata branches.
2. Rename `tm_icb_*` / `iceberg_*` aliases to committed-tier names that match DuckLake (`committed_*` already maps to `tm_cq_*`).
3. Stop installing the DuckDB `iceberg` extension unless a supported external Iceberg read path is explicitly required.
4. Rename cache dir `iceberg_metadata` (see DuckLake writer metadata pointer below).

### 1.4 Compaction / maintenance — **trim Iceberg branch**

File: `src/compaction/executor.rs`

- If `config.ducklake.is_some()`, `MaintenanceExecutor::new` skips Iceberg catalog init and `run_once` → `run_once_ducklake`.
- Else: Iceberg REST catalog, rewrite plans, snapshot expire, etc.

**Cleanup**: Keep DuckLake maintenance only; delete Iceberg catalog fields and helpers. Align config flags (`compaction.*`) docs with DuckLake CALL APIs.

### 1.5 Config — **`IcebergConfig` still required in YAML shape**

`src/config.rs`:

- `Config.iceberg: IcebergConfig` with `#[serde(default)]` — DuckLake deploys do not need a real catalog, but every config struct still carries warehouse / REST URI / write sizes.
- `Config.ducklake: Option<DuckLakeConfig>` — primary.

YAML files that still declare `iceberg:`:

- `tests/config/test.yaml`, `test-r2.yaml`, `test-gcp.yaml`
- `config-local-s3-test.yaml`, `config-aws-*.yaml`, `config-gcs-benchmark.yaml`

**Cleanup**:

1. Stop requiring / documenting `iceberg:` in new configs.
2. After query/compaction Iceberg branches are gone, remove `IcebergConfig` (or gate behind `#[cfg(feature = "iceberg_catalog")]` until the feature itself is removed).
3. Drop env overrides such as `ICEBERG_WAREHOUSE`, `ICEBERG_DISABLE_TLS_VALIDATION` once unused.

### 1.6 Naming / comments only

| Location | Issue |
| --- | --- |
| `src/storage/mod.rs` | Doc comments still say “Iceberg” / “flush callback” to Iceberg |
| `src/storage/schema.rs` | Stub comment: “Schema definitions moved to Iceberg…” |
| `src/storage/ducklake/mod.rs` | Writes metadata under cache `iceberg_metadata/` |
| `Cargo.toml` package description | Still says “Iceberg” |
| Domain model docs | “matches the Iceberg schema…” |

Cheap wins: rename dirs/comments in the same PR as schema extraction.

---

## 2. Dependencies and features

```toml
# Cargo.toml (current)
default = ["iceberg_catalog"]
iceberg_catalog = ["iceberg", "iceberg-catalog-rest", "object_store"]
```

**Cleanup goals**:

1. Default features should not pull Iceberg once runtime no longer needs it.
2. Remove `iceberg` / `iceberg-catalog-rest` crates after schema extraction + Iceberg writer/compaction deletion.
3. Re-evaluate whether `object_store` was only needed for Iceberg I/O.

---

## 3. Infrastructure and local stack

### 3.1 `docker-compose.yml`

Still defines:

- `ducklake-postgres` (active)
- `db` / `migrate` / `lakekeeper` / volumes `lakekeeper-postgres` (legacy Iceberg REST catalog)

**Cleanup**: Remove Lakekeeper services and volumes if no remaining test or script requires `:8181`. Update any README / make targets that wait on Lakekeeper health.

### 3.2 Benchmark / ops scripts

| Path | Legacy behavior |
| --- | --- |
| `scripts/aws_benchmark.sh` | Deploys Lakekeeper compose, bootstraps warehouse `key-prefix: iceberg`, emits `iceberg:` in generated config |
| `scripts/safe_aws_test.sh` | Clears `cache/iceberg_metadata`, restarts Lakekeeper |
| `scripts/benchmark_gcs_server.sh` | Documents Iceberg REST catalog overrides |
| `scripts/manual_test_guide.md` | Instructs `iceberg_scan('s3://warehouse/traces')` |
| `scripts/TEST_RESULTS.md` | Archived Iceberg verification narrative |

**Cleanup**: Retarget benchmarks to DuckLake ATTACH (see `docs/adhoc-duckdb-ducklake.md`); delete Lakekeeper deploy steps; rewrite manual verification SQL.

---

## 4. Tests

### 4.1 Misnamed but still useful

| Path | Reality |
| --- | --- |
| `tests/util/iceberg.rs` | Loads config + assigns **unique DuckLake** paths; derives `wal_bucket` from `iceberg.warehouse` as fallback |
| `tests/integration/iceberg.rs` | Mix of skipped IcebergWriter tests, DuckLake roundtrips, and Iceberg-catalog-only cases gated on `config.ducklake.is_none()` |
| `tests/integration/storage_contract_validation.rs` | Explicitly DuckLake contracts; still imports `util::iceberg` |
| `tests/fixtures/legacy_verify_iceberg.sql` | DuckLake SQL; filename historical |
| Makefile | `ICEBERG_TEST_TYPE=local|r2`, isolated prefix `integration::iceberg::` |

### 4.2 Recommended test cleanup

1. Rename `tests/util/iceberg.rs` → `tests/util/storage_config.rs` (or similar); stop reading warehouse from `config.iceberg`.
2. Split `tests/integration/iceberg.rs`:
   - Keep DuckLake e2e roundtrips under a neutral name (`ingest_commit_query.rs`, etc.).
   - Delete or archive pure IcebergWriter / REST catalog tests.
3. Rename fixtures: `legacy_verify_iceberg.sql` → `verify_ducklake.sql` (or drop if superseded by `docs/adhoc-duckdb-ducklake.md`).
4. Replace `ICEBERG_TEST_TYPE` with something like `STORAGE_TEST_TYPE` / `E2E_BACKEND` (or drop if only `local` remains).
5. Delete or rewrite `tests/config/test-r2.yaml` Iceberg-catalog narrative if R2 Iceberg catalog is no longer a product path; keep R2 only as S3-compatible object store for DuckLake `data_path` if still needed.
6. Performance tests (`tests/integration/performance.rs`) still query `iceberg_logs` and assert `iceberg_recreates` — update to DuckLake view counters / SQL names.

---

## 5. Documentation drift found at the time

The pre-cleanup audit found Iceberg-primary material in the root README,
design, goals, storage design, decision log, Grafana guide, and cloud benchmark
guides.

That documentation migration is now closed:

- current runtime truth is [`../design.md`](../design.md);
- current decisions are in [`../decision_log.md`](../decision_log.md);
- current ad hoc SQL guidance is
  [`../adhoc-duckdb-ducklake.md`](../adhoc-duckdb-ducklake.md);
- superseded Iceberg, buffering, WAL, Grafana, benchmark, and migration
  documents are retained in this `legacy/` directory.

---

## 6. Historical proposed cleanup phases

The bullets below preserve the original proposed sequencing. They are not a
live checklist or a description of the current tree.

### Phase A — Stop the bleeding (low risk)

- Ban new Iceberg-only APIs; document DuckLake as the only write path.
- Fix misleading comments in `storage/mod.rs`, models, Cargo package description.
- Mark ADR-001 superseded; add a DuckLake ADR.
- Banner outdated docs (`design.md`, `grafana.md`) with “historical / pre-DuckLake”.

### Phase B — Delete unreachable Iceberg write path (medium)

- Delete Iceberg-only integration tests / skip branches.
- Remove `IcebergWriter`, `writer.rs`, `catalog.rs` once unreferenced.
- Remove Lakekeeper from `docker-compose.yml` and AWS benchmark Lakekeeper deploy.
- Drop `iceberg:` from default test YAML; stop deriving WAL bucket from Iceberg warehouse.

### Phase C — Query + maintenance simplification (medium–high)

- Delete Iceberg ATTACH / `iceberg_scan` / pinned-metadata code in `duckdb.rs`.
- Delete Iceberg branch of `MaintenanceExecutor`.
- Remove `INSTALL iceberg` from session init if unused.
- Rename `iceberg_metadata` cache directory and `iceberg_*` SQL aliases.

### Phase D — Schema decoupling + dependency removal (highest leverage)

- Extract telemetry schema + Arrow conversion out of `iceberg::spec`.
- Remove `IcebergConfig` from `Config`.
- Remove Cargo feature `iceberg_catalog` and crates `iceberg`, `iceberg-catalog-rest`.
- Update CI / Makefile env vars and rename `tests/integration/iceberg*`.

### Phase E — Doc & script finish

- Rewrite or archive Iceberg-centric guides; align Grafana / manual test SQL with DuckLake.
- Clean `scripts/TEST_RESULTS.md` and legacy fixture names.

**Quality gates:** every phase PR must satisfy [§9](#9-quality-gates-for-the-cleanup) before merge.

---

## 7. What *not* to delete blindly

1. **`tables.rs` / `arrow.rs` column definitions** — DuckLake and query buffer paths need them until replaced with Arrow-native schema.
2. **DuckLake maintenance CALL wrappers** in `compaction/executor.rs` — these are the post-migration equivalent of Iceberg compaction.
3. **Historical ADRs** — supersede, do not erase decision history.
4. **Object-store / S3 config** — still required for DuckLake `data_path`; only Iceberg *catalog* pieces are legacy.

---

## 8. Quick grep checklist for PRs

Use these to confirm a cleanup PR actually shrinks the footprint:

```bash
rg -n 'IcebergWriter|IcebergCatalog|iceberg_catalog_rest|TYPE ICEBERG|iceberg_scan' src tests
rg -n 'lakekeeper|Lakekeeper|ICEBERG_TEST_TYPE|ICEBERG_WAREHOUSE' .
rg -n 'iceberg::' Cargo.toml src
```

Success criteria for “Iceberg legacy cleaned”:

- No Lakekeeper in default compose.
- No `iceberg` Rust crates in default build.
- No `iceberg_scan` / REST Iceberg ATTACH in query workers.
- Schema module has no `iceberg::` imports.
- Tests and docs say DuckLake for durable storage.

Full exit checklist (including local e2e): [§9.6](#96-exit-criteria-for-cleanup-done).

---

## 9. Quality gates for the cleanup

Cleanup will span multiple PRs. Each phase must pass the gates below before merge.
Do not rely on “it compiles” alone — DuckLake attach, MinIO, and Postgres-catalog
paths fail in ways unit tests miss.

### 9.1 Local end-to-end environment at inventory time

| Component | How to start | Needed for |
| --- | --- | --- |
| MinIO (`:9000`, bucket `warehouse`) | `make setup-local` or `make setup-minio` | Almost all `integration-e2e` tests (`check-local`) |
| DuckLake Postgres (`localhost:5432`, db/user `ducklake`) | `make setup-local` | Tenant registry, OTLP isolation, promotion DDL/metadata tests |
| Lakekeeper (`:8181`) | Was still in `docker-compose.yml` | **Not required** for DuckLake e2e; proposed for removal in Phase B |

```bash
make setup-local          # MinIO + ducklake-postgres
make check-local          # MinIO only (required by test-local)
make check-local-postgres # Optional but required for tenant/promotion suites
make test-quick           # Lib + lightweight tests (no MinIO)
make test-local           # Full integration-e2e (isolated processes)
make test                 # test-quick + test-local  ← default pre-merge bar
make lint && make check-fmt
```

Automated DuckLake shape checks live in
`tests/integration/storage_contract_validation.rs`
(replaces removed `verify_e2e.sh` / Iceberg SQL scripts).
Ad hoc SQL: [adhoc-duckdb-ducklake.md](../adhoc-duckdb-ducklake.md) / `make duckdb-shell`.

**Note:** `make test-local` sets `ICEBERG_TEST_TYPE=local` for historical reasons;
backend under test is DuckLake + MinIO. Rename env vars in Phase D, not as a gate blocker.

`make check-local` (used by `test-local`) only verifies MinIO. Many ingest/query tests use
**file DuckLake metadata** + `s3://warehouse` data. Start Postgres via `setup-local` whenever
changing tenant/promotion code.

### 9.2 Mandatory gate matrix (every cleanup PR)

| Gate | Command | Fail means |
| --- | --- | --- |
| Format | `make check-fmt` | Style drift |
| Lint | `make lint` (`clippy -D warnings`) | API misuse / dead warns |
| Unit / light | `make test-quick` | Core regressions without Docker |
| Local e2e | `make test-local` (after `setup-local`) | Ingest → commit → union/query broken |
| Default bar | `make test` | Same as quick + local |

Optional / phase-dependent:

| Gate | When | Command |
| --- | --- | --- |
| Postgres catalog suites | Any PR touching tenants, promotion, or `DuckLakeScopeResolver` | Ensure `check-local-postgres`; confirm `tenant_*` / `promotion_*` ran under `test-local` |
| Build without Iceberg feature | Phase D (dependency removal) | `cargo build --no-default-features` (once defaults no longer require `iceberg_catalog`) |
| R2 object store | Only if still a product path | `make test-r2` (credentials required; do not block local cleanup) |
| Perf smoke | After query-engine changes (Phase C) | `INTEGRATION_PERF_TESTS` subset already invoked by `test-local` |
| Manual DuckDB attach | After catalog/ATTACH changes | `make duckdb-shell` + `SELECT 1` / count on `traces` |

CI note: repo GitHub Actions coverage is thin (see `.github/workflows/performance-tests.yml`).
Treat **local Make targets** as the authoritative gate for this cleanup.

### 9.3 Per-phase gate focus

| Phase | Extra focus beyond `make test` |
| --- | --- |
| **A** Docs/comments | `make test-quick` sufficient if no code paths change |
| **B** Delete Iceberg writer / Lakekeeper | Full `make test`; explicitly run `storage_contract_validation` and former roundtrip tests (renamed or still under `integration::iceberg::*`); confirm compose no longer needs `:8181` |
| **C** Query + maintenance trim | Full `make test` + perf subset; spot-check `union_spans` / `union_logs` and that `iceberg_scan` is gone from hot path |
| **D** Schema + crate removal | `make test` + `cargo build` with new default features; grep gate: no `iceberg::` in `src/` |
| **E** Docs/scripts | Doc-only; optional `make duckdb-shell` smoke |

### 9.4 Must-not-break behaviors (assert via existing tests)

Keep these green; if a test is deleted as Iceberg-only, **replace** with a DuckLake equivalent first:

1. Ingest flush → durable DuckLake rows → readable via `union_*` / qualified tables  
   (`storage_contract_validation`, bulk roundtrips in `integration/iceberg.rs`)
2. WAL replay / cleanup after flush (WAL tests in same module)
3. Multi-tenant isolation (`tenant_otlp_isolation`)
4. Promotion apply on tenant scope (`promotion_business_table_ddl`, telemetry columns)
5. Maintenance when DuckLake configured (`ducklake_merge_adjacent_files` path — do not only test Iceberg expire)

### 9.5 Suggested PR slicing + gate ownership

1. One phase ≈ one PR (or stacked PRs), each green on §9.2.
2. Never combine “delete Iceberg writer” with “rewrite schema off `iceberg::spec`” in the same PR.
3. Before deleting `tests/integration/iceberg.rs` cases, list which DuckLake tests supersede them in the PR description.
4. After removing Lakekeeper from compose, run `make setup-local && make test` on a clean machine/VM once to catch hidden `:8181` assumptions.

### 9.6 Exit criteria for cleanup done

- [x] `make test` green with MinIO + ducklake-postgres only (no Lakekeeper)
- [x] Default `cargo build` does not depend on `iceberg` / `iceberg-catalog-rest`
- [x] `rg 'iceberg_scan|IcebergWriter|lakekeeper' src tests docker-compose.yml` is empty (or only historical comments)
- [x] Docs point to DuckLake e2e (`make test-local` + `storage_contract_validation`), not Iceberg REST

---

## Related docs

- [Ad hoc DuckDB / DuckLake](../adhoc-duckdb-ducklake.md) — current query CLI path
- [Legacy decision log](decision-log-iceberg-era.md) — superseded ADR-001 Iceberg
- [Storage design](storage-design-iceberg.md) — pre-migration Iceberg analysis (historical)
