# DuckLake ingest cleanup (buffer + hollow tiering)

> **Legacy migration record.** The resulting flush-through behavior is
> documented in [`../design.md`](../design.md). This file preserves the
> completed cleanup rationale and stress observations.

Status: **done** — flush-through ingest; OTel collector owns batching; catalog concurrency via official DuckLake backends.

## 1. How often we commit

Ingest is flush-through to DuckLake. Each OTLP request becomes one DuckLake transaction:

`BEGIN` → temp Parquet → `INSERT … SELECT read_parquet` → `COMMIT`

That always updates the **metadata catalog**. Row bodies land in the catalog when DuckLake **data inlining** applies (`DATA_INLINING_ROW_LIMIT` on ATTACH, Softprobe default **10000**). Otherwise data is Parquet under `data_path`.

**Inlining is intentional:** small collector batches should inline rather than create tiny object-store files and snapshot storms. Prefer scaling Postgres (prod) for hot inlined rows over small-file amplification.

Concurrent commit conflicts are retried inside the DuckLake extension (`ducklake_max_retry_count=10`, backoff `1.5`). Softprobe does not wrap INSERT in a second retry loop; exhausted writes return HTTP **503** so exporters retry.

## 2. Catalog concurrency (official)

| `catalog_type` | Role |
|----------------|------|
| **postgres** | Production / multi-tenant (registry scopes) |
| **sqlite** | Local multi-client (`META_JOURNAL_MODE=WAL`, `META_BUSY_TIMEOUT=5000`) |
| **duckdb** | **Rejected** — DuckLake single-client only |

Softprobe does **not** reattach / `mem::forget` query connections after writes. Visibility follows the catalog backend.

### Writer pool + inlining

Each catalog scope key (`catalog_type|metadata_path|metadata_schema|data_path`) owns a pool of already-`ATTACH`'d DuckDB connections (`ducklake.writer_pool_size`, default **4**, clamped 1..=16). Checkout → `INSERT`/`COMMIT` → release. Concurrent same-tenant commits are intended; DuckLake retries conflicts on the Postgres/sqlite catalog.

`DATA_INLINING_ROW_LIMIT` (default **10000**) keeps collector-sized batches in the catalog instead of tiny object-store Parquet files. Raise the limit when OTLP batches regularly exceed it; keep pool size ≥1 so flush-through GCS/parquet paths can still parallelize when inlining does not apply.

DuckDB SQL for writes runs on `spawn_blocking` so Tokio workers are not pinned during GCS/Postgres wait.

**Postgres+GCS stress defaults (validated):**

| Knob | Default | Notes |
|------|---------|--------|
| `data_inlining_row_limit` | **10000** | At batch ≤10k: near-zero data parquet; large latency win vs `0`. Keep unless collectors flush bigger than the limit. |
| `writer_pool_size` | **4** | Fine with inlining; under pure inlining pool=1 is similar. Pool helps more when inserts spill to object storage. **Avoid 8+** — stress showed high HTTP 503 / catalog contention. |

Sweep driver: `scripts/stress_writer_pool_inline.sh`.

## 3. Mental model

| Belief | Reality |
|--------|---------|
| Inlining replaced WAL + buffer | WAL/staged were hollow. Buffer was the batch gate. Inlining chooses catalog rows vs Parquet **after** write. |
| Buffer safe to remove | Yes when the OTel collector batches. |
| Query needs Softprobe reattach hacks | No — use postgres (prod) or sqlite (local). |

## 4. Quality gates (hard bar)

Both MinIO and DuckLake Postgres are **mandatory** for pre-merge:

```bash
make setup-local
make check-local
make check-local-postgres
make test                   # test-quick + full integration-e2e
```

### Exit checklist

- [x] Flush-through ingest; hollow WAL/staged/buffer removed
- [x] Inlining default 10000 kept as feature
- [x] Local sqlite + prod postgres; duckdb catalog rejected
- [x] Writer connection reuse; DuckLake retry defaults pinned
- [x] Per-scope writer pool (`writer_pool_size`, default 4) + spawn_blocking ingest commits
- [x] No Softprobe catalog reattach / metadata-pointer hacks
- [x] Maintenance walks registry tenant scopes
- [x] Write failures → HTTP 503
