# DuckLake ingest cleanup (buffer + hollow tiering)

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
| **sqlite** | Local multi-client (`META_JOURNAL_MODE=WAL`, `META_BUSY_TIMEOUT=500`) |
| **duckdb** | **Rejected** — DuckLake single-client only |

Softprobe does **not** reattach / `mem::forget` query connections after writes. Visibility follows the catalog backend.

## 3. Mental model

| Belief | Reality |
|--------|---------|
| Inlining replaced WAL + buffer | WAL/staged were hollow. Buffer was the batch gate. Inlining chooses catalog rows vs Parquet **after** write. |
| Buffer safe to remove | Yes when the OTel collector batches. |
| Query needs Softprobe reattach hacks | No — use postgres (prod) or sqlite (local). |

## 4. Quality gates (hard bar)

Both MinIO and DuckLake Postgres are **mandatory** for pre-merge. Redis is also required for tenant OTLP/session e2e suites:

```bash
make setup-local
make check-local
make check-local-postgres
make check-local-redis
make test                   # test-quick + full integration-e2e
```

### Exit checklist

- [x] Flush-through ingest; hollow WAL/staged/buffer removed
- [x] Inlining default 10000 kept as feature
- [x] Local sqlite + prod postgres; duckdb catalog rejected
- [x] Writer connection reuse; DuckLake retry defaults pinned
- [x] No Softprobe catalog reattach / metadata-pointer hacks
- [x] Maintenance walks registry tenant scopes
- [x] Write failures → HTTP 503
