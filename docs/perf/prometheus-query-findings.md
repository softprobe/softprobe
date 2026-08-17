# Prometheus query performance — findings, plan, and benchmark

**Status:** In progress — Phase A/B/C partially landed with kill-case measurements  
**Date:** 2026-08-14  
**Context:** Host killed under OpenTelemetry Demo traffic + Grafana Prom dashboards; DuckDB/PromQL path felt unacceptably slow.  
**Scope:** Metrics storage + Prometheus-compatible **query** path (`DuckLakeMetricsBackend` + PromQL eval). Traces/logs are out of scope except where shared DuckLake maintenance applies.

Related:

- Compat design: [`../compat/phase1-prometheus.md`](../compat/phase1-prometheus.md)
- VARIANT shredding: [`../variant_shredding.md`](../variant_shredding.md)
- Column promotion: [`../promotion.md`](../promotion.md)
- Architecture invariants: [`../decision_log.md`](../decision_log.md) (flush-through ingest, no app buffer)
- Positioning: storage/SQL first; Prom is query compatibility, not a second TSDB write path ([`../positioning.md`](../positioning.md), [`../goals.md`](../goals.md))
- **Proposed layout (implementation source of truth for this problem):** [`../metrics-timeseries-layout.md`](../metrics-timeseries-layout.md) — DuckLake postings + skinny samples + TWCS-shaped maintenance (Greptime-inspired) + 5m/1h ladder + collapse; 39 machine ACs including 30d/90d, snapshots, small files, histograms, GOLD

---

## 1. Problem statement

Softprobe’s product bet is **durable DuckLake/Parquet evidence**, with Prometheus/Grafana as a **query-only** convenience. We do **not** need to beat purpose-built TSDBs on raw PromQL latency. We do need:

1. Demo + Grafana (and similar SRE dashboards) to run without OOMing or wedging the host.
2. Query latency that is **acceptable** for interactive Grafana (seconds, not tens of seconds / timeouts) at modest cardinality.
3. Storage features we already built (VARIANT shredding, promotion, compaction, file caches) actually helping the Prom path — today several of them do not.

---

## 2. Findings (current code)

Audit date: 2026-08-14 against `src/compat/backends/ducklake_metrics.rs`, `src/query/{duckdb,cache}.rs`, `src/storage/ducklake/{writer,compaction}`, `src/config.rs`, and related docs/tests.

### 2.1 Column promotion and VARIANT shredding — write yes, Prom query yes (lean scalars)

| Feature | Write / storage | Prometheus query path |
|---------|-----------------|------------------------|
| VARIANT on `metrics.attributes` / `resource_attributes` | Yes — JSON staging → `::JSON::VARIANT` on INSERT; typed shredding for a few hot keys (`gen_ai.usage.*_tokens`, `sp.cost.total`) | **Used.** Equality matchers and SELECT project per-key `CAST(col['k'] AS VARCHAR)` (and promoted columns). **No** `CAST(... AS JSON)` on the sample path. |
| Telemetry column promotion | Opt-in via `POST /v1/promotions/apply`; ingest extracts into typed columns | **Used when applied.** Canonical hot-label manifest [`docs/promotion/metrics-prom-hot-labels.yaml`](../promotion/metrics-prom-hot-labels.yaml); COALESCE(promoted, VARIANT path). Softprobe does not auto-promote arbitrary keys. |

**Implication:** Series identity \(N\) is resolved from reserved aliases ∪ matcher/grouping labels ∪ active promotion sources ∪ cached `ducklake_file_variant_stats` paths (capped). Missing keys yield NULL labels — never a full JSON blob decode. Bare VARIANT SELECT remains unsupported by duckdb-rs.

### 2.2 Global locks — largely avoided on hot paths

| Path | Locking |
|------|---------|
| Query | Independent DuckDB worker pool, round-robin; no process-global query mutex |
| Writer | Per-scope connection pool; `try_lock` across pool, then block on one connection |
| Promotion apply | Process mutex (SQLite) / `pg_advisory_xact_lock` (Postgres) — DDL only |
| Tenant engine create | Per-tenant creation lock only |

**Implication:** Lock contention is unlikely the primary Grafana failure mode. Catalog serialization conflicts can still cause **compaction to skip**, which worsens small-file pressure under concurrent ingest.

### 2.3 Small Parquet files — mitigated in design, undermined by flush-through + demo churn

**Mitigations present:**

- `maintenance.target_file_size_bytes` default **64 MiB**
- Default-on `ducklake_merge_adjacent_files` + snapshot/orphan cleanup (hourly)
- Writer sets `target_file_size` + `hive_file_pattern`; metrics `ORDER BY record_date, metric_name, timestamp`
- DuckLake `data_inlining_row_limit` default **10_000** (small batches stay in catalog metadata)

**Gaps:**

- Ingest is **flush-through** (ADR invariant): every OTLP request commits immediately; no application buffer/WAL ([`decision_log.md`](../decision_log.md)). Upstream collector batching is assumed.
- OTel Demo + many exporters produce high-frequency small batches → many files until merge catches up.
- Compaction may **skip** on transient DuckLake metadata conflicts.
- Staging Parquet uses default `WriterProperties` (no explicit bloom / row-group sizing).

**Implication:** Under demo load, file count can explode before merge; Prom scans pay metadata + open cost repeatedly.

### 2.4 Bloom filters and predicate pushdown — weak for Prom

- **Effective SQL pushdown today:** `timestamp` range + equality on typed `metric_name` + promoted-column / VARIANT field equality for `job` / `instance` / safe Prom labels.
- **SELECT projects scalar labels only** (promoted + per-key VARIANT VARCHAR); full-blob JSON cast removed from the Prom sample path.
- **No pushdown:** regex / inequality matchers — applied after scan/projection in Rust, with scan cap `max(max_series*10, 10000)`.

**Implication:** Narrow Grafana selectors (`{job=...}`) prune early. Unfiltered panels still avoid full attribute JSON materialization by projecting the known identity key set \(N\) as scalars.

### 2.5 Caching — DuckDB file/object caches yes; Prom result cache no

When `query.cache_dir` is set (default `/var/tmp/softprobe/duckdb`):

- DuckDB: `enable_object_cache`, `enable_external_file_cache`, `enable_http_metadata_cache`, `parquet_metadata_cache`, `experimental_metadata_reuse`
- Optional `cache_httpfs` on-disk wrap for S3/httpfs

**Gaps:**

- No application-level Prom series / query-result cache across Grafana refreshes.
- Within one `query_range`, one SQL fetch + in-memory step eval is already good (avoids O(steps) SQL).
- “Segment cache” in the TSDB sense does not exist; rely on DuckDB byte/metadata caches only.

---

## 3. Root-cause summary (ranked)

1. **Prom SQL materializes full series identity as VARCHAR scalars** (promoted + VARIANT paths); full-blob JSON cast eliminated — remaining cost is wide \(N\) or missing promotions.
2. **Flush-through + demo churn → many small files** → expensive scans; compaction lag / skip under contention.
3. **Prom-level result reuse** helps Grafana refresh storms (short TTL); cold first paint still pays scan cost.
4. **No bloom / weak attribute pushdown** beyond VARIANT field / promoted equality → cannot skip row groups for every selector shape.
5. Locks are a lesser concern on the hot path.

---

## 4. Improvement plan

Principles:

- Keep **OTLP as the only write path** and Prom as **query-only** ([`compat/matrix.md`](../compat/matrix.md)).
- Prefer unlocking **existing** storage features over inventing a TSDB.
- Stay compatible with flush-through unless a measured ADR change is approved (collector batching first).
- Optimize for **acceptable Grafana** + **storage efficiency**, not VictoriaMetrics-class latency.

### Phase A — Prom path uses storage features (highest leverage)

**A1. Stop `CAST(attributes AS JSON)` in filter predicates.**  
Push equality (and later regex) matchers as VARIANT field access:

```sql
CAST(resource_attributes['service.name'] AS VARCHAR) = '...'
-- or attributes['job'], etc.
```

Keep JSON cast only for the final projection needed to expand classic series, or project only keys required by matchers + known Prom label set.

**A2. Extend SQL pushdown beyond `__name__` / `job`.**  
Priority order: `instance`, `service.name` / `job`, common OTel resource keys, then equality matchers on low-cardinality labels. Keep scan caps fail-loud.

**A3. Optional telemetry promotion for hot Prom labels.**  
Document a recommended `telemetry_columns` manifest for metrics (e.g. `service_name`, `instance`) so Grafana selectors hit typed columns. Do **not** auto-promote every label.

**A4. Select fewer columns / avoid fidelity payloads when unused.**  
Gauges/sums should not pull histogram arrays when the selector cannot need them.

**Exit criteria:** Same Grafana GOLD panel mix stays under a fixed RSS/CPU budget on a sized VM; p95 `query_range` for common selectors drops substantially vs baseline (see §5).

### Phase B — File hygiene under continuous ingest

**Status (2026-08-14):** partially implemented.

**B1. Measure file count / avg size under demo** (`ducklake` metadata tables) before/after.  
Micro-bench now records `parquet_before_compact` / `parquet_after_compact`. Use
`BENCH_FORCE_PARQUET=1` for a small-file stress corpus.

**B2. Tighten maintenance under churn:** default maintenance interval **300s**
(was 3600); compact **metrics first**; merge uses **8 serialization retries × 2
waves**; warn when ≥200 Parquet files remain after a pass.

**B3. Collector-side batching guidance** (docs + demo overlay): larger OTLP
export batches / longer flush intervals so flush-through does not create tiny
files. Prefer this over reintroducing an app buffer (non-goal today). Churn
bench overlay documents the anti-pattern (1s scrape + `send_batch_size: 8`).

**B4. Write-path Parquet properties** (evaluate): larger row groups; enable bloom filters on `metric_name` / promoted columns **if** DuckLake/Parquet path honors them on read. Prototype behind a measured A/B — do not assume Iceberg-era bloom docs still apply.

**Exit criteria:** Under sustained demo ingest, metrics table median data file size trends toward target (or inlining absorbs small batches); file count plateaus with maintenance on.

### Phase C — Caching and Grafana-friendly behavior

**C1. Confirm cache_httpfs + external_file_cache are active** in the demo/deploy config (`query.cache_dir` writable).

**C2. Short-TTL Prom query cache** (optional, tenant-scoped): cache `(tenant, query, start, end, step)` → encoded result for 5–30s to absorb Grafana refresh storms. Invalidate on short TTL only (no cross-tenant state). Keep correctness caps.

**C3. Dashboard guidance:** prefer `rate()` / narrow matchers; document expensive patterns (unmatched high-cardinality `sum(rate(...))` over long ranges).

**Exit criteria:** Repeated identical `query_range` within TTL is cheap; demo host remains stable with Astronomy Shop + Softprobe PromQL folders.

### Phase D — Explicit non-goals (unless ADR revisited)

- Prometheus `remote_write` ingest.
- Reintroducing application WAL / staged tier ([`decision_log.md`](../decision_log.md)).
- Full PromQL / native histogram function parity.
- Beating single-node VictoriaMetrics on alert-rule QPS.

---

## 5. Benchmark strategy

### 5.1 Goal

Compare Softprobe **fairly** against other systems on a **public, open workload**, with:

- **Primary product metric:** storage footprint and open Parquet/DuckLake accessibility (bytes on disk/object store, file count, SQL ad hoc queryability).
- **Secondary gate:** Prom query latency/error rate **acceptable** for Grafana-style and alert-style reads — OK to be slower than purpose-built TSDBs within published bounds.

### 5.2 Primary open benchmark: VictoriaMetrics `prometheus-benchmark`

**Repo:** [VictoriaMetrics/prometheus-benchmark](https://github.com/VictoriaMetrics/prometheus-benchmark) (Apache-2.0)  
**Why this one:**

- Open, documented, used publicly to compare VictoriaMetrics / Mimir / Cortex / Thanos.
- Supports **OTLP write** (Host Metrics Receiver → OTLP) — matches Softprobe’s canonical ingest ([README](https://github.com/VictoriaMetrics/prometheus-benchmark)).
- Supports **Prometheus Instant API** read load via `vmalert` + shared `alerts.yaml` (node_exporter-style rules).
- Can drive **multiple backends in parallel** under `remoteStorages` for head-to-head numbers.
- Optional churn knobs (Kubernetes-like series churn).

**Softprobe wiring:**

| Side | How |
|------|-----|
| Write | OTel Collector → Softprobe OTLP `/v1/metrics` (Bearer tenant key). Disable remote_write path for Softprobe. |
| Read | Point `vmalert` at Softprobe `/api/v1` with the same auth. Restrict rules to the declared PromQL subset ([`compat/matrix.md`](../compat/matrix.md)); unsupported rules must be removed or expected as errors, not silent skips. |
| Compare | Same chart values, same duration, same hardware class. |

**Suggested competitor set (same box / same k8s node pool):**

1. Softprobe (DuckLake + Postgres catalog + local or MinIO/S3 data path)
2. Prometheus single-node (baseline “good enough” interactive)
3. VictoriaMetrics single-node (fast TSDB reference — expect Softprobe slower)
4. Optional: Grafana Mimir monolithic or ClickHouse + Prometheus remote — only if ops cost is justified

**Published metrics to record** (from benchmark monitor + whitebox):

| Metric | Source | Softprobe acceptance stance |
|--------|--------|------------------------------|
| OTLP accept rate / failed sends | `otelcol_exporter_*` | Must keep up with configured load without sustained drop growth |
| Pending / retry signals | collector / Softprobe logs | No unbounded backlog |
| Query p99 iteration duration | `vmalert_iteration_duration_seconds` | **≤ 5× Prometheus single-node** on the same alert set after Phase A, or absolute p99 ≤ 5s for the curated subset — pick one bar and publish it |
| Query error rate | `vmalert_execution_errors_total` | 0 for curated supported rules |
| On-disk / object bytes | whitebox | Report vs competitors; Softprobe may win or lose; always report Parquet openness |
| File count / avg file size | DuckLake metadata | Trend toward target size under maintenance |

“Acceptable” means: curated alert/dashboard queries succeed reliably and stay within the published latency bar; we **do not** claim TSDB leadership.

### 5.3 Secondary workloads (complementary)

| Workload | Role |
|----------|------|
| **Option A micro-bench** `make bench-prom-baseline` | Softprobe-only hostmetrics + curated PromQL; writes `docs/perf/results/`. Use this to A/B each fix. |
| **Existing** `make test-perf` / `perf_stress` | Internal regression; warm SQL SLOs (`.github/workflows/performance.yml`). Keep as CI-adjacent gate, not competitor compare. |
| **Grafana manual** `make grafana-up` + Astronomy Shop | Real multi-panel stress; pass = host stable + panels refresh without 5xx/timeout storms. |
| **TSBS `cpu-only`** ([timescale/tsbs](https://github.com/timescale/tsbs)) | Optional storage-shape compare (host count × interval). Load Softprobe via an OTLP converter; competitors via their native loaders. Use for **bytes and scan cost**, not as the primary PromQL bake-off (loader asymmetry). |

### 5.4 Benchmark runbook (minimal)

**Local iteration (Option A — implemented):**

```bash
make bench-prom-baseline                                    # label=baseline
BENCH_LABEL=variant-pushdown make bench-prom-baseline       # after a fix
# High-cardinality kill-case (no hostmetrics; OTLP loadgen):
BENCH_CARDINALITY=40 BENCH_WARMUP_SECS=20 BENCH_MEASURE_SECS=30 BENCH_REPEAT=2 \
  BENCH_LABEL=killcase make bench-prom-baseline
make bench-prom-down
```

Harness: `tests/compat/prometheus/benchmark/`. Results: `docs/perf/results/<stamp>-<label>.{json,md}`.
Fails closed if `ok_requests == 0` (latency without success is meaningless).

### 5.4.1 Kill-case A/B (2026-08-14)

Workload: 40 jobs × 3 instances gauge `bench_http_requests`, 20s warmup / 30s measure, repeat=2.

| Label | overall p50 | unfiltered `{__name__=...}` p50 | `{job=...}` p50 | ok |
|-------|-------------|----------------------------------|-----------------|-----|
| `killcase-before` (`1d0a827`) | 222ms | 236ms | 53ms | 140/140 |
| `killcase-after` (lean scan + fidelity + scan cache) | 132ms | 139ms | 32ms | 160/160 |
| `killcase-after-cache` (+ Prom `query_range` TTL cache) | 27ms | 30ms | 22ms | 240/240 |
| `killcase-lean-scalar` (no JSON + hot-label promo + caches) | 25ms | 25ms | 20ms | 220/220 |

p95 stays high on cold misses (full scan still ~250–400ms); the cache is for Grafana refresh storms, not first-panel paint. Lean scalar SELECT + static hot-label promotion keep cold-ish identity projection off the full JSON path.

Bare-VARIANT SELECT was attempted and **rejected**: DuckDB client error `decoding Variant columns is not supported`. SELECT uses per-key `CAST(col['k'] AS VARCHAR)` and promoted columns — not bare VARIANT and not full-blob `CAST(... AS JSON)`. Artifact: `docs/perf/results/20260814T043035Z-killcase-lean-scalar.{json,md}`.

**Competitor compare (Option B — not yet wired):**

1. Pin versions: Softprobe commit, DuckDB/DuckLake, Postgres, competitor image tags, prometheus-benchmark commit.
2. Hardware: single documented VM size (vCPU/RAM/disk); no noisy neighbors.
3. Warmup 10–15 min; measure 30–60 min steady state.
4. Softprobe config: maintenance on, `query.cache_dir` on local SSD, realistic `data_inlining_row_limit`, collector batching documented.
5. Capture: monitor VMUI screenshots/metrics export + Softprobe `/health` + DuckLake file stats + `ps` RSS high-water.
6. Store results under `docs/perf/results/<date>-prometheus-benchmark.md` (tables + methodology; no secrets).

### 5.5 Success definition for this program

| Gate | Pass |
|------|------|
| Stability | Demo + Grafana GOLD folder does not OOM the sized host |
| Open benchmark | Softprobe completes prometheus-benchmark OTLP+vmalert run with 0 errors on curated rules |
| Latency | Meets published “≤ 5× Prometheus” or “p99 ≤ 5s” bar on that curated set |
| Storage story | Results doc includes Parquet/DuckLake size + “query same data with SQL” smoke |
| Feature use | Phase A done: Prom predicates use VARIANT (and optional promoted columns), not JSON-extract for pushed matchers |

---

## 6. Implementation sketch (engineering tickets)

1. **Done (Option A):** `make bench-prom-baseline` harness + results dir + high-card loadgen (`BENCH_CARDINALITY`).
2. **Done:** labeled `baseline` / `killcase-before` runs before lean-scan changes.
3. **Done:** VARIANT pushdown for `job` / `instance` / safe equality labels; unit tests; kill-case remeasure.
4. **Done:** skip histogram fidelity columns for plain gauges; drop SQL ORDER BY; 15s scan cache + Prom `query_range` result cache.
5. **Done:** lean scalar Prom SELECT (no `CAST(... AS JSON)`); resolve \(N\) from variant stats + promotions + aliases; promotion-aware matchers.
6. **Done:** canonical metrics hot-label manifest + apply in bench/grafana-up before ingest.
7. **Done (partial):** compaction interval 300s, metrics-first merge, retries (see Phase B).
8. (Later) Wire VictoriaMetrics prometheus-benchmark overlay for competitor numbers.
9. Publish first results; adjust latency bar if needed with evidence.
10. (Next) Grafana/demo stress validation (`make grafana-up` / Astronomy Shop) under the caches.

---

## 7. References

- [VictoriaMetrics/prometheus-benchmark](https://github.com/VictoriaMetrics/prometheus-benchmark)
- [Benchmarking Prometheus-compatible time series databases](https://victoriametrics.com/blog/remote-write-benchmark/)
- Softprobe Prom design: [`../compat/phase1-prometheus.md`](../compat/phase1-prometheus.md)
- VARIANT: [`../variant_shredding.md`](../variant_shredding.md)
- Promotion: [`../promotion.md`](../promotion.md)
- Flush-through ADR: [`../decision_log.md`](../decision_log.md)
