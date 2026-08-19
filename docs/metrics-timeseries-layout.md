# Metrics time-series layout (DuckLake)

**Status:** Implementation in progress (2026-08-17; +AC-H3..H6 multi-window hist; was 49/49 on 2026-08-16)  
**Date:** 2026-08-15  
**Audience:** Implementation agents. Do not treat this as done until **every required AC-\* id in §10.3.1 (53 ids)** has a passing row in the validated result JSON from §10.3. Evidence: [`docs/perf/results/20260816T084205Z-metrics-layout.json`](perf/results/20260816T084205Z-metrics-layout.json).

Related: [`goals.md`](goals.md), [`design.md`](design.md), [`decision_log.md`](decision_log.md), [`compat/phase1-prometheus.md`](compat/phase1-prometheus.md), [`compat/capability.v0.yaml`](compat/capability.v0.yaml), [`perf/prometheus-query-findings.md`](perf/prometheus-query-findings.md).

This document replaces the assumption that OTLP metrics can live as one fat `metrics` event table and still serve Grafana. Traces and logs are **out of scope** (keep current tables).

**How to toll:** §2.1 maps each goal to AC ids. An implementing agent may say “ready for verification” only when:

```bash
CARGO_PROFILE_FLAG=--release PERF_SUITE=metrics-layout \
  METRICS_LAYOUT_PROFILE=release_full COMPARE_GREPTIME=1 \
  make test-perf
```

writes `docs/perf/results/<stamp>-metrics-layout.json` that validates against §10.3.1 (**every** required AC-\* = pass, `binary_profile=release`, `fixture_profile=release_full`, Greptime ratio rows present). Missing an id, floor-only profile, debug binary, paused ingest, or deleted data = fail. PR floors (`fixture_profile=pr_floor`) never unlock “ready for verification.”

**Research note:** GreptimeDB was cloned at workspace `./greptime` (Apache-2.0 core) and indexed with CodeGraph for this redesign. Softprobe **learns from** Greptime’s storage/index/compaction ideas and **benchmarks against** a pinned Greptime binary (G9). Softprobe does **not** vendor, fork, or link Greptime code into the runtime.

---

## 1. Problem

The current `metrics` table is an evidence-event log:

- one row per sample with `description`, `unit`, two VARIANT maps, and histogram columns
- no DuckLake `PARTITIONED BY` / `SET SORTED BY` (sort/partition catalogs empty)
- flush-through ingest + 7-day snapshot expiry floored to **days** → thousands of snapshots that never expire on the ingest day
- compacted Parquet files still have `metric_name` min=`aspnetcore…` max=`v8js…`

Measured on the Astronomy Shop stack (2026-08-14, ~9h ingest, data kept):

| Fact | Value |
|------|--------|
| Live metric rows | 28.5M |
| Live metrics Parquet | ~276 MB in four ~64 MB files, each spanning the full name dictionary |
| DuckLake snapshots | 4,135 |
| `ducklake_file_variant_stats` | ~394k rows vs 19 live files |
| `GET /api/v1/label/__name__/values` | 30.005s `limit_exceeded: query deadline exceeded` |
| `query_range k6_vus` (30m, one gauge) | same 30s timeout |
| Capability `max_query_range_seconds` | 86400 (1 day) — long Grafana windows rejected even if scans were fast (design now: **remove** this Softprobe ceiling; Greptime-like) |

`INSERT … ORDER BY record_date, metric_name, timestamp` only sorts **that batch**. `ducklake_merge_adjacent_files` only re-sorts when a table has `SET SORTED BY`. `hive_file_pattern` does nothing without `PARTITIONED BY`. Snapshot expiry and orphan cleanup both use `max(1, age/86400)` **days** (see `src/compaction/executor.rs`).

Wiping the corpus or pausing the load generator is **not** a solution and must not appear in the implementation as a workaround.

---

## 2. Goals

Every goal is **mandatory**. A change that makes Grafana green by dropping data, shrinking the window, or pausing ingest **fails** the corresponding goal.

### G1 — DuckLake is the only store

Metrics identity, samples, histograms, downsamples, and collapse tables are DuckLake tables in the **tenant** catalog (Postgres metadata in production, SQLite locally; Parquet under `data_path`).

- No Prometheus / VictoriaMetrics / Mimir / ClickHouse / Greptime sidecar.
- No application WAL, ingest buffer, or staged tier ([`decision_log.md`](decision_log.md) flush-through invariant). Softprobe does **not** adopt Greptime’s memtable+WAL write path.
- OTLP remains the only write API. Prom/Grafana stay query-only ([`goals.md`](goals.md) §7).
- SQL accessibility stays: public `union_metrics` / `committed_metrics` names still work ([`goals.md`](goals.md) §4). Schema for the new tables lives in the existing shared schema module (not a second metrics stack).

### G2 — Fast interactive queries (release binary)

On a **release** `softprobe-runtime` (`cargo build --release` / `make build-release`), with ingest **still running**, these classes must succeed **without** hitting `query_timeout_seconds` (default 30s). Latency is **end-to-end HTTP** at `/api/v1/*`. Debug-binary timeouts do **not** count as a pass.

| Class | Shape | Window | p95 | Notes |
|-------|--------|--------|-----|--------|
| **Q-tall-short** | 1 series (`k6_vus` or fixture equivalent) | 30m | ≤ **1s** | Raw grain |
| **Q-tall-long** | 1 series | **30d** | ≤ **3s** | Planner uses 1h grain |
| **Q-tall-90d** | 1 series | **90d** | ≤ **3s** | 1h grain only |
| **Q-wide-resolve** | Matcher `{__name__=X, job=Y}` over the wide fixture | 30m | ≤ **2s** to resolve ≤ `max_series` ids **or** `limit_exceeded` | Must not scan samples to discover series |
| **Q-collapse-long** | `sum by (job) (rate(X[5m]))` | **30d** | ≤ **5s** | Collapse table, not full width |
| **Q-collapse-90d** | same | **90d** | ≤ **5s** | Collapse / 1h only |
| **Q-discover** | `GET /api/v1/label/__name__/values` | n/a | ≤ **500ms** | Catalog/postings, not `GROUP BY` samples |
| **Q-hist-short** | Classic histogram `{__name__="…_bucket"}` (or `_count`/`_sum`) | 30m | ≤ **2s** | `metric_hist_samples` + postings, not fat `metrics` |
| **Q-hist-mid** | Same classic hist selector | **3h** (beyond old 2h planner cliff) | ≤ **3s**, **≥ 1 series / non-empty** when F-hist spans the window | Must use `metric_hist_samples`, **not** `metric_samples_1h` |
| **Q-hist-long** | Same | **24h** (and **30d** in harness when F-hist backdated) | ≤ **5s**, non-empty when data exists | Same grain rule as Q-hist-mid |
| **Q-gold** | All 15 GOLD overview PromQL exprs (see §10.1 F-gold) | 30m | each p95 ≤ **5s**, 0 timeouts | Concurrent ingest; machine AC, not screenshots |
| **Q-window-matrix** | Planner unit matrix: gauge / counter / hist / summary × {30m, 2h, 3h, 24h, 30d, 90d} | n/a | grain table matches §9.1 (hist/summary → always hist table) | Compile-time / `cargo test`; catches empty-grain regressions |

### G3 — Tall and wide cardinality

| Mode | Definition | Must |
|------|------------|------|
| **Tall** | Few series, many points (e.g. 1 series × 90d of scrapes) | Skip to those `series_id`s; do not read other series’ samples |
| **Wide** | Many series, few points each (e.g. 100k series for one metric name, distinct `instance`/`pod`/`user_id`) | Resolve via **per-day postings**, not a sample scan. Unbounded selectors **fail loud** at `max_series` (default 10_000) |
| **Churn** | Series appear and disappear (pod recycle) | Dead series exist only on the `record_date`s they were seen. Today’s `metric_postings` row count must **not** include yesterday’s dead `pod`s |

Full-cardinality 15s rollups of every series are **forbidden** (that explodes with width). Downsamples follow **resolved `series_id`s**, not “every series × every 15s bucket.”

### G4 — Long query windows (no Softprobe-imposed range ceiling)

Match Greptime: **do not** reject Prom ranges with a fixed Softprobe max (not 1d, not 90d, not 180d). How far back you can read is governed by **retained data** (`maintenance.metrics_retention_days` / TTL-equivalent), not `max_query_range_seconds`.

- Remove / disable the Prom API range ceiling: `max_query_range_seconds` is **unset / `null` / `0` = unlimited** in [`compat/capability.v0.yaml`](compat/capability.v0.yaml) and `QueryLimits` (AC-W1). A `query_range` with `end - start` of 180d, 365d, or longer must **not** fail with `range exceeds` solely because of window length.
- **Still test** interactive long windows as SLOs (fixtures use backdated OTLP — no wall-clock waits):
  - **30d** — Q-tall-long / Q-collapse-long
  - **90d** — Q-tall-90d / Q-collapse-90d
  - **180d** — accept + serve from 1h / collapse when data exists (AC-W6); same p95 bars as 90d when the fixture is present
- Ranges **> 48h** are served **only** from `metric_samples_1h` and/or `metric_collapse_job_1h` (no upper bound on that rule). Selecting raw grain for those ranges is a planner bug.
- Raw scrape resolution is **not** required beyond ~2h.
- Empty long ranges (no retained samples) return success with empty series — **not** a Softprobe max-range error.
- Tests **must not wait wall-clock days**. Inject OTLP points with backdated `time_unix_nano` (see §10.1).

### G5 — Bounded snapshots (compatible with flush-through)

Flush-through creates **one snapshot per successful `/v1/metrics` commit**. The live snapshot count is then ≈ `commit_rate × max_snapshot_age`. A flat “≤ 500” with no assumed rate is not an invariant.

Let `A = maintenance.max_snapshot_age_seconds` (default **60**), `I = maintenance.metadata_interval_seconds` (default **60**), `C` = minimum seconds between OTLP metric commits (demo overlay `batch/softprobe.timeout` = **15s**, with `send_batch_size` large enough that timeout dominates).

PromQL does not use DuckLake time-travel; snapshot history is unused. After a maintenance pass:

| Metric | Bar |
|--------|-----|
| Age of every live `ducklake_snapshot` row | **< `A + I`** |
| `count(ducklake_snapshot)` after a pass | **≤ 50** (AC-N6). F-snap formula still `≤ ceil(A / C) + 20` (AC-N3) |
| Default `A` | **60**, not 3600 and not 604800 (7 days) |
| Expiry + orphan cleanup SQL | **Second-granularity** `INTERVAL '… seconds'` (fix `max(1, age/86400)` day flooring in **both** `ducklake_expire_snapshots` and `ducklake_cleanup_old_files`) |
| Sample/index **data** | Unchanged by snapshot expiry (time-travel history only) |

If an environment commits faster than 60s, the count bar scales with `C`; the **age** bar does not. The 4,135-snapshot failure was expiry never applying to “today,” not an inevitable flush-through tax.

### G6 — Bounded small files (TWCS-shaped)

After maintenance, **closed** time windows are merged toward `target_file_size_bytes` (64 MiB) using Softprobe’s **TWCS policy** (§7) — never merge across calendar days for raw/index tables.

`days_retained` = number of distinct `record_date` values that have live sample files in the fixture (or `maintenance.metrics_retention_days` once that config exists).

| Metric | Bar |
|--------|-----|
| Live Parquet files for `metric_samples` with `record_date` **before** the current UTC day | ≤ **2 × `days_retained`** (one/two files per closed day) |
| Same bar, separately, for `metric_postings`, `metric_series`, `metric_hist_samples` | ≤ **2 × `days_retained`** each (or 1 file if that table’s closed-day bytes < 8 MiB) |
| Median file size of those closed-day **sample** files | ≥ **8 MiB**, **or** the partition holds < 8 MiB of data (then 1 file is enough) |
| Current UTC day after a merge pass | `metric_samples` live files ≤ **20** |
| Closed-day parquet per family after a pass (AC-F8) | **1** file, or **2** if that day’s bytes **> 64 MiB**; median ≥ **8 MiB** unless the partition holds < 8 MiB |
| Inlined catalog bytes for skinny tables (AC-F7) | **0** for `metric_samples`, `metric_hist_samples`, `metric_postings` (`data_inlining_row_limit` default **0**; VARIANT shredding stays on Parquet for `metric_series`) |
| Existing warn threshold (≥200 files) | Must not fire on any metrics-family table after a successful maintenance pass on a 30d fixture |

Do **not** hive-partition by `metric_name` (hundreds of tiny files per OTLP batch). Greptime’s metric-engine exists precisely because table-per-metric is too heavy; Softprobe keeps **one physical table family**.

### G7 — Cheap storage (no label soup on samples)

- Raw sample rows are `(series_id, timestamp, value)` only. Histogram payloads live only on `metric_hist_samples`.
- Labels, `description`, `unit`, and VARIANT maps live on `metric_series`, **once per series per day**, not on every sample.
- **Both** must hold: `metric_samples` has **no** VARIANT columns, **and** its Parquet bytes are **< 20%** of an equivalent fat-row dump of the same fixture (AC-S1 has no OR escape).

### G8 — Keep data; speed comes from layout

- No implementation may pass SLOs by deleting tenant telemetry, pausing OTel Demo / the test sender, or shrinking Grafana’s default range.
- During every timed AC-Q\* / AC-H\* / AC-W\* **including AC-Q9**, a background OTLP sender must stay alive and increase `metric_samples` (or hist) **row count by ≥ 1** (commit-counter-only is **not** enough).
- Layout rewrite (compact / downsample / collapse) **keeps row-equivalent facts** (raw remains until its retention; downsamples are additive).
- Maintenance must not starve interactive Prom queries (AC-Q9) — Softprobe’s analog of Greptime’s separate ingest/query/compact runtimes.
- G2 timed ACs are measured **with ingest on** (open-day small files may exist). AC-F4’s ≤20 today files is measured **after** a maintenance pass, not as a mid-storm excuse to skip G2.

### G9 — Not significantly slower than Greptime (programmable ratio)

G2 absolute Softprobe bars remain mandatory. Separately, Softprobe must not be **significantly** slower than a pinned Greptime on the **same** OTLP fixtures and PromQL:

| Rule | Value |
|------|--------|
| Ratio bar **R** | **10** (Softprobe_p95 ≤ 10 × Greptime_p95 per gated query) |
| Expected healthy gap | ~2–10× (architecture budget in §4.4); R=10 is the fail-closed gate |
| Write fairness | **OTLP HTTP metrics both sides** (Softprobe `/v1/metrics`; Greptime OTLP metrics). Greptime **remote_write-only** runs are invalid for G9 |
| Profile | release binaries; `METRICS_LAYOUT_PROFILE=release_full`; `COMPARE_GREPTIME=1` |
| Pins | Softprobe git SHA, Greptime git SHA (workspace `./greptime` or release tag), machine class, DuckDB/DuckLake, Postgres — in result JSON (AC-G0) |
| Mode | **Ingest-on** both sides during measure. Softprobe: AC-Q0. Greptime: harness must keep an equivalent OTLP heartbeat writer alive and assert commit/row progress in JSON `preconditions.greptime_sender_alive=true` (AC-G0 fails if false). Quiescent-only Greptime runs are invalid for G9 |
| Non-goal | Softprobe_p95 ≤ Greptime_p95 (beating Greptime). Absolute “Grafana feels fine” **without** Greptime timings does **not** satisfy G9 |

Gated queries: AC-G1…AC-G5 map to T-Q1, T-Q2, T-Q3, T-Q6, T-Q5 (see §10.2).

### 2.1 Goal → AC map (tollgate)

| Goal | Must-pass AC ids |
|------|------------------|
| G1 DuckLake-only + SQL names | AC-D1, AC-D2, AC-D3, AC-D4 |
| G2 Fast query | AC-Q0, AC-Q1, AC-Q2, AC-Q3, AC-Q4, AC-Q5, AC-Q6, AC-Q7, AC-Q8, AC-Q9, AC-H1, AC-H2, AC-H3, AC-H4, AC-H5, AC-H6 |
| G3 Tall / wide / churn | AC-C1, AC-C2, AC-C3, AC-C4, AC-Q3, AC-Q4 |
| G4 Long windows (no API ceiling; 30d/90d/180d SLOs) | AC-W1, AC-W2, AC-W3, AC-W4, AC-W5, AC-W6, AC-Q2, AC-Q5 |
| G5 Snapshots | AC-N1, AC-N2, AC-N3, AC-N4, AC-N5, AC-N6 |
| G6 Small files + TWCS | AC-F1, AC-F2, AC-F3, AC-F4, AC-F5, AC-F6, AC-F7, AC-F8 |
| G7 Cheap samples | AC-S1 |
| G8 Keep data + concurrent ingest | AC-Q0, AC-Q9, AC-S2, AC-S3, AC-M2 |
| Maintenance targets new tables | AC-M1 |
| G9 vs Greptime | AC-G0, AC-G1, AC-G2, AC-G3, AC-G4, AC-G5, AC-G6 |

---

## 3. Non-goals

- Sub-second alerting at high QPS (TSDB-class).
- Unbounded `{high_card_metric}` over 30d/90d with no matcher and no aggregation — **must** return `limit_exceeded`.
- Hive partitioning by high-cardinality labels.
- A second write protocol (`remote_write`) for Softprobe (G9 still uses OTLP on Greptime for fairness).
- Changing traces/logs physical layout in this work.
- **Beating** Greptime / VictoriaMetrics (Softprobe_p95 ≤ Greptime_p95). Softprobe **must** meet G9 ratio **R=10**.
- Claiming Greptime/VictoriaMetrics **parity**; G2 bars are Softprobe product SLOs, not a TSDB bake-off win condition.
- Native exponential histograms (`unsupported_feature` in capability). Classic histograms and summaries stay **preserved**.
- Extra public Make targets (`test-metrics-layout`, `test-metrics-vs-greptime`, …). Layout + G9 live under **`make test-perf`** knobs (`PERF_SUITE`, `COMPARE_GREPTIME`, `METRICS_LAYOUT_PROFILE`).
- Forking or embedding GreptimeDB (or any second engine) into `softprobe-runtime`.
- Softprobe application WAL / memtable (rejected by ADR even though Greptime uses them).

---

## 4. Lessons from GreptimeDB (learn, do not fork)

Studied under Softprobe’s problems: fat mixed-name Parquet, discovery timeouts, flush-through snapshot bloat, no postings, day-floored expiry, 1-day Prom ceiling. Sources: Greptime RFCs (`docs/rfcs/2023-11-03-inverted-index.md`, `2023-07-10-metric-engine.md`, `2023-02-01-table-compaction.md`, `2025-08-16-async-index-build.md`, `2022-12-20-promql-in-rust`), mito2 TWCS (`src/mito2/src/compaction/twcs.rs`, `region/options.rs`), inverted-index Puffin applier (`src/mito2/src/sst/index/inverted_index/`), metric-engine multiplexing, OTLP metrics encoder (`src/servers/src/otlp/metrics.rs`).

### 4.1 Comparison

| Softprobe problem | Greptime approach | Softprobe redesign (this doc) |
|-------------------|-------------------|-------------------------------|
| Fat sample rows with labels on every point | Tags as primary-key columns on a physical mito region; fields are values | **Skinny samples** `(series_id, ts, value)` + day-local `metric_series` for labels (Prom identity model) |
| Series discovery = `GROUP BY` samples | **Inverted index** per SST (tag value → row-group bitmaps, FST, Puffin sidecar) | **`metric_postings`** per day (label → `series_id`) then skinny sample scan. **Not** SST row-group prune — Prometheus-style resolve, not Greptime II equivalence |
| Mixed-name files, no prune | TWCS + PK ranges + inverted index prune | `PARTITIONED BY (record_date)` + `SET SORTED BY (series_id, timestamp)` + postings resolve **before** sample scan |
| Small-file storm under continuous write | Memtable → flush SST → **TWCS** (never merge across time windows; size-tier inside window) | Keep **flush-through**; add Softprobe **TWCS policy** on DuckLake merge (§7). Upstream collector batching still required |
| Index build blocking writes | Sync or **async** index build after flush/compact | Ingest writes postings in the same txn (discoverability). Maintenance **re-merges** postings with samples (async-index lesson: compact must not starve ingest/query) |
| Table-per-metric too heavy | **Metric engine**: many logical metric tables multiplex one physical mito table | Softprobe already wants **one table family** — keep it. Do **not** create DuckLake tables per `__name__` |
| Long-range PromQL | PromQL on DataFusion; Flow for continuous aggregation | Keep Softprobe PromQL evaluator; **maintenance-built** 5m/1h + `job` collapse = Softprobe’s Flow analog |
| Histograms | Expand OTLP hist to `_bucket` / `_sum` / `_count` logical tables | Softprobe keeps **`metric_hist_samples`** (evidence fidelity) and expands to Prom series at query time |
| Snapshot / catalog churn | Region manifests + WAL, not DuckLake snapshot-per-request | Softprobe-specific: **second-granularity** snapshot expiry (G5). Greptime does not solve this for us |

### 4.2 Explicitly rejected Greptime patterns

| Pattern | Why Softprobe rejects it |
|---------|--------------------------|
| Memtable + WAL as the durable write path | Violates flush-through ADR; Softprobe commits each OTLP request into DuckLake |
| Wide physical table with nullable tag columns for all metrics | Fights DuckLake evidence SQL / VARIANT promotion story; label soup returns via series catalog instead |
| Puffin inverted-index blobs next to Parquet | Would require a Softprobe-owned index reader outside DuckDB; day-partitioned postings tables reuse DuckLake |
| DataFusion PromQL plans | Softprobe already has PromQL-on-DuckDB; swapping engines is out of scope |
| Embedding / forking Greptime into one binary | Wrong product shape; license is fine for OSS core, maintenance cost is not |

### 4.3 Softprobe write vs Greptime write (mental model)

```text
Greptime:   OTLP/remote_write → memtable (+ WAL) → flush SST + index → TWCS compact → object store
Softprobe:  OTLP → one DuckLake txn (series + postings + skinny samples) → TWCS-shaped maintenance → object store
```

Same user-visible contract (OTLP in, Prom out). Different durability mid-layer. Softprobe’s speed must come from **layout + maintenance**, not from copying Greptime’s LSM.

### 4.4 Performance budget vs non-negotiables

**Positioning:** Softprobe Prom/Grafana is query-compatible evidence over DuckLake, not a TSDB. G2 bars are Softprobe product SLOs. G9 (R=10) is the programmable “not significantly slower than Greptime” gate. Matching Greptime p50 requires relaxing G1 or flush-through (escape hatches below) — do not ship a hidden second engine.

#### Non-negotiables (KEEP unless product reopens an ADR)

| Item | KEEP reason | Paid cost (budget) |
|------|-------------|--------------------|
| DuckLake-only (G1) | One store, SQL evidence | No mito memtable/SST/II; layout+maintenance only |
| No app WAL / no staged tier | ADR flush-through | Collector batching mandatory; open-day small files until TWCS |
| OTLP-only Softprobe write | Product write API | No Softprobe `remote_write`; G9 still OTLP on Greptime for fairness |
| `union_metrics` / `committed_metrics` names | goals.md SQL | View may JOIN; **Prom path must never** scan fat/view (AC-Q7). No interactive SQL latency AC in this work — SQL is compatibility, not G2 |
| Snapshot per successful metrics commit | DuckLake txn model | Bound age/count (G5); measure catalog conflict / compaction-skip under AC-Q0 |
| Softprobe PromQL evaluator | Existing compat stack | Resolve→SQL→eval; collapse only for documented AST shapes |
| Day-partitioned `metric_postings` | DuckLake-native, churn-bounded | Not SST row-group prune; budget resolve cost (AC-Q3/Q4/Q6, G9) |

#### MEASURE-AND-DECIDE before relaxing KEEP

| Signal | Stay KEEP if | Fail → candidate RELAX (in order) |
|--------|--------------|-----------------------------------|
| AC-Q1/Q6 with ingest on while today’s live sample files ≫ 20 mid-storm | Still pass absolute + G9 | (1) collector batch SLO as AC, (2) shorter maintenance / aggressive today-merge, (3) limited ingest coalescing **ADR** exception, (4) last resort: sidecar (**breaks G1**) |
| AC-Q3/Q4 posting intersect at full F-wide | Pass | Hour-sharded postings, in-process posting cache — still DuckLake |
| Catalog conflict / compaction skip rate under AC-Q0 | Merge still meets AC-F\* / AC-Q9 | Snapshot coalescing research; never silent buffer without ADR |
| AC-Q8 exprs that cannot use collapse | Pass absolute | Extend collapse keys with product sign-off, or lower those exprs’ bar in a doc revision |
| G2 pass but G9 fail (ratio > 10) | — | Reopen product: raise R, lower G2, relax flush-through, **or** relax G1 — do not hide a TSDB |

#### Gap band (architecture estimate, enforced by G9 not vibes)

| Softprobe state | vs healthy Greptime interactive Prom |
|-----------------|--------------------------------------|
| Layout+TWCS steady, closed days merged | ~2–10× slower (must still pass R=10) |
| Open day, merge lag / small-file storm | often ~10–50× — must still meet G2 absolute with ingest on, or hit MEASURE escape hatch |
| Pre-layout fat `metrics` (today) | timeouts / not comparable |

If G2 cannot be met after MEASURE items (1)–(3), **stop** and reopen product: relax flush-through XOR relax G1 XOR lower G2 — do not ship a hidden TSDB.

---

## 5. Architecture

```text
OTLP /v1/metrics  ── one DuckLake transaction ──┐
                                                │
    metric_series     (id → name, type, labels) │  PARTITION BY record_date
    metric_postings   (label → series_id)       │  PARTITION BY record_date
    metric_samples    (id, ts, value)           │  PARTITION BY record_date
    metric_hist_samples (id, ts, count, sum, …) │  PARTITION BY record_date
                                                ▼
                    maintenance (existing scheduler + TWCS policy)
                                                │
                    merge within closed day windows (samples + indexes)
                    metric_samples_5m / metric_samples_1h   (same series_id)
                    metric_collapse_job_1h                  (name, job, hour)
                                                ▼
                     Prom planner  →  /api/v1/query{_range}
                     SQL view union_metrics → samples JOIN series
```

**Time shard = calendar day** (`record_date`). Ninety days ⇒ ~90 partitions (lifecycle + prune, not one partition per series). This is Softprobe’s TWCS window for raw/index tables.

**Identity = Prometheus-style postings**, per day, so churn does not accumulate globally. This is Softprobe’s DuckLake-native **series resolve** path — **not** Greptime’s per-SST tag→row-group inverted index.

**Long range = resolution ladder + collapse**, Softprobe’s analog of Greptime Flow / Thanos 5m/1h downsample.

```text
Query window / Grafana step
  ≤ 2h  and |series| small     → metric_samples (raw) or metric_hist_samples
  ≤ 2d                         → metric_samples_5m   (or raw if fewer series)
  > 48h (30d / 90d / 180d / …) → metric_samples_1h
  sum by (job) over ≥ 2h       → metric_collapse_job_1h
  |series| > max_series        → limit_exceeded (before sample scan)
```

No Softprobe `max_query_range` reject. Retention TTL decides whether points exist; the planner always uses 1h/collapse for long windows.

Sources Softprobe learns from: Greptime **TWCS + inverted index + metric-engine multiplexing**; Prometheus/Mimir **2h blocks + index**; Thanos **raw / 5m / 1h** downsampling.

---

## 6. Physical schema

All tables in the tenant DuckLake schema. After `CREATE TABLE`, **must** run:

```sql
ALTER TABLE <t> SET PARTITIONED BY (record_date);
```

and the `SET SORTED BY` listed below. `CREATE TABLE AS … LIMIT 0` alone is insufficient (today’s bug).

### 6.1 `metric_series`

| Column | Type | Notes |
|--------|------|--------|
| `series_id` | `UBIGINT` | Stable hash of `(metric_name, canonical label map)` |
| `metric_name` | `VARCHAR` | Storage name (OTel; Prom `_bucket`/`_sum`/`_count` stripped as today) |
| `metric_type` | `VARCHAR` | gauge / sum / histogram / summary |
| `unit` | `VARCHAR` | |
| `description` | `VARCHAR` | |
| `labels` | `VARIANT` | Resource + datapoint labels (Prom projection rules unchanged) |
| `record_date` | `DATE` | Partition; day the series was **seen** |

`SET SORTED BY (metric_name, series_id)`.

A series that exists on many days has one catalog row **per day it appears**. That is the churn bound.

### 6.2 `metric_postings`

| Column | Type |
|--------|------|
| `label_name` | `VARCHAR` (`__name__`, `job`, `instance`, …) |
| `label_value` | `VARCHAR` |
| `series_id` | `UBIGINT` |
| `record_date` | `DATE` |

`SET SORTED BY (label_name, label_value, series_id)`.

Softprobe’s stand-in for Greptime’s tag → row-group inverted index: here the posting target is **`series_id`**, because Prom resolve is series-first, then a skinny sample scan by id.

`__name__` is a posting (`label_name='__name__'`). Discovery is:

```sql
SELECT DISTINCT label_value
FROM metric_postings
WHERE label_name = '__name__' AND record_date >= $lookback
LIMIT max_series+1;
```

### 6.3 `metric_samples` (raw, gauges/sums)

| Column | Type |
|--------|------|
| `series_id` | `UBIGINT` |
| `timestamp` | `TIMESTAMPTZ` |
| `value` | `DOUBLE` |
| `record_date` | `DATE` |

`SET SORTED BY (series_id, timestamp)`.

This mirrors Greptime’s “primary-key locality” goal (series then time) without putting tags on every sample row.

No VARIANT, no `metric_name` on this table (join series or filter via resolved ids).

### 6.4 `metric_hist_samples`

Same keys (`series_id`, `timestamp`, `record_date`) plus `count`, `sum`, `bucket_counts`, `explicit_bounds` (existing fidelity types). Gauges never land here. Summaries use this table for `_count`/`_sum` (quantile expansion stays out of scope per capability).

`SET SORTED BY (series_id, timestamp)`.

Deliberate divergence from Greptime: Softprobe keeps classic histogram **payload** in one hist table for evidence SQL; Prom `_bucket`/`_count`/`_sum` expansion stays in the Prom projection layer.

### 6.5 Downsamples (same `series_id`)

`metric_samples_5m` and `metric_samples_1h`:

| Column | Type | Why |
|--------|------|-----|
| `series_id` | `UBIGINT` | |
| `window_ts` | `TIMESTAMPTZ` | Bucket start |
| `record_date` | `DATE` | Partition by **window** day |
| `count` | `UBIGINT` | |
| `sum` | `DOUBLE` | |
| `min` | `DOUBLE` | |
| `max` | `DOUBLE` | |
| `last` | `DOUBLE` | Last sample in bucket |
| `last_ts` | `TIMESTAMPTZ` | For `rate()` / lookback |

Thanos/Greptime-style multi-aggregate buckets: a single average breaks `rate()`/`increase()`. Softprobe stores count/sum/min/max/last like Thanos compact.

`SET SORTED BY (series_id, window_ts)`. Histogram downsamples may wait; Q-hist-short is a 30m raw-hist SLO. Until a dedicated hist ladder exists, classic Prom hist selectors keep reading `metric_hist_samples` for any window (correctness over empty 1h numeric fallback).

### 6.6 Collapse (wide × long × `sum by (job)`)

`metric_collapse_job_1h`:

| Column | Type |
|--------|------|
| `metric_name` | `VARCHAR` |
| `job` | `VARCHAR` | Prom `job` / `service.name` alias |
| `window_ts` | `TIMESTAMPTZ` |
| `record_date` | `DATE` |
| `count`, `sum`, `min`, `max`, `last` | as 6.5 |

`SET SORTED BY (metric_name, job, window_ts)`.

Initial collapse key is **`job` only**. Do not add high-card keys (`pod`, `user_id`). GOLD 30m panels that `sum by (category)` or `avg by (container_name)` use raw/5m over a small series set (F-gold), not collapse.

Softprobe’s analog of Greptime **Flow** continuous aggregation: built in maintenance, not a separate Flow engine.

### 6.7 SQL compatibility

```sql
-- Public name unchanged; rewritten to tm_all_metric as today.
CREATE VIEW union_metrics AS
SELECT s.metric_name, s.description, s.unit, s.metric_type,
       sm.timestamp, sm.value, s.labels AS attributes, …
FROM metric_samples sm
JOIN metric_series s
  ON sm.series_id = s.series_id AND sm.record_date = s.record_date;
```

Exact view column list must satisfy existing SQL/telemetry tests that read `union_metrics` / `metrics` for gauges (AC-D4). Histogram fidelity comes from `metric_hist_samples`.

Stop writing the legacy fat `metrics` table. Optional one-time **rewrite** of existing fat rows into the new tables is allowed; deleting those facts to pass a benchmark is not.

---

## 7. Maintenance — Softprobe TWCS + ladder

Existing scheduler (default 300s). Per **tenant** scope, **in this order**:

### 7.1 TWCS policy (learned from Greptime mito2)

Greptime’s default compaction is TWCS: group SSTs by time window, never compact across windows, size-tier inside a window (`TwcsOptions`: `trigger_file_num`, `time_window`, `max_output_file_size`).

Softprobe maps that onto DuckLake:

| Softprobe knob | Value | Greptime analog |
|----------------|-------|-----------------|
| Raw/index time window | **1 calendar day** (`record_date`) | `compaction.twcs.time_window` |
| Merge trigger | Closed day has ≥ **2** live files (complete compact toward 1 file, or 2 if > 64 MiB) | `trigger_file_num` |
| Target merged file size | `maintenance.target_file_size_bytes` (64 MiB) | `max_output_file_size` |
| Cross-window merge | **Forbidden** for `metric_samples` / `metric_postings` / `metric_series` / `metric_hist_samples` | TWCS invariant |
| Today (open window) | Cap live sample files ≤ 20 after a pass; do not force full-day single-file merge while the day is open | Active window caution |

Implementation must not call a blind global `ducklake_merge_adjacent_files` that rewrites across days if DuckLake cannot respect partition boundaries — merge **per `record_date` partition** (or equivalent filter). AC-F\* are the gate.

### 7.2 Pass order

1. `SET SORTED BY` / `SET PARTITIONED BY` if missing (idempotent).
2. **TWCS merge** on `metric_samples`, `metric_hist_samples`, `metric_series`, `metric_postings`, plus downsample/collapse tables (**metrics family first**). Loop bounded waves until closed-day file bars (AC-F8) **and** until today’s live files are ≤20 (AC-F4). Open-day uses a 256-file CALL when over 256 live files; do not stop after one 32-file wave. Do **not** only merge legacy `metrics`.
3. Build/append `metric_samples_5m` from raw older than **2h** (closed hours only).
4. Build/append `metric_samples_1h` from 5m (or raw) older than **24h**.
5. Build/append `metric_collapse_job_1h` from 1h (or raw) grouped by `(metric_name, job, hour)`.
6. `ducklake_expire_snapshots` with **second-granularity** `older_than` (G5).
7. `ducklake_cleanup_old_files` with **second-granularity** `older_than` when configured + drop orphan variant stats for files no longer live.

Downsample jobs must be **incremental** (watermark per table, or `INSERT … WHERE window_ts > last_built`). Rebuilding 30d every 5 minutes fails G2 (ingest starvation) and G6.

### 7.3 Foreground vs background (learned from Greptime runtimes)

Greptime isolates ingest / query / compact on separate async runtimes so compact cannot deadlock query. Softprobe must:

- Keep interactive `/api/v1/*` deadlines independent of a long merge.
- Prefer short, partition-scoped merge waves over one multi-day rewrite.
- Fail AC-Q9 if a forced downsample/merge pass pushes T-Q1 p95 above **5s**.

---

## 8. Ingest

1. Decode OTLP (unchanged). Softprobe does **not** adopt Greptime’s multi-logical-table OTLP expansion as the storage model.
2. Canonicalize labels (existing Prom projection).
3. `series_id = hash(metric_name, sorted label pairs)`.
4. **One** `BEGIN … COMMIT` per successful `/v1/metrics` request:
   - `INSERT` into `metric_series` / `metric_postings` for the sample’s `record_date` (idempotent on `(record_date, series_id)` / `(record_date, label_name, label_value, series_id)`).
   - `INSERT` into `metric_samples` or `metric_hist_samples`.
5. Do **not** write 5m/1h/collapse on the ingest path (keeps flush-through small). Maintenance builds them (Flow analog).

Collector batching stays required (`batch/softprobe` 60s / 8192 in the demo overlay). Greptime hides batching behind a memtable; Softprobe cannot — the collector **is** the batcher.

---

## 9. Query planner

Location: `src/compat/backends/ducklake_metrics.rs` + a dedicated module if the file splits. HTTP handlers stay SQL-free.

### 9.1 Algorithm

1. Parse PromQL (existing subset).
2. For each vector selector, collect equality matchers (regex still post-filter, as today, but **only after** id resolution from equality postings; if there is **no** equality matcher at all, fail `limit_exceeded` when posting cardinality for the remaining constraint exceeds `max_series`).
3. `record_date BETWEEN date(start) AND date(end)` — prune days (TWCS window prune).
4. Intersect postings per day:

   ```sql
   SELECT series_id FROM metric_postings
   WHERE record_date = $d AND label_name = $n AND label_value = $v
   ```

   Intersect ids in Rust or SQL `INTERSECT`. Union across days. If `|ids| > max_series` → `limit_exceeded` with message containing `max_series` (no sample scan).
5. If the AST is `sum by (job) (rate|irate|increase (selector))` and the window ≥ 2h → read `metric_collapse_job_1h` (Q-collapse-long / 90d).
6. Else pick grain:

   | Condition | Table |
   |-----------|--------|
   | histogram/summary selector (`_bucket` / `_sum` / `_count`) | Always `metric_hist_samples` (Prom expands count/sum/buckets at read time). Do **not** switch to `metric_samples_1h` — Softprobe does not materialize separate numeric `_count`/`_sum` series there, so that switch returns empty Grafana panels (e.g. cart `now-3h`). |
   | `end - start ≤ 2h` | `metric_samples` |
   | `end - start ≤ 48h` | `metric_samples_5m` |
   | else (**> 48h**, any length) | `metric_samples_1h` |

   If Grafana `step` ≥ 1h, skip to 1h even for shorter ranges.
7. `SELECT … FROM <grain> WHERE series_id IN (…) AND timestamp/window_ts BETWEEN start AND end`.
8. Time predicates use **timestamptz literals or `make_timestamptz`**, never `to_timestamp(epoch_ms/1000.0)` (that blocks zone-maps).
9. Deadline still on `TenantContext`; **do not** hold `NAME_VALUES_CACHE` (or any cache mutex) across DuckDB execution.

Greptime evaluates PromQL as DataFusion plans with series-local batches. Softprobe keeps **resolve ids → one SQL fetch → in-memory Prom eval** (already the phase-1 design). Do not port DataFusion Prom plans.

### 9.2 Capability / limits

| Limit | Today | Required |
|-------|--------|----------|
| `max_query_range_seconds` | 86400 | **unlimited** (`null` / `0` / omit check) — Greptime-like; retention TTL is the bound |
| `max_series` | 10000 | keep; fail loud |
| `query_timeout_seconds` | 30 | keep for Grafana; SLOs are tighter |
| `classic_histogram` | preserved | still preserved; gated by AC-H\* |
| scan_cap on fat `union_metrics` | 100k rows | Prom path **must not** use that scan; if it does, AC-Q\* fail |

Update [`compat/capability.v0.yaml`](compat/capability.v0.yaml) and `QueryLimits` tests. **Do not** reject solely because `end - start` exceeds 90d or 180d.

---

## 10. Acceptance criteria and test plan

**Pass rule:** “ready for verification” only after §10.3 is green **and** the result JSON lists every AC-\* id. A human/agent then fills [`.agent/reports/latest-verification.md`](../.agent/reports/latest-verification.md) mapping each id to a log snippet or JSON row. Missing an AC id = **not done**.

### 10.0 Make / CI contract (do not invent targets)

Public Make surface stays `test` / `test-e2e` / `test-perf` / `ci` / `release`.

| Gate | Profile | What runs |
|------|---------|-----------|
| `make test` | dev | Fast unit ACs including TWCS unit, planner EXPLAIN units, seconds expiry |
| `make test-e2e` | dev | Existing e2e + AC-D4. **No** 100k / Greptime ratio |
| `make test-perf` | **dev** floors on PR; **`--release` + `release_full`** for ready | `PERF_SUITE=metrics-layout` must be a **valid** suite value (extend Makefile enum). Softprobe-absolute ACs always; G9 when `COMPARE_GREPTIME=1` |
| `make release` | `--release` | Must run Softprobe-absolute `metrics-layout` at `release_full`. G9 (`COMPARE_GREPTIME=1`) is **required** for “ready for verification”; may be `workflow_dispatch`/release-machine if PR budget cannot hold Greptime binary bring-up — still machine-checkable JSON |

Do **not** add `test-metrics-layout` or `test-metrics-vs-greptime` public targets.

Budgets: Softprobe-absolute layout may use `PERF_LAYOUT_GOAL_SECS` (default **1200** when `PERF_SUITE=metrics-layout`) so the historical 480s `test-perf` envelope does not force forever-floors. Document elapsed in the verification report.

| Fixture | PR `pr_floor` | `release_full` (ready gate) |
|---------|---------------|------------------------------|
| F-tall | 30d backdated | same |
| F-wide | **15_000** series | **100_000** series |
| F-collapse | 10 × 20 × 30d hourly | 50 × **1** × 30d hourly (covered by 90d load) |
| F-collapse-90d | 10 × 20 × 90d hourly **only** (no “extend 30d” OR) | 50 × **1** × 90d hourly (I=1; see §13) |
| G9 ratio | optional / skip | **required** (`COMPARE_GREPTIME=1`) |
| Repeats | 5 for long; **20** for Q-tall-short and Q-discover | same |

Floor JSON must set `fixture_profile: "pr_floor"` and **must not** be accepted by the ready validator.

### 10.1 Fixture generator (shared)

Add `tests/compat/prometheus/layout/`. Generator: OTLP HTTP to `/v1/metrics` with explicit timestamps. Reuse `tests/util/otlp.rs`. **Do not** require Docker Demo. **Do not** sleep 30/90 days.

Pin `LAYOUT_FIXTURE_SEED`, freeze UTC `record_date` for “today”, and write `fixture_hash` into the result JSON.

| Fixture | How to build | Size (full bar) |
|---------|--------------|-----------------|
| **F-tall** | 1 metric `layout_tall`, 1 series, points every 15s for **180d** via backdated `time_unix_nano` (90d subset used by T-W5; full span for T-W6) | ~1.04M raw; 1h grain ~4320 |
| **F-wide** | 1 metric `layout_wide`, **N** series (`instance="i-{0..N-1}"`), **one** sample each, timestamps in “today”; N=15k floor / 100k full | N samples + N series + postings |
| **F-churn** | Same metric name, `pod="p1"` on `today-2`, `pod="p2"` on `today`; no overlap | 2 days × 1 series each |
| **F-collapse** | Metric `layout_http`, J `job`s × I `instance`s, 30d of **hourly** points | collapse = 50 × 720; raw width I is optional (AC asserts series=J) |
| **F-collapse-90d** | Same shape, timestamps spanning **90d** (not a 30d extension hack) | collapse = 50 × 2160; `release_full` uses **I=1** |
| **F-hist** | Classic histogram `layout_latency` (explicit bounds, 5 buckets), 10 series, 30m of 15s points | hist samples only |
| **F-gold** | Exact series needed for the 15 GOLD exprs (names/labels from `astronomy-shop-overview.json`); include `k6_vus` (1 series) | small; not 100k |
| **F-files** | ≥ 30 OTLP batches into **two closed** days + today; closed-day sample bytes **≥ 16 MiB** before merge; force maintenance | TWCS + size bars |
| **F-snap** | Commits every **1s** for **120s** with test `A=60`, `I=10`, then one maintenance pass | snapshot expiry + age bar |
| **F-sql** | Small gauge set for `union_metrics` / `committed_metrics` | row-fact equality |

Background sender: 1 OTLP metrics batch/s of `layout_ingest_heartbeat` for the whole measure window (AC-Q0), including during AC-Q9.

### 10.2 Acceptance criteria (machine-checkable)

Each row: **pass** is the only success; **fail** is anything listed plus timeout / debug binary / paused sender / deleted rows.

#### DuckLake (G1)

| ID | Pass | Fail | Test |
|----|------|------|------|
| **AC-D1** | After ingest, `metric_series`, `metric_postings`, `metric_samples`, `metric_hist_samples` exist in the **tenant** DuckLake schema (`ducklake_table`). Prom reads those tables. | Fat-only `metrics` writes continue as the Prom source | `T-D1` |
| **AC-D2** | Each of those tables has **non-empty** `ducklake_partition_info` and `ducklake_sort_info` | Either catalog empty (today’s bug) | `T-D2` |
| **AC-D3** | No new metrics durable write path besides DuckLake. Unit: Prom backend is `DuckLakeMetricsBackend`. Grep `src/` for `remote_write` / `victoria` / `prometheus::TSDB` / `greptime` writer must not add a writer | Sidecar TSDB | `T-D3` |
| **AC-D4** | `SELECT` via `union_metrics` **and** `committed_metrics` on F-sql returns the same metric_name / timestamp / value facts as before the split (existing SQL tests green) | Either name missing or wrong join; gauges only in new tables but SQL broken | `T-D4` |

#### Fast query (G2) — ingest sender **on** (AC-Q0)

| ID | Pass | Fail | Test |
|----|------|------|------|
| **AC-Q0** | During T-Q1–Q9, T-H1, T-W3, T-W5: heartbeat **row count** increased by **≥ 1**; sender process still alive at end | Sender stopped, counts unchanged, or only commit counter moved | `T-Q0` |
| **AC-Q1** | F-tall `query_range layout_tall` 30m step=15s: HTTP 200, `status=success`, p95 ≤ **1s** over 20 repeats | Timeout, `limit_exceeded`, p95 > 1s | `T-Q1` |
| **AC-Q2** | F-tall `query_range` **30d** step=1h: p95 ≤ **3s**, ≥ 1 series, ≥ 600 points; EXPLAIN uses `metric_samples_1h` **not** raw | Range rejected, timeout, empty, p95 > 3s, or raw grain | `T-Q2` |
| **AC-Q3** | F-wide `{__name__="layout_wide",instance="i-1"}` 30m: p95 ≤ **2s**, result series count **= 1** | p95 > 2s, ≠1 series, or timeout | `T-Q3` |
| **AC-Q4** | F-wide `{__name__="layout_wide"}` 30m with default `max_series=10000`: HTTP error, body contains `limit_exceeded` (and `max_series`), duration **< 5s** | HTTP 200 with thousands of series, or ≥30s hang | `T-Q4` |
| **AC-Q5** | F-collapse `sum by (job) (rate(layout_http[5m]))` 30d step=1h: p95 ≤ **5s**, result series count **= J** (50 full / 10 floor); EXPLAIN references `metric_collapse_job_1h` | Timeout, un-collapsed width, or raw/5m grain | `T-Q5` |
| **AC-Q6** | `/api/v1/label/__name__/values` p95 ≤ **500ms** (20 repeats) while F-wide is loaded; EXPLAIN/SQL contains `metric_postings` and `label_name = '__name__'` | p95 > 500ms, 30s timeout, or `GROUP BY` `metric_samples` without postings | `T-Q6` |
| **AC-Q7** | EXPLAIN (or query log) for T-Q3: SQL references `metric_postings` and `metric_samples` with `series_id IN` / id join; **no** `FROM metrics` fat table | Fat `union_metrics` scan | `T-Q7` |
| **AC-Q8** | For **each** GOLD expr below, `query_range` 30m against F-gold: HTTP 200, p95 ≤ **5s**, 0 timeouts (5 repeats). Sender on. | Any expr timeout or p95 > 5s | `T-Q8` |
| **AC-Q9** | While a forced downsample/merge pass runs, T-Q1 still p95 ≤ **5s** (maintenance must not starve interactive queries) | Timeouts during maintenance | `T-Q9` |

GOLD exprs (from `tests/compat/grafana/dashboards/astronomy/astronomy-shop-overview.json`):

1. `sum by (job) (rate(http_server_request_duration_count[5m]))`
2. `sum by (job) (rate(traces_span_metrics_calls[5m]))`
3. `sum by (job) (rate(rpc_server_call_duration_count[5m]))`
4. `sum by (job) (rate(http_client_request_duration_count[5m]))`
5. `sum by (category) (rate(demo_ad_served_total[5m]))`
6. `sum(rate(demo_cart_add_item_latency_count[5m]))`
7. `sum(rate(demo_payment_transactions[5m]))`
8. `sum(rate(demo_shipping_items_shipped[5m]))`
9. `sum(rate(demo_exchange_conversions_counter[5m]))`
10. `sum(rate(quotes[5m]))`
11. `k6_vus`
12. `sum(rate(k6_iterations[5m]))`
13. `sum(k6_http_req_failed_total)`
14. `topk(8, avg by (container_name) (container_cpu_utilization))`
15. `topk(8, avg by (container_name) (container_memory_percent))`

#### Histograms (G2 + capability `classic_histogram: preserved`)

| ID | Pass | Fail | Test |
|----|------|------|------|
| **AC-H1** | F-hist: rows land in `metric_hist_samples` only (0 `metric_samples` for `layout_latency`). Prom `layout_latency_count` or `_bucket` `query_range` 30m p95 ≤ **2s**, HTTP 200 | Fat-table hist, timeout, or empty | `T-H1` |
| **AC-H2** | EXPLAIN for T-H1 references `metric_hist_samples` and `metric_postings`, not fat `metrics` | Fat scan | `T-H2` |
| **AC-H3** | F-hist (or unit DuckLake seed spanning ≥3h): Prom `layout_latency_count` `query_range` with `end−start = 3h` returns HTTP 200, **≥ 1 series**, **≥ 1 point**; samples SQL / EXPLAIN references `metric_hist_samples` and **must not** reference `metric_samples_1h` | Empty series, 1h-grain SQL, or timeout | `T-H3` |
| **AC-H4** | Same as AC-H3 for `end−start = 24h` (and harness **30d** when F-hist is backdated): non-empty when data exists; grain = `metric_hist_samples` | Empty with data present, or `metric_samples_1h` grain | `T-H4` |
| **AC-H5** | Summary-style selector (`…_sum` / `…_count` on a summary metric type) uses the same hist grain rule as AC-H3 (always `metric_hist_samples` for classic suffixes) | Routed to numeric 1h/5m grain | `T-H5` |
| **AC-H6** | `cargo test` window×type matrix (`Q-window-matrix`): for every classic hist/summary `__name__` suffix, windows {30m, 2h, 3h, 24h, 30d, 90d} select `SampleGrain::Hist` / table `metric_hist_samples`; gauge/counter select Raw/FiveMin/OneHour per §9.1 | Any hist window selects `OneHour`/`FiveMin` | `T-H6` |

#### Cardinality (G3)

| ID | Pass | Fail | Test |
|----|------|------|------|
| **AC-C1** | F-churn: `pod` values for `today-2` window = `{p1}` only; today = `{p2}` only | Both pods in both days | `T-C1` |
| **AC-C2** | F-wide: `SELECT count(*) FROM metric_series WHERE record_date = today` = **N** (15k or 100k) | Count ≠ N, or labels exploded onto sample rows | `T-C2` |
| **AC-C3** | F-tall query of that one series: returned samples belong only to the tall `series_id` (no F-wide ids) | Result includes wide series | `T-C3` |
| **AC-C4** | F-churn: `SELECT count(*) FROM metric_postings WHERE record_date = today AND label_name='pod' AND label_value='p1'` = **0** | Yesterday’s pod still in today’s postings | `T-C4` |

#### Long window (G4)

| ID | Pass | Fail | Test |
|----|------|------|------|
| **AC-W1** | Prom path does **not** reject on window length: `max_query_range_seconds` is unlimited (`null`/`0`/check removed) in capability YAML **and** `QueryLimits`. `query_range` with `end-start = 180d` and `365d` returns HTTP 200 (empty series OK if no fixture), **not** `range exceeds` | Still 86400 / 7776000 / any hard ceiling | `T-W1` |
| **AC-W2** | `query_range` `end-start = 30d` returns 200, not `range exceeds` | 4xx range error | `T-W2` (= T-Q2) |
| **AC-W3** | F-collapse-90d `sum by (job) (rate(layout_http[5m]))` `end-start = 90d` step=1h: p95 ≤ **5s**, series count = J; EXPLAIN references `metric_collapse_job_1h` | Rejected, timeout, un-collapsed width, or raw grain | `T-W3` |
| **AC-W4** | `query_range` `end-start = 31d` of F-wide selector that exceeds `max_series` fails loud **< 5s** (no 30s hang) | Hang or 200 with huge series | `T-W4` |
| **AC-W5** | F-tall `query_range` **90d** step=1h: p95 ≤ **3s**, ≥ 1 series, ≥ 1800 hourly points; EXPLAIN uses `metric_samples_1h` not raw | Rejected, raw grain, timeout | `T-W5` |
| **AC-W6** | F-tall-180d (or F-tall extended): `query_range` **180d** step=1h: HTTP 200, **not** range-rejected; p95 ≤ **3s**; EXPLAIN uses `metric_samples_1h` | Softprobe max-range reject, raw grain, or timeout | `T-W6` |

#### Snapshots (G5)

| ID | Pass | Fail | Test |
|----|------|------|------|
| **AC-N1** | Default `max_snapshot_age_seconds == 60` in `config.rs` | Still 3600 or 604800 | `T-N1` |
| **AC-N2** | Expiry SQL uses an interval derived from **seconds**, not `max(1, age/86400)` days (3600s → `INTERVAL '3600 seconds'` or equivalent hours, **not** `INTERVAL '1 days'`) | 1h config still 1-day floor | `T-N2` |
| **AC-N3** | F-snap (`A=60`, `C=1`, ≥120 commits, then maintenance): `count(ducklake_snapshot) ≤ ceil(60/1)+20 = 80` **and** every remaining snapshot age **< A+I** (70s if `I=10`) | Thousands of snaps, or rows older than `A+I` | `T-N3` |
| **AC-N4** | After expiry, `count(*) FROM metric_samples` unchanged vs pre-expiry | Samples dropped | `T-N4` |
| **AC-N5** | `ducklake_cleanup_old_files` SQL uses a **seconds** interval when `remove_orphan_older_than_seconds` is set (same day-floor bug as expiry) | Still `INTERVAL 'N days'` from `max(1, s/86400)` | `T-N5` |
| **AC-N6** | After a maintenance pass: `count(ducklake_snapshot) ≤ 50` **and** no live snapshot older than `A + I` (default 60+60) | Snapshot pile-up, or rows older than keep window + interval | `T-N6` |

#### Small files + TWCS (G6)

| ID | Pass | Fail | Test |
|----|------|------|------|
| **AC-F1** | F-files + maintenance: closed-day `metric_samples` live files (`end_snapshot IS NULL`) ≤ **2 × days_retained** (1 closed day ⇒ ≤ 2) | Dozens of <1MB files remain | `T-F1` |
| **AC-F2** | F-files **must** leave closed-day `metric_samples` bytes **≥ 16 MiB** before merge (`preconditions.AC-F2_bytes_before_merge` in JSON). After merge: median file size ≥ **8 MiB**. No “if sum ≥ 8MiB” skip | Precondition not met, or median < 8 MiB | `T-F2` |
| **AC-F3** | Partition transform is `record_date` only — no `SET PARTITIONED BY (metric_name)` on samples | Hive-per-name | `T-F3` |
| **AC-F4** | After merge, **today’s** `metric_samples` live files ≤ **20** | Today unbounded small files | `T-F4` |
| **AC-F5** | Closed-day live files for `metric_postings`, `metric_series`, `metric_hist_samples` each ≤ **2 × days_retained**. F-files sizes indexes so each family is ≥ 8 MiB before merge when asserting size; JSON `precondition_met: true` | Index tables explode into tiny files; precondition false while claiming pass | `T-F5` |
| **AC-F6** | After forced merge of a **2-day** F-files corpus: every output sample Parquet file maps to a **single** `record_date`; merge SQL/plan is partition-scoped (`record_date = $d`). Unit `twcs_merge_does_not_cross_record_date` | Unfiltered global merge across days | `T-F6` |
| **AC-F7** | Inlined catalog bytes for `metric_samples`, `metric_hist_samples`, `metric_postings` **= 0**. Default `data_inlining_row_limit` is **0** (opt-in inlining remains for the scores/inlined-reader test) | Skinny tables sit in Postgres `ducklake_inlined_data_*` and skip TWCS | `T-F7` |
| **AC-F8** | After a pass: each closed-day parquet partition per family is **1** file, or **2** if that day’s bytes **> 64 MiB**; median file size ≥ **8 MiB** unless the partition holds < 8 MiB | Many tiny closed-day files remain | `T-F8` |

#### Cheap storage + keep data (G7, G8)

| ID | Pass | Fail | Test |
|----|------|------|------|
| **AC-S1** | F-wide: `metric_samples` has **no** VARIANT in `ducklake_column` **and** `skinny_bytes / fat_bytes < 0.20`. Fat dump = same N points written through the **legacy fat `metrics` column list** on a throwaway DuckLake path; JSON records both byte sizes | VARIANT on samples, ratio ≥ 0.20, or fat methodology missing from JSON | `T-S1` |
| **AC-S2** | After downsample, `count(*) FROM metric_samples` is **≥** pre-downsample count (raw not deleted). Downsample tables are extra rows | Raw deleted to “make 30d fast” | `T-S2` |
| **AC-S3** | `grafana-manual-up.sh` builds **release** (`cargo build --release` / `build-release`), not `target/debug` | Debug binary for demo SLOs | `T-S3` |

#### Maintenance

| ID | Pass | Fail | Test |
|----|------|------|------|
| **AC-M1** | Compaction/maintenance table list includes exactly the metrics family: `metric_samples`, `metric_postings`, `metric_series`, `metric_hist_samples`, `metric_samples_5m`, `metric_samples_1h`, `metric_collapse_job_1h` (first family still metrics, not traces) | Still only legacy `"metrics"`; missing downsample/collapse names | `T-M1` |
| **AC-M2** | Second maintenance pass with no new closed windows inserts **0** new rows into `metric_samples_5m` / `metric_samples_1h` / `metric_collapse_job_1h` (watermark incremental) | Full 30d rebuild every pass | `T-M2` |

#### Softprobe vs Greptime (G9)

| ID | Pass | Fail | Test |
|----|------|------|------|
| **AC-G0** | Result JSON `versions` records Softprobe git SHA, Greptime git SHA, DuckDB/DuckLake, Postgres, OS/CPU class, `R=10`; `preconditions.greptime_sender_alive=true` | Missing pins or Greptime sender not alive | `T-G0` |
| **AC-G1** | F-tall 30m (same OTLP fixture both sides): Softprobe_p95 ≤ **10 ×** Greptime_p95 | Ratio > 10 or Greptime side missing | `T-G1` |
| **AC-G2** | F-tall 30d step=1h: Softprobe_p95 ≤ **10 ×** Greptime_p95 | Ratio > 10 or missing | `T-G2` |
| **AC-G3** | F-wide resolve `{__name__,instance}`: Softprobe_p95 ≤ **10 ×** Greptime_p95 | Ratio > 10 or missing | `T-G3` |
| **AC-G4** | `__name__/values` discovery: Softprobe_p95 ≤ **10 ×** Greptime_p95 | Ratio > 10 or missing | `T-G4` |
| **AC-G5** | F-collapse 30d `sum by (job) (rate(...))`: Softprobe_p95 ≤ **10 ×** Greptime_p95 | Ratio > 10 or missing | `T-G5` |
| **AC-G6** | Harness asserts **OTLP metrics** write path on Greptime (not remote_write-only). Fail if Greptime run used remote_write as the sole ingest | remote_write-only compare | `T-G6` |

### 10.3 Test commands

```bash
make test
CARGO_PROFILE_FLAG=--release PERF_SUITE=metrics-layout \
  METRICS_LAYOUT_PROFILE=release_full COMPARE_GREPTIME=1 \
  make test-perf
# Post-step (required for ready): validate JSON schema + all required AC ids
# scripts/validate-metrics-layout-results.py docs/perf/results/*-metrics-layout.json
```

`PERF_SUITE=metrics-layout` must:

1. Run unit leftovers if not already in `make test`.
2. Start runtime + Postgres catalog, load F-\* (scale from `METRICS_LAYOUT_PROFILE`), run Softprobe-absolute T-\*.
3. When `COMPARE_GREPTIME=1`: start pinned Greptime, load **same OTLP fixtures**, run AC-G\*.
4. Write `docs/perf/results/<stamp>-metrics-layout.{json,md}`.
5. Exit **non-zero** if any required AC fails, any required id is missing, profiles are wrong, or validator rejects the JSON.

#### 10.3.1 Result JSON schema (machine gate)

```json
{
  "schema_version": 1,
  "suite": "metrics-layout",
  "binary_profile": "release",
  "fixture_profile": "release_full",
  "git_sha": "...",
  "fixture_hash": "...",
  "stamp": "...",
  "versions": {
    "softprobe": "...",
    "greptime": "...",
    "duckdb": "...",
    "ducklake": "...",
    "postgres": "...",
    "machine_class": "...",
    "R": 10
  },
  "preconditions": {
    "AC-F2_bytes_before_merge": 16777216,
    "AC-F5_precondition_met": true,
    "sender_alive": true,
    "greptime_sender_alive": true
  },
  "acs": {
    "AC-Q1": {
      "pass": true,
      "p95_ms": 120,
      "softprobe_p95_ms": 120,
      "greptime_p95_ms": null,
      "ratio": null,
      "fixture_scale": {},
      "explain_ok": true,
      "notes": ""
    },
    "AC-G1": {
      "pass": true,
      "softprobe_p95_ms": 800,
      "greptime_p95_ms": 120,
      "ratio": 6.67,
      "notes": "OTLP both sides; ingest-on"
    }
  }
}
```

Ready validator **rejects** unless: `binary_profile=="release"`, `fixture_profile=="release_full"`, every required AC id present with `pass==true`, and (when claiming G9) Greptime fields populated for AC-G\*.

**Required AC ids (56):**  
`AC-D1`…`AC-D4`, `AC-Q0`…`AC-Q9`, `AC-H1`…`AC-H6`, `AC-C1`…`AC-C4`, `AC-W1`…`AC-W6`, `AC-N1`…`AC-N6`, `AC-F1`…`AC-F8`, `AC-S1`…`AC-S3`, `AC-M1`, `AC-M2`, `AC-G0`…`AC-G6`.

Unit tests (fast, no 100k / no Greptime):

| Test fn (suggested) | Asserts |
|---------------------|---------|
| `expire_snapshots_sql_honors_seconds` | 3600s → seconds interval, **not** `INTERVAL '1 days'` |
| `cleanup_old_files_sql_honors_seconds` | same for orphan cleanup |
| `planner_picks_1h_for_30d` | range 30d → `metric_samples_1h` |
| `planner_picks_1h_for_90d` | range 90d → `metric_samples_1h`, not raw |
| `planner_picks_collapse_for_sum_by_job_rate` | AST match when window ≥ 2h |
| `planner_fails_when_ids_exceed_max_series` | no `execute_query` on samples |
| `discover_sql_uses_postings` | SQL contains `metric_postings` and `label_name = '__name__'` |
| `hist_selector_always_uses_hist_table` | `_bucket`/`_count`/`_sum` → `metric_hist_samples` for 30m **and** 3h/24h/30d/90d |
| `window_series_type_grain_matrix` | AC-H6: gauge/counter/hist/summary × windows → §9.1 grain |
| `time_predicate_is_timestamptz` | no `to_timestamp(` |
| `name_values_cache_lock_not_held_across_await` | cache get/put without await under lock |
| `maintenance_tables_include_metric_family` | AC-M1 exact list |
| `max_query_range_is_unlimited` | no Softprobe length reject (null/0/disabled) |
| `planner_picks_1h_for_180d` | range 180d → `metric_samples_1h`, not raw |
| `twcs_merge_does_not_cross_record_date` | AC-F6 |

### 10.4 Manual Grafana gate (not a substitute for §10.3)

After `make grafana-up` (release binary):

1. Leave Astronomy Shop ingest running. Do not wipe `/tmp/thelake-grafana-manual`.
2. GOLD overview, last **30m**, 10s refresh: **zero** panel timeouts for 5 minutes.
3. Live demo 30d may be sparse; do **not** call empty 30d panels a pass. Long-window correctness is AC-Q5 / AC-W3 / AC-W5 / AC-W6.
4. Record screenshot or Grafana inspector JSON in the verification report **in addition to** AC-Q8, never instead of it.

§10.4 is required for human **Done**, not for machine **ready for verification**.

### 10.5 Done / not done

| Phrase | Allowed when |
|--------|----------------|
| Implementation ready for verification | `make test` green **and** release_full `PERF_SUITE=metrics-layout` with `COMPARE_GREPTIME=1` green **and** JSON validates (§10.3.1) with all **53** AC ids pass |
| Done | Verification report maps each AC to evidence; `make ci` still green; §10.4 recorded |

If ingest is stopped, the window shortened, data deleted, a debug binary used, `pr_floor` JSON offered as ready, or Greptime side omitted while claiming G9, those ACs are **failed**.

### 10.6 Greptime comparison harness (G9)

- **Shared OTLP generator** → Softprobe and Greptime (same series, timestamps, labels, batch sizes).
- Greptime ingest: OTLP metrics endpoint (**AC-G6**). Do not use Greptime’s remote_write harness as the Softprobe comparator.
- Queries: map AC-G1…G5 to the same PromQL/windows as T-Q1, T-Q2, T-Q3, T-Q6, T-Q5.
- Warmup + repeats match Softprobe timed ACs; release Greptime binary; pin SHA in JSON.
- Make ownership: `COMPARE_GREPTIME=1` under existing `make test-perf` — **no** new public target.
- Softprobe findings.md competitor text (§5) defers metrics-layout leadership claims to **this** harness (G9), not VictoriaMetrics-only prometheus-benchmark.
- **Binary location (not vendored into Softprobe):** research clone at workspace sibling `../greptime` (pinned SHA recorded in result JSON). Build with `(cd ../greptime && make build RELEASE=true)` → `../greptime/target/release/greptime` (requires `protoc` / `protobuf-compiler`). Harness resolution when `COMPARE_GREPTIME=1`: `GREPTIME_BIN` if set, else that release path, else `GREPTIME_URL` for an already-running HTTP base. Standalone defaults HTTP `:4000`; OTLP metrics ingest is **POST `/v1/otlp/v1/metrics`** (`Content-Type: application/x-protobuf`) — not remote_write (`/v1/prometheus/write`).

## 11. Implementation sequence (for agents)

Do not start with Grafana. Each step has tests from §10. Start at this section.

1. **DDL helpers** — `SET PARTITIONED BY (record_date)` + `SET SORTED BY` on create; T-D2.
2. **Snapshot + cleanup seconds** — default A=3600; T-N1, T-N2, T-N5.
3. **Tables + one-txn ingest** — series, postings, samples, hist; stop fat writes; T-D1, T-S1, T-C2, T-H1 ingest half, T-M1.
4. **Prom resolve via postings** — T-Q3, T-Q4, T-Q6, T-Q7, T-C1, T-C4.
5. **Grain planner + unlimited range** — T-W1, T-Q1, T-Q2, T-W5, T-W6.
6. **Hist Prom path** — T-H1, T-H2.
7. **TWCS maintenance + downsample + collapse + files + snaps** — T-Q5, T-W3, T-S2, T-F1–F6, T-N3, T-N4, T-Q9, T-M2.
8. **`union_metrics` / `committed_metrics` + GOLD** — T-D4, T-Q8, T-Q0.
9. **Make/JSON validator + G9 harness** — schema §10.3.1, AC-G0…G6, `COMPARE_GREPTIME=1`.
10. **grafana-up release** — T-S3; then §10.4.

Keep work in commit-sized units. Softprobe-absolute green without G9 is **not** ready for verification.

---

## 12. Risks

| Risk | Mitigation |
|------|------------|
| DuckLake `SET PARTITIONED BY` does not rewrite old files | Tests use new ingest; optional rewrite job copies fat → new tables (keep facts) |
| Posting intersect in SQL is slow at 100k | Sorted postings + equality; T-Q3/Q4 + G9 resolve |
| DuckLake merge crosses days despite intent | Partition-scoped merge; **AC-F6** |
| Downsample maintenance fights ingest | Incremental watermarks (**AC-M2**); closed hours only; T-Q9 |
| Collapse only covers `job` | GOLD 30m uses raw/5m (AC-Q8); 30d/90d `sum by (job)` uses collapse (AC-Q5/W3) |
| Open-day small-file storm vs G2+ingest-on | §4.4 MEASURE escape hatch; do not silently add WAL |
| G9 unfair write path | **AC-G6** OTLP-both-sides |
| Floor JSON claimed as ready | Validator requires `release_full` |
| Temptation to embed Greptime / Thanos | G1 + AC-D3 forbid; §4.4 escape hatch is explicit ADR reopen |
| Catalog snapshot-per-commit latency | G5 count/age + measure skip rate under AC-Q0 |

---

## 13. Implementation progress (machine gate)

Latest fail-closed run (2026-08-15 `pr_floor` + `COMPARE_GREPTIME=1`, **release** binary + day-scoped posting cache, `LAYOUT_G3_SCOPED=1` for wall-clock): see `docs/perf/results/20260815T185007Z-metrics-layout.json`. **AC-G3 PASS** — Softprobe_p95≈150ms, Greptime_p95≈73ms, **ratio≈2.06 ≤ R=10**. Not ready overall (`LAYOUT_G3_SCOPED` skipped tall/collapse/files; many other ACs still fail; `binary_profile=release` but `fixture_profile=pr_floor`).

### Open failure clusters (ordered)

| Cluster | ACs | Greptime lesson (learn, do not fork) | Softprobe action |
|---------|-----|--------------------------------------|------------------|
| **Long grain empty** | Q2, Q5, W3, W5 | Greptime Flow / laminar Flow continuous agg materializes windows independently of the write memtable (`docs/rfcs/2025-09-08-laminar-flow.md`). Softprobe’s analog is maintenance ladder + optional harness SQL materialize when maintenance is paused during fixture load. | Fix Prom 1h/collapse **visibility**: catalog rows exist but `query_range` returns 0 points — debug grain scan (`window_ts`/`last`), collapse AST wire, tenant catalog prefix on ladder tables. |
| **release_full collapse load** | W3, Q5 (wall-clock) | Flow builds collapse independently of ingest width. Softprobe AC-W3 needs **J=50** collapse series over 90d, not I×J raw. | **Harness:** `collapse_i=1` for `release_full`; single 90d OTLP seed (covers 30d); larger OTLP flushes; `materialize_query_grains` uses same SQL as `collapse.rs`. Do **not** ingest I=200×90d. |
| **G3 ratio > R** | ~~G3 (≈16× debug)~~ → **PASS ≈2.06× release** | Greptime SST inverted index (tag→row-group bitmaps + FST in Puffin) makes wide equality resolve cheap (`docs/rfcs/2023-11-03-inverted-index.md`). Softprobe **rejects** Puffin; §4.4 MEASURE = **in-process day-scoped posting cache**. | **Done:** `PostingSetCache` keyed by `(engine, tenant, record_date, label_name, label_value)` + in-process INTERSECT in `resolve_series_ids` (`postings_resolve.rs` / `ducklake_metrics.rs`). TTL 30s; day key prevents cross-day stale. Re-measure without `LAYOUT_G3_SCOPED` on next full gate. |
| **F-files / TWCS bars** | F1, F2, F4, F5 | mito2 TWCS: `trigger_file_num`, window-local merge (`src/mito2/src/compaction/twcs.rs`). Softprobe already maps day=`time_window`. | Automate F-files fixture + maintenance pass in harness; assert live-file / size bars. |
| **F-snap / N3–N4** | N3, N4 | Greptime does not solve DuckLake snapshot-per-commit; Softprobe-specific seconds expiry. | Run F-snap (A=60, 120s) in harness. |
| **Ladder honesty** | S2, M2, Q9 | Flow incremental sequences; Softprobe watermarks. | Automate second-pass 0-insert + maintenance-under-load Q1. |
| **Demo binary** | S3 | — | `grafana-manual-up.sh` → `--release`. |

**Still KEEP:** G1 DuckLake-only, no app WAL, OTLP-only Softprobe write, no Greptime embed. G9 still OTLP on Greptime (`/v1/otlp/v1/metrics`), not remote_write-only.

**Harness note:** Pausing Softprobe maintenance *during fixture load* then running an equivalent closed-hour 5m/1h/collapse SQL materialize (same SQL as `src/compaction/downsample.rs` / `collapse.rs`) is allowed for the gate — it mirrors Greptime’s separation of write path vs Flow materialization. Production still uses the maintenance scheduler. For `release_full` F-collapse, prefer **thin raw (I=1) + materialize** over wide I×days OTLP; AC-W3 remains honest at series count = J.

---

## 14. Document control

- **2026-08-14:** Goals G1–G8 and original 39 ACs reviewed adversarially.
- **2026-08-15:** Redesign after GreptimeDB study (§4, TWCS, reject fork/WAL/Puffin/DataFusion).
- **2026-08-15 (review loop):** Senior-architect pass — G9, §4.4, AC-F6/M2/G\*, JSON `release_full` gate.
- **2026-08-18 (snapshots + parquet TWCS):** Default `A=60`, inlining postponed (`data_inlining_row_limit=0`), collector demo batch **15s**, TWCS closed-day complete merge, AC-N6/F7/F8. **Required AC ids = 56**.
- **2026-08-17 (multi-window hist):** AC-H3..H6 + Q-hist-mid/long + window×type matrix; classic hist/summary always `metric_hist_samples` (no >2h divert to empty 1h grain). **Required AC ids = 53**.
- **2026-08-15 (range ceiling):** Drop Softprobe-imposed `max_query_range` (Greptime-like). Retention TTL bounds data; 30d/90d/180d remain tested SLOs. **Required AC ids = 49** (added AC-W6).
- **2026-08-15 (implement loop):** Harness + G9 OTLP compare live; 34/49 on pr_floor. §13 open clusters; Greptime II/Flow inform Softprobe cache + materialize — still no fork/WAL/Puffin.
- **2026-08-15 (AC-G3 MEASURE):** Day-scoped in-process posting cache + release binary. Scoped re-measure `20260815T185007Z`: **AC-G3 pass ratio≈2.06** (soft≈150ms / gp≈73ms). No R reopen. Full ungated re-measure still owed.
- **2026-08-16 (release_full collapse load):** Stuck r5 on F-collapse 90d with I=200 after tall+wide+30d. Harness now uses **I=1**, single 90d seed, larger OTLP batches; AC-W3 still requires J=50 collapse series over 90d.

When implementing, set status to **Accepted** only after the verification report maps every required AC-\* id. Link the result JSON from [`docs/perf/results/`](perf/results/).
