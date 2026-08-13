# Upstream PromQL regression fixtures (curated)

**Source:** [prometheus/prometheus](https://github.com/prometheus/prometheus)  
**Pin:** tag `v2.54.1` (commit `79f755062047bc5d621f6105e1980e3e2f7486e0`)  
**Path upstream:** `promql/promqltest/testdata/`  
**License:** Apache License 2.0 (see Prometheus NOTICE / LICENSE)

Files under `curated/` are **trimmed excerpts** of upstream `.test` DSL cases that exercise Softprobe’s declared Phase 1 PromQL subset. They are not a full copy of the corpus.

## Runner

`make test-promqltest` (Docker required):

1. Parse `load` / `clear` / `eval instant|range`
2. Expand series into OpenMetrics → pinned `prom/prometheus:v2.54.1` TSDB
3. Expand the same series into OTLP → Softprobe lake (labels as datapoint attrs; no `service.name` so projection matches Prom labels)
4. Execute each supported `eval` against both; compare with `diff_normalize` (label order + float eps only)
5. Skip evals whose AST is outside the declared subset (`unsupported_feature`), recorded in the run log

## Adding cases

Prefer copying a contiguous upstream block, keep the attribution header, drop only unsupported `eval` lines (or leave them for the runner to skip).

Put `# softprobe: counter` near the top when series must ingest as cumulative sums (rate/increase). The runner enumerates every `curated/*.test` file.

Samples and eval times are shifted from unix-0 to a shared base (`EVAL_BASE_MS`) so OTLP does not treat timestamp 0 as “now”.
