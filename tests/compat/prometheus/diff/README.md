# Prometheus mini-diff fixtures

Pinned oracle: `prom/prometheus:v2.54.1` ([`references.v0.yaml`](../../../../docs/compat/references.v0.yaml)).

| File | Role |
|------|------|
| `samples.openmetrics` | Shared series loaded into Prometheus TSDB via `promtool` |
| `cases.json` | Curated query/query_range cases compared after normalize |

Run: `make test-prom-diff` (requires Docker).

Normalization (label order + float tolerance only) lives in
`src/compat/prometheus/diff_normalize.rs`.
