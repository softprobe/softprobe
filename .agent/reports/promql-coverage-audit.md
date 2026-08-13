# PromQL Phase 1 — missing test coverage audit (updated)

**Conversation:** `615825ed-06b5-495e-beb4-9279e91140b0`  
**Corpus pin:** Prometheus `v2.54.1`

## Softprobe curated (current)

| | Count |
|--|------:|
| Curated fixtures | 12 `.test` files |
| Instant evals | 68 |
| Range evals | 3 |
| **Total oracle diffs** | **71** |

Includes: range trio, irate/rate/increase (dense), compare/`bool`, by(job), `%`/`^`, rate×arith, literals, selector edges.

## Still deferred (explicit)

- Sparse rate extrapolation parity (`queryability.md`)
- Prometheus `stale` markers
- Native histogram corpus
- Full upstream 837 success-eval corpus (Phase 5 / #28 harness)
- Grafana smoke (#30 lists; separate lane)
