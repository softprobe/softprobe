# Verification — reproducible Grafana + richer Prom smoke

## Acceptance mapping

| Plan item | Evidence |
|-----------|----------|
| `make grafana-up` / `grafana-down` | Scripts + Makefile; up printed URL; already-up re-seeds |
| Host Softprobe + /tmp sqlite | `CONFIG_FILE=/tmp/thelake-grafana-manual/config.yaml` |
| Dense OTLP seed | `grafana_seed_otlp` 120×2 samples; `rate()` series=2 |
| Expand CI smoke | offset, over_time, sum by, topk, compare, arith, range &lt;2s |
| Enrich dashboard | panels for same shapes in `softprobe-prom-smoke.json` |
| Docs | `tests/compat/grafana/README.md`, matrix, phase1, `make help` |

## Gates

| Gate | Result |
|------|--------|
| `make check-fmt && make lint` | green |
| `make test-grafana-prom-smoke` | pass |
| Manual `make grafana-up` | ready @ :3000 / :8090 |

## Not claimed DONE

Full `make test` / `make test-prom-compat` not re-run in this slice (unchanged PromQL evaluator; smoke + fmt/lint sufficient for this change).
