# Verification — curated promqltest (option A)

**Conversation:** `615825ed-06b5-495e-beb4-9279e91140b0`  
**Feature:** Phase 1 Prometheus + curated upstream promqltest (#30 / option A)

## Acceptance mapping

| Criterion | Evidence |
|-----------|----------|
| Curated upstream `.test` fixtures + attribution | `tests/compat/prometheus/promqltest/` |
| Load into lake + pinned Prom; compare normalized JSON | `tests/integration/prometheus_promqltest.rs` |
| Shared oracle (DRY with mini-diff) | `tests/compat/support/prometheus_oracle.rs` |
| Make targets | `make test-promqltest`, `make test-prom-compat` |
| Docs | `docs/compat/phase1-prometheus.md` |
| PromQL parity: drop `__name__` on unary minus / vector-scalar arith | `src/compat/promql/eval.rs` + unit tests |

## Gates run

```text
make check-fmt && make lint && make test   # green
make test-prom-compat                      # green
```

Curated run counts (logged): aggregators 11, selectors 6, operators 7, rate 2; unsupported skipped 0.
