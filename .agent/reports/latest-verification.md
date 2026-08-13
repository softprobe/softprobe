# Verification — Grafana PromQL pack (option C)

**Conversation:** `615825ed-06b5-495e-beb4-9279e91140b0`  
**PR:** https://github.com/softprobe/thelake/pull/35

## New supported surface

`sum|avg|min|max|count|last_over_time`, `and`/`or`/`unless` (default matching, ignore `__name__`), `topk`/`bottomk`, `delta`/`idelta`, `abs`/`ceil`/`floor`/`round`, `offset`.

Still unsupported: `@`, subquery, `on()`/`ignoring()`, `group_*`, full catalog, hist funcs.

## Gates (2026-08-13)

```text
make check-fmt && make lint && make test   # green
make test-prom-diff                        # green
make test-promqltest                       # green — 100 curated oracle evals
```

End-to-end proof: curated fixtures under `tests/compat/prometheus/promqltest/curated/` (Apache-2.0 excerpts from Prometheus v2.54.1) load into Softprobe + pinned `prom/prometheus:v2.54.1`; responses normalized then `assert_eq!` on `resultType` + `result`.

Senior-review blocking items fixed in-turn: Prom `round` half-ties, parenthesized range-vector args, `avg_over_time` NaN poisoning, range+offset oracle, shared `funcs` allowlist.
