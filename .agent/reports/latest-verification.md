# Verification — Phase 1 leftovers + Grafana Prom smoke

**Conversation:** `615825ed-06b5-495e-beb4-9279e91140b0`  
**PR:** https://github.com/softprobe/thelake/pull/35  
**Issues:** #30 leftovers, #27 Prom-only smoke

## Delivered

- POST `query`/`query_range` GET parity + content-type negative
- Seeded no-panic `parse_promql` property test
- Prom `extrapolatedRate` for rate/increase/delta + sparse curated fixture
- Instant NaN/stale omit; OTLP `NO_RECORDED_VALUE` → NaN preserved through DuckDB JSON (`"NaN"` string) → Prom omit
- Grafana Prom provisioning (`${SOFTPROBE_URL}` / `${SOFTPROBE_API_KEY}`) + Bearer smoke (POST range + `rate()`)

## Gates

```text
make check-fmt && make lint && make test   # green
make test-prom-compat                      # green
```

Senior-review blockers (NaN→0.0, Grafana env syntax, smoke gaps) fixed in-turn.
