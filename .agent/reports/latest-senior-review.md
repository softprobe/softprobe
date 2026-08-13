# Senior engineer review — Phase 1 leftovers + Grafana Prom smoke

**Branch:** `feat/compat-phase1-prometheus`  
**Conversation:** `615825ed-06b5-495e-beb4-9279e91140b0`  
**PR:** https://github.com/softprobe/thelake/pull/35  
**Initial verdict:** REQUEST_CHANGES  
**After disposition:** APPROVE_WITH_FIXES

## Findings → disposition

| # | Finding | Disposition |
|---|---------|-------------|
| 1 | DuckLake NaN/stale → JSON Null → `unwrap_or(0.0)` | **Fixed** — `finite_or_special_float` emits `"NaN"`/`±Inf` strings; `cell_f64` parses them; OTLP flag integration test |
| 2 | Grafana `${VAR:-default}` invalid for pin 11.2 | **Fixed** — `${SOFTPROBE_URL}` / `${SOFTPROBE_API_KEY}` only + README |
| 3 | Smoke missing POST range + `rate()` | **Fixed** |
| 4 | `irate`/`idelta` skipped NaN filter | **Fixed** |
| 5 | DRY authenticated_router | Accepted for this slice (smoke mirrors phase0; fold later) |
| 6–7 | Docs / hist flags | avg_over_time comment fixed; hist NO_RECORDED deferred |

## Verification

```text
make check-fmt && make lint && make test   # green
make test-prom-compat                      # green (incl. sparse rate fixture)
```
