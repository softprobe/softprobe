# Senior engineer review — curated promqltest (option A)

**Branch:** `feat/compat-phase1-prometheus`  
**Conversation:** `615825ed-06b5-495e-beb4-9279e91140b0`  
**Initial verdict:** REQUEST_CHANGES  
**After disposition:** APPROVE_WITH_FIXES

## Principle scorecard (final)

| Principle | Score |
|-----------|-------|
| I Scope only | PASS |
| II Simple/DRY | PASS |
| III Fail fast | PASS |
| IV Docs with change | PASS |
| V Existing contracts | PASS |
| VI Verification before done | PASS |
| VII Persist | PASS |
| AGENTS Always / Never | PASS |

## Findings → disposition

| Finding | Disposition |
|---------|-------------|
| Oracle harness duplicated vs mini-diff | **Fixed** — `tests/compat/support/prometheus_oracle.rs`; both suites call it |
| Compat gate silent-skip without Docker | **Fixed** — `require_docker()` panic + Makefile `docker info` preflight |
| Hardcoded curated file list | **Fixed** — enumerate `curated/*.test`; `# softprobe: counter` header |
| Fixed host port recycle race | **Fixed** — ephemeral `127.0.0.1::9090` + `docker port` |
| Range eval weaker asserts | **Fixed** — status + resultType + result |
| Docs Tests table omit promqltest | **Fixed** |
| `expected_lines` unused | **Rejected** — option A is lake↔Prom differential only |
| Dead `start-incx` stub in expand | **Fixed** — removed |
| Unary-minus `__name__` unit test | **Fixed** |
| Attribution NOTICE pointer | **Deferred** — ATTRIBUTION.md sufficient for curated excerpts |

## Verification after disposition

```text
make check-fmt && make lint && make test   # green
make test-prom-compat                      # green (mini-diff + promqltest)
```
