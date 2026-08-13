# Senior engineer review — Phase 1 Prometheus (#30)

**Branch:** `feat/compat-phase1-prometheus`  
**Conversation:** `615825ed-06b5-495e-beb4-9279e91140b0`  
**Criteria:** AGENTS.md Always/Never + Validation gate; constitution Principles I–VII; `docs/coding-rules.md`; holistic DRY  
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

## Blocking + prior advisory → disposition

| Finding | Disposition |
|---------|-------------|
| Many-to-one silent matching | Fixed + unit test |
| scan_cap error honesty | Fixed |
| Limit path unit tests | Fixed |
| queryability Limits stale | Fixed |
| `json_to_string_map` DRY | Fixed via `variant_json_to_string_map` |
| Dual capability YAML | Rejected (symlink) |
| Handler preamble DRY | Fixed — `prepare()` + `respond_data()` |
| URL encode helper fold | Fixed — `tests/compat/support/prometheus.rs` |
| Invalid UTF-8 POST form silence | Fixed — `BadRequest` |
| `max_response_bytes` unused | Fixed — `success_response_limited` + unit test + docs |
| Matcher SQL pushdown | Deferred (product follow-up; scan model documented) |
| Rate boundary extrapolation | Deferred (documented in queryability; dense mini-diff green) |

## Verification after disposition

```text
make check-fmt && make lint && make test   # green
make test-prom-diff                        # green
```
