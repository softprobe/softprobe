# Senior engineer review — reproducible Grafana + richer Prom smoke

**Conversation:** `615825ed-06b5-495e-beb4-9279e91140b0`  
**Reviewer agent:** `e39da0df-8520-46bb-9085-410904f04a68` (initial REQUEST_CHANGES) + parent disposition

## Initial verdict

`REQUEST_CHANGES` — soft wait loops, unsafe “already up”, empty matrix allowed, DRY seed vs test helper, help/docs gaps.

## Disposition

| Finding | Decision | Action |
|---------|----------|--------|
| Auth/Grafana waits never fail | **Agree — fixed** | Fail after timeout like Softprobe wait |
| Already-up trusts foreign :8090 | **Agree — fixed** | Require owned pid + softprobe-runtime cmdline; re-seed; refuse foreign :8090 |
| Port preflight | **Agree — fixed** | Error if :8090 foreign or :3000 occupied without our Grafana |
| Empty matrix OK in smoke | **Agree — fixed** | `assert_matrix_ok` requires non-empty |
| Dashboard vs smoke compare drift | **Agree — fixed** | Both use `http_requests > 40` |
| make help omit grafana | **Agree — fixed** | Help lists grafana-up/down/smoke |
| DRY seed vs gauge_series_otlp | **Defer** | Bin cannot depend on test harness; comment cross-links twin |
| Fold OTLP into lib | **Defer** | Avoid expanding public lib surface for a demo seed |

## Re-review

**Verdict:** `APPROVE_WITH_FIXES`  
**needs_rereview:** false

## Evidence

- `make check-fmt && make lint` green
- `make test-grafana-prom-smoke` pass
- `make grafana-up` prints URL; already-up re-seeds; `rate()` series=2
