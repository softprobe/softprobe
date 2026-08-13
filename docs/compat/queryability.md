# Read-after-write and queryability guarantees

**Status:** Phase 0 contract  
**Last updated:** 2026-08-12

## Ingest commit boundary

Each successful OTLP HTTP (or gRPC traces) ingest request commits through the
tenant-scoped DuckLake writer before returning success. After a `2xx` ingest
response, data is durable in that tenant's DuckLake scope.

Compatibility query adapters (Phases 1–3) and SQL/telemetry APIs read from the
same DuckLake catalog. There is no separate application-level WAL or staged
tier that delays visibility after a successful ingest response.

## Visibility for clients

| Scenario | Guarantee |
|----------|-----------|
| Client receives `2xx` from `/v1/metrics`, `/v1/logs`, or `/v1/traces` | Rows are committed; subsequent queries in the same tenant scope can observe them |
| Client retries an ingest after network failure without seeing a response | Duplicate rows may appear; ingest is not idempotent by payload hash |
| Query during an in-flight ingest on another connection | Uncommitted rows are not visible |

## Ordering and late data

| Case | Behavior |
|------|----------|
| Out-of-order timestamps within a batch | Stored as-is; query adapters sort deterministically for protocol responses |
| Duplicate timestamps for the same series | Both samples retained; PromQL-style "last sample wins" is an adapter concern |
| Counter resets | Preserved as raw samples; PromQL `rate`/`irate`/`increase` treat a downward step as a reset (add previous value) |
| `rate` / `increase` window math | Phase 1 uses first→last sample span within the selected range vector (no Prometheus range-boundary extrapolation). Dense series match the pinned oracle in `make test-prom-diff`; sparse-series extrapolation parity is deferred |
| Late-arriving records (older than recent ingest) | Accepted and stored; no reject-by-staleness gate in Phase 0 |

## Empty / invalid tenant

| Case | Behavior |
|------|----------|
| Missing Bearer on protected routes | `401` |
| Bearer resolves but tenant scope is not provisioned | Engine/scope resolution error (`4xx`/`5xx` per existing control-plane behavior) |
| Empty tenant id in authenticated context | Treated as invalid operational state; handlers must not invent a default tenant for cross-tenant data |

## Limits (defaults)

See `capability.v0.yaml` `limits` for defaults. Phase 1 Prometheus adapters enforce:

| Limit | Behavior |
|-------|----------|
| `max_query_range_seconds` | `limit_exceeded` / Prom `bad_data` when both start and end are present and the span is too large |
| `max_series` | Hard fail when series identities or distinct label values exceed the cap |
| scan_cap (`max(max_series*10, 10000)`) | Full-window scan with `LIMIT scan_cap+1`; overrun → `limit_exceeded` (narrow the time window). Matchers are applied after the scan and do not reduce SQL load |
| `query_timeout_seconds` | Deadline on `TenantContext`; overrun → `limit_exceeded` |
| `max_response_bytes` | Enforced when encoding Prometheus success envelopes; overrun → `limit_exceeded` |

Exceeding enforced limits returns a stable `limit_exceeded` Softprobe code (Prometheus `errorType: bad_data`).
