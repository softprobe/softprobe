# Compatibility matrix (v0)

**Status:** Approved for Phase 0  
**Version:** `compat.v0`  
**Last updated:** 2026-08-12

OpenTelemetry is the **canonical write path**. Prometheus, Loki, and Tempo
compatibility is **query-only**. Write, push, remote_write, tail, alerting, and
ruler APIs are out of scope.

Machine-readable companion: [`capability.v0.yaml`](capability.v0.yaml).
Reference pins: [`references.v0.yaml`](references.v0.yaml).

Auth and tenant rules: [`auth.md`](auth.md).  
Read-after-write: [`queryability.md`](queryability.md).  
Attribute projections: [`projections.md`](projections.md).

## Common contract rules

| Concern | Contract |
|---------|----------|
| Auth | `Authorization: Bearer <softprobe-api-key>` required on all compatibility routes |
| Tenant selection | From authenticated context only; never from query params or body |
| Content types | `application/json` responses unless a protocol requires otherwise |
| Unsupported feature | HTTP `501` + protocol-native error envelope; Softprobe code `unsupported_feature` is prefixed in the error message (Tempo also sets `softprobe_code`) |
| Auth failure | `401` missing/invalid bearer shape; `403` unresolved or mismatched tenant (middleware is status-only; scope mismatch uses protocol-native JSON) |
| Default timeout | 30s query deadline unless a route documents a lower limit |
| Default series/response caps | See capability manifest `limits` |

## Prometheus HTTP API (query-only)

Base path: `/api/v1` (Prometheus-compatible). Phase 0 registers auth + stub;
Phase 1 implements the supported subset.

| Method | Path | Phase 0 | Phase 1 target |
|--------|------|---------|----------------|
| GET\|POST | `/api/v1/query` | stub `unsupported_feature` | supported subset |
| GET\|POST | `/api/v1/query_range` | stub | supported subset |
| GET | `/api/v1/labels` | stub | supported |
| GET | `/api/v1/label/{name}/values` | stub | supported |
| GET | `/api/v1/series` | stub | supported |
| GET | `/api/v1/metadata` | stub | supported |

**Out of scope:** remote write/read, admin, TSDB, alerts, rules, targets.

**Headers:** `Authorization` required (`supported`). Optional Grafana org headers ignored for tenancy (`ignored`).

**Error envelope (Phase 0):** `{ "status":"error", "errorType":"execution", "error":"unsupported_feature: ..." }` (`application/json`).  
**Success envelope (Phase 1 target):** `{ "status":"success", "data": { "resultType", "result" } }` — see `tests/compat/fixtures/prometheus_success_minimal.json`.

### Prometheus endpoint parameters

Status values describe the **declared Phase N contract** (what adapters will do).
Phase 0 stubs authenticate and return the error envelope without parsing these
params (`ignored` at runtime today).

Status legend: `supported` | `ignored` | `unsupported_feature` | `phase_1`.

| Route | Param / field | In | Status | Notes |
|-------|---------------|----|--------|-------|
| `query` / `query_range` | `query` | query/body | `unsupported_feature` | PromQL; Phase 0 stubs ignore body |
| `query` / `query_range` | `time` / `start` / `end` / `step` | query/body | `unsupported_feature` | |
| `query` / `query_range` | `timeout` | query/body | `ignored` | Server uses `limits.query_timeout_seconds` |
| `query` / `query_range` | `tenant_id` | query/body | `ignored` | Must not change tenant scope |
| `labels` / `label/{name}/values` / `series` | `match[]` / `start` / `end` | query | `unsupported_feature` | |
| `metadata` | `metric` / `limit` | query | `unsupported_feature` | |
| all | response `status`/`data` | out | `phase_1` | Stub returns error envelope only |
| all | response `errorType`/`error` | out | `supported` | Phase 0 stub |

## Loki HTTP API (query-only)

Base path: `/loki/api/v1`.

| Method | Path | Phase 0 | Phase 2 target |
|--------|------|---------|----------------|
| GET | `/loki/api/v1/query` | stub | supported subset |
| GET | `/loki/api/v1/query_range` | stub | supported subset |
| GET | `/loki/api/v1/labels` | stub | supported |
| GET | `/loki/api/v1/label/{name}/values` | stub | supported |
| GET | `/loki/api/v1/series` | stub | supported |

**Out of scope:** push, tail, index stats, delete, ruler.

**Headers:** `Authorization` required (`supported`). `X-Scope-OrgID` must match authenticated tenant when present (`supported` consistency check; see [`auth.md`](auth.md)).

**Error envelope (Phase 0):** `{ "status":"error", "error":"unsupported_feature: ..." }`.  
**Success envelope (Phase 2 target):** `{ "status":"success", "data": { "resultType", "result" } }`.

### Loki endpoint parameters

Phase 0 stubs do not parse query params (declared Phase 2 contract below).

| Route | Param / field | In | Status | Notes |
|-------|---------------|----|--------|-------|
| `query` / `query_range` | `query` | query | `unsupported_feature` | LogQL |
| `query` / `query_range` | `limit` / `time` / `start` / `end` / `step` / `direction` | query | `unsupported_feature` | |
| `query` / `query_range` | `timeout` | query | `ignored` | Uses capability timeout |
| all | `tenant_id` | query/body | `ignored` | Never selects tenant |
| `labels` / `label/{name}/values` / `series` | `start` / `end` / `match[]` | query | `unsupported_feature` | |
| all | response `status`/`data` | out | `phase_2` | |
| all | response `error` | out | `supported` | Phase 0 stub |

## Tempo HTTP API (query-only)

| Method | Path | Phase 0 | Phase 3 target |
|--------|------|---------|----------------|
| GET | `/api/traces/{traceID}` | stub | supported |
| GET | `/api/v2/traces/{traceID}` | stub | supported |
| GET | `/api/search` | stub | supported subset |
| GET | `/api/search/tags` | stub | supported |
| GET | `/api/search/tag/{tag}/values` | stub | supported |

**Out of scope:** write/push APIs, TraceQL full parity before subset is proven.

**Headers:** `Authorization` required (`supported`). Tempo tenant header (`X-Scope-OrgID`) must match authenticated tenant when present (`supported`).

**Error envelope (Phase 0):** `{ "message":"unsupported_feature: ...", "softprobe_code":"unsupported_feature" }`.  
**Success envelope (Phase 3 target):** trace JSON / search hits — see `tests/compat/fixtures/tempo_success_minimal.json`.

### Tempo endpoint parameters

Phase 0 stubs do not parse path/query params beyond routing (declared Phase 3 contract below).

| Route | Param / field | In | Status | Notes |
|-------|---------------|----|--------|-------|
| `/api/traces/{traceID}` | `traceID` | path | `unsupported_feature` | Parsed later in Phase 3 |
| `/api/v2/traces/{traceID}` | `traceID` | path | `unsupported_feature` | |
| `/api/search` | `tags` / `minDuration` / `maxDuration` / `limit` / `start` / `end` / `q` | query | `unsupported_feature` | |
| `/api/search/tags` | (none required) | query | `unsupported_feature` | |
| `/api/search/tag/{tag}/values` | `tag` | path | `unsupported_feature` | |
| all | `tenant_id` | query/body | `ignored` | |
| all | response body | out | `phase_3` success / `supported` error | |

## Grafana

Phase 4 validates native Prometheus/Loki/Tempo datasources against the lake.
Phase 0 only provisions placeholder docs under `tests/compat/grafana/`.
No custom Grafana datasource plugin in initial scope.

## Canonical data fidelity (storage)

| Signal | Supported now (Phase 0) | Explicit unsupported |
|--------|-------------------------|----------------------|
| Metrics gauge/sum | Full scalar + attributes | — |
| Classic histogram | count, sum, bucket_counts, explicit_bounds, temporality, exemplars | Absent OTLP `sum` stores SQL NULL (scalar `value` stays `0.0` for backward SQL). |
| Summary | count, sum, quantiles | — |
| Exponential / native histogram | Datapoint skipped with `unsupported_feature` log | Ingest of exponential hist datapoints |
| Structured attributes | Scalars + arrays/kvlists (VARIANT nested JSON) + bytes (base64) | — |
| Traces | Spans, events, status, resource/span attributes, HTTP body columns | Span links, instrumentation scope columns ([#33](https://github.com/softprobe/thelake/issues/33) Phase 3) |
| Logs | Body, severity, attributes, resource attributes, trace/span ids | — |

When a batch mixes supported and exponential-histogram datapoints, supported
points are committed and the request may still return `2xx` with
`ingested_count` reflecting only stored points. An all-exponential batch yields
`ingested_count = 0` with `2xx` today; a stricter partial-failure envelope is a
follow-up.

## Language feature parity

Full PromQL / LogQL / TraceQL parity is **not** claimed in v0. Each phase
adds a supported subset; everything else returns `unsupported_feature`.
