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
| Unsupported feature | HTTP `501` (or protocol envelope with error) and stable code `unsupported_feature` |
| Auth failure | `401` missing/invalid bearer shape; `403` unresolved or mismatched tenant |
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

**Response envelope (target):** Prometheus JSON `{ "status": "success"|"error", ... }`.  
**Errors:** `errorType` + `error` string; Softprobe also emits `unsupported_feature` in shared error mapping docs when the request hits an unsupported language feature.

**Headers:** `Authorization` required. Optional Grafana org headers ignored for tenancy.

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

**Headers:** `Authorization` required. `X-Scope-OrgID` must match authenticated tenant when present (see [`auth.md`](auth.md)).

## Tempo HTTP API (query-only)

| Method | Path | Phase 0 | Phase 3 target |
|--------|------|---------|----------------|
| GET | `/api/traces/{traceID}` | stub | supported |
| GET | `/api/v2/traces/{traceID}` | stub | supported |
| GET | `/api/search` | stub | supported subset |
| GET | `/api/search/tags` | stub | supported |
| GET | `/api/search/tag/{tag}/values` | stub | supported |

**Out of scope:** write/push APIs, TraceQL full parity before subset is proven.

**Headers:** `Authorization` required. Tempo tenant header (`X-Scope-OrgID` or configured equivalent) must match authenticated tenant when present.

## Grafana

Phase 4 validates native Prometheus/Loki/Tempo datasources against the lake.
Phase 0 only provisions placeholder docs under `tests/compat/grafana/`.
No custom Grafana datasource plugin in initial scope.

## Canonical data fidelity (storage)

| Signal | Supported now (Phase 0) | Explicit unsupported |
|--------|-------------------------|----------------------|
| Metrics gauge/sum | Full scalar + attributes | — |
| Classic histogram | count, sum, bucket_counts, explicit_bounds, temporality, exemplars | — |
| Summary | count, sum, quantiles | — |
| Exponential / native histogram | Datapoint skipped with `unsupported_feature` log | Ingest of exponential hist datapoints |

When a batch mixes supported and exponential-histogram datapoints, supported
points are committed and the request may still return `2xx` with
`ingested_count` reflecting only stored points. An all-exponential batch yields
`ingested_count = 0` with `2xx` today; a stricter partial-failure envelope is a
follow-up.
| Traces | Spans, events, status, resource/span attributes, HTTP body columns | Span links, instrumentation scope columns (Phase 3 gap) |
| Logs | Body, severity, attributes, resource attributes, trace/span ids | — |

## Language feature parity

Full PromQL / LogQL / TraceQL parity is **not** claimed in v0. Each phase
adds a supported subset; everything else returns `unsupported_feature`.
