# Compatibility matrix (v0)

**Status:** Phase 0/1 baseline remains approved; Prometheus Phase 1, Loki Phase 2, and Tempo Phase 3 repository subsets are implemented; Grafana Phase 4 repository wiring and its validation-only harness are implemented, while real service-backed acceptance requires external evidence
**Version:** `compat.v0`  
**Last updated:** 2026-08-15

OpenTelemetry is the **canonical write path**. Prometheus, Loki, and Tempo
compatibility is **query-only**. Write, push, remote_write, tail, alerting, and
ruler APIs are out of scope.

Machine-readable companion: [`capability.v0.yaml`](capability.v0.yaml).
Reference pins: [`references.v0.yaml`](references.v0.yaml).

Auth and tenant rules: [`auth.md`](auth.md).  
Read-after-write: [`queryability.md`](queryability.md).  
Attribute projections: [`projections.md`](projections.md).

## Baseline and acceptance status

The **Phase 0 baseline remains approved**: OpenTelemetry is the canonical write
path, compatibility routes are query-only, and shared authentication, tenant,
error, limit, and storage-fidelity rules remain the baseline contract. Phase 1
adds the implemented Prometheus discovery and declared PromQL subset; its
unsupported features remain explicit. Later implementation status does not
replace or silently broaden that baseline.

Repository-side Loki and Tempo query subsets, shared fixtures, authenticated
contract suites, and Grafana provisioning are implemented. The repository also
contains a **mock/validation-only harness** for manifest and Grafana checks.
That harness validates local routing, envelopes, fixtures, tenant boundaries,
and provisioning shape; a passing mock run is not evidence that a real Loki,
Tempo, Prometheus, or Grafana service accepted the same workload.

Tracked scope and evidence work: parent compatibility scope [#25](https://github.com/softprobe/thelake/issues/25),
Grafana integration [#27](https://github.com/softprobe/thelake/issues/27), and
manifest/conformance evidence [#28](https://github.com/softprobe/thelake/issues/28).

### Real service-backed evidence prerequisites

The repository-side Loki, Tempo, and Grafana work is implemented as described
above. The following prerequisites are only for real service-backed acceptance;
they are distinct from repository implementation and mock validation:

1. Provide a Docker daemon and verify the pinned references with
   `make check-compat-reference-pins`: `prom/prometheus:v2.54.1`,
   `grafana/loki:3.1.1`, `grafana/tempo:2.6.1`, and
   `grafana/grafana:11.2.0`.
2. Run `make test-prom-compat`, `make test-loki-diff`, and
   `make test-tempo-diff` against those real reference services. Each run must
   retain complete raw and normalized per-case evidence and have no unapproved
   normalized differences.
3. For Grafana, supply `GRAFANA_REFERENCE_DIGEST` matching the pinned
   `grafana/grafana:11.2.0` image plus `SOFTPROBE_URL`,
   `SOFTPROBE_API_KEY`, `SOFTPROBE_TENANT_A_API_KEY`,
   `SOFTPROBE_TENANT_B_API_KEY`, `SOFTPROBE_TENANT_A_ID`, and
   `SOFTPROBE_TENANT_B_ID`. Run `make test-grafana-system` and retain
   passing, sanitized evidence for G1-G8; a mock selector cannot satisfy this
   gate.
4. Run each of the following without `--mock`, after the protocol and Grafana
   gates:
   `scripts/compat/conformance.sh --protocol prometheus --out target/compat/conformance/prometheus`,
   `scripts/compat/conformance.sh --protocol loki --out target/compat/conformance/loki`,
   and `scripts/compat/conformance.sh --protocol tempo --out target/compat/conformance/tempo`.
   Every manifest case must retain request, Softprobe, reference, normalized,
   diff, and outcome artifacts, with every normalized comparison equal.
5. Redact bearer tokens, API keys, tenant secrets, cookies, passwords, and
   credential-bearing URLs before uploading evidence. A missing prerequisite or
   an unrun case is not a pass.

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

Base path: `/api/v1` (Prometheus-compatible). Phase 1 implements discovery and
a declared PromQL subset; see [`phase1-prometheus.md`](phase1-prometheus.md).

| Method | Path | Phase 1 status |
|--------|------|----------------|
| GET\|POST | `/api/v1/query` | PromQL subset (Slice B) |
| GET\|POST | `/api/v1/query_range` | PromQL subset (Slice B) |
| GET | `/api/v1/labels` | **supported** |
| GET | `/api/v1/label/{name}/values` | **supported** |
| GET | `/api/v1/series` | **supported** |
| GET | `/api/v1/metadata` | **supported** |

**Out of scope:** remote write/read, admin, TSDB, alerts, rules, targets.

**Headers:** `Authorization` required (`supported`). Optional Grafana org headers ignored for tenancy (`ignored`).

**Error envelope:** `{ "status":"error", "errorType":"execution"|"bad_data", "error":"unsupported_feature: ..." }`.  
**Success (discovery):** `{ "status":"success", "data": ... }` (labels/values = string array; series = label objects; metadata = name → `[{type,help,unit}]`).  
**Success (query):** `{ "status":"success", "data": { "resultType", "result" } }`.

### PromQL subset (Phase 1)

| Supported | Explicit unsupported |
|-----------|----------------------|
| Vector selectors + matchers `=` `!=` `=~` `!~` | Native/exponential histogram functions |
| Instant + range vectors; `offset` modifier | Subqueries, `@` modifier |
| Aggregations `sum`/`min`/`max`/`avg`/`count`/`topk`/`bottomk` + `by`/`without` | Other aggregations (`quantile`, `stddev`, …) |
| Arithmetic + comparison (filtering) + set ops `and`/`or`/`unless` (default matching = all labels except `__name__`, matching Prometheus `signatureFunc`) | Explicit `on()`/`ignoring()`; `group_left`/`group_right` |
| `rate`, `irate`, `increase`, `delta`, `idelta` | Full function catalog; recording rules / alerts |
| `sum|avg|min|max|count|last_over_time` | |
| `abs`, `ceil`, `floor`, `round` | |

Classic histogram fidelity columns are exposed as `_bucket`/`_sum`/`_count` series for selectors; histogram *functions* stay unsupported. Summary series expose `_sum`/`_count` plus the base name; per-quantile `_quantile{quantile=…}` expansion is **not** implemented in Phase 1 (documented unsupported).

### Prometheus endpoint parameters

Status legend: `supported` | `ignored` | `unsupported_feature` | `phase_1`.

| Route | Param / field | In | Status | Notes |
|-------|---------------|----|--------|-------|
| `query` / `query_range` | `query` | query/body | `phase_1` | Declared PromQL subset |
| `query` / `query_range` | `time` / `start` / `end` / `step` | query/body | `phase_1` | |
| `query` / `query_range` | `timeout` | query/body | `ignored` | Server uses `limits.query_timeout_seconds` |
| `query` / `query_range` | `tenant_id` | query/body | `ignored` | Must not change tenant scope |
| `labels` / `label/{name}/values` / `series` | `match[]` / `start` / `end` | query | `supported` | |
| `metadata` | `metric` / `limit` | query | `supported` | |
| discovery | response `status`/`data` | out | `supported` | |
| query | response `resultType`/`result` | out | `phase_1` | |
| all | response `errorType`/`error` | out | `supported` | |

## Loki HTTP API (query-only)

Base path: `/loki/api/v1`.

The repository-side Phase 2 query-only subset and mock contract suite are
implemented. Reference oracle for the real differential lane: `grafana/loki:3.1.1`, pinned in
[`references.v0.yaml`](references.v0.yaml). The lane is explicit/nightly or manual;
it is not part of the fast unit/PR gate, and its mock path is not acceptance
evidence.

| Method | Path | Phase 2 status |
|--------|------|----------------|
| GET | `/loki/api/v1/query` | **supported subset** |
| GET | `/loki/api/v1/query_range` | **supported subset** |
| GET | `/loki/api/v1/labels` | **supported** |
| GET | `/loki/api/v1/label/{name}/values` | **supported** |
| GET | `/loki/api/v1/series` | **supported** |

**Out of scope:** push, tail, index stats, delete, ruler.

**Headers:** `Authorization` required (`supported`). `X-Scope-OrgID` must match authenticated tenant when present (`supported` consistency check; see [`auth.md`](auth.md)).

**Error envelope:** `{ "status":"error", "error":"unsupported_feature: ..." }` for
unsupported features, with `400` for malformed requests and `403` for a mismatched
tenant scope.
**Success envelope:** `{ "status":"success", "data": { "resultType":"streams", "result": [...] } }`
for query routes; discovery routes return `{ "status":"success", "data": [...] }`.

### Loki LogQL subset (Phase 2)

Supported stream queries start with a stream selector and may use label matchers
(`=`, `!=`, `=~`, `!~`), line filters (`|=`, `!=`, `|~`, `!~`), bare `json` or
`logfmt` parser stages, and parsed-field matchers after those parser stages. Query
results are log streams, preserving nanosecond timestamps and structured metadata.
The implementation is intentionally a stream-query subset; full LogQL parity is not
claimed.

Explicitly unsupported and returned as `501 unsupported_feature`: `interval` and
`step` sampling, `unwrap`, parser field expressions such as `json foo` or
`logfmt foo`, LogQL functions (including `count_over_time`, `rate`, and
`sum_over_time`), metric queries/range aggregations, and pipeline stages not listed
above. Malformed duration values or zero/negative durations are `400 bad_request`;
valid `interval`/`step` values are still rejected as unsupported stream sampling.

### Loki endpoint parameters

Phase 2 parses the parameters below; unknown or unlisted LogQL behavior is not
implicitly supported.

| Route | Param / field | In | Status | Notes |
|-------|---------------|----|--------|-------|
| `query` / `query_range` | `query` | query | `supported` | Declared LogQL subset |
| `query` / `query_range` | `limit` / `time` / `start` / `end` / `since` / `direction` | query | `supported` | `query_range` requires `start`+`end` or `since`; `time` is for instant query |
| `query_range` | `interval` / `step` | query | `unsupported_feature` | Stream results are not sampled |
| `query` / `query_range` | `timeout` | query | `ignored` | Uses capability timeout |
| all | `tenant_id` | query/body | `ignored` | Never selects tenant |
| `labels` / `label/{name}/values` / `series` | `start` / `end` / `match[]` | query | `supported` | `match[]` is a stream selector |
| all | response `status`/`data` | out | `supported` | Native Loki success envelope |
| all | response `error` | out | `supported` | Includes `unsupported_feature:` prefix when applicable |

## Tempo HTTP API (query-only)

The repository-side Phase 3 implementation provides a bounded, query-only Tempo
subset. The five routes are live,
and the canonical projection preserves nanosecond timestamps, parent topology,
all stored resource and span attributes, status, events, links, and
instrumentation scope name/version. Selector-oriented TraceQL remains
intentionally bounded; the supported resource selectors cover stored resource
attributes. `instrumentation.name` and `instrumentation.version` selectors are
supported; arbitrary instrumentation-scope fields remain explicit
`unsupported_feature`. See
[`phase3-tempo.md`](phase3-tempo.md).

| Method | Path | Phase 0/1 baseline | Current repository status |
|--------|------|--------------------|--------------------------|
| GET | `/api/traces/{traceID}` | declared route/auth contract | supported subset |
| GET | `/api/v2/traces/{traceID}` | declared route/auth contract | supported subset |
| GET | `/api/search` | declared route/auth contract | supported subset |
| GET | `/api/search/tags` | declared route/auth contract | supported subset |
| GET | `/api/search/tag/{tag}/values` | declared route/auth contract | supported subset |

**Out of scope:** write/push APIs, TraceQL full parity, structural TraceQL
operators, TraceQL metrics/exemplars, and TraceQL predicates over event, link,
or instrumentation-scope fields. The corresponding response fields are
preserved when present in canonical OTLP data.

**Headers:** `Authorization` required (`supported`). Tempo tenant header (`X-Scope-OrgID`) must match authenticated tenant when present (`supported`).

**Error envelope:** `{ "message":"unsupported_feature: ...", "softprobe_code":"unsupported_feature" }` for explicit unsupported behavior; malformed requests use the protocol's bad-request envelope and missing traces return `404`.
**Success envelope:** trace JSON / search hits — see
`tests/compat/fixtures/tempo_success_minimal.json`.

### Tempo supported subset

Trace lookup returns the v1/v2 envelope with parent span IDs, span names/kinds,
string attributes, all stored resource attributes, status, nanosecond event
timestamps, links, and instrumentation scope name/version. Search supports `tags`,
`minDuration`, `maxDuration`, `start`, `end`, `limit`, and the selector-oriented
`q` subset. The supported selector fields are stored span and resource
attributes, instrumentation `name`/`version`, plus intrinsic
name/kind/status/status message/duration, with boolean `&&`/`||` and
comparison/regex operators. Tag discovery is deterministically ordered and
scoped to the authenticated tenant.

The following return `501 unsupported_feature`: event/link TraceQL fields,
instrumentation-scope fields other than `name` and `version`,
parent/child/descendant operators, pipelines, aggregations, metrics, exemplars,
and query filters on the tag-discovery routes. Output fidelity for links and
instrumentation scope name/version is independent of those unsupported TraceQL
field predicates.

### Tempo endpoint parameters

The Phase 3 subset parses the path and the declared search parameters below.

| Route | Param / field | In | Status | Notes |
|-------|---------------|----|--------|-------|
| `/api/traces/{traceID}` | `traceID` | path | `supported_subset` | Tenant-scoped lookup |
| `/api/v2/traces/{traceID}` | `traceID` | path | `supported_subset` | Tenant-scoped lookup |
| `/api/search` | `tags` / `minDuration` / `maxDuration` / `limit` / `start` / `end` / `q` | query | `supported_subset` | Declared subset only |
| `/api/search/tags` | (none required) | query | `supported_subset` | Query filters are unsupported |
| `/api/search/tag/{tag}/values` | `tag` | path | `supported_subset` | Query filters are unsupported |
| all | `tenant_id` | query/body | `ignored` | |
| all | response body | out | `supported_subset` success / `supported` error | |

## Grafana

Phase 4 repository work is implemented as native Prometheus/Loki/Tempo
datasource provisioning, dashboards, cross-signal wiring, and a
mock/validation-only system harness. **Prom-only smoke landed:** Grafana-shaped Bearer HTTP sequence in
`tests/integration/grafana_prom_smoke.rs` plus provisioning YAML under
`tests/compat/grafana/provisioning/datasources/prometheus.yaml` (pin
`grafana/grafana:11.2.0`). Loki/Tempo provisioning and Explore smoke artifacts
are also present. Manual stack: `make grafana-up` / `make grafana-down`
(OpenTelemetry Demo → Softprobe; see `tests/compat/grafana/README.md`).

The mock/validation-only harness does not constitute real Grafana acceptance.
Real acceptance requires the external digest, runtime credentials, pinned
service-backed protocol gates, and passing G1-G8 evidence listed above.
No custom Grafana datasource plugin in initial scope.

## Canonical data fidelity (storage)

| Signal | Supported now (Phase 0) | Explicit unsupported |
|--------|-------------------------|----------------------|
| Metrics gauge/sum | Full scalar + attributes | — |
| Classic histogram | count, sum, bucket_counts, explicit_bounds, temporality, exemplars | Absent OTLP `sum` stores SQL NULL (scalar `value` stays `0.0` for backward SQL). |
| Summary | count, sum, quantiles | — |
| Exponential / native histogram | Datapoint skipped with `unsupported_feature` log | Ingest of exponential hist datapoints |
| Structured attributes | Scalars + arrays/kvlists (VARIANT nested JSON) + bytes (base64) | — |
| Traces | Spans, parent topology, nanosecond timestamps, span/resource attributes, status, events, links, instrumentation scope name/version | TraceQL predicates over event/link fields or arbitrary instrumentation-scope fields; structural TraceQL operators and aggregations |
| Logs | Body, severity, attributes, resource attributes, trace/span ids | — |

When a batch mixes supported and exponential-histogram datapoints, supported
points are committed and the request may still return `2xx` with
`ingested_count` reflecting only stored points. An all-exponential batch yields
`ingested_count = 0` with `2xx` today; a stricter partial-failure envelope is a
follow-up.

## Language feature parity

Full PromQL / LogQL / TraceQL parity is **not** claimed in v0. Each phase
adds a supported subset; everything else returns `unsupported_feature`.
