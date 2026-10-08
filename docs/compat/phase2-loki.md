# Loki compatibility Phase 2

**Status:** implemented query-only subset for issue [#29](https://github.com/softprobe/thelake/issues/29)  
**Reference:** `grafana/loki:3.1.1` from [`references.v0.yaml`](references.v0.yaml)  
**Differential command:** `make test-loki-diff` (explicit Docker/reference gate)

The Phase 2 surface is the five implemented GET routes below. OpenTelemetry is
still the canonical write path; Loki `push`, `tail`, ruler, delete, and index
administration APIs are out of scope.

| Route | Status | Contract |
|---|---|---|
| `/loki/api/v1/query` | supported subset | Instant stream query; `time`, `limit`, and `direction` are supported |
| `/loki/api/v1/query_range` | supported subset | Stream range query; `start`/`end` or `since`, `limit`, and `direction` |
| `/loki/api/v1/labels` | supported | Label-name discovery, optionally bounded by `start`/`end` |
| `/loki/api/v1/label/{name}/values` | supported | Label-value discovery with optional `match[]`, `start`, and `end` |
| `/loki/api/v1/series` | supported | Stream-label discovery with optional `match[]`, `start`, and `end` |

## Supported LogQL

The accepted subset is stream selectors with label matchers `=`, `!=`, `=~`, and
`!~`; line filters `|=`, `!=`, `|~`, and `!~`; bare `json` and `logfmt` parser
stages; and parsed-field matchers after a parser stage. Responses are Loki
`streams` results with nanosecond timestamps and structured metadata.

The following are explicit compatibility boundaries and return
`501 unsupported_feature`: `interval` sampling, `step` sampling, `unwrap`, parser
field expressions (`json foo`, `logfmt foo`), LogQL functions such as
`count_over_time`, `rate`, and `sum_over_time`, metric/range aggregations, and
unlisted pipeline stages. A malformed or non-positive duration is instead
`400 bad_request`.

## Tenant and isolation semantics

Every route requires the bearer-authenticated tenant context. `tenant_id` in a
query string or body is ignored and cannot select data. When supplied,
`X-Scope-OrgID` must exactly match the authenticated tenant; a mismatch is `403`.
Queries, label discovery, series discovery, and structured metadata are scoped
to that tenant’s data. See [`auth.md`](auth.md) for the shared authentication
contract.

## Differential evidence

The fixture is `tests/compat/loki/phase2.json`; the pinned oracle is
`grafana/loki:3.1.1`; and response normalization is
`tests/compat/support/loki.rs::normalize_loki_response`. The test helper consumes
`LOKI_RAW_ARTIFACT` and `LOKI_NORMALIZED_ARTIFACT`. Failure evidence is written
per case below `target/compat/loki/<case>/`, including raw request, lake, and
oracle responses plus normalized lake/oracle responses when available. This gate
is intentionally separate from `make test`/`make ci` because it needs Docker and
the reference image. Loki's `data.stats` is execution metadata, so it remains in
the raw evidence but is omitted from normalized semantic comparison; result
streams, labels, timestamps, entries, structured metadata, and ordering remain
compared.
