# Tempo compatibility Phase 3

**Status:** implemented query-only subset with nanosecond rich-trace fidelity for
the canonical OTLP fields required by issue [#31](https://github.com/softprobe/thelake/issues/31)  
**Reference:** `grafana/tempo:2.6.1` from
[`references.v0.yaml`](references.v0.yaml)  
**Differential command:** `make test-tempo-diff` (explicit Docker/reference gate)

OpenTelemetry remains the canonical trace write path. This phase adds five
tenant-scoped GET routes; Tempo write/push APIs, full TraceQL, structural
operators, metrics, and exemplars remain out of scope.

## Implemented route subset

| Route | Status | Contract |
|---|---|---|
| `/api/traces/{traceID}` | supported subset | v1 trace lookup |
| `/api/v2/traces/{traceID}` | supported subset | v2 trace lookup |
| `/api/search` | supported subset | Bounds, duration, limit, tags, and selector-oriented `q` |
| `/api/search/tags` | supported subset | Deterministic tag-name discovery |
| `/api/search/tag/{tag}/values` | supported subset | Deterministic values for projected tags |

The accepted TraceQL subset supports boolean selector expressions over stored
span attributes, stored resource attributes (including `resource.service.name`),
instrumentation `name`/`version`, and intrinsic name/kind/status/status message/
duration fields. Equality, inequality, regex, and ordered comparisons are
supported where the field is projected. Unsupported TraceQL operators, arbitrary
instrumentation-scope fields, and tag-discovery query filters return an explicit
`501 unsupported_feature`.
Malformed selectors, bounds, durations, and limits use the bad-request contract.

## Explicit fidelity boundary

The current projection preserves span identity, parent span IDs, deterministic
ordering, nanosecond span and event timestamps, string span/resource attributes,
status, events, links, and instrumentation scope name/version. TraceQL predicates
over event, link, and arbitrary instrumentation-scope fields remain explicitly
unsupported; that query-language boundary does not remove supported scope
name/version fields from trace responses.

## Differential evidence

The shared fixture is `tests/compat/tempo/phase3.json`. The pinned oracle is
`grafana/tempo:2.6.1`; the response normalizer is
`tests/compat/support/tempo.rs::normalize_tempo_response`. The differential
target enables `integration-e2e` and ignored tests, reads the image pin from
`docs/compat/references.v0.yaml`, and writes per-case raw/normalized evidence
under `target/compat/tempo/` plus the configured artifact paths. The gate is
manual/nightly because it requires Docker and the reference image.

The differential lane must not be used to waive unsupported query-language
features above. Normalization sorts response collections and canonicalizes
timestamp JSON types without adding or removing canonical trace fields.

Reference-pin validation: `make check-compat-reference-pins` parses
`docs/compat/references.v0.yaml` and verifies the Loki and Tempo images used by
the differential targets. `make compat-reference-image SIGNAL=tempo|loki`
prints the manifest-derived image for CI Docker pulls.
