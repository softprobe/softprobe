# OTel attribute projection policies

**Status:** Phase 0 contract  
**Last updated:** 2026-08-12

Projection is implemented in shared `compat::projection` code, not in HTTP
handlers. Handlers call typed backends that already apply these policies.

## Shared rules

1. **Resource vs datapoint/span attributes:** datapoint/span attributes win on
   key collision when projecting to a flat label/tag map.
2. **Non-string OTel values:** stringify with stable formatting (`true`/`false`,
   decimal integers, JSON for arrays/maps when retained as structured metadata).
3. **Empty keys:** dropped.
4. **Cardinality:** projected label/tag sets are subject to
   `limits.max_labels_per_series` (see capability manifest). Excess keys are
   dropped in lexicographic key order after reserved keys are kept; adapters
   must not silently invent values.
5. **Promoted columns:** tenant promotion adds SQL columns; projection still
   reads canonical attribute maps unless a phase explicitly maps a promoted
   column into a protocol label.

## Prometheus labels

| Source | Projection |
|--------|------------|
| Metric name | `__name__` (sanitized) |
| Resource + datapoint attributes | Labels; keys sanitized to Prometheus label regex |
| `service.name` | Prefer `job` alias when `job` absent |
| `service.instance.id` / `host.name` | Prefer `instance` alias when `instance` absent |
| Histogram / summary | Classic Prom naming (`_bucket`, `_sum`, `_count`, `_quantile`) applied in Phase 1 adapters from canonical columns |

Invalid Prometheus label characters are replaced with `_`. Names starting with
a digit get a leading `_`.

## Loki labels and structured metadata

| Source | Projection |
|--------|------------|
| Low-cardinality resource attrs (`service.name`, `deployment.environment`, …) | Stream labels (allowlist in capability / projection code) |
| Remaining attributes | Structured metadata (not stream labels) |
| Log body | Log line |

High-cardinality keys must not become stream labels. Default allowlist is
conservative; tenants may promote columns for SQL without expanding Loki
stream cardinality.

## Tempo tags

| Source | Projection |
|--------|------------|
| Span and resource attributes | Search tags |
| Intrinsic fields | `traceID`, `spanID`, `name`, `status`, duration derived from timestamps |

Span **links** and **instrumentation scope** are not first-class storage
columns in Phase 0 (documented matrix gap). Phase 3 may extend storage before
claiming TraceQL parity for those fields.

## Explicit non-goals

- Caller-supplied tenant ids as labels
- Trusting Grafana datasource UIDs as tenancy
- Expanding every OTel attribute into Prometheus labels without sanitization
  or cardinality caps
