# Compatibility authentication and tenant isolation

**Status:** Phase 0 contract  
**Last updated:** 2026-08-12

## Canonical identity

Softprobe Runtime authenticates callers with a Softprobe **API key** presented
as an HTTP Bearer token:

```http
Authorization: Bearer <softprobe-api-key>
```

The key is resolved through the configured auth service (`SOFTPROBE_AUTH_URL`)
to a tenant identity (`TenantInfo` / `TenantContext`). There is no local JWT
parse path for compatibility routes.

Grafana Prometheus, Loki, and Tempo datasources should configure the same
Bearer token in their HTTP auth settings.

## Tenant constitution

Operational and compatibility handlers **must not** accept `tenant_id` (or
equivalent) from query parameters or request bodies. Tenant scope comes only
from the authenticated context established by middleware.

## Protocol scope headers

Some Grafana/Loki/Tempo clients also send tenant scope headers. Softprobe treats
them as **informational consistency checks**, never as the source of truth.

| Protocol | Header | Behavior |
|----------|--------|----------|
| Prometheus / Grafana Prom | (none required beyond Bearer) | Extra org headers ignored for tenancy |
| Loki | `X-Scope-OrgID` | If present and non-empty, **must equal** authenticated `tenant_id`; otherwise `403` |
| Tempo | `X-Scope-OrgID` (same convention) | If present and non-empty, **must equal** authenticated `tenant_id`; otherwise `403` |

Missing scope headers are allowed when Bearer auth succeeded: the authenticated
tenant is used.

An unauthorized caller cannot select another tenant by forging `X-Scope-OrgID`
alone — middleware still requires a valid Bearer, and a mismatched header is
denied.

## Auth outcomes

| Condition | HTTP status |
|-----------|-------------|
| Missing `Authorization` | `401` |
| Malformed Bearer | `401` |
| Unknown / rejected API key | `403` |
| Scope header mismatches authenticated tenant | `403` |
| Authenticated, feature not implemented | `501` + `unsupported_feature` |

## Compatibility route prefixes

The following path prefixes require the same runtime Bearer middleware as
`/v1/*` (CORS `OPTIONS` preflight exempt where applicable):

- `/api/v1/` — Prometheus-compatible
- `/loki/api/v1/` — Loki-compatible
- `/api/traces`, `/api/v2/traces`, `/api/search` — Tempo-compatible

Admin provisioning (`POST /v1/tenants`) remains admin-key only and is unrelated
to compatibility query routes.
