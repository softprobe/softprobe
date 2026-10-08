# Datasource provisioning

Native Grafana Phase 4 provisioning for Prometheus, Loki, and Tempo. Each
signal has a stable tenant-A and tenant-B UID; the dashboards select those UIDs
through explicit variables.

Grafana expands `${VAR}` from the process environment (not shell `${VAR:-default}`).

Required when applying the full tenant-A/tenant-B fixture:

- `SOFTPROBE_URL` — Softprobe HTTP base (e.g. `http://127.0.0.1:8090`)
- `SOFTPROBE_TENANT_A_API_KEY` / `SOFTPROBE_TENANT_B_API_KEY` — bearer keys
- `SOFTPROBE_TENANT_A_ID` / `SOFTPROBE_TENANT_B_ID` — exact `X-Scope-OrgID` values

The provisioned UIDs are `softprobe-prom-a|b`, `softprobe-loki-a|b`, and
`softprobe-tempo-a|b`. Loki derived fields link `trace_id` log content to the
matching Tempo datasource; Tempo trace-to-logs uses the matching Loki UID.

The existing manual stack remains a single-tenant live OTel Demo smoke path.
It does not add deterministic correlated fixture data: the permitted Phase 4
write set contains only Grafana provisioning, dashboards, and this README.
Therefore trace/log pivots and cross-signal panels require correlated OTLP data
to be ingested by the runtime; no Grafana-only ingestion path is implied.
