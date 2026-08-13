# Grafana compatibility (Phase 4)

Pinned Grafana image: `grafana/grafana:11.2.0` (see `docs/compat/references.v0.yaml`).

## Prom-only smoke (landed)

- Provisioning: [`provisioning/datasources/prometheus.yaml`](provisioning/datasources/prometheus.yaml)
- Automated HTTP smoke (Grafana-shaped Prom API + Bearer): `tests/integration/grafana_prom_smoke.rs` via `make test`

This proves the native Prometheus datasource contract Softprobe exposes (auth headers, discovery, instant/range, POST form). It does **not** yet start a Grafana container or Playwright Explore UI.

## Still pending (full #27)

- Loki / Tempo datasource provisioning and smoke
- Pinned Grafana container + Explore / dashboard JSON assertions
- Cross-signal links and multi-tenant Grafana credentials
