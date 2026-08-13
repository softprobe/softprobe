# Datasource provisioning

Prom-only fixture for Softprobe Phase 4 (#27). Loki/Tempo YAML will land with those adapters.

Grafana expands `${VAR}` from the process environment (not shell `${VAR:-default}`).

Required when applying this file:

- `SOFTPROBE_URL` — Softprobe HTTP base (e.g. `http://127.0.0.1:8090`)
- `SOFTPROBE_API_KEY` — Softprobe API key (header becomes `Bearer <key>`)
