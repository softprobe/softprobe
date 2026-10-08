# Compat fixtures

Signal-neutral OTel fixtures and protocol contract goldens shared across
Prometheus, Loki, Tempo, and Grafana tests.

| File | Purpose |
|------|---------|
| `metrics_classic_histogram.json` | Classic histogram signal fixture |
| `prometheus_error_unsupported.json` | Phase 0 Prom stub error envelope |
| `loki_error_unsupported.json` | Phase 0 Loki stub error envelope |
| `tempo_error_unsupported.json` | Phase 0 Tempo stub error envelope |
| `*_success_minimal.json` | Target success shapes for Phase 1+ |
| `auth_missing_bearer.json` | Missing Authorization → 401 (status-only) |
| `auth_forbidden.json` | Rejected API key → 403 (status-only) |
| `auth_scope_mismatch.json` | Scope header mismatch → 403 + protocol body |
| `auth_status_only.md` | Auth middleware body contract notes |

Prefer builders in `tests/util/otlp.rs` and helpers under `tests/compat/support/`.

Canonical docs: `docs/compat/matrix.md`.
