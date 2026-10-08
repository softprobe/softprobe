# Compatibility reference and corpus provenance

The compatibility images are selected from upstream release tags and consumed
by digest. A tag documents the human-readable release; the `digest` field in
[`references.v0.yaml`](references.v0.yaml) is the immutable CI/manual input.
Changing a tag or digest requires updating this manifest and the compatibility
matrix in the same review.

| Component | Source | License / attribution |
| --- | --- | --- |
| Prometheus `v2.54.1` | [prometheus/prometheus](https://github.com/prometheus/prometheus/tree/v2.54.1) | Apache-2.0; Prometheus Authors |
| Loki `3.1.1` | [grafana/loki](https://github.com/grafana/loki/tree/v3.1.1) | AGPL-3.0-only; Grafana Loki contributors |
| Tempo `2.6.1` | [grafana/tempo](https://github.com/grafana/tempo/tree/v2.6.1) | AGPL-3.0-only; Grafana Tempo contributors |
| Grafana `11.2.0` | [grafana/grafana](https://github.com/grafana/grafana/tree/v11.2.0) | AGPL-3.0-only; Grafana contributors |
| OpenTelemetry Collector Contrib `0.111.0` | [opentelemetry-collector-contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.111.0) | Apache-2.0; OpenTelemetry contributors |
| OpenTelemetry Demo `3.0.0` | [opentelemetry-demo](https://github.com/open-telemetry/opentelemetry-demo/tree/v3.0.0) | Apache-2.0; OpenTelemetry Demo contributors |
| WireMock, PostgreSQL, MinIO, and `mc` CI fixtures | Upstream project repositories named by each image | Their upstream licenses and notices; these are test infrastructure, not product artifacts. |

## Corpus provenance

The supported compatibility corpus is the checked-in manifest
[`tests/compat/manifests/cases.v0.yaml`](../../tests/compat/manifests/cases.v0.yaml).
Each case names its fixture, normalization policy, expected outcome, and
reference service. The PromQL subset additionally records its upstream-derived
test material in [`tests/compat/prometheus/promqltest/ATTRIBUTION.md`](../../tests/compat/prometheus/promqltest/ATTRIBUTION.md).
The Loki and Tempo fixtures are repository-owned phase fixtures documented in
[`phase2-loki.md`](phase2-loki.md) and [`phase3-tempo.md`](phase3-tempo.md).
Generated evidence must retain the case ID, fixture ID, request fingerprint,
reference version/image, normalization policy, classification, review status,
and `release_evidence` marker so every result is traceable to this corpus.
