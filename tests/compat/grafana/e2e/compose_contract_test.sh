#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/compat/grafana/docker-compose.ci.yml"

for service in auth-mock postgres minio softprobe grafana-seed grafana; do
  grep -Eq "^  ${service}:$" "$COMPOSE_FILE" || {
    echo "missing self-contained Grafana service: $service" >&2
    exit 1
  }
done

grep -Fq 'SOFTPROBE_AUTH_URL: http://auth-mock:8080/validate' "$COMPOSE_FILE"
grep -Fq 'GRAFANA_SOFTPROBE_URL' "$COMPOSE_FILE"
grep -Fq 'image: ${GRAFANA_COMPOSE_IMAGE:?GRAFANA_COMPOSE_IMAGE must be supplied from docs/compat/references.v0.yaml}' "$COMPOSE_FILE"
grep -Fq 'grafana-phase4-tenant-a' "$COMPOSE_FILE"
grep -Fq 'grafana-phase4-tenant-b' "$COMPOSE_FILE"
grep -Fq 'CARGO_TARGET_DIR: /tmp/thelake-grafana-target' "$COMPOSE_FILE"
grep -Fq 'cargo run --locked --bin softprobe-runtime' "$COMPOSE_FILE"
grep -Fq 'cargo run --locked --bin grafana_seed_otlp' "$COMPOSE_FILE"
grep -Fq 'condition: service_completed_successfully' "$COMPOSE_FILE"

grep -Fq 'GRAFANA_CHECK_DASHBOARD_QUERIES=1' "$ROOT_DIR/Makefile"
grep -Fq 'GRAFANA_REFERENCE_IMAGE=' "$ROOT_DIR/Makefile"
grep -Fq 'GRAFANA_REFERENCE_DIGEST=' "$ROOT_DIR/Makefile"
grep -Fq 'GRAFANA_COMPOSE_IMAGE=' "$ROOT_DIR/Makefile"
grep -Fq 'Docker unavailable' "$ROOT_DIR/Makefile"
grep -Fq 'GNU timeout unavailable' "$ROOT_DIR/Makefile"

grep -Fq '"uid": "softprobe-prom-a"' "$ROOT_DIR/tests/compat/grafana/dashboards/smoke/softprobe-cross-signal.json"

grep -Fq 'GRAFANA_REFERENCE_DIGEST' "$ROOT_DIR/scripts/grafana-system-smoke.sh"
grep -Fq 'validate_grafana_reference_pin' "$ROOT_DIR/scripts/grafana-system-smoke.sh"
if grep -Fq 'DIGEST_VALIDATION_REQUIRED' "$ROOT_DIR/tests/compat/grafana/README.md"; then
  echo 'Grafana README still contains the retired digest placeholder' >&2
  exit 1
fi

tempo_fixture="$(mktemp "${TMPDIR:-/tmp}/grafana-tempo-contract.XXXXXX.json")"
trap 'rm -f "$tempo_fixture"' EXIT
printf '%s\n' '{"batches":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"tenant-a"}}]},"scopeSpans":[{"scope":{"name":"grafana-seeder"},"spans":[{"traceId":"qqqqqqqqqqqqqqqqqqqqqg==","spanId":"ERINVALID","startTimeUnixNano":"1700000010000000000","endTimeUnixNano":"1700000011000000000","status":{"code":42},"events":[{"name":"checkout","timeUnixNano":"1700000010500000000"}],"links":[{"traceId":"qqqqqqqqqqqqqqqqqqqqqg==","spanId":"AQEBAQEBAQE="}]}]}]}]}' > "$tempo_fixture"
if bash -c 'source "$1"; validate_tempo_trace_response "$2" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa tenant-a tenant-b' _ "$ROOT_DIR/scripts/grafana-system-smoke.sh" "$tempo_fixture"; then
  echo 'Tempo response contract did not reject an invalid span ID/status enum' >&2
  exit 1
fi

echo 'Grafana CI compose contract: PASS'
