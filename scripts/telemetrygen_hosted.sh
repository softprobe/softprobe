#!/usr/bin/env bash
# Send OTLP traces, metrics, and logs to a hosted softprobe-runtime (or any OTLP/HTTP
# endpoint) using opentelemetry-collector-contrib's telemetrygen.
#
# Requires: telemetrygen on PATH, e.g.
#   go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest
# Or run the same image interactively and invoke telemetrygen inside:
#   docker run --rm -e SOFTPROBE_TOKEN -e OTLP_ENDPOINT ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest traces --help
#
# Authentication: hosted runtimes expect Authorization: Bearer <api key>.
#   export SOFTPROBE_TOKEN="your-api-key"
#   Optional: source repo-root .env (this script loads ../../.env if present).
#
# Environment:
#   SOFTPROBE_TOKEN or SOFTPROBE_API_KEY   API key for Bearer auth (required)
#   OTLP_ENDPOINT     host:port for OTLP HTTP/TLS (default: runtime.softprobe.dev:443)
#   TELEMETRYGEN_CMD  override binary (default: telemetrygen)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

# Accept either name (repo .env often uses SOFTPROBE_API_KEY).
SOFTPROBE_TOKEN="${SOFTPROBE_TOKEN:-${SOFTPROBE_API_KEY:-}}"

OTLP_ENDPOINT="${OTLP_ENDPOINT:-runtime.softprobe.dev:443}"
TELEMETRYGEN_CMD="${TELEMETRYGEN_CMD:-telemetrygen}"

usage() {
  cat <<'EOF'
Usage: telemetrygen_hosted.sh <traces|metrics|logs|all> [extra telemetrygen args...]

Examples:
  SOFTPROBE_TOKEN="$KEY" ./telemetrygen_hosted.sh traces --traces 5
  # Or rely on SOFTPROBE_API_KEY from repo-root .env after sourcing:
  ./telemetrygen_hosted.sh all
  OTLP_ENDPOINT=localhost:8090 SOFTPROBE_TOKEN="$KEY" ./telemetrygen_hosted.sh all

Subcommands:
  traces   OTLP HTTP traces -> /v1/traces
  metrics  OTLP HTTP metrics -> /v1/metrics
  logs     OTLP HTTP logs    -> /v1/logs
  all      run traces, then metrics, then logs (small batches)
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

SUB="${1:-}"
shift || true

if [[ -z "${SUB}" ]]; then
  usage >&2
  exit 1
fi

if [[ -z "${SOFTPROBE_TOKEN:-}" ]]; then
  echo "error: set SOFTPROBE_TOKEN (API key for Bearer auth)" >&2
  exit 1
fi

# telemetrygen expects --otlp-header in the form key="value" (see telemetrygen traces --help).
AUTH_HEADER="Authorization=\"Bearer ${SOFTPROBE_TOKEN}\""

# Subcommand (traces|metrics|logs) must be the first argument after the binary (cobra).
otlp_flags=(
  --otlp-http
  --otlp-endpoint "${OTLP_ENDPOINT}"
  --otlp-header "${AUTH_HEADER}"
)

run_traces() {
  "${TELEMETRYGEN_CMD}" traces "${otlp_flags[@]}" --otlp-http-url-path /v1/traces "$@"
}

run_metrics() {
  "${TELEMETRYGEN_CMD}" metrics "${otlp_flags[@]}" --otlp-http-url-path /v1/metrics "$@"
}

run_logs() {
  "${TELEMETRYGEN_CMD}" logs "${otlp_flags[@]}" --otlp-http-url-path /v1/logs "$@"
}

case "${SUB}" in
  traces)
    run_traces "$@"
    ;;
  metrics)
    run_metrics "$@"
    ;;
  logs)
    run_logs "$@"
    ;;
  all)
    run_traces --traces 300 --workers 10 "$@"
    run_metrics --metrics 300 --workers 10 "$@"
    run_logs --logs 300 --workers 10 "$@"
    ;;
  *)
    echo "error: unknown subcommand: ${SUB}" >&2
    usage >&2
    exit 1
    ;;
esac
