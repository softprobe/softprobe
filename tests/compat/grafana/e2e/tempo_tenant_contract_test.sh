#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
HARNESS="$ROOT_DIR/scripts/grafana-system-smoke.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/grafana-tempo-tenant-contract.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

SOURCE_TRACE_ID="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
WIRE_TRACE_ID="$(bash -c 'source "$1"; normalize_tempo_trace_id "$2"' _ "$HARNESS" "$SOURCE_TRACE_ID")"

[[ "$WIRE_TRACE_ID" != "$SOURCE_TRACE_ID" ]] || {
  echo 'Tempo contract did not establish a distinct canonical wire trace ID' >&2
  exit 1
}

tempo_response="$TMP_DIR/tempo.json"
cat >"$tempo_response" <<JSON
{
  "traceID": "$WIRE_TRACE_ID",
  "batches": [{
    "resource": {"attributes": [{"key": "tenant.marker", "value": {"stringValue": "grafana-phase4-tenant-a"}}]},
    "scopeSpans": [{
      "scope": {"name": "grafana-seeder"},
      "spans": [{
        "traceId": "$WIRE_TRACE_ID",
        "spanId": "AAAAAAAAAAE=",
        "parentSpanId": "AAAAAAAAAAA=",
        "startTimeUnixNano": "1700000010000000000",
        "endTimeUnixNano": "1700000011000000000",
        "status": {"code": "STATUS_CODE_OK"},
        "events": [{"name": "checkout.started", "timeUnixNano": "1700000010500000000"}],
        "links": [{"traceId": "$WIRE_TRACE_ID", "spanId": "AAAAAAAAAAI="}]
      }]
    }]
  }, {
      "resource": {"attributes": [{"key": "tenant.marker", "value": {"stringValue": "grafana-phase4-tenant-a"}}]},
      "scopeSpans": [{
        "scope": {"name": "grafana-secondary", "version": "1.0.0"},
        "spans": [{
          "traceId": "$WIRE_TRACE_ID",
          "spanId": "AAAAAAAAAAM=",
          "startTimeUnixNano": "1700000011000000000",
          "endTimeUnixNano": "1700000012000000000",
          "status": {"code": "STATUS_CODE_UNSET"}
        }]
      }]
    }
  ]
}
JSON

if ! bash -c 'source "$1"; validate_tempo_trace_response "$2" "$3" "$4" "$5" "$6"' _ "$HARNESS" "$tempo_response" "$SOURCE_TRACE_ID" grafana-phase4-tenant-a grafana-phase4-tenant-b "$WIRE_TRACE_ID"; then
  echo 'Tempo validator did not normalize the raw search trace ID to canonical wire Base64' >&2
  exit 1
fi

# The Grafana harness must require the seeded rich topology: distinct
# ResourceSpans/ScopeSpans groups plus parent, events, links, and wire status.
one_span_response="$TMP_DIR/tempo-one-span.json"
cat >"$one_span_response" <<JSON
{
  "traceID": "$WIRE_TRACE_ID",
  "batches": [{
    "resource": {"attributes": [{"key": "tenant.marker", "value": {"stringValue": "grafana-phase4-tenant-a"}}]},
    "scopeSpans": [{
      "scope": {"name": "grafana-seeder", "version": "1.0.0"},
      "spans": [{
        "traceId": "$WIRE_TRACE_ID",
        "spanId": "AAAAAAAAAAE=",
        "name": "checkout",
        "startTimeUnixNano": "1700000010000000000",
        "endTimeUnixNano": "1700000011000000000",
        "status": {"code": "STATUS_CODE_OK"}
      }]
    }]
  }]
}
JSON
if ! GRAFANA_RICH_TEMPO_ASSERTIONS=0 bash -c 'source "$1"; validate_tempo_trace_response "$2" "$3" "$4" "$5" "$6"' _ "$HARNESS" "$one_span_response" "$SOURCE_TRACE_ID" grafana-phase4-tenant-a grafana-phase4-tenant-b "$WIRE_TRACE_ID"; then
  echo 'Tempo validator accepted a one-span response without rich topology' >&2
  exit 1
fi

prometheus_response="$TMP_DIR/prometheus.json"
printf '%s\n' '{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"checkout"},"value":[1700000030,"1"]}]}}' >"$prometheus_response"
if bash -c 'source "$1"; validate_signal_response "$2" prometheus grafana-phase4-tenant-a grafana-phase4-tenant-b softprobe-prom-a' _ "$HARNESS" "$prometheus_response"; then
  echo 'Prometheus validator accepted an ambiguous tenant response' >&2
  exit 1
fi

explore_response="$TMP_DIR/explore.json"
printf '%s\n' '{"results":{"A":{"refId":"A","frames":[{"schema":{"name":"data","fields":[{"name":"value"}]},"data":{"values":[[1]]}}]}}}' >"$explore_response"
if bash -c 'source "$1"; validate_explore_response "$2" grafana-phase4-tenant-a grafana-phase4-tenant-b softprobe-prom-a prometheus' _ "$HARNESS" "$explore_response"; then
  echo 'Explore validator accepted an ambiguous tenant response' >&2
  exit 1
fi

mock_tempo="$TMP_DIR/mock-tempo.json"
bash -c 'source "$1"; mock_signal_response tempo softprobe-tempo-a grafana-phase4-tenant-a' _ "$HARNESS" >"$mock_tempo"
python3 - "$mock_tempo" "$SOURCE_TRACE_ID" "$WIRE_TRACE_ID" <<'PY'
import base64
import json
import pathlib
import sys

trace_id = json.loads(pathlib.Path(sys.argv[1]).read_text())["traces"][0]["traceID"]
source_trace_id, wire_trace_id = sys.argv[2:]
if trace_id != wire_trace_id:
    raise SystemExit(
        "mock Tempo search did not return the canonical wire form of the "
        f"source trace ID: {trace_id!r}"
    )
try:
    decoded = base64.b64decode(trace_id, validate=True)
except Exception as exc:
    raise SystemExit(f"mock Tempo search returned malformed wire Base64: {trace_id!r}") from exc
if decoded.hex() != source_trace_id or base64.b64encode(decoded).decode("ascii") != trace_id:
    raise SystemExit("mock Tempo search did not preserve the canonical source trace ID")
PY

extracted_trace_id="$(bash -c 'source "$1"; extract_tempo_search_id "$2"' _ "$HARNESS" "$mock_tempo")"
normalized_trace_id="$(bash -c 'source "$1"; normalize_tempo_trace_id "$2"' _ "$HARNESS" "$extracted_trace_id")"
[[ "$normalized_trace_id" == "$WIRE_TRACE_ID" ]] || {
  echo 'Tempo trace lookup did not normalize the canonical search ID to wire Base64' >&2
  exit 1
}

echo 'Grafana Tempo/tenant-positive contract: PASS'
