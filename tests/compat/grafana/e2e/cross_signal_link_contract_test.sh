#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
HARNESS="$ROOT_DIR/scripts/grafana-system-smoke.sh"
TEMPO_DATASOURCE="$ROOT_DIR/tests/compat/grafana/provisioning/datasources/tempo.yaml"
ARTIFACT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/grafana-cross-signal-contract.XXXXXX")"
trap 'rm -rf "$ARTIFACT_DIR"' EXIT

# Source the guarded harness as a library.  This exercises the production
# expansion helper without entering main(), whose G1-G8 lane is intentionally
# broader than this focused G7 contract.
export ARTIFACT_DIR MOCK=1 GRAFANA_TEMPO_DATASOURCE="$TEMPO_DATASOURCE"
# shellcheck disable=SC1090
source "$HARNESS"

source_trace_id="qqqqqqqqqqqqqqqqqqqqqg=="
bad_loki="$ARTIFACT_DIR/bad-loki.json"
good_loki="$ARTIFACT_DIR/good-loki.json"
python3 - "$bad_loki" "$good_loki" "$source_trace_id" <<'PY'
import json
import pathlib
import sys

bad, good, trace_id = sys.argv[1:]
base = {"status": "success", "data": {"resultType": "streams", "result": []}}
bad_obj = json.loads(json.dumps(base))
bad_obj["data"]["result"] = [{"stream": {"trace_id": trace_id}, "values": [["1", "{\"message\":\"no trace here\"}"]]}]
good_obj = json.loads(json.dumps(base))
good_obj["data"]["result"] = [{"stream": {"service_name": "checkout"}, "values": [["1", json.dumps({"trace_id": trace_id})]]}]
pathlib.Path(bad).write_text(json.dumps(bad_obj))
pathlib.Path(good).write_text(json.dumps(good_obj))
PY
if validate_loki_trace_link "$bad_loki" "$source_trace_id" "$source_trace_id"; then
  echo 'Loki trace validator accepted an ID outside data.result[*].values' >&2
  exit 1
fi
validate_loki_trace_link "$good_loki" "$source_trace_id" "$source_trace_id" || {
  echo 'Loki trace validator rejected a trace ID in a stream value' >&2
  exit 1
}
endpoints="$ARTIFACT_DIR/endpoints.txt"
: > "$endpoints"
for tenant in a b; do
  expand_tempo_trace_to_logs "$tenant" "$source_trace_id" >> "$endpoints"
done

python3 - "$endpoints" "$TEMPO_DATASOURCE" "$source_trace_id" <<'PY'
import pathlib
import re
import sys
from urllib.parse import parse_qs, unquote, urlsplit

endpoints_path = pathlib.Path(sys.argv[1])
tempo_yaml = pathlib.Path(sys.argv[2]).read_text()
source_trace_id = sys.argv[3]
endpoints = [line.strip() for line in endpoints_path.read_text().splitlines() if line.strip()]
if len(endpoints) != 2:
    raise SystemExit(f"expected one generated request per tenant, got {len(endpoints)}")
start_match = re.search(r"spanStartTimeShift:\s*(-?\d+)m", tempo_yaml)
end_match = re.search(r"spanEndTimeShift:\s*\+?(\d+)m", tempo_yaml)
if not start_match or not end_match:
    raise SystemExit("Tempo datasource is missing explicit trace-to-log time shifts")
trace_start_s = 1_700_000_000
trace_end_s = 1_700_000_060
expected_start = str((trace_start_s + int(start_match.group(1)) * 60) * 1_000_000_000)
expected_end = str((trace_end_s + int(end_match.group(1)) * 60) * 1_000_000_000)
for endpoint in endpoints:
    query = parse_qs(urlsplit(unquote(endpoint)).query)
    trace_query = query.get("query", [""])[0]
    if source_trace_id not in trace_query:
        raise SystemExit("G7 Tempo-to-Loki expansion omitted the canonical padded Base64 trace ID")
    if query.get("start") != [expected_start] or query.get("end") != [expected_end]:
        raise SystemExit(
            "G7 Tempo-to-Loki expansion did not preserve shifted bounds: "
            f"expected {expected_start}..{expected_end}, got "
            f"{query.get('start')}..{query.get('end')}"
        )
PY

printf 'Grafana G7 generated trace-to-log expansion contract: PASS\n'
