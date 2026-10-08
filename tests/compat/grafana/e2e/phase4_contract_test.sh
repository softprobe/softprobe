#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
HARNESS="$ROOT_DIR/scripts/grafana-system-smoke.sh"
PROVISIONING="$ROOT_DIR/tests/compat/grafana/provisioning/datasources"
DASHBOARDS="$ROOT_DIR/tests/compat/grafana/dashboards"

grep -Fq 'run_static_contracts' "$HARNESS" || {
  echo 'Grafana CI smoke entrypoint does not enforce the static Phase 4 contracts' >&2
  exit 1
}
for contract in compose_contract_test.sh phase4_contract_test.sh tempo_tenant_contract_test.sh; do
  grep -Fq "$contract" "$HARNESS" || {
    echo "Grafana CI smoke entrypoint does not enforce $contract" >&2
    exit 1
  }
done
[[ -f "$ROOT_DIR/tests/compat/grafana/e2e/artifact_redaction_test.sh" ]] || {
  echo 'missing standalone Grafana artifact-redaction regression' >&2
  exit 1
}

# Keep this contract tied to the checked-in provisioning layout.  A previous
# version looked one directory too high and therefore passed without checking
# the Loki and Tempo datasource contracts at all.
[[ -d "$PROVISIONING" ]] || {
  echo "Grafana datasource provisioning directory is missing: $PROVISIONING" >&2
  exit 1
}
for datasource in prometheus loki tempo; do
  [[ -f "$PROVISIONING/$datasource.yaml" ]] || {
    echo "missing Grafana datasource provisioning: $PROVISIONING/$datasource.yaml" >&2
    exit 1
  }
done
for contract_file in "$ROOT_DIR/tests/compat/grafana/e2e"/*.sh; do
  # This file contains the stale-path pattern as the assertion itself; inspect
  # the other static contracts so the regression check cannot self-match.
  [[ "$contract_file" == "$ROOT_DIR/tests/compat/grafana/e2e/phase4_contract_test.sh" ]] && continue
  if grep -Eq 'provisioning/(prometheus|loki|tempo)\.yaml' "$contract_file"; then
    echo "Grafana contract uses the stale datasource provisioning path: $contract_file" >&2
    exit 1
  fi
done

for symbol in \
  validate_grafana_reference_pin \
  validate_dashboard_round_trip \
  check_dashboard_variables \
  check_dashboard_panels \
  validate_explore_response \
  validate_cross_signal_links \
  check_cross_signal \
  check_errors \
  redact; do
  grep -Eq "^${symbol}[[:space:]]*\(\)" "$HARNESS" || {
    echo "Grafana harness is missing required assertion: $symbol" >&2
    exit 1
  }
done

for case_id in G1 G2 G3 G7 G8; do
  grep -Fq "record_case $case_id" "$HARNESS" || {
    echo "Grafana harness does not record $case_id" >&2
    exit 1
  }
done
grep -Fq 'run_signal_case G4 prometheus' "$HARNESS"
grep -Fq 'run_signal_case G5 loki' "$HARNESS"
grep -Fq 'run_signal_case G6 tempo' "$HARNESS"

grep -Fq 'GRAFANA_CHECK_DASHBOARD_QUERIES=1' "$HARNESS"
grep -Fq 'GRAFANA_MOCK_PANEL_LIMIT' "$HARNESS"
grep -Fq 'GRAFANA_MOCK_PANEL_LIMIT=1' "$ROOT_DIR/tests/compat/grafana/e2e/artifact_redaction_test.sh"
grep -Fq 'collapsed distinct ResourceSpans groups' "$HARNESS"
grep -Fq 'collapsed distinct spans/groups' "$HARNESS"
grep -Fq 'GRAFANA_REFERENCE_DIGEST' "$HARNESS"
grep -Fq 'GRAFANA_REFERENCE_MANIFEST' "$HARNESS"
grep -Fq 'docker image inspect' "$HARNESS"
grep -Fq 'sha256:' "$HARNESS"
grep -Fq 'base64.b64decode' "$HARNESS"
grep -Fq 'base64.b64encode' "$HARNESS"
grep -Fq 'malformed/noncanonical trace ID' "$HARNESS"
grep -Fq 'validate_repeat_response' "$HARNESS"
grep -Fq 'validate_explore_response' "$HARNESS"
grep -Fq 'validate_signal_response' "$HARNESS"
grep -Fq 'validate_error_response' "$HARNESS"
grep -Fq 'credential_probe' "$HARNESS"
grep -Fq 'missing_credentials' "$HARNESS"
grep -Fq 'invalid_credentials' "$HARNESS"
grep -Fq 'mismatched_tenant' "$HARNESS"
grep -Fq '__missing__' "$HARNESS"
grep -Fq 'Authorization' "$HARNESS"
grep -Fq 'direct_softprobe_credential_probe' "$HARNESS"
grep -Fq 'SOFTPROBE_DIRECT_URL' "$HARNESS"
grep -Fq '"errorSource"' "$HARNESS"
grep -Fq '"softprobe"' "$HARNESS"
grep -Fq 'write_case_bundle' "$HARNESS"
grep -Fq '| redact' "$HARNESS"
grep -Fq 'X-Scope-OrgID' "$HARNESS"
grep -Fq 'SOFTPROBE_TENANT_A_API_KEY' "$HARNESS"
grep -Fq 'SOFTPROBE_TENANT_B_API_KEY' "$HARNESS"

# The redaction regression launches the smoke harness itself.  It must remain
# a standalone test rather than being called by the harness's own static-test
# loop, otherwise MOCK execution recurses indefinitely.
if sed -n '/^  for contract in \\/,/^  done$/p' "$HARNESS" | grep -Fq 'artifact_redaction_test.sh'; then
  echo 'Grafana smoke statics must not recursively invoke artifact_redaction_test.sh' >&2
  exit 1
fi
DATASOURCES="$PROVISIONING"
grep -Fq 'X-Scope-OrgID' "$DATASOURCES/prometheus.yaml"
grep -Fq 'X-Scope-OrgID' "$DATASOURCES/loki.yaml"
grep -Fq 'X-Scope-OrgID' "$DATASOURCES/tempo.yaml"
grep -Fq 'tracesToLogsV2' "$DATASOURCES/tempo.yaml"
grep -Fq 'derivedFields' "$DATASOURCES/loki.yaml"

python3 - "$DASHBOARDS" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
dashboards = {}
for path in sorted(root.rglob("*.json")):
    document = json.loads(path.read_text())
    dashboard = document.get("dashboard", document)
    uid = dashboard.get("uid")
    if not uid:
        raise SystemExit(f"dashboard has no stable uid: {path}")
    if uid in dashboards:
        raise SystemExit(f"duplicate dashboard uid {uid}")
    dashboards[uid] = dashboard

required = {
    "softprobe-prom-smoke",
    "softprobe-loki-smoke",
    "softprobe-tempo-smoke",
    "softprobe-cross-signal",
}
missing = required - dashboards.keys()
if missing:
    raise SystemExit(f"missing representative dashboards: {sorted(missing)}")

cross = dashboards["softprobe-cross-signal"]
variables = {item.get("name") for item in cross.get("templating", {}).get("list", [])}
if not {"job", "service"} <= variables:
    raise SystemExit(f"cross-signal dashboard variables are incomplete: {variables}")

text = json.dumps(cross, sort_keys=True)
for uid in ("softprobe-prom-a", "softprobe-loki-a", "softprobe-tempo-a"):
    if uid not in text:
        raise SystemExit(f"cross-signal dashboard is missing datasource {uid}")

def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)

for uid in required:
    panels = [item for item in walk(dashboards[uid]) if "targets" in item and item.get("type")]
    if not panels:
        raise SystemExit(f"dashboard {uid} has no query panels")
PY

# Round-trip validation must reject a changed panel/target structure while
# tolerating Grafana's response metadata.
round_trip_tmp="$(mktemp -d "${TMPDIR:-/tmp}/grafana-round-trip-contract.XXXXXX")"
trap 'rm -rf "$round_trip_tmp"' EXIT
# "-" reads the validation script from stdin; argv[1] is the dashboard fixture.
python3 - "$DASHBOARDS/smoke/softprobe-cross-signal.json" "$round_trip_tmp" <<'PY'
import copy
import json
import pathlib
import sys

fixture = json.loads(pathlib.Path(sys.argv[1]).read_text())
dashboard = copy.deepcopy(fixture)
dashboard["id"] = 99123
dashboard["version"] = int(dashboard.get("version", 0)) + 7
dashboard["meta_from_grafana"] = {"folderId": 42}
good = {"meta": {"folderTitle": "Softprobe", "folderUid": "softprobe-folder"}, "dashboard": dashboard}
bad = copy.deepcopy(good)
bad["dashboard"]["panels"][0]["targets"][0]["expr"] = "tampered_panel_target"
root = pathlib.Path(sys.argv[2])
(root / "good.json").write_text(json.dumps(good))
(root / "bad.json").write_text(json.dumps(bad))
PY
export ARTIFACT_DIR="$round_trip_tmp" MOCK=1 GRAFANA_DASHBOARD_DIR="$DASHBOARDS"
# shellcheck disable=SC1090
source "$HARNESS"
validate_dashboard_round_trip "$round_trip_tmp/good.json" softprobe-cross-signal || {
  echo 'dashboard round-trip validator rejected tolerated Grafana metadata' >&2
  exit 1
}
if validate_dashboard_round_trip "$round_trip_tmp/bad.json" softprobe-cross-signal; then
  echo 'dashboard round-trip validator accepted a mutated panel target' >&2
  exit 1
fi

echo 'Grafana Phase 4 static contract: PASS'
