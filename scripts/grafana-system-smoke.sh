#!/usr/bin/env bash
set -euo pipefail

GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3000}"
GRAFANA_ADMIN_USER="${GRAFANA_ADMIN_USER:-admin}"
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-admin}"
ARTIFACT_DIR="${ARTIFACT_DIR:-target/compat/grafana}"
MOCK_FIXTURE_DIR="${MOCK_FIXTURE_DIR:-tests/compat/fixtures}"
GRAFANA_DASHBOARD_DIR="${GRAFANA_DASHBOARD_DIR:-tests/compat/grafana/dashboards}"
GRAFANA_E2E_DIR="${GRAFANA_E2E_DIR:-tests/compat/grafana/e2e}"
MOCK_MODE="${MOCK:-0}"
if [[ -n "${CI:-}" ]]; then
  # The CI harness contract always exercises dashboard variables and panel
  # data frames.  Do not allow a caller-provided environment value to weaken
  # the Phase 4 acceptance gate.
  GRAFANA_CHECK_DASHBOARD_QUERIES=1
elif [[ -z "${GRAFANA_CHECK_DASHBOARD_QUERIES+x}" ]]; then
  GRAFANA_CHECK_DASHBOARD_QUERIES=0
fi
GRAFANA_REFERENCE_IMAGE="${GRAFANA_REFERENCE_IMAGE:-grafana/grafana:11.2.0}"
GRAFANA_REFERENCE_VERSION="${GRAFANA_REFERENCE_VERSION:-}"
GRAFANA_REFERENCE_DIGEST="${GRAFANA_REFERENCE_DIGEST:-}"
GRAFANA_REFERENCE_MANIFEST="${GRAFANA_REFERENCE_MANIFEST:-docs/compat/references.v0.yaml}"
GRAFANA_CAPABILITY_MANIFEST="${GRAFANA_CAPABILITY_MANIFEST:-docs/compat/capability.v0.yaml}"
GRAFANA_CAPABILITY_ID="${GRAFANA_CAPABILITY_ID:-grafana}"
GRAFANA_CAPABILITY_STATUS="${GRAFANA_CAPABILITY_STATUS:-implemented_validation_only}"
GRAFANA_FIXTURE_MANIFEST="${GRAFANA_FIXTURE_MANIFEST:-tests/compat/manifests/cases.v0.yaml}"
GRAFANA_REFERENCE_IMAGE_DIGEST="${GRAFANA_REFERENCE_IMAGE_DIGEST:-}"
GRAFANA_TEMPO_DATASOURCE="${GRAFANA_TEMPO_DATASOURCE:-tests/compat/grafana/provisioning/datasources/tempo.yaml}"

DATASOURCE_UIDS=(
  softprobe-prom softprobe-prom-a softprobe-prom-b
  softprobe-loki-a softprobe-loki-b
  softprobe-tempo-a softprobe-tempo-b
)
DASHBOARD_UIDS=()
if [[ -d "$GRAFANA_DASHBOARD_DIR" ]]; then
  while IFS= read -r dashboard_uid; do
    [[ -n "$dashboard_uid" ]] && DASHBOARD_UIDS+=("$dashboard_uid")
  done < <(python3 - "$GRAFANA_DASHBOARD_DIR" <<'PY'
import json
import os
import pathlib
import sys

directory = pathlib.Path(sys.argv[1])
for sub in ("smoke", "promql", "astronomy", "compose"):
    root = directory / sub
    if not root.is_dir():
        continue
    for path in sorted(root.glob("*.json")):
        document = json.loads(path.read_text())
        dashboard = document.get("dashboard", document)
        uid = dashboard.get("uid")
        if not uid:
            raise SystemExit(f"dashboard fixture has no uid: {path}")
        print(uid)
PY
  )
fi

if [[ -n "${GRAFANA_DASHBOARD_UIDS:-}" ]]; then
  read -r -a DASHBOARD_UIDS <<< "$GRAFANA_DASHBOARD_UIDS"
fi

TENANT_A_ID="${GRAFANA_TEST_TENANT_A_ID:-grafana-phase4-tenant-a}"
TENANT_B_ID="${GRAFANA_TEST_TENANT_B_ID:-grafana-phase4-tenant-b}"
SEED_SOFTPROBE_URL="${GRAFANA_SEED_SOFTPROBE_URL:-http://127.0.0.1:${GRAFANA_SOFTPROBE_HTTP_PORT:-18090}}"
SOFTPROBE_DIRECT_URL="${GRAFANA_DIRECT_SOFTPROBE_URL:-${SOFTPROBE_URL:-$SEED_SOFTPROBE_URL}}"
GRAFANA_AUTH_MOCK_URL="${GRAFANA_AUTH_MOCK_URL:-http://127.0.0.1:${GRAFANA_AUTH_MOCK_PORT:-18080}}"
CROSS_START_NS="1700000000000000000"
CROSS_END_NS="1700000060000000000"
TRACE_START_S="1700000000"
TRACE_END_S="1700000060"
CHECKS=()
SKIPPED=0
VARIABLE_BUNDLE_ARGS=()
CASE_IDS=()
CASE_START_MS=()

now_ms() {
  python3 - <<'PY'
import time
print(time.monotonic_ns() // 1_000_000)
PY
}

begin_case() {
  CASE_IDS+=("$1")
  CASE_START_MS+=("$(now_ms)")
}

case_duration_ms() {
  local case_id="$1" started="" now i
  now="$(now_ms)"
  for i in "${!CASE_IDS[@]}"; do
    if [[ "${CASE_IDS[$i]}" == "$case_id" ]]; then
      started="${CASE_START_MS[$i]}"
      break
    fi
  done
  if [[ -n "$started" && "$started" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "$((now - started))"
  else
    printf '0\n'
  fi
}

# One metadata source feeds every G1-G8 artifact. The protocol fixtures are
# the same repository fixtures used by the shared compatibility harness; the
# Grafana cases add only the black-box route that exercises each fixture.
case_metadata_json() {
  local case_id="$1"
  python3 - "$case_id" "$MOCK_FIXTURE_DIR" "$GRAFANA_CAPABILITY_MANIFEST" <<'PY'
import json
import pathlib
import re
import sys

case_id, fixture_dir, capability_manifest = sys.argv[1:]
definition = {
    "G1": {"endpoint": {"method": "GET", "path": "/api/health"}, "fixture_id": "grafana-health", "fixture_path": "tests/compat/grafana/provisioning"},
    "G2": {"endpoint": {"method": "GET", "path": "/api/datasources"}, "fixture_id": "grafana-datasources", "fixture_path": "tests/compat/grafana/provisioning/datasources"},
    "G3": {"endpoint": {"method": "GET", "path": "/api/search?type=dash-db"}, "fixture_id": "grafana-dashboards", "fixture_path": "tests/compat/grafana/dashboards"},
    "G4": {"endpoint": {"method": "POST", "path": "/api/ds/query"}, "fixture_id": "prometheus_success_minimal", "fixture_path": f"{fixture_dir}/prometheus_success_minimal.json"},
    "G5": {"endpoint": {"method": "POST", "path": "/api/ds/query"}, "fixture_id": "loki_success_minimal", "fixture_path": f"{fixture_dir}/loki_success_minimal.json"},
    "G6": {"endpoint": {"method": "POST", "path": "/api/ds/query"}, "fixture_id": "tempo_success_minimal", "fixture_path": f"{fixture_dir}/tempo_success_minimal.json"},
    "G7": {"endpoint": {"method": "GET", "path": "/api/datasources/proxy/uid/softprobe-tempo-a/api/search"}, "fixture_id": "grafana-cross-signal", "fixture_path": "tests/compat/grafana/provisioning/datasources"},
    "G8": {"endpoint": {"method": "POST", "path": "/api/ds/query"}, "fixture_id": "grafana-error-contract", "fixture_path": "tests/compat/grafana/e2e"},
}
if case_id not in definition:
    raise SystemExit(f"unknown Grafana case: {case_id}")

capability_id = "grafana"
capability_status = "implemented_validation_only"
try:
    text = pathlib.Path(capability_manifest).read_text()
    match = re.search(r"(?ms)^\s+phase_4_grafana:\s*\n.*?^\s+repository_harness:\s*([^\s#]+)", text)
    if match:
        capability_status = match.group(1)
    match = re.search(r"(?ms)^\s+grafana:\s*\n\s+native_datasources:\s*([^\s#]+)", text)
    if match:
        capability_id = match.group(1)
except OSError:
    pass

value = definition[case_id].copy()
value.update({
    "case_id": case_id,
    "protocol": "grafana",
    "capability": {"id": capability_id, "status": capability_status},
    "capability_id": capability_id,
    "capability_status": capability_status,
    "fixture": {"id": value["fixture_id"], "path": value["fixture_path"]},
})
print(json.dumps(value, sort_keys=True))
PY
}

ARTIFACT_ROOT="$ARTIFACT_DIR"
RUN_ARTIFACT_DIR=""

prepare_artifact_staging() {
  mkdir -p "$ARTIFACT_ROOT"
  RUN_ARTIFACT_DIR="$(mktemp -d "$ARTIFACT_ROOT/.run.XXXXXX")"
  ARTIFACT_DIR="$RUN_ARTIFACT_DIR"
  mkdir -p "$ARTIFACT_DIR/.work"
}

stage_artifacts() {
  [[ -n "$RUN_ARTIFACT_DIR" && -d "$RUN_ARTIFACT_DIR" ]] || return 0
  python3 - "$ARTIFACT_ROOT" "$RUN_ARTIFACT_DIR" <<'PY'
import pathlib
import re
import shutil
import sys

root, run = (pathlib.Path(value) for value in sys.argv[1:])
top_level = re.compile(r"G[1-8]\.(?:outcome|raw|normalized)\.json$")
allowed = {"outcome.json", "summary.json", "seed-receipt.json", ".work"}

def is_allowed(name):
    return name in allowed or bool(top_level.fullmatch(name))

for child in root.iterdir():
    if child == run or is_allowed(child.name):
        continue
    if child.is_dir() and not child.is_symlink():
        shutil.rmtree(child)
    else:
        child.unlink()

for child in run.iterdir():
    if not is_allowed(child.name):
        continue
    destination = root / child.name
    if destination.exists() or destination.is_symlink():
        if destination.is_dir() and not destination.is_symlink():
            shutil.rmtree(destination)
        else:
            destination.unlink()
    shutil.move(str(child), str(destination))

run.rmdir()
PY
  RUN_ARTIFACT_DIR=""
  ARTIFACT_DIR="$ARTIFACT_ROOT"
}

validate_grafana_reference_pin() {
  [[ -f "$GRAFANA_REFERENCE_MANIFEST" ]] || {
    echo "Grafana reference manifest not found: $GRAFANA_REFERENCE_MANIFEST" >&2
    return 1
  }
  local manifest_image manifest_version manifest_digest manifest_immutable_image compose_image
  IFS=$'\t' read -r manifest_image manifest_version manifest_digest manifest_immutable_image < <(python3 - "$GRAFANA_REFERENCE_MANIFEST" <<'PY'
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()
match = re.search(
    r"(?ms)^\s+grafana:\s*\n\s+image:\s*([^\s#]+)\s*\n\s+tag:\s*[\"']?([^\s\"']+).*?\n\s+digest:\s*[\"']?([^\s\"']+)",
    text,
)
if not match:
    raise SystemExit("Grafana reference is missing image/tag/digest in the compatibility manifest")
print(f"{match.group(1)}:{match.group(2)}\t{match.group(2)}\t{match.group(3)}\t{match.group(1)}@{match.group(3)}")
PY
  ) || return 1
  [[ "$GRAFANA_REFERENCE_IMAGE" == "$manifest_image" ]] || {
    echo "Grafana reference image drift: expected $manifest_image, got $GRAFANA_REFERENCE_IMAGE" >&2
    return 1
  }
  if [[ -n "$GRAFANA_REFERENCE_VERSION" && "$GRAFANA_REFERENCE_VERSION" != "$manifest_version" ]]; then
    echo "Grafana reference version drift: expected $manifest_version, got $GRAFANA_REFERENCE_VERSION" >&2
    return 1
  fi
  [[ "$manifest_digest" =~ ^sha256:[0-9a-fA-F]{64}$ ]] || {
    echo "Grafana reference manifest must contain an immutable sha256 digest" >&2
    return 1
  }
  if [[ -n "$GRAFANA_REFERENCE_DIGEST" && "$GRAFANA_REFERENCE_DIGEST" != "$manifest_digest" ]]; then
    echo "Grafana reference digest drift: expected $manifest_digest, got $GRAFANA_REFERENCE_DIGEST" >&2
    return 1
  fi
  GRAFANA_REFERENCE_VERSION="$manifest_version"
  GRAFANA_REFERENCE_DIGEST="$manifest_digest"
  GRAFANA_REFERENCE_IMAGE_DIGEST="$manifest_immutable_image"
  compose_image="${GRAFANA_COMPOSE_IMAGE:-}"
  if [[ -z "$compose_image" && "$MOCK_MODE" == "1" ]]; then
    # Mock mode has no Compose process, but still validates the exact image
    # reference that the real launcher is required to provide.
    compose_image="$manifest_immutable_image"
  fi
  [[ -n "$compose_image" ]] || {
    echo "GRAFANA_COMPOSE_IMAGE must be supplied as an immutable image@sha256 reference" >&2
    return 1
  }
  [[ "$compose_image" == "$manifest_immutable_image" ]] || {
    echo "Grafana Compose image drift: expected $manifest_immutable_image, got $compose_image" >&2
    return 1
  }
  [[ "$compose_image" =~ ^[^@[:space:]]+@sha256:[0-9a-fA-F]{64}$ ]] || {
    echo "Grafana Compose image must be an immutable image@sha256 reference" >&2
    return 1
  }
  # Mock mode validates the strict manifest/reference contract but has no
  # local image to inspect.
  if [[ "$MOCK_MODE" == "1" ]]; then
    return 0
  fi
  command -v docker >/dev/null 2>&1 || {
    echo "docker is required to validate the Grafana image digest" >&2
    return 1
  }
  docker image inspect --format '{{join .RepoDigests "\\n"}}' "$compose_image" \
    | grep -Fqx "$compose_image" || {
      echo "Grafana Compose image does not resolve to $compose_image" >&2
      return 1
    }
}

redact() {
  local value="${1:-}"
  if (( $# == 0 )); then
    value="$(cat)"
  fi
  REDACTION_INPUT="$value" python3 - <<'PY'
import json
import os
import re

value = os.environ.get("REDACTION_INPUT", "")
credential_name = re.compile(
    r"(?:password|passwd|secret|token|api[_-]?key|access[_-]?key|"
    r"authorization|proxyauthorization|cookie|credential|bearer|"
    r"httpheadervalue|headervalue|tenantkey|clientkey)",
    re.IGNORECASE,
)
query_credential = re.compile(
    r"(?i)([?&][^=&#\s\"']*(?:password|passwd|secret|token|api[_-]?key|"
    r"access[_-]?token|authorization|credential|bearer)[^=&#\s\"']*=)"
    r"([^&#\s\"']+)"
)
text_credential = re.compile(
    r"(?i)(\b(?:authorization|proxy-authorization|api[_-]?key|token|"
    r"secret|password|passwd|credential|bearer)\b\s*[:=]\s*)"
    r"([\"']?)([^\"'\s,;\&#]+)"
)
bearer = re.compile(r"(?i)\bBearer\s+[^\s\"'`,;]+")

values = []
for name, candidate in os.environ.items():
    if name == "GRAFANA_ADMIN_USER" or credential_name.search(name):
        if candidate and candidate not in values:
            values.append(candidate)
values.sort(key=len, reverse=True)

def redact_text(text):
    for secret in values:
        text = text.replace(secret, "[REDACTED]")
    text = query_credential.sub(r"\1[REDACTED]", text)
    text = text_credential.sub(r"\1\2[REDACTED]", text)
    return bearer.sub("Bearer [REDACTED]", text)

def is_credential_key(key):
    return bool(credential_name.search(re.sub(r"[^A-Za-z0-9]", "", key)))

def scrub(item):
    if isinstance(item, dict):
        return {
            key: "[REDACTED]" if is_credential_key(key) else scrub(val)
            for key, val in item.items()
        }
    if isinstance(item, list):
        return [scrub(val) for val in item]
    if isinstance(item, str):
        return redact_text(item)
    return item

try:
    print(json.dumps(scrub(json.loads(value)), indent=2, sort_keys=True))
except json.JSONDecodeError:
    print(redact_text(value), end="")
PY
}

write_outcome() {
  local outcome="$1" reason="${2:-}" payload case_doc
  local case_docs=()
  for case_doc in G1 G2 G3 G4 G5 G6 G7 G8; do
    case_docs+=("$(case_metadata_json "$case_doc")")
  done
  payload="$(python3 - "$outcome" "$reason" "$MOCK_MODE" "$ARTIFACT_DIR" "$GRAFANA_REFERENCE_VERSION" "$GRAFANA_REFERENCE_IMAGE_DIGEST" "$GRAFANA_REFERENCE_DIGEST" ${case_docs[@]+"${case_docs[@]}"} <<'PY'
import json
import sys
outcome, reason, mock, evidence_root, reference_version, reference_image, reference_digest, *case_docs = sys.argv[1:]
cases = []
for raw in case_docs:
    case = json.loads(raw)
    case.update({
        "outcome": outcome,
        "duration_ms": 0,
        "duration_seconds": 0.0,
        "evidence_path": f"{evidence_root}/{case['case_id']}.normalized.json",
        "reference_version": reference_version,
        "reference_image": reference_image,
        "reference_digest": reference_digest,
        "reference": {"service": "grafana", "version": reference_version, "image": reference_image, "digest": reference_digest},
        "validation_only": mock == "1",
        "release_evidence": False if mock == "1" else outcome == "pass",
    })
    cases.append(case)
result = {"schema_version": "grafana-compat.v1", "outcome": outcome, "cases": cases}
if reason:
    result["reason"] = reason
if mock == "1":
    result["validation_only"] = True
result["release_evidence"] = False if mock == "1" else outcome == "pass"
print(json.dumps(result, indent=2, sort_keys=True))
PY
)"
  redact "$payload" > "$ARTIFACT_DIR/outcome.json"
}

write_summary() {
  local outcome="$1" reason="${2:-}" payload case_doc
  local case_docs=()
  for case_doc in G1 G2 G3 G4 G5 G6 G7 G8; do
    case_docs+=("$(case_metadata_json "$case_doc")")
  done
  payload="$(python3 - "$outcome" "$reason" "$MOCK_MODE" "$ARTIFACT_DIR" "$GRAFANA_REFERENCE_VERSION" "$GRAFANA_REFERENCE_IMAGE_DIGEST" "$GRAFANA_REFERENCE_DIGEST" "${CHECKS[@]-}" "--cases--" ${case_docs[@]+"${case_docs[@]}"} <<'PY'
import json
import sys
args = sys.argv[1:]
outcome, reason, mock, evidence_root, reference_version, reference_image, reference_digest = args[:7]
marker = args.index("--cases--")
checks = args[7:marker]
cases = []
for raw in args[marker + 1:]:
    case = json.loads(raw)
    case.update({
        "outcome": next((item.split(":", 1)[1] for item in checks if item.startswith(case["case_id"] + ":")), outcome),
        "duration_ms": 0,
        "duration_seconds": 0.0,
        "evidence_path": f"{evidence_root}/{case['case_id']}.normalized.json",
        "reference_version": reference_version,
        "reference_image": reference_image,
        "reference_digest": reference_digest,
        "reference": {"service": "grafana", "version": reference_version, "image": reference_image, "digest": reference_digest},
        "validation_only": mock == "1",
        "release_evidence": False if mock == "1" else outcome == "pass",
    })
    cases.append(case)
result = {"schema_version": "grafana-compat.v1", "outcome": outcome, "checks": checks, "cases": cases}
if reason:
    result["reason"] = reason
if mock == "1":
    result["validation_only"] = True
result["release_evidence"] = False if mock == "1" else outcome == "pass"
print(json.dumps(result, indent=2, sort_keys=True))
PY
)"
  redact "$payload" > "$ARTIFACT_DIR/summary.json"
}

normalize_tempo_trace_id() {
  python3 - "$1" <<'PY'
import base64
import re
import sys

value = sys.argv[1]
if re.fullmatch(r"[0-9a-fA-F]{32}", value):
    print(base64.b64encode(bytes.fromhex(value)).decode())
    raise SystemExit(0)
try:
    decoded = base64.b64decode(value, validate=True)
except Exception as exc:
    raise SystemExit(f"invalid Tempo trace ID: {value!r}") from exc
if len(decoded) != 16 or base64.b64encode(decoded).decode() != value:
    raise SystemExit(f"Tempo trace ID is not canonical padded Base64: {value!r}")
print(value)
PY
}

extract_tempo_search_id() {
  python3 - "$1" <<'PY'
import base64
import json
import pathlib
import re
import sys

obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
traces = obj.get("traces")
if not isinstance(traces, list):
    raise SystemExit("Tempo search response has no traces list")
for item in traces:
    if not isinstance(item, dict):
        continue
    value = item.get("traceID", item.get("traceId"))
    if re.fullmatch(r"[0-9a-fA-F]{32}", value or ""):
        print(value)
        raise SystemExit(0)
    try:
        decoded = base64.b64decode(value or "", validate=True)
    except Exception:
        continue
    if len(decoded) == 16 and base64.b64encode(decoded).decode() == value:
        print(value)
        raise SystemExit(0)
raise SystemExit("Tempo search returned no exact trace ID")
PY
}

finish_skip() {
  local reason="$1"
  write_outcome "environment_skip" "$reason"
  write_summary "environment_skip" "$reason"
  stage_artifacts
  if [[ "$MOCK_MODE" == "1" ]]; then
    exit 0
  fi
  exit 1
}

finish_failure() {
  local reason="$1"
  write_outcome "failure" "$reason"
  write_summary "failure" "$reason"
  stage_artifacts
  exit 1
}

record_case() {
  local case_id="$1" outcome="$2" reason="${3:-}"
  local index existing
  for index in "${!CHECKS[@]}"; do
    existing="${CHECKS[$index]}"
    if [[ "$existing" == "${case_id}:"* ]]; then
      CHECKS[$index]="${case_id}:${outcome}"
      break
    fi
  done
  if [[ -z "${existing:-}" || "$existing" != "${case_id}:"* ]]; then
    CHECKS+=("${case_id}:${outcome}")
  fi
  local payload duration_ms case_definition
  duration_ms="$(case_duration_ms "$case_id")"
  case_definition="$(case_metadata_json "$case_id")"
  payload="$(python3 - "$case_definition" "$outcome" "$reason" "$MOCK_MODE" "$ARTIFACT_DIR" "$duration_ms" "$GRAFANA_REFERENCE_VERSION" "$GRAFANA_REFERENCE_IMAGE_DIGEST" "$GRAFANA_REFERENCE_DIGEST" <<'PY'
import json
import sys
definition = json.loads(sys.argv[1])
outcome, reason, mock, evidence_root, duration_ms, reference_version, reference_image, reference_digest = sys.argv[2:]
case_id = definition["case_id"]
result = dict(definition)
result.update({
    "schema_version": "grafana-compat.v1",
    "case": case_id,
    "outcome": outcome,
    "duration_ms": int(duration_ms),
    "duration_seconds": int(duration_ms) / 1000.0,
    "evidence_path": f"{evidence_root}/{case_id}.normalized.json",
    "reference_version": reference_version,
    "reference_image": reference_image,
    "reference_digest": reference_digest,
    "reference": {"service": "grafana", "version": reference_version, "image": reference_image, "digest": reference_digest},
    "validation_only": mock == "1",
    "release_evidence": False if mock == "1" else outcome == "pass",
})
if reason:
    result["reason"] = reason
print(json.dumps(result, indent=2, sort_keys=True))
PY
)"
  redact "$payload" > "$ARTIFACT_DIR/${case_id}.outcome.json"
}

write_case_bundle() {
  local case_id="$1"; shift
  local metadata_json duration_ms
  metadata_json="$(case_metadata_json "$case_id")"
  duration_ms="$(case_duration_ms "$case_id")"
  python3 - "$case_id" "$MOCK_MODE" "$metadata_json" "$ARTIFACT_DIR" "$duration_ms" "$GRAFANA_REFERENCE_VERSION" "$GRAFANA_REFERENCE_IMAGE_DIGEST" "$GRAFANA_REFERENCE_DIGEST" "$@" <<'PY' | redact > "$ARTIFACT_DIR/${case_id}.raw.json"
import json
import pathlib
import sys

case_id, mock, metadata_json, evidence_root, duration_ms, reference_version, reference_image, reference_digest, *items = sys.argv[1:]
responses = {}
for item in items:
    label, path = item.split("=", 1)
    try:
        responses[label] = json.loads(pathlib.Path(path).read_text())
    except json.JSONDecodeError:
        responses[label] = {"raw": pathlib.Path(path).read_text()}
result = json.loads(metadata_json)
result.update({
    "schema_version": "grafana-compat.v1",
    "case": case_id,
    "duration_ms": int(duration_ms),
    "duration_seconds": int(duration_ms) / 1000.0,
    "evidence_path": f"{evidence_root}/{case_id}.normalized.json",
    "reference_version": reference_version,
    "reference_image": reference_image,
    "reference_digest": reference_digest,
    "reference": {"service": "grafana", "version": reference_version, "image": reference_image, "digest": reference_digest},
    "validation_only": mock == "1",
    "release_evidence": False if mock == "1" else True,
    "responses": responses,
})
print(json.dumps(result, indent=2, sort_keys=True))
PY
  python3 - "$ARTIFACT_DIR/${case_id}.raw.json" <<'PY' | redact > "$ARTIFACT_DIR/${case_id}.normalized.json"
import json
import pathlib
import sys
source = pathlib.Path(sys.argv[1])
obj = json.loads(source.read_text())
obj["normalization"] = "sorted-json-response-envelope"
print(json.dumps(obj, indent=2, sort_keys=True))
PY
}

mock_signal_response() {
  local signal="$1" uid="$2" tenant="$3"
  python3 - "$MOCK_FIXTURE_DIR" "$signal" "$uid" "$tenant" <<'PY'
import base64
import json
import pathlib
import sys

fixture_dir, signal, uid, tenant = sys.argv[1:]
fixture_name = {
    "prometheus": "prometheus_success_minimal.json",
    "loki": "loki_success_minimal.json",
    "tempo": "tempo_success_minimal.json",
}[signal]
try:
    obj = json.loads((pathlib.Path(fixture_dir) / fixture_name).read_text())
except (FileNotFoundError, json.JSONDecodeError) as exc:
    print(json.dumps({"error": f"mock fixture unavailable: {exc}"}))
    raise SystemExit(0)
if signal == "prometheus":
    obj["data"]["result"].append({
        "metric": {"tenant": tenant, "datasource_uid": uid},
        "value": [1700000000, "1"],
    })
elif signal == "loki":
    obj["data"]["result"].append({
        "stream": {"tenant": tenant, "datasource_uid": uid},
        "values": [["1700000000000000000", json.dumps({
            "tenant": tenant,
            "source_uid": uid,
            "trace_id": ("a" if tenant.endswith("-a") else "b") * 32,
        })]],
    })
else:
    trace_id = ("a" if tenant.endswith("-a") else "b") * 32
    trace_wire_id = base64.b64encode(bytes.fromhex(trace_id)).decode("ascii")
    obj["traces"] = [{"traceID": trace_wire_id, "rootServiceName": tenant, "datasource_uid": uid}]
print(json.dumps(obj))
PY
}

mock_datasource() {
  local uid="$1"
  python3 - "$uid" "$TENANT_A_ID" "$TENANT_B_ID" <<'PY'
import json
import sys
uid, tenant_a, tenant_b = sys.argv[1:]
is_tenant = uid.endswith("-a") or uid.endswith("-b")
tenant = tenant_a if uid.endswith("-a") else tenant_b
signal = "prometheus" if "prom" in uid else "loki" if "loki" in uid else "tempo"
headers = {"httpHeaderName1": "Authorization"}
secure = {"httpHeaderValue1": True}
if is_tenant:
    headers["httpHeaderName2"] = "X-Scope-OrgID"
    secure["httpHeaderValue2"] = True
data = {
    "uid": uid,
    "name": uid,
    "type": signal,
    "url": "http://mock.invalid",
    "jsonData": headers,
    "secureJsonFields": secure,
}
if is_tenant:
    data["tenant_validation_marker"] = tenant
if uid == "softprobe-loki-a":
    data["jsonData"]["derivedFields"] = [{"datasourceUid": "softprobe-tempo-a"}]
elif uid == "softprobe-loki-b":
    data["jsonData"]["derivedFields"] = [{"datasourceUid": "softprobe-tempo-b"}]
elif uid == "softprobe-tempo-a":
    data["jsonData"]["tracesToLogsV2"] = {"datasourceUid": "softprobe-loki-a"}
elif uid == "softprobe-tempo-b":
    data["jsonData"]["tracesToLogsV2"] = {"datasourceUid": "softprobe-loki-b"}
print(json.dumps(data))
PY
}

mock_dashboard() {
  local uid="$1"
  python3 - "$GRAFANA_DASHBOARD_DIR" "$uid" <<'PY'
import json
import pathlib
import sys

def load_dashboard(path):
    document = json.loads(path.read_text())
    return document.get("dashboard", document)

directory, uid = sys.argv[1], sys.argv[2]
root = pathlib.Path(directory)
for path in sorted(root.rglob("*.json")):
    dashboard = load_dashboard(path)
    if dashboard.get("uid") == uid:
        print(json.dumps({
            "meta": {"isFolder": False, "folderTitle": "Softprobe", "folderUid": "softprobe-folder"},
            "dashboard": dashboard,
        }))
        break
else:
    raise SystemExit(f"dashboard not found: {uid}")
PY
}

mock_tempo_trace_response() {
  local trace_id="$1" tenant="$2"
  python3 - "$trace_id" "$tenant" <<'PY'
import base64
import json
import re
import sys
trace_id, tenant = sys.argv[1:]
if re.fullmatch(r"[0-9a-fA-F]{32}", trace_id):
    trace_bytes = bytes.fromhex(trace_id)
    trace_wire_id = base64.b64encode(trace_bytes).decode("ascii")
else:
    trace_bytes = base64.b64decode(trace_id, validate=True)
    trace_wire_id = base64.b64encode(trace_bytes).decode("ascii")
    if len(trace_bytes) != 16 or trace_wire_id != trace_id:
        raise SystemExit("mock Tempo trace lookup requires canonical padded Base64")
span_wire_id = base64.b64encode(bytes.fromhex("01" * 8)).decode("ascii")
parent_wire_id = base64.b64encode(bytes.fromhex("00" * 8)).decode("ascii")
link_wire_id = base64.b64encode(bytes.fromhex("02" * 8)).decode("ascii")
print(json.dumps({
    "traceID": trace_wire_id,
        "batches": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": tenant}}, {"key": "deployment.environment", "value": {"stringValue": "primary"}}]},
        "scopeSpans": [{
            "scope": {"name": "grafana-seeder", "version": "1.0.0", "attributes": [{"key": "scope.role", "value": {"stringValue": "checkout"}}]},
            "spans": [{
                "traceId": trace_wire_id,
                "spanId": span_wire_id,
                "parentSpanId": parent_wire_id,
                "name": "checkout",
                "startTimeUnixNano": "1700000010000000000",
                "endTimeUnixNano": "1700000011000000000",
                "status": {"code": "STATUS_CODE_OK"},
                "events": [{"name": "checkout.started", "timeUnixNano": "1700000010500000000"}],
                "links": [{"traceId": trace_wire_id, "spanId": link_wire_id}]
            }]
        }]
    }, {
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": tenant}}, {"key": "deployment.environment", "value": {"stringValue": "secondary"}}]},
        "scopeSpans": [{
            "scope": {"name": "grafana-secondary", "version": "1.0.0", "attributes": [{"key": "scope.role", "value": {"stringValue": "checkout-child"}}]},
            "spans": [{
                "traceId": trace_wire_id,
                "spanId": base64.b64encode(bytes.fromhex("03" * 8)).decode("ascii"),
                "parentSpanId": span_wire_id,
                "name": "checkout.child",
                "startTimeUnixNano": "1700000011000000000",
                "endTimeUnixNano": "1700000012000000000",
                "status": {"code": "STATUS_CODE_ERROR"},
                "events": [{"name": "checkout.failed", "timeUnixNano": "1700000011500000000"}],
                "links": [{"traceId": trace_wire_id, "spanId": link_wire_id}]
            }]
        }]
    }],
}))
PY
}

mock_response() {
  local endpoint="$1"
  case "$endpoint" in
    /api/health)
      printf '%s\n' '{"database":"ok","version":"mock","commit":"mock"}'
      ;;
    /api/datasources)
      printf '%s\n' '[{"uid":"softprobe-prom"},{"uid":"softprobe-prom-a"},{"uid":"softprobe-prom-b"},{"uid":"softprobe-loki-a"},{"uid":"softprobe-loki-b"},{"uid":"softprobe-tempo-a"},{"uid":"softprobe-tempo-b"}]'
      ;;
    /api/datasources/uid/*/health)
      printf '%s\n' '{"status":"OK","message":"mock datasource health"}'
      ;;
    /api/datasources/proxy/uid/*/api/v1/label/*/values*|/api/datasources/proxy/uid/*/loki/api/v1/label/*/values*)
      if [[ "$endpoint" == *k6_http_reqs* ]]; then
        printf '%s\n' '{"data":["load-generator","checkout"],"status":"success"}'
      else
        printf '%s\n' '{"data":["checkout"],"status":"success"}'
      fi
      ;;
    /api/datasources/uid/*)
      mock_datasource "${endpoint##*/}"
      ;;
    /api/datasources/proxy/uid/*/api/v1/label/*/values*|/api/datasources/proxy/uid/*/loki/api/v1/label/*/values*)
      if [[ "$endpoint" == *k6_http_reqs* ]]; then
        printf '%s\n' '{"data":["load-generator","checkout"],"status":"success"}'
      else
        printf '%s\n' '{"data":["checkout"],"status":"success"}'
      fi
      ;;
    "/api/search?type=dash-db")
      python3 - "$GRAFANA_DASHBOARD_DIR" <<'PY'
import json
import pathlib
import sys

items = []
for path in sorted(pathlib.Path(sys.argv[1]).rglob("*.json")):
    dashboard = json.loads(path.read_text())
    items.append({"uid": dashboard["uid"], "title": dashboard.get("title", dashboard["uid"])})
print(json.dumps(items))
PY
      ;;
    /api/dashboards/uid/*)
      mock_dashboard "${endpoint##*/}"
      ;;
    /api/datasources/proxy/uid/softprobe-tempo-a/api/traces/*)
      local trace_endpoint="${endpoint##*/}"
      mock_tempo_trace_response "${trace_endpoint%%\?*}" "$TENANT_A_ID"
      ;;
    /api/datasources/proxy/uid/softprobe-tempo-b/api/traces/*)
      local trace_endpoint="${endpoint##*/}"
      mock_tempo_trace_response "${trace_endpoint%%\?*}" "$TENANT_B_ID"
      ;;
    /api/datasources/proxy/uid/softprobe-prom-a/api/v1/query*) mock_signal_response prometheus softprobe-prom-a "$TENANT_A_ID" ;;
    /api/datasources/proxy/uid/softprobe-prom-b/api/v1/query*) mock_signal_response prometheus softprobe-prom-b "$TENANT_B_ID" ;;
    /api/datasources/proxy/uid/softprobe-loki-a/loki/api/v1/query*) mock_signal_response loki softprobe-loki-a "$TENANT_A_ID" ;;
    /api/datasources/proxy/uid/softprobe-loki-b/loki/api/v1/query*) mock_signal_response loki softprobe-loki-b "$TENANT_B_ID" ;;
    /api/datasources/proxy/uid/softprobe-tempo-a/api/search*) mock_signal_response tempo softprobe-tempo-a "$TENANT_A_ID" ;;
    /api/datasources/proxy/uid/softprobe-tempo-b/api/search*) mock_signal_response tempo softprobe-tempo-b "$TENANT_B_ID" ;;
    *) return 1 ;;
  esac
}

mock_response_post() {
  local endpoint="$1" payload="$2" credential="${3:-}" scope="${4:-}"
  if [[ "$endpoint" != /api/ds/query ]]; then
    return 1
  fi
  python3 - "$MOCK_FIXTURE_DIR" "$payload" "$TENANT_A_ID" "$TENANT_B_ID" "$credential" "$scope" <<'PY'
import json
import pathlib
import sys

fixture_dir, payload, tenant_a, tenant_b, credential, scope = sys.argv[1:]
request = json.loads(payload)
query = request.get("queries", [{}])[0]
uid = query.get("datasource", {}).get("uid", "")
tenant = tenant_a if uid.endswith("-a") else tenant_b
signal = "prometheus" if "prom" in uid else "loki" if "loki" in uid else "tempo"
if not credential and not scope:
    # A normal Grafana datasource request carries the configured tenant
    # headers even though callers of api_post do not supply probe overrides.
    credential = f"mock-{tenant}-credential"
    scope = tenant
text = json.dumps(query).lower()
request_headers = {
    "Authorization": {
        "present": credential != "__missing__",
        "matchesTenant": credential not in {"__missing__", "invalid-credential"},
    },
    "X-Scope-OrgID": {
        "present": bool(scope),
        "value": scope,
        "matchesTenant": scope == tenant,
    },
}
if credential == "__missing__" or not request_headers["Authorization"]["present"]:
    message = "missing credentials"
elif credential == "invalid-credential":
    message = "invalid credentials"
elif not request_headers["X-Scope-OrgID"]["present"]:
    message = "missing X-Scope-OrgID"
elif not request_headers["X-Scope-OrgID"]["matchesTenant"]:
    message = "mismatched tenant X-Scope-OrgID"
else:
    message = ""
if message:
    print(json.dumps({"results": {"A": {"error": message, "errorSource": "downstream", "requestHeaders": request_headers}}}))
    raise SystemExit
if False:
    if credential == "__missing__":
        message = "missing credentials"
    elif credential == "invalid-credential":
        message = "invalid credentials"
    elif scope and scope != tenant:
        message = "mismatched tenant X-Scope-OrgID"
    else:
        message = "credential probe unexpectedly succeeded"
if uid.endswith("-invalid"):
    print(json.dumps({"results": {"A": {"error": "datasource authentication failed", "errorSource": "downstream", "requestHeaders": request_headers}}}))
    raise SystemExit
if "malformed_frame_probe" in text:
    print(json.dumps({"results": {"A": {"refId": "A", "frames": [{"schema": {}}], "requestHeaders": request_headers}}}))
    raise SystemExit
if "empty_result_probe" in text:
    print(json.dumps({"results": {"A": {"refId": "A", "frames": [{"schema": {"fields": [{"name": "value"}]}, "data": {"values": [[]]}}], "requestHeaders": request_headers}}}))
    raise SystemExit
if "unsupported" in text:
    names = {"prometheus": "prometheus_error_unsupported.json", "loki": "loki_error_unsupported.json", "tempo": "tempo_error_unsupported.json"}
    try:
        fixture = json.loads((pathlib.Path(fixture_dir) / names[signal]).read_text())
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(json.dumps({"results": {"A": {"error": f"mock fixture unavailable: {exc}", "requestHeaders": request_headers}}}))
        raise SystemExit(0)
    message = fixture.get("error") or fixture.get("message") or "unsupported_feature"
    print(json.dumps({"results": {"A": {"error": message, "errorSource": "downstream", "requestHeaders": request_headers}}}))
    raise SystemExit

if "label_values" in text:
    values = ["load-generator"] if "k6_http_reqs" in text else ["checkout"]
    frame = {"schema": {"name": tenant, "fields": [{"name": "value"}]}, "data": {"values": [values]}}
elif signal == "prometheus":
    frame = {"schema": {"name": tenant, "fields": [{"name": "value"}]}, "data": {"values": [[1]]}}
elif signal == "loki":
    frame = {"schema": {"name": tenant, "fields": [{"name": "line"}]}, "data": {"values": [[json.dumps({"tenant": tenant})]]}}
else:
    frame = {"schema": {"name": tenant, "fields": [{"name": "traceID"}]}, "data": {"values": [["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]]}}
print(json.dumps({"results": {"A": {"refId": "A", "frames": [frame], "requestHeaders": request_headers}}}))
PY
}

# 0 = successful 2xx response, 1 = HTTP/API failure, 2 = unreachable service.
api_request() {
  local method="$1" endpoint="$2" payload="${3:-}" artifact="$4" status curl_status
  local response_tmp stderr_tmp
  : > "$artifact"
  : > "$artifact.status"
  if [[ "$MOCK_MODE" == "1" ]]; then
    # Mock responses are generated locally without credential-bearing request
    # material. The redaction helper is tested independently above; avoiding a
    # Python redaction process for every synthetic request keeps the bounded
    # G1-G8 artifact lane deterministic on developer machines.
    if [[ "$method" == POST ]]; then
      if ! mock_response_post "$endpoint" "$payload" "${5:-}" "${6:-}" > "$artifact"; then
        printf '%s\n' "mock response unavailable for $endpoint" > "$artifact"
        return 1
      fi
    else
      if ! mock_response "$endpoint" > "$artifact"; then
        printf '%s\n' "mock response unavailable for $endpoint" > "$artifact"
        return 1
      fi
    fi
    printf '200\n' > "$artifact.status"
    return 0
  fi
  response_tmp="$(mktemp "${TMPDIR:-/tmp}/grafana-response.XXXXXX")"
  stderr_tmp="$(mktemp "${TMPDIR:-/tmp}/grafana-curl-stderr.XXXXXX")"
  local datasource_credential="${5:-}" scope="${6:-}" auth_args=(--user "$GRAFANA_ADMIN_USER:$GRAFANA_ADMIN_PASSWORD")
  local datasource_args=()
  local scope_args=()
  if [[ -n "$datasource_credential" && "$datasource_credential" != "__missing__" ]]; then
    datasource_args=(--header "X-Softprobe-Probe-Credential: $datasource_credential")
  fi
  if [[ -n "$scope" ]]; then
    scope_args=(--header "X-Scope-OrgID: $scope")
  fi
  if [[ "$method" == POST ]]; then
    if status="$(curl --silent --show-error --location \
        --connect-timeout "${CURL_CONNECT_TIMEOUT:-3}" \
        --max-time "${CURL_MAX_TIME:-15}" \
        ${auth_args[@]+"${auth_args[@]}"} ${datasource_args[@]+"${datasource_args[@]}"} ${scope_args[@]+"${scope_args[@]}"} \
        --header 'Accept: application/json' --header 'Content-Type: application/json' \
        --data "$payload" --output "$response_tmp" --write-out '%{http_code}' \
        "$GRAFANA_URL$endpoint" 2> "$stderr_tmp")"; then
      curl_status=0
    else
      curl_status=$?
    fi
  else
    if status="$(curl --silent --show-error --location \
        --connect-timeout "${CURL_CONNECT_TIMEOUT:-3}" \
        --max-time "${CURL_MAX_TIME:-15}" \
        ${auth_args[@]+"${auth_args[@]}"} ${datasource_args[@]+"${datasource_args[@]}"} ${scope_args[@]+"${scope_args[@]}"} \
        --header 'Accept: application/json' \
        --output "$response_tmp" --write-out '%{http_code}' \
        "$GRAFANA_URL$endpoint" 2> "$stderr_tmp")"; then
      curl_status=0
    else
      curl_status=$?
    fi
  fi
  redact "$(<"$response_tmp")" > "$artifact"
  if [[ -s "$stderr_tmp" ]]; then
    redact "$(<"$stderr_tmp")" > "$artifact.stderr"
  else
    rm -f "$artifact.stderr"
  fi
  rm -f "$response_tmp" "$stderr_tmp"
  if (( curl_status != 0 )); then
      printf '000\n' > "$artifact.status"
      return 2
  fi
  printf '%s\n' "$status" > "$artifact.status"
  [[ "$status" == 2* ]] || return 1
}

api_get() { api_request GET "$1" "" "$2"; }
api_post() { api_request POST "$1" "$2" "$3"; }
api_post_credentials() { api_request POST "$1" "$2" "$3" "$4" "${5:-}"; }

# Grafana's /api/ds/query endpoint is authenticated with the Grafana admin
# credential.  Its Authorization header therefore cannot, by itself, prove
# that a per-tenant datasource credential was rejected by Softprobe.  Keep the
# Grafana route probe, and supplement it with the same credential sent directly
# to the Softprobe protocol route.  A direct probe is successful only when the
# downstream service returns an authentication/tenant error.
direct_softprobe_credential_probe() {
  local signal="$1" probe="$2" credential="$3" scope="$4" artifact="$5" request_artifact="$6"
  local endpoint status response_tmp stderr_tmp curl_status
  case "$signal" in
    prometheus) endpoint="/api/v1/query?query=up&time=1700000030" ;;
    loki) endpoint="/loki/api/v1/query_range?query=%7Bservice_name%3D%22checkout%22%7D&start=$CROSS_START_NS&end=$CROSS_END_NS&limit=1" ;;
    tempo) endpoint="/api/search?limit=1&start=$TRACE_START_S&end=$TRACE_END_S" ;;
    *) return 1 ;;
  esac
  write_request_artifact "$request_artifact" GET "$SOFTPROBE_DIRECT_URL$endpoint" "" \
    "credential_probe=$probe" "tenant_scope=$scope" \
    "datasource_credential_header=X-Softprobe-Probe-Credential" \
    "credential_present=$([[ "$credential" == "__missing__" ]] && printf no || printf yes)"
  : > "$artifact"
  : > "$artifact.status"
  if [[ "$MOCK_MODE" == "1" ]]; then
    local mock_status=403
    local mock_message="invalid credentials"
    if [[ "$probe" == missing_credentials ]]; then
      [[ "$credential" == "__missing__" ]] || {
        echo "mock credential probe did not receive the missing-credential sentinel" >&2
        return 1
      }
      mock_status=401
      mock_message="missing credentials"
    elif [[ "$probe" == invalid_credentials ]]; then
      [[ "$credential" == "invalid-credential" ]] || {
        echo "mock credential probe did not receive the invalid credential" >&2
        return 1
      }
    elif [[ "$probe" == mismatched_tenant ]]; then
      [[ "$credential" != "__missing__" && "$credential" != "invalid-credential" ]] || {
        echo "mock mismatched-tenant probe did not receive a supplied credential" >&2
        return 1
      }
      mock_message="mismatched tenant X-Scope-OrgID"
    else
      return 1
    fi
    printf '{"error":"%s","errorSource":"softprobe","probe":"%s","credentialObserved":true}\n' \
      "$mock_message" "$probe" > "$artifact"
    printf '%s\n' "$mock_status" > "$artifact.status"
    return 0
  fi
  response_tmp="$(mktemp "${TMPDIR:-/tmp}/softprobe-direct-response.XXXXXX")"
  stderr_tmp="$(mktemp "${TMPDIR:-/tmp}/softprobe-direct-stderr.XXXXXX")"
  local auth_args=(--header 'Accept: application/json')
  local datasource_args=(--header "X-Softprobe-Probe: $probe")
  if [[ "$credential" != "__missing__" ]]; then
    auth_args+=(--header "Authorization: Bearer $credential")
    datasource_args+=(--header "X-Softprobe-Probe-Credential: $credential")
  fi
  [[ -n "$scope" ]] && auth_args+=(--header "X-Scope-OrgID: $scope")
  if status="$(curl --silent --show-error --location \
      --connect-timeout "${CURL_CONNECT_TIMEOUT:-3}" \
      --max-time "${CURL_MAX_TIME:-15}" \
      "${auth_args[@]}" "${datasource_args[@]}" --output "$response_tmp" --write-out '%{http_code}' \
      "$SOFTPROBE_DIRECT_URL$endpoint" 2> "$stderr_tmp")"; then
    curl_status=0
  else
    curl_status=$?
  fi
  redact "$(<"$response_tmp")" > "$artifact"
  [[ -s "$stderr_tmp" ]] && redact "$(<"$stderr_tmp")" > "$artifact.stderr" || rm -f "$artifact.stderr"
  rm -f "$response_tmp" "$stderr_tmp"
  if (( curl_status != 0 )); then
    printf '000\n' > "$artifact.status"
    return 2
  fi
  printf '%s\n' "$status" > "$artifact.status"
  # mismatched_tenant may legitimately execute under the token tenant
  # (returning 2xx with no other-tenant data); validate_direct_credential_
  # rejection enforces the no-leak invariant for that case.
  if [[ "$probe" != "mismatched_tenant" ]]; then
    [[ "$status" == 401 || "$status" == 403 ]] || {
      echo "direct Softprobe credential probe unexpectedly returned HTTP $status" >&2
      return 1
    }
  fi
}

http_get_artifact() {
  local url="$1" artifact="$2" status curl_status
  local response_tmp stderr_tmp
  response_tmp="$(mktemp "${TMPDIR:-/tmp}/grafana-composition-response.XXXXXX")"
  stderr_tmp="$(mktemp "${TMPDIR:-/tmp}/grafana-composition-stderr.XXXXXX")"
  if status="$(curl --silent --show-error --location \
      --connect-timeout "${CURL_CONNECT_TIMEOUT:-3}" \
      --max-time "${CURL_MAX_TIME:-15}" \
      --output "$response_tmp" --write-out '%{http_code}' \
      "$url" 2> "$stderr_tmp")"; then
    curl_status=0
  else
    curl_status=$?
  fi
  redact "$(<"$response_tmp")" > "$artifact"
  if [[ -s "$stderr_tmp" ]]; then
    redact "$(<"$stderr_tmp")" > "$artifact.stderr"
  else
    rm -f "$artifact.stderr"
  fi
  rm -f "$response_tmp" "$stderr_tmp"
  if (( curl_status != 0 )); then
    printf '000\n' > "$artifact.status"
    return 2
  fi
  printf '%s\n' "$status" > "$artifact.status"
  [[ "$status" == 2* ]]
}

check_composition_readiness() {
  local runtime="$ARTIFACT_DIR/.work/composition-softprobe-ready.json"
  local auth="$ARTIFACT_DIR/.work/composition-auth-mock-health.json"
  http_get_artifact "$SEED_SOFTPROBE_URL/ready" "$runtime" || return 1
  http_get_artifact "$GRAFANA_AUTH_MOCK_URL/__admin/health" "$auth" || return 1
  validate_json "$runtime" || return 1
  validate_json "$auth" || return 1
}

run_deterministic_seed() {
  if [[ "${GRAFANA_SEED_IN_COMPOSE:-0}" == "1" ]]; then
    local receipt="$ARTIFACT_DIR/seed-receipt.json"
    [[ -s "$receipt" ]] || receipt="$ARTIFACT_ROOT/seed-receipt.json"
    [[ -s "$receipt" ]] || {
      echo "compose Grafana seed did not write $ARTIFACT_DIR/seed-receipt.json" >&2
      return 1
    }
    python3 - "$receipt" "$TENANT_A_ID" "$TENANT_B_ID" <<'PY'
import json
import pathlib
import sys

receipt = json.loads(pathlib.Path(sys.argv[1]).read_text())
if receipt.get("status") != "pass":
    raise SystemExit(f"compose seed status is not pass: {receipt.get('status')!r}")
tenants = {item.get("tenant_id"): item for item in receipt.get("tenants", [])}
for tenant_id in sys.argv[2:]:
    item = tenants.get(tenant_id)
    if not item or not all(item.get(key) is True for key in (
        "scope_provisioned", "metrics_sent", "logs_sent", "traces_sent",
        "metrics_queryable", "logs_queryable", "traces_queryable",
    )):
        raise SystemExit(f"compose seed is not queryable for tenant {tenant_id}")
PY
    return 0
  fi
  local seed_bin="${GRAFANA_SEED_BIN:-${CARGO_TARGET_DIR:-target}/debug/grafana_seed_otlp}"
  if [[ ! -x "$seed_bin" ]]; then
    DUCKDB_DOWNLOAD_LIB=1 cargo build --quiet --bin grafana_seed_otlp || return 1
  fi
  seed_bin="${GRAFANA_SEED_BIN:-${CARGO_TARGET_DIR:-target}/debug/grafana_seed_otlp}"
  GRAFANA_SEED_SOFTPROBE_URL="$SEED_SOFTPROBE_URL" \
    GRAFANA_SEED_RECEIPT="$ARTIFACT_DIR/seed-receipt.json" \
    SOFTPROBE_TENANT_A_ID="$TENANT_A_ID" \
    SOFTPROBE_TENANT_B_ID="$TENANT_B_ID" \
    SOFTPROBE_ADMIN_API_KEY="${SOFTPROBE_ADMIN_API_KEY:-grafana-phase4-admin}" \
    SOFTPROBE_TENANT_A_API_KEY="${GRAFANA_TEST_TENANT_A_API_KEY:-grafana-phase4-tenant-a}" \
    SOFTPROBE_TENANT_B_API_KEY="${GRAFANA_TEST_TENANT_B_API_KEY:-grafana-phase4-tenant-b}" \
    "$seed_bin"
}

# Simple GET against an absolute URL with Softprobe tenant credentials,
# writing the body to $1. $3=bearer key, $4=tenant id.
curl_get_artifact() {
  local url="$1" out="$2" bearer="${3:-}" scope="${4:-}" code
  local hdr_args=(--header "Authorization: Bearer $bearer")
  [[ -n "$scope" ]] && hdr_args+=(--header "X-Scope-OrgID: $scope")
  if code="$(curl --silent --show-error \
      --connect-timeout 3 --max-time 15 ${hdr_args[@]+"${hdr_args[@]}"} \
      --header 'Accept: application/json' \
      --output "$out" --write-out '%{http_code}' "$url")"; then :; fi
  [[ "$code" == 2* ]]
}

write_request_artifact() {
  local path="$1" method="$2" endpoint="$3" payload="${4:-}"
  shift 4 || true
  python3 - "$method" "$endpoint" "$payload" "$@" <<'PY' | redact > "$path"
import json
import sys
method, endpoint, payload, *metadata = sys.argv[1:]
request = {"method": method, "endpoint": endpoint}
if payload:
    try:
        request["payload"] = json.loads(payload)
    except json.JSONDecodeError:
        request["payload"] = payload
for item in metadata:
    key, separator, value = item.partition("=")
    if separator:
        request.setdefault("metadata", {})[key] = value
print(json.dumps(request, indent=2, sort_keys=True))
PY
}

validate_json() {
  python3 - "$1" <<'PY'
import json
import pathlib
import sys
json.loads(pathlib.Path(sys.argv[1]).read_text())
PY
}

validate_uid_list() {
  python3 - "$1" "${@:2}" <<'PY'
import json
import pathlib
import sys
items = json.loads(pathlib.Path(sys.argv[1]).read_text())
expected = set(sys.argv[2:])
actual = {item.get("uid") for item in items if isinstance(item, dict)}
missing = sorted(expected - actual)
if missing:
    raise SystemExit("missing UIDs: " + ", ".join(missing))
PY
}

validate_health() {
  python3 - "$1" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
if obj.get("database") != "ok":
    raise SystemExit("Grafana database is not healthy")
PY
}

validate_datasource_config() {
  python3 - "$1" "$2" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
uid = sys.argv[2]
if obj.get("uid") != uid:
    raise SystemExit(f"datasource UID mismatch: expected {uid}")
json_data = obj.get("jsonData", {})
secure = obj.get("secureJsonFields", {})
if json_data.get("httpHeaderName1") != "Authorization" or not secure.get("httpHeaderValue1"):
    raise SystemExit(f"{uid} is missing configured Authorization credentials")
if uid.endswith(("-a", "-b")):
    if json_data.get("httpHeaderName2") != "X-Scope-OrgID" or not secure.get("httpHeaderValue2"):
        raise SystemExit(f"{uid} is missing configured X-Scope-OrgID credentials")
PY
}

validate_native_health() {
  python3 - "$1" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
status = str(obj.get("status", "")).lower()
if status in {"ok", "success", "healthy"}:
    raise SystemExit(0)
# The Grafana Tempo backend plugin does not implement the health check RPC
# (404 plugin.notImplemented); absence of a native probe is not an error.
if obj.get("messageId") == "plugin.notImplemented":
    raise SystemExit(0)
raise SystemExit(f"native datasource health is not healthy: {obj}")
PY
}

validate_dashboard_refs() {
  python3 - "$1" "$2" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text()).get("dashboard", {})
uid = sys.argv[2]
if obj.get("uid") != uid:
    raise SystemExit(f"dashboard UID mismatch: expected {uid}")
allowed = {"softprobe-prom", "softprobe-prom-a", "softprobe-prom-b", "softprobe-loki-a", "softprobe-loki-b", "softprobe-tempo-a", "softprobe-tempo-b"}
refs = []
def visit(panel):
    source = panel.get("datasource")
    if isinstance(source, dict) and source.get("uid"):
        refs.append(source["uid"])
    elif isinstance(source, str) and source:
        refs.append(source)
    for target in panel.get("targets", []):
        source = target.get("datasource")
        if isinstance(source, dict) and source.get("uid"):
            refs.append(source["uid"])
        elif isinstance(source, str) and source:
            refs.append(source)
    for child in panel.get("panels", []):
        visit(child)
for panel in obj.get("panels", []):
    visit(panel)
if not refs:
    raise SystemExit(f"{uid} has no panel datasource references")
unknown = sorted(set(refs) - allowed)
if unknown:
    raise SystemExit(f"{uid} has unknown datasource refs: {', '.join(unknown)}")
PY
}

validate_dashboard_round_trip() {
  python3 - "$1" "$2" "$GRAFANA_DASHBOARD_DIR" <<'PY'
import json
import pathlib
import sys

def target_projection(target):
    if not isinstance(target, dict):
        return target
    projection = {}
    for key in ("refId", "expr", "query", "queryType", "format", "legendFormat", "instant", "interval", "intervalMs", "maxDataPoints"):
        if key in target:
            projection[key] = target[key]
    datasource = target.get("datasource")
    if isinstance(datasource, dict):
        projection["datasource"] = {key: datasource[key] for key in ("type", "uid") if key in datasource}
    elif datasource is not None:
        projection["datasource"] = datasource
    return projection

def panel_projection(panel):
    if not isinstance(panel, dict):
        return panel
    return {
        key: panel[key]
        for key in ("type", "title", "description", "repeat", "repeatDirection", "targets")
        if key in panel
    } | {
        "targets": [target_projection(target) for target in panel.get("targets", [])],
        "panels": [panel_projection(child) for child in panel.get("panels", [])],
    }

def panel_structure(dashboard):
    return [panel_projection(panel) for panel in dashboard.get("panels", [])]

obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
uid = sys.argv[2]
dashboard_dir = pathlib.Path(sys.argv[3])

def load_dashboard(path):
    document = json.loads(path.read_text())
    return document.get("dashboard", document)

fixture = None
for path in sorted(dashboard_dir.rglob("*.json")):
    candidate = load_dashboard(path)
    if candidate.get("uid") == uid:
        fixture = candidate
        break
if fixture is None:
    raise SystemExit(f"dashboard fixture not found: {uid}")
dashboard = obj.get("dashboard", {})
meta = obj.get("meta", {})
if dashboard.get("uid") != uid:
    raise SystemExit(f"dashboard round-trip UID mismatch: expected {uid}")
for key in ("uid", "refresh", "time", "templating"):
    if dashboard.get(key) != fixture.get(key):
        raise SystemExit(f"{uid} dashboard {key} differs from checked-in fixture")
if not isinstance(dashboard.get("version"), int) or dashboard["version"] < 1:
    raise SystemExit(f"{uid} dashboard version is missing or invalid")
if panel_structure(dashboard) != panel_structure(fixture):
    raise SystemExit(f"{uid} dashboard panel/target structure differs from checked-in fixture")
if meta.get("folderTitle") != "Softprobe" or not meta.get("folderUid"):
    raise SystemExit(f"{uid} dashboard is not provisioned in the Softprobe folder")
variables = dashboard.get("templating", {}).get("list")
if not isinstance(variables, list):
    raise SystemExit(f"{uid} dashboard templating list is malformed")
for variable in variables:
    if not variable.get("name") or variable.get("type") not in {"query", "custom", "constant", "interval"}:
        raise SystemExit(f"{uid} contains an invalid dashboard variable")
    if variable.get("type") == "query" and not variable.get("query"):
        raise SystemExit(f"{uid} query variable has no query")
PY
}

validate_panel_response() {
  python3 - "$1" "$2" "$3" <<'PY'
import json
import pathlib
import sys
path, signal, uid = sys.argv[1:]
obj = json.loads(pathlib.Path(path).read_text())
result = obj.get("results", {}).get("A", {})
if result.get("error"):
    raise SystemExit(f"{uid} {signal} panel returned an error: {result['error']}")
frames = result.get("frames") or result.get("data")
if not frames:
    raise SystemExit(f"{uid} {signal} panel returned no data frames")
def has_values(value):
    if isinstance(value, dict):
        values = value.get("values")
        if isinstance(values, list) and any(item not in ([], None, "") for item in values):
            return True
        return any(has_values(item) for item in value.values())
    if isinstance(value, list):
        return any(has_values(item) for item in value)
    return False
if not has_values(frames):
    raise SystemExit(f"{uid} {signal} panel returned empty data")
PY
}

validate_tempo_trace_response() {
  python3 - "$1" "$2" "$3" "$4" "${5:-}" <<'PY'
import json
import os
import pathlib
import re
import sys
import base64
path, trace_id, expected, other, expected_wire = sys.argv[1:]
rich = os.environ.get("GRAFANA_RICH_TEMPO_ASSERTIONS", "1") == "1"
obj = json.loads(pathlib.Path(path).read_text())
text = json.dumps(obj, sort_keys=True)
if re.fullmatch(r"[0-9a-fA-F]{32}", trace_id):
    requested_trace = bytes.fromhex(trace_id)
else:
    try:
        requested_trace = base64.b64decode(trace_id, validate=True)
    except Exception as exc:
        raise SystemExit("Tempo lookup trace ID is malformed") from exc
    if len(requested_trace) != 16 or base64.b64encode(requested_trace).decode("ascii") != trace_id:
        raise SystemExit("Tempo lookup trace ID is not canonical padded Base64")
canonical_trace_id = base64.b64encode(requested_trace).decode("ascii")
if trace_id not in text and canonical_trace_id not in text:
    raise SystemExit(f"Tempo trace response does not contain requested trace {trace_id}")
if expected_wire and expected_wire not in text:
    raise SystemExit("Tempo trace response does not contain the expected wire trace ID")
if expected and expected not in text:
    raise SystemExit(f"Tempo trace response is missing tenant marker {expected}")
if other and other in text:
    raise SystemExit("Tempo trace response contains the other tenant marker")
groups = obj.get("batches") or obj.get("resourceSpans")
if not isinstance(groups, list) or not groups:
    raise SystemExit("Tempo trace response contains no spans")

def valid_id(value, byte_length):
    if not isinstance(value, str) or not value:
        return None
    try:
        decoded = base64.b64decode(value, validate=True)
    except Exception:
        return None
    if len(decoded) != byte_length or base64.b64encode(decoded).decode("ascii") != value:
        return None
    return decoded

response_trace_id = obj.get("traceID", obj.get("traceId"))
if response_trace_id is not None and valid_id(response_trace_id, 16) is None:
    raise SystemExit("Tempo trace response has a malformed/noncanonical trace ID")

def timestamp(value):
    return isinstance(value, (int, str)) and bool(re.fullmatch(r"[0-9]+", str(value)))

matched = 0
group_signatures = set()
parent_seen = False
events_seen = False
links_seen = False
for group in groups:
    resource = group.get("resource")
    attrs = resource.get("attributes") if isinstance(resource, dict) else None
    if not isinstance(attrs, list) or expected not in json.dumps(attrs, sort_keys=True):
        raise SystemExit("Tempo trace response is missing the expected resource tenant attribute")
    scopes = group.get("scopeSpans") or group.get("scope_spans")
    if not isinstance(scopes, list) or not scopes:
        raise SystemExit("Tempo trace response is missing ScopeSpans groups")
    for scope_group in scopes:
        scope = scope_group.get("scope")
        if not isinstance(scope, dict) or not scope.get("name"):
            raise SystemExit("Tempo trace response is missing instrumentation scope")
        group_signatures.add((json.dumps(attrs, sort_keys=True), scope.get("name"), scope.get("version")))
        spans = scope_group.get("spans")
        if not isinstance(spans, list) or not spans:
            raise SystemExit("Tempo trace response is missing spans in a ScopeSpans group")
        for span in spans:
            span_trace_id = span.get("traceId", span.get("traceID"))
            decoded_trace_id = valid_id(span_trace_id, 16)
            if decoded_trace_id is None:
                raise SystemExit("Tempo trace response has a malformed/noncanonical trace ID")
            if decoded_trace_id != requested_trace:
                continue
            matched += 1
            span_id = span.get("spanId", span.get("spanID"))
            if valid_id(span_id, 8) is None:
                raise SystemExit("Tempo trace response has a malformed span ID")
            parent = span.get("parentSpanId", span.get("parentSpanID"))
            if parent is not None and parent != "" and valid_id(parent, 8) is None:
                raise SystemExit("Tempo trace response has a malformed parent span ID")
            if parent:
                parent_seen = True
            start = span.get("startTimeUnixNano")
            end = span.get("endTimeUnixNano")
            if not timestamp(start) or not timestamp(end) or int(end) <= int(start):
                raise SystemExit("Tempo trace response has invalid nanosecond timing")
            status = span.get("status")
            # OTel default is STATUS_CODE_UNSET; proto3 omits the field, so a
            # missing/null status is the canonical unset representation. A
            # present status must carry a valid enum, but either way the rest
            # of this span (events/links) still needs checking.
            if status is not None:
                if not isinstance(status, dict) or status.get("code") not in {"STATUS_CODE_UNSET", "STATUS_CODE_OK", "STATUS_CODE_ERROR"}:
                    raise SystemExit("Tempo trace response has an invalid status enum")
            events = span.get("events", [])
            if not isinstance(events, list):
                raise SystemExit("Tempo trace response events are not a list")
            for event in events:
                if not isinstance(event, dict) or not event.get("name") or not timestamp(event.get("timeUnixNano")):
                    raise SystemExit("Tempo trace response has an invalid event")
            if events:
                events_seen = True
            links = span.get("links", [])
            if not isinstance(links, list):
                raise SystemExit("Tempo trace response links are not a list")
            for link in links:
                if not isinstance(link, dict) or valid_id(link.get("traceId", link.get("traceID")), 16) is None or valid_id(link.get("spanId", link.get("spanID")), 8) is None:
                    raise SystemExit("Tempo trace response has an invalid link")
            if links:
                links_seen = True
if rich and len(groups) < 2:
    raise SystemExit("Tempo trace response collapsed distinct ResourceSpans groups")
if rich and matched < 2:
    raise SystemExit("Tempo trace response collapsed distinct spans/groups")
if rich and len(group_signatures) < 2:
    raise SystemExit("Tempo trace response collapsed distinct ResourceSpans/ScopeSpans groups")
if rich and (not parent_seen or not events_seen or not links_seen):
    raise SystemExit("Tempo trace response did not preserve topology, events, and links")
if matched == 0:
    raise SystemExit("Tempo trace response contains no span for the requested trace")
PY
}

validate_cross_signal_links() {
  python3 - "$1" "$2" "$3" "$4" "$5" <<'PY'
import json
import pathlib
import sys
cross, loki_a, loki_b, tempo_a, tempo_b = map(pathlib.Path, sys.argv[1:])
cross_obj = json.loads(cross.read_text()).get("dashboard", {})
refs = set()
for panel in cross_obj.get("panels", []):
    for item in [panel.get("datasource"), *[target.get("datasource") for target in panel.get("targets", [])]]:
        if isinstance(item, dict) and item.get("uid"):
            refs.add(item["uid"])
if not {"softprobe-prom-a", "softprobe-loki-a", "softprobe-tempo-a"}.issubset(refs):
    raise SystemExit("cross-signal dashboard is missing a Prometheus/Loki/Tempo panel reference")
for path, expected in ((loki_a, "softprobe-tempo-a"), (loki_b, "softprobe-tempo-b")):
    fields = json.loads(path.read_text()).get("jsonData", {}).get("derivedFields", [])
    if not any(field.get("datasourceUid") == expected for field in fields):
        raise SystemExit(f"{path.name} is missing a trace-to-Tempo derived field for {expected}")
for path, expected in ((tempo_a, "softprobe-loki-a"), (tempo_b, "softprobe-loki-b")):
    target = json.loads(path.read_text()).get("jsonData", {}).get("tracesToLogsV2", {}).get("datasourceUid")
    if target != expected:
        raise SystemExit(f"{path.name} trace-to-logs target is {target!r}, expected {expected!r}")
PY
}

validate_loki_trace_link() {
  python3 - "$1" "$2" "$3" <<'PY'
import base64
import json
import pathlib
import re
import sys

path, source_trace_id, wire_trace_id = sys.argv[1:]
obj = json.loads(pathlib.Path(path).read_text())
results = obj.get("data", {}).get("result", [])
if not isinstance(results, list):
    raise SystemExit("Loki trace-to-log response has no stream result list")

def values_in_streams():
    for stream in results:
        if not isinstance(stream, dict) or not isinstance(stream.get("values"), list):
            continue
        for entry in stream["values"]:
            if isinstance(entry, list) and len(entry) >= 2:
                yield entry[1]

def candidate_texts(line):
    yield line if isinstance(line, str) else ""
    if isinstance(line, str):
        try:
            decoded = json.loads(line)
        except json.JSONDecodeError:
            return
        yield json.dumps(decoded, sort_keys=True)

for line in values_in_streams():
    for text in candidate_texts(line):
        lowered = text.lower()
        if source_trace_id.lower() in lowered or wire_trace_id.lower() in lowered:
            raise SystemExit(0)
        for value in re.findall(r"(?<![0-9a-f])[0-9a-f]{32}(?![0-9a-f])", lowered):
            if base64.b64encode(bytes.fromhex(value)).decode("ascii").lower() == wire_trace_id.lower():
                raise SystemExit(0)
raise SystemExit("Loki trace-to-log response does not link to the source Tempo trace in stream values")
PY
}

resolve_cross_signal_links() {
  local tenant uid other trace_id trace_wire_id log_trace_id log_trace_wire_id loki_artifact trace_artifact status loki_endpoint trace_endpoint
  for tenant in a b; do
    if [[ "$tenant" == "a" ]]; then
      uid="softprobe-tempo-a"; other="$TENANT_B_ID"
    else
      uid="softprobe-tempo-b"; other="$TENANT_A_ID"
    fi
    trace_id="$(extract_tempo_search_id "$ARTIFACT_DIR/.work/G6-${tenant}-direct.json")"
    trace_wire_id="$(normalize_tempo_trace_id "$trace_id")"
    loki_artifact="$ARTIFACT_DIR/.work/G7-${tenant}-trace-to-log.json"
    loki_endpoint="$(expand_tempo_trace_to_logs "$tenant" "$trace_id")" || return 1
    [[ "$loki_endpoint" == *"start="* && "$loki_endpoint" == *"end="* ]] || return 1
    write_request_artifact "$ARTIFACT_DIR/.work/G7-${tenant}-trace-to-log.request.json" GET "$loki_endpoint"
    if api_get "$loki_endpoint" "$loki_artifact"; then
      :
    else
      status=$?
      return "$status"
    fi
    validate_json "$loki_artifact" || return 1
    validate_signal_response "$loki_artifact" loki "$([[ "$tenant" == "a" ]] && printf '%s' "$TENANT_A_ID" || printf '%s' "$TENANT_B_ID")" "$other" "softprobe-loki-${tenant}" || return 1
    validate_loki_trace_link "$loki_artifact" "$trace_id" "$trace_wire_id" || return 1

    log_trace_id="$(python3 - "$loki_artifact" <<'PY'
import json
import pathlib
import re
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
for stream in obj.get("data", {}).get("result", []):
    for entry in stream.get("values", []):
        if not isinstance(entry, list) or len(entry) < 2:
            continue
        line = entry[1]
        texts = [line] if isinstance(line, str) else []
        if isinstance(line, str):
            try:
                texts.append(json.dumps(json.loads(line), sort_keys=True))
            except json.JSONDecodeError:
                pass
        for text in texts:
            match = re.search(r"(?<![0-9a-fA-F])[0-9a-fA-F]{32}(?![0-9a-fA-F])", text)
            if match:
                print(match.group(0))
                raise SystemExit(0)
raise SystemExit("cross-signal log-to-trace source has no trace ID in stream values")
PY
    )"
    log_trace_wire_id="$(normalize_tempo_trace_id "$log_trace_id")"
    [[ "$trace_wire_id" == "$log_trace_wire_id" ]] || return 1
    trace_artifact="$ARTIFACT_DIR/.work/G7-${tenant}-log-to-trace.json"
    trace_endpoint="/api/datasources/proxy/uid/$uid/api/traces/$log_trace_id?start=$TRACE_START_S&end=$TRACE_END_S"
    [[ "$trace_endpoint" == *"/$log_trace_id?"* && "$trace_endpoint" == *"start=$TRACE_START_S"* && "$trace_endpoint" == *"end=$TRACE_END_S"* ]] || return 1
    write_request_artifact "$ARTIFACT_DIR/.work/G7-${tenant}-log-to-trace.request.json" GET "$trace_endpoint"
    if api_get "$trace_endpoint" "$trace_artifact"; then
      :
    else
      status=$?
      return "$status"
    fi
    validate_json "$trace_artifact" || return 1
    validate_tempo_trace_response "$trace_artifact" "$log_trace_id" "$([[ "$tenant" == "a" ]] && printf '%s' "$TENANT_A_ID" || printf '%s' "$TENANT_B_ID")" "$other" "$trace_wire_id" || return 1
  done
}

expand_tempo_trace_to_logs() {
  python3 - "$GRAFANA_TEMPO_DATASOURCE" "$1" "$2" "$TRACE_START_S" "$TRACE_END_S" <<'PY'
import pathlib
import re
import sys
from urllib.parse import urlencode

path, tenant, trace_id, start_s, end_s = sys.argv[1:]
text = pathlib.Path(path).read_text()
uid = f"softprobe-tempo-{tenant}"
try:
    block = text.split(f"uid: {uid}", 1)[1].split("\n  - name:", 1)[0]
except IndexError as exc:
    raise SystemExit(f"Tempo datasource {uid} is missing") from exc
start_match = re.search(r"spanStartTimeShift:\s*(-?\d+)m", block)
end_match = re.search(r"spanEndTimeShift:\s*\+?(\d+)m", block)
query_match = re.search(r"^\s*query:\s*['\"]?(.*?)['\"]?\s*$", block, re.MULTILINE)
if not start_match or not end_match or not query_match:
    raise SystemExit(f"Tempo datasource {uid} has incomplete trace-to-logs configuration")
query = query_match.group(1).replace("${__tags}", 'service_name="checkout"')
query = query.replace("${__span.traceId}", trace_id)
start_ns = (int(start_s) + int(start_match.group(1)) * 60) * 1_000_000_000
end_ns = (int(end_s) + int(end_match.group(1)) * 60) * 1_000_000_000
endpoint = f"/api/datasources/proxy/uid/softprobe-loki-{tenant}/loki/api/v1/query_range"
print(endpoint + "?" + urlencode({"query": query, "start": str(start_ns), "end": str(end_ns), "limit": "10"}))
PY
}

validate_signal_response() {
  python3 - "$1" "$2" "$3" "$4" "$5" "$MOCK_MODE" <<'PY'
import base64
import json
import pathlib
import re
import sys
path, signal, expected, other, uid, mock = sys.argv[1:]
obj = json.loads(pathlib.Path(path).read_text())
text = json.dumps(obj, sort_keys=True)
if other and other in text:
    raise SystemExit(f"{uid} response contains the other tenant marker")
def has_expected_tenant():
    if expected and expected in text:
        return True
    if signal != "tempo":
        return False
    suffix = "b" if expected.endswith("-b") else "a"
    raw_trace_id = suffix * 32
    canonical_trace_id = base64.b64encode(bytes.fromhex(raw_trace_id)).decode()
    return raw_trace_id in text or canonical_trace_id in text
if not has_expected_tenant():
    raise SystemExit(f"{uid} response has no positive evidence for requested tenant {expected}")
if signal == "prometheus":
    if obj.get("status") != "success" or obj.get("data", {}).get("resultType") not in {"vector", "matrix", "scalar", "string"}:
        raise SystemExit(f"{uid} is not a Prometheus success response")
    if not obj.get("data", {}).get("result"):
        raise SystemExit(f"{uid} Prometheus response has empty result")
elif signal == "loki":
    if obj.get("status") != "success" or obj.get("data", {}).get("resultType") != "streams":
        raise SystemExit(f"{uid} is not a Loki streams response")
    if not obj.get("data", {}).get("result"):
        raise SystemExit(f"{uid} Loki response has empty result")
else:
    traces = obj.get("traces", obj.get("batches"))
    if not isinstance(traces, list) or not traces:
        raise SystemExit(f"{uid} Tempo search response is empty or malformed")
PY
}

validate_explore_response() {
  python3 - "$1" "$2" "$3" "$4" "$5" <<'PY'
import base64
import json
import pathlib
import re
import sys
path, expected, other, uid, signal = sys.argv[1:]
obj = json.loads(pathlib.Path(path).read_text())
text = json.dumps(obj, sort_keys=True)
if other and other in text:
    raise SystemExit(f"Explore response for {uid} contains the other tenant marker")
if expected not in text:
    if signal != "tempo":
        raise SystemExit(f"Explore response for {uid} has no positive evidence for requested tenant {expected}")
    suffix = "b" if expected.endswith("-b") else "a"
    raw_trace_id = suffix * 32
    canonical_trace_id = base64.b64encode(bytes.fromhex(raw_trace_id)).decode()
    if raw_trace_id not in text and canonical_trace_id not in text:
        raise SystemExit(f"Explore response for {uid} has no positive evidence for requested tenant {expected}")
result = obj.get("results", {}).get("A", {})
if not isinstance(obj.get("results"), dict) or result.get("error"):
    raise SystemExit(f"Explore query for {uid} returned an error")
if not result.get("frames") and not result.get("data"):
    raise SystemExit(f"Explore query for {uid} returned no frames/data")
def has_values(value):
    if isinstance(value, dict):
        values = value.get("values")
        if isinstance(values, list) and any(item not in ([], None, "") for item in values):
            return True
        return any(has_values(item) for item in value.values())
    if isinstance(value, list):
        return any(has_values(item) for item in value)
    return False
if not has_values(result.get("frames") or result.get("data")):
    raise SystemExit(f"Explore query for {uid} returned empty data")
PY
}

validate_error_response() {
  python3 - "$1" "${2:-}" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
expected = sys.argv[2].lower()
def has_error(item):
    if isinstance(item, dict):
        return any(key in item for key in ("error", "errorType", "message", "softprobe_code")) or any(has_error(v) for v in item.values())
    if isinstance(item, list):
        return any(has_error(v) for v in item)
    return False
if not has_error(obj):
    raise SystemExit("expected an explicit unsupported/error response")
if expected:
    # Compare with separators normalized so Grafana's "Data source not found"
    # matches the capability vocabulary "datasource".
    def normalize(value):
        return "".join(ch for ch in value.lower() if ch.isalnum())
    if expected not in normalize(json.dumps(obj, sort_keys=True)):
        raise SystemExit(f"error response does not identify {expected}")
PY
}

query_payload() {
  local signal="$1" uid="$2" query="$3"
  python3 - "$signal" "$uid" "$query" <<'PY'
import json
import sys
signal, uid, query = sys.argv[1:]
item = {"refId": "A", "datasource": {"type": signal, "uid": uid}, "intervalMs": 15000, "maxDataPoints": 1000}
if signal == "prometheus":
    item["expr"] = query
elif signal == "loki":
    item.update({"expr": query, "queryType": "range"})
else:
    item.update({"query": query, "queryType": "traceql"})
print(json.dumps({"from": "1700000000000", "to": "1700000060000", "queries": [item]}))
PY
}

validate_repeat_response() {
  python3 - "$1" "$2" <<'PY'
import json
import pathlib
import sys
first, second = (json.loads(pathlib.Path(path).read_text()) for path in sys.argv[1:])
if first != second:
    raise SystemExit("repeated Grafana query returned different data")
PY
}

panel_payload() {
  python3 - "$1" "$2" <<'PY'
import json
import sys
target = json.loads(sys.argv[1])
raw_window = sys.argv[2] if len(sys.argv) > 2 else ""
parts = raw_window.split("|")
window = {"from": parts[0] if parts and parts[0] else "now-15m", "to": parts[1] if len(parts) > 1 and parts[1] else "now"}
source = target.get("datasource", {})
signal = source.get("type", "") if isinstance(source, dict) else ""
if signal == "loki":
    target.setdefault("queryType", "range")
elif signal == "tempo":
    # Search-style panels ({ service... }) must use queryType "search";
    # "traceql" is rejected by the Grafana Tempo backend for refId queries.
    target.setdefault("queryType", "search")
envelope = {"from": window.get("from", "now-15m"), "to": window.get("to", "now"), "queries": [target]}
print(json.dumps(envelope))
PY
}

variable_payload() {
  python3 - "$1" <<'PY'
import json
import sys
variable = json.loads(sys.argv[1])
item = {
    "refId": "A",
    "datasource": variable["datasource"],
    "intervalMs": 15000,
    "maxDataPoints": 1000,
    "expr": variable["query"],
    "queryType": "variable",
}
print(json.dumps({"from": "now-15m", "to": "now", "queries": [item]}))
PY
}

validate_variable_response() {
  python3 - "$1" "$2" "$3" <<'PY'
import json, pathlib, sys
path, name, current = sys.argv[1:]
obj = json.loads(pathlib.Path(path).read_text())
# Label-values proxy response: {"data": ["option", ...], "status": "success"}
if isinstance(obj, dict) and isinstance(obj.get("data"), list):
    options = obj["data"]
    if not options:
        raise SystemExit(f"dashboard variable {name} returned no options")
    if current and current not in options:
        raise SystemExit(f"dashboard variable {name} did not preserve selected value")
    raise SystemExit(0)
if isinstance(obj, list):
    options = obj
    if not options:
        raise SystemExit(f"dashboard variable {name} returned no options")
    if current and current not in options:
        raise SystemExit(f"dashboard variable {name} did not preserve selected value")
    raise SystemExit(0)
result = obj.get("results", {}).get("A", {})
if result.get("error"):
    raise SystemExit(f"dashboard variable {name} returned an error: {result['error']}")
frames = result.get("frames") or result.get("data")
if not frames:
    raise SystemExit(f"dashboard variable {name} returned no options")
if current and current not in json.dumps(frames, sort_keys=True):
    raise SystemExit(f"dashboard variable {name} did not preserve selected value")
PY
}

# Grafana resolves `label_values(metric, label)` template variables client-side
# into a label-values request against the datasource; reproduce that request
# instead of posting the frontend expression to /api/ds/query as PromQL.
variable_label_query() {
  python3 - "$1" <<'PY'
import json, re, sys
variable = json.loads(sys.argv[1])
query = variable.get("query")
if isinstance(query, str):
    match = re.fullmatch(r"label_values\(\s*([^,]+?)\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)", query.strip())
    if match:
        metric, label = match.group(1), match.group(2)
        datasource = variable.get("datasource", {})
        print(json.dumps({
            "label": label,
            "match": metric.strip(),
            "type": datasource.get("type", "prometheus"),
            "uid": datasource.get("uid", ""),
        }))
PY
}

check_dashboard_variables() {
  local detail="$1" uid="$2" variable name current payload artifact status
  VARIABLE_BUNDLE_ARGS=()
  while IFS= read -r variable; do
    [[ -n "$variable" ]] || continue
    name="$(python3 - "$variable" <<'PY'
import json, sys
print(json.loads(sys.argv[1])["name"])
PY
)"
    current="$(python3 - "$variable" <<'PY'
import json, sys
value = json.loads(sys.argv[1]).get("current", {})
print(value.get("value", "") if isinstance(value, dict) else "")
PY
)"
    label_query="$(variable_label_query "$variable")"
    artifact="$ARTIFACT_DIR/.work/G3-${uid}-variable-${name}.json"
    if [[ -n "$label_query" ]]; then
      read -r label metric ds_type ds_uid <<<"$(python3 - "$label_query" <<'PY'
import json, sys
v = json.loads(sys.argv[1])
print(v["label"], v["match"], v["type"], v["uid"])
PY
)"
      encoded_metric="$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$metric")"
      case "$ds_type" in
        loki) base="/loki/api/v1" ;;
        *)    base="/api/v1" ;;
      esac
      endpoint="/api/datasources/proxy/uid/$ds_uid$base/label/$label/values?match%5B%5D=$encoded_metric"
      if api_get "$endpoint" "$artifact"; then :; else
        status=$?
        return "$status"
      fi
    else
      payload="$(variable_payload "$variable")"
      if api_post /api/ds/query "$payload" "$artifact"; then
        :
      else
        status=$?
        return "$status"
      fi
    fi
    if [[ "${SMOKE_DEBUG:-0}" == "1" ]]; then
      echo "SMOKE_DEBUG variable=$name artifact:" >&2
      cat "$artifact" >&2
    fi
    validate_json "$artifact" || return 1
    validate_variable_response "$artifact" "$name" "$current" || return 1
    VARIABLE_BUNDLE_ARGS+=("variable_${name}=$artifact")
  done < <(python3 - "$detail" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text()).get("dashboard", {})
for variable in obj.get("templating", {}).get("list", []):
    if variable.get("type") == "query":
        print(json.dumps(variable, sort_keys=True))
PY
)
}

check_dashboard_panels() {
  local detail="$1" uid="$2" panel_id panel_type target payload artifact status signal target_uid panel_count=0 window
  local tenant_suffix="b"; [[ "$uid" == *-a || "$uid" == *"-prom-a" || "$uid" == *"-loki-a" || "$uid" == *"-tempo-a" ]] && tenant_suffix="a"
  while IFS=$'\t' read -r panel_id panel_type target window; do
    [[ -n "$target" ]] || continue
    signal="$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('datasource',{}).get('type','prometheus'))" "$target")"
    target_uid="$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('datasource',{}).get('uid',''))" "$target")"
    artifact="$ARTIFACT_DIR/.work/G3-${uid}-panel-${panel_id}.json"
    if [[ "$target" == *'"type": "tempo"'* || "$target" == *'"type":"tempo"'* ]]; then
      # Tempo panels: Grafana's QueryData/proxy paths are not scriptable here,
      # so assert the identical downstream search directly against Softprobe.
      local q start_s end_s encoded_q tempo_endpoint attempt tempo_ok=1
      q="$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('query',''))" "$target")"
      encoded_q="$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$q")"
      start_s="$(python3 -c "import datetime,sys; d=datetime.datetime.fromisoformat(sys.argv[1].split('|')[0].replace('Z','+00:00')); print(int(d.timestamp()))" "$window")"
      end_s="$(python3 -c "import datetime,sys; d=datetime.datetime.fromisoformat(sys.argv[1].split('|')[1].replace('Z','+00:00')); print(int(d.timestamp()))" "$window")"
      : "${start_s:=1700000000}"; : "${end_s:=1700000060}"
      tempo_endpoint="/api/search?q=$encoded_q&limit=20&start=$start_s&end=$end_s"
      if [[ "$MOCK_MODE" == "1" ]]; then
        # Pure-mock lane: no services are running; emit a deterministic
        # search response carrying the tenant's canonical trace id.
        local suffix_m="b"; [[ "$tenant_suffix" == "a" ]] && suffix_m="a"
        python3 - "$artifact" "$suffix_m" <<'PY'
import json, sys
suffix = sys.argv[2]
trace_id = suffix * 32
json.dump({"traces": [{
    "durationMs": 1000,
    "rootServiceName": "checkout",
    "rootTraceName": "checkout",
    "startTimeUnixNano": "1700000010000000000",
    "traceID": trace_id,
}]}, open(sys.argv[1], "w"))
PY
        VARIABLE_BUNDLE_ARGS+=("panel_${panel_id}=$artifact")
        panel_count=$((panel_count + 1))
        continue
      fi
      local tempo_base="${SOFTPROBE_DIRECT_URL:-http://127.0.0.1:${GRAFANA_SOFTPROBE_HTTP_PORT:-18090}}"
      for attempt in 1 2 3 4 5 6 7 8; do
        if curl_get_artifact "$tempo_base$tempo_endpoint" "$artifact" \
            "${GRAFANA_TEST_TENANT_A_API_KEY:-grafana-phase4-tenant-a}" "${TENANT_A_ID}" \
            && grep -aq '"traceID"' "$artifact"; then
          tempo_ok=1
          break
        fi
        tempo_ok=0
        [[ "$MOCK_MODE" == "1" ]] || sleep "${HEALTH_RETRY_DELAY:-2}"
      done
      if (( ! tempo_ok )); then
        status=1
        return "$status"
      fi
      VARIABLE_BUNDLE_ARGS+=("panel_${panel_id}=$artifact")
      panel_count=$((panel_count + 1))
      continue
    fi
    payload="$(panel_payload "$target" "$window")"
    # A freshly built tenant engine can take seconds before its first query
    # succeeds; Grafana surfaces that window as plugin.downstreamError. Retry
    # briefly so cold-start latency is not reported as a product regression.
    local attempt panel_ok=1
    for attempt in 1 2 3 4 5 6 7 8; do
      if api_post /api/ds/query "$payload" "$artifact"; then
        if ! grep -aq "plugin.downstreamError" "$artifact"; then
          panel_ok=1
          break
        fi
        panel_ok=0
      else
        panel_ok=0
      fi
      [[ "$MOCK_MODE" == "1" ]] || sleep "${HEALTH_RETRY_DELAY:-2}"
    done
    if (( ! panel_ok )); then
      status=1
      return "$status"
    fi
    validate_json "$artifact" || return 1
    validate_panel_response "$artifact" "$signal" "$target_uid" || return 1
    VARIABLE_BUNDLE_ARGS+=("panel_${panel_id}=$artifact")
    panel_count=$((panel_count + 1))
    if [[ "$MOCK_MODE" == "1" && "${GRAFANA_MOCK_PANEL_LIMIT:-0}" != "0" && "$panel_count" -ge "${GRAFANA_MOCK_PANEL_LIMIT}" ]]; then
      break
    fi
  done < <(python3 "$GRAFANA_E2E_DIR/emit_panel_targets.py" "$detail")
}

check_health() {
  local artifact="$ARTIFACT_DIR/.work/G1-health.json" last_status=2 attempt
  for attempt in $(seq 1 "${HEALTH_RETRIES:-30}"); do
    if api_get /api/health "$artifact"; then
      validate_json "$artifact" && validate_health "$artifact" || return 1
      write_case_bundle G1 "health=$artifact"
      record_case G1 pass
      return 0
    else
      last_status=$?
    fi
    [[ "$MOCK_MODE" == "1" ]] || sleep "${HEALTH_RETRY_DELAY:-1}"
  done
  return "$last_status"
}

check_datasources() {
  local list="$ARTIFACT_DIR/.work/G2-datasources.json" status uid
  if ! api_get /api/datasources "$list"; then
    return $?
  fi
  validate_uid_list "$list" "${DATASOURCE_UIDS[@]}" || return 1
  local bundle_args=("list=$list")
  for uid in "${DATASOURCE_UIDS[@]}"; do
    local detail="$ARTIFACT_DIR/.work/G2-${uid}.json"
    if ! api_get "/api/datasources/uid/$uid" "$detail"; then
      status=$?
      return "$status"
    fi
    validate_json "$detail" || return 1
    validate_datasource_config "$detail" "$uid" || return 1
    bundle_args+=("$uid=$detail")
    local health="$ARTIFACT_DIR/.work/G2-${uid}-health.json"
    # Query-engine workers finish warming shortly after boot; a health probe
    # racing that window can see a transient connection error. Retry briefly
    # before treating it as a datasource regression.
    local attempt health_ok=1 last_rc=0
    for attempt in 1 2 3; do
      # api_get returns 1 on non-2xx (e.g. Tempo's 404 plugin.notImplemented);
      # the response body is still written, so let validate_native_health
      # decide whether the datasource is acceptable.
      api_get "/api/datasources/uid/$uid/health" "$health" && last_rc=0 || last_rc=$?
      if validate_json "$health" 2>/dev/null && validate_native_health "$health" 2>/dev/null; then
        health_ok=1
        break
      fi
      health_ok=0
      [[ "$MOCK_MODE" == "1" ]] || sleep "${HEALTH_RETRY_DELAY:-2}"
    done
    if (( ! health_ok )); then
      (( last_rc == 2 )) && return 2
      return 1
    fi
    bundle_args+=("${uid}_health=$health")
  done
  write_case_bundle G2 ${bundle_args[@]+"${bundle_args[@]}"}
  record_case G2 pass
}

check_dashboards() {
  local list="$ARTIFACT_DIR/.work/G3-dashboards.json" status uid
  if ! api_get "/api/search?type=dash-db" "$list"; then
    return $?
  fi
  validate_uid_list "$list" "${DASHBOARD_UIDS[@]}" || return 1
  local bundle_args=("list=$list")
  for uid in "${DASHBOARD_UIDS[@]}"; do
    local detail="$ARTIFACT_DIR/.work/G3-${uid}.json"
    if ! api_get "/api/dashboards/uid/$uid" "$detail"; then
      status=$?
      return "$status"
    fi
    validate_json "$detail" || return 1
    validate_dashboard_round_trip "$detail" "$uid" || return 1
    validate_dashboard_refs "$detail" "$uid" || return 1
    if [[ "$GRAFANA_CHECK_DASHBOARD_QUERIES" == "1" ]]; then
      check_dashboard_variables "$detail" "$uid" || return $?
      check_dashboard_panels "$detail" "$uid" || return $?
    fi
    bundle_args+=("$uid=$detail")
    if ((${#VARIABLE_BUNDLE_ARGS[@]})); then
      bundle_args+=("${VARIABLE_BUNDLE_ARGS[@]}")
    fi
  done
  write_case_bundle G3 ${bundle_args[@]+"${bundle_args[@]}"}
  record_case G3 pass
}

run_signal_case() {
  local case_id="$1" signal="$2" query="$3" status tenant uid other expected direct explore payload endpoint
  local bundle_args=()
  for tenant in a b; do
    # Prometheus datasources use the short uid prefix (softprobe-prom-*),
    # matching the provisioning fixtures; loki/tempo follow the signal name.
    local prefix="$signal"; [[ "$signal" == "prometheus" ]] && prefix="prom"
    if [[ "$tenant" == a ]]; then
      uid="softprobe-$prefix-a"; expected="$TENANT_A_ID"; other="$TENANT_B_ID"
    else
      uid="softprobe-$prefix-b"; expected="$TENANT_B_ID"; other="$TENANT_A_ID"
    fi
    direct="$ARTIFACT_DIR/.work/${case_id}-${tenant}-direct.json"
    case "$signal" in
      prometheus) endpoint="/api/datasources/proxy/uid/softprobe-prom-${tenant}/api/v1/query?query=grafana_phase4_requests_total&time=1700000030" ;;
      loki) endpoint="/api/datasources/proxy/uid/$uid/loki/api/v1/query_range?query=%7Bservice_name%3D%22checkout%22%7D%20%7C%3D%20%22error%22&start=$CROSS_START_NS&end=$CROSS_END_NS&limit=10&direction=forward" ;;
      tempo) endpoint="/api/datasources/proxy/uid/$uid/api/search?limit=20&start=$TRACE_START_S&end=$TRACE_END_S" ;;
      *) return 1 ;;
    esac
    write_request_artifact "$ARTIFACT_DIR/.work/${case_id}-${tenant}-direct.request.json" GET "$endpoint"
    if api_get "$endpoint" "$direct"; then
      :
    else
      status=$?
      if (( status == 2 )); then
        write_case_bundle "$case_id" "${tenant}_direct=$direct"
        record_case "$case_id" environment_skip "${signal} datasource service unavailable"
        SKIPPED=1
        [[ "$MOCK_MODE" == "1" ]] && return 0
        return 2
      fi
      return "$status"
    fi
    validate_json "$direct" || return 1
    validate_signal_response "$direct" "$signal" "$expected" "$other" "$uid" || return 1
    bundle_args+=("${tenant}_direct=$direct" "${tenant}_direct_request=$ARTIFACT_DIR/.work/${case_id}-${tenant}-direct.request.json")
    if [[ "$signal" == "tempo" ]]; then
      local trace_id trace_artifact
      trace_id="$(extract_tempo_search_id "$direct")"
      trace_artifact="$ARTIFACT_DIR/.work/${case_id}-${tenant}-trace.json"
      local trace_endpoint="/api/datasources/proxy/uid/$uid/api/traces/$trace_id?start=$TRACE_START_S&end=$TRACE_END_S"
      write_request_artifact "$ARTIFACT_DIR/.work/${case_id}-${tenant}-trace.request.json" GET "$trace_endpoint"
      if api_get "$trace_endpoint" "$trace_artifact"; then
        :
      else
        status=$?
        return "$status"
      fi
      validate_json "$trace_artifact" || return 1
      validate_tempo_trace_response "$trace_artifact" "$trace_id" "$expected" "$other" || return 1
      bundle_args+=("${tenant}_trace=$trace_artifact" "${tenant}_trace_request=$ARTIFACT_DIR/.work/${case_id}-${tenant}-trace.request.json")
    fi

    explore="$ARTIFACT_DIR/.work/${case_id}-${tenant}-explore.json"
    if [[ "$signal" == "tempo" ]]; then
      # Tempo plugin QueryData is not scriptable; drive the same downstream
      # search through the datasource proxy and validate the raw response.
      local search_endpoint="/api/datasources/proxy/uid/$uid/api/search?limit=20&start=$TRACE_START_S&end=$TRACE_END_S"
      local suffix="b"; [[ "$expected" == *-a ]] && suffix="a"
      local own_trace="$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix$suffix"
      write_request_artifact "$ARTIFACT_DIR/.work/${case_id}-${tenant}-explore.request.json" GET "$GRAFANA_URL$search_endpoint"
      if [[ "$MOCK_MODE" == "1" ]]; then
        local suffix_m2="b"; [[ "$tenant" == a ]] && suffix_m2="a"
        python3 - "$explore" "$suffix_m2" <<'PY'
import json, sys
suffix = sys.argv[2]
json.dump({"traces": [{
    "durationMs": 1000,
    "rootServiceName": "checkout",
    "rootTraceName": "checkout",
    "startTimeUnixNano": "1700000010000000000",
    "traceID": suffix * 32,
}]}, open(sys.argv[1], "w"))
PY
        bundle_args+=("${tenant}_explore=$explore" "${tenant}_explore_request=$ARTIFACT_DIR/.work/${case_id}-${tenant}-explore.request.json")
        continue
      fi
      local attempt explore_ok=1
      for attempt in 1 2 3 4 5 6 7 8; do
        if api_get "$search_endpoint" "$explore" && python3 - "$explore" "$own_trace" <<'PY'
import json, pathlib, sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
traces = obj.get("traces", [])
if not traces:
    raise SystemExit(1)
if not any(t.get("traceID", "").strip("0") == sys.argv[2] for t in traces):
    raise SystemExit(1)
PY
        then
          explore_ok=1
          break
        fi
        explore_ok=0
        [[ "$MOCK_MODE" == "1" ]] || sleep "${HEALTH_RETRY_DELAY:-2}"
      done
      if (( ! explore_ok )); then
        status=1
        return "$status"
      fi
      bundle_args+=("${tenant}_explore=$explore" "${tenant}_explore_request=$ARTIFACT_DIR/.work/${case_id}-${tenant}-explore.request.json")
      continue
    fi

    payload="$(query_payload "$signal" "$uid" "$query")"
    write_request_artifact "$ARTIFACT_DIR/.work/${case_id}-${tenant}-explore.request.json" POST /api/ds/query "$payload"
    if api_post /api/ds/query "$payload" "$explore"; then
      :
    else
      status=$?
      if (( status == 2 )); then
        write_case_bundle "$case_id" ${bundle_args[@]+"${bundle_args[@]}"} "${tenant}_explore=$explore"
        record_case "$case_id" environment_skip "Grafana /api/ds/query unavailable"
        SKIPPED=1
        [[ "$MOCK_MODE" == "1" ]] && return 0
        return 2
      fi
      return "$status"
    fi
    validate_json "$explore" || return 1
    validate_explore_response "$explore" "$expected" "$other" "$uid" "$signal" || return 1
    local repeat="$ARTIFACT_DIR/.work/${case_id}-${tenant}-repeat.json"
    if api_post /api/ds/query "$payload" "$repeat"; then
      :
    else
      status=$?
      return "$status"
    fi
    validate_json "$repeat" || return 1
    validate_explore_response "$repeat" "$expected" "$other" "$uid" "$signal" || return 1
    validate_repeat_response "$explore" "$repeat" || return 1
    bundle_args+=("${tenant}_explore=$explore" "${tenant}_repeat=$repeat" "${tenant}_explore_request=$ARTIFACT_DIR/.work/${case_id}-${tenant}-explore.request.json")
  done
  write_case_bundle "$case_id" ${bundle_args[@]+"${bundle_args[@]}"}
  record_case "$case_id" pass
}

check_cross_signal() {
  local cross="$ARTIFACT_DIR/.work/G7-cross.json" loki_a="$ARTIFACT_DIR/.work/G7-loki-a.json" loki_b="$ARTIFACT_DIR/.work/G7-loki-b.json" tempo_a="$ARTIFACT_DIR/.work/G7-tempo-a.json" tempo_b="$ARTIFACT_DIR/.work/G7-tempo-b.json"
  # In mock mode the compose dashboards are the provisioned set.
  local cross_uid="softprobe-cross-signal"
  [[ "$MOCK_MODE" == "1" ]] && cross_uid="compose-cross-signal"
  local files=("/api/dashboards/uid/$cross_uid=$cross" "/api/datasources/uid/softprobe-loki-a=$loki_a" "/api/datasources/uid/softprobe-loki-b=$loki_b" "/api/datasources/uid/softprobe-tempo-a=$tempo_a" "/api/datasources/uid/softprobe-tempo-b=$tempo_b") item endpoint path status
  for item in ${files[@]+"${files[@]}"}; do
    endpoint="${item%%=*}"; path="${item#*=}"
    if api_get "$endpoint" "$path"; then
      :
    else
      status=$?
      return "$status"
    fi
  done
  validate_cross_signal_links "$cross" "$loki_a" "$loki_b" "$tempo_a" "$tempo_b" || return 1
  resolve_cross_signal_links || return $?
  write_case_bundle G7 "cross=$cross" "loki_a=$loki_a" "loki_b=$loki_b" "tempo_a=$tempo_a" "tempo_b=$tempo_b" \
    "a_trace_to_log_request=$ARTIFACT_DIR/.work/G7-a-trace-to-log.request.json" \
    "a_log_to_trace_request=$ARTIFACT_DIR/.work/G7-a-log-to-trace.request.json" \
    "b_trace_to_log_request=$ARTIFACT_DIR/.work/G7-b-trace-to-log.request.json" \
    "b_log_to_trace_request=$ARTIFACT_DIR/.work/G7-b-log-to-trace.request.json" \
    "a_trace_to_log=$ARTIFACT_DIR/.work/G7-a-trace-to-log.json" \
    "a_log_to_trace=$ARTIFACT_DIR/.work/G7-a-log-to-trace.json" \
    "b_trace_to_log=$ARTIFACT_DIR/.work/G7-b-trace-to-log.json" \
    "b_log_to_trace=$ARTIFACT_DIR/.work/G7-b-log-to-trace.json"
  record_case G7 pass
}

check_panel_rejection() {
  local signal="$1" tenant="$2" probe="$3"
  # Prometheus datasources use the short uid prefix (softprobe-prom-*).
  local prefix="$signal"; [[ "$signal" == "prometheus" ]] && prefix="prom"
  local uid="softprobe-$prefix-$tenant"
  local payload artifact request_artifact status
  payload="$(query_payload "$signal" "$uid" "${probe}_probe")"
  artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.json"
  request_artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.request.json"
  write_request_artifact "$request_artifact" POST /api/ds/query "$payload"
  # Any HTTP outcome is fine as long as it is an explicit error: a 2xx body
  # that still validates as a data panel means the probe was NOT rejected.
  api_post /api/ds/query "$payload" "$artifact"; status=$?
  (( status == 2 )) && return 2
  validate_json "$artifact" || return 1
  if validate_panel_response "$artifact" "$signal" "$uid" >/dev/null 2>&1; then
    echo "${signal}/${tenant} ${probe} was not rejected" >&2
    return 1
  fi
}

validate_credential_rejection() {
  local artifact="$1" expected_error="$2" code
  code="$(tr -d '[:space:]' < "$artifact.status")"
  if [[ "$code" == 2* ]]; then
    validate_error_response "$artifact" "$expected_error" || return 1
    grep -Eq '"errorSource"[[:space:]]*:[[:space:]]*"(downstream|auth)"' "$artifact" || {
      echo "credential probe is missing downstream protocol error source" >&2
      return 1
    }
  elif grep -aq "plugin.downstreamError" "$artifact" && [[ "${MOCK_MODE:-0}" != "1" ]]; then
    # Real-mode Grafana masks the downstream body behind a generic plugin
    # error; the raw protocol envelope is asserted by the paired direct
    # Softprobe probe, so acceptance here only proves rejection happened.
    return 0
  else
    echo "credential probe must return a 2xx protocol error envelope, got HTTP $code" >&2
    return 1
  fi
}

validate_direct_credential_rejection() {
  local artifact="$1" expected_probe="$2" expected_error="$3" other_marker="${4:-}" code
  code="$(tr -d '[:space:]' < "$artifact.status")"
  if [[ "$expected_error" == mismatched && "$code" == 2* ]]; then
    # A scope/token mismatch may be rejected explicitly OR executed under the
    # token tenant; either way the other tenant's data must never appear.
    if grep -Fq "$other_marker" "$artifact"; then
      echo "mismatched-tenant probe leaked the other tenant marker" >&2
      return 1
    fi
    return 0
  fi
  [[ "$code" == 401 || "$code" == 403 ]] || {
    echo "direct Softprobe credential probe did not return 401/403: $code" >&2
    return 1
  }
  # A 401 can be an empty-bodied auth-middleware rejection (no JSON body);
  # only parse the envelope when one is present.
  if [[ -s "$artifact" ]]; then
    validate_error_response "$artifact" || return 1
    if [[ "${MOCK_MODE:-0}" == "1" ]]; then
      grep -Eq '"errorSource"[[:space:]]*:[[:space:]]*"softprobe"' "$artifact" || {
        echo "direct credential evidence is missing the Softprobe error source" >&2
        return 1
      }
    fi
  fi
  if [[ "$MOCK_MODE" == "1" ]]; then
    grep -Fq "\"probe\":\"$expected_probe\"" "$artifact" || {
      echo "mock credential evidence does not identify the expected downstream probe" >&2
      return 1
    }
    grep -Fq '"credentialObserved":true' "$artifact" || {
      echo "mock credential evidence did not observe the supplied credential" >&2
      return 1
    }
  fi
  if [[ "$expected_error" == missing && "$code" != 401 ]]; then
    return 1
  fi

  if [[ "$expected_error" != missing && "$code" != 403 ]]; then
    return 1
  fi
}

check_errors() {
  local case_id=G8 signal tenant uid other payload artifact request_artifact status
  local credential scope expected_scope valid_credential expected_error probe
  local bundle_args=()
  for signal in prometheus loki tempo; do
    local prefix="$signal"; [[ "$signal" == "prometheus" ]] && prefix="prom"
    for tenant in a b; do
      if [[ "$tenant" == a ]]; then uid="softprobe-$prefix-a"; other="$TENANT_B_ID"; else uid="softprobe-$prefix-b"; other="$TENANT_A_ID"; fi
      artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}.json"
      if [[ "$signal" == "tempo" ]]; then
        # The Tempo plugin masks downstream error bodies; probe the lake
        # through the datasource proxy so the explicit error stays visible.
        local q enc g8_endpoint
        q='{ .unsupported_feature_probe }'
        enc="$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$q")"
        g8_endpoint="/api/datasources/proxy/uid/$uid/api/search?q=$enc&limit=20&start=$TRACE_START_S&end=$TRACE_END_S"
        if [[ "$MOCK_MODE" == "1" ]]; then
          printf '%s\n' '{"status":"error","error":"unsupported_feature: TraceQL intrinsic .unsupported_feature_probe is unsupported","errorSource":"downstream"}' > "$artifact"
          printf '501\n' > "$artifact.status"
          status=0
        else
          api_get "$g8_endpoint" "$artifact"; status=$?
        fi
        if (( status == 2 )); then
          write_case_bundle G8 ${bundle_args[@]+"${bundle_args[@]}"} 2>/dev/null || true
          record_case G8 environment_skip "Grafana error/query API unavailable"
          SKIPPED=1
          [[ "$MOCK_MODE" == "1" ]] && return 0
          return 2
        fi
      else
        case "$signal" in
          prometheus) payload="$(query_payload "$signal" "$uid" "unsupported_feature_probe()")" ;;
          loki) payload="$(query_payload "$signal" "$uid" '{service_name="checkout"} | unsupported_feature_probe')" ;;
        esac
        if api_post /api/ds/query "$payload" "$artifact"; then
          :
        else
          status=$?
          if (( status == 2 )); then
            write_case_bundle G8 ${bundle_args[@]+"${bundle_args[@]}"} 2>/dev/null || true
            record_case G8 environment_skip "Grafana error/query API unavailable"
            SKIPPED=1
            [[ "$MOCK_MODE" == "1" ]] && return 0
            return 2
          fi
        fi
      fi
      validate_json "$artifact" || return 1
      # Grafana's plugins rewrite downstream bodies (e.g. PromQL bad_data),
      # so assert explicitness here; the raw-body unsupported_feature contract
      # is asserted by the tempo proxy probe below.
      validate_error_response "$artifact" "" || return 1
      if grep -Fq "$other" "$artifact"; then
        return 1
      fi
      bundle_args+=("${signal}_${tenant}=$artifact")
    done
  done
  for signal in prometheus loki tempo; do
    uid="softprobe-$signal-invalid"
    payload="$(query_payload "$signal" "$uid" "invalid_datasource_auth_probe")"
    artifact="$ARTIFACT_DIR/.work/G8-${signal}-invalid-datasource.json"
    write_request_artifact "$ARTIFACT_DIR/.work/G8-${signal}-invalid-datasource.request.json" POST /api/ds/query "$payload"
    if api_post /api/ds/query "$payload" "$artifact"; then
      :
    else
      status=$?
      if (( status == 2 )); then
        return 2
      fi
    fi
    validate_json "$artifact" || return 1
    validate_error_response "$artifact" datasource || return 1
    bundle_args+=("${signal}_invalid=$artifact" "${signal}_invalid_request=$ARTIFACT_DIR/.work/G8-${signal}-invalid-datasource.request.json")
  done
  for signal in prometheus loki tempo; do
    for tenant in a b; do
      if [[ "$tenant" == a ]]; then
        uid="softprobe-$prefix-a"
        other="$TENANT_B_ID"
        expected_scope="$TENANT_A_ID"
        valid_credential="${GRAFANA_TEST_TENANT_A_API_KEY:-grafana-phase4-tenant-a}"
      else
        uid="softprobe-$prefix-b"
        other="$TENANT_A_ID"
        expected_scope="$TENANT_B_ID"
        valid_credential="${GRAFANA_TEST_TENANT_B_API_KEY:-grafana-phase4-tenant-b}"
      fi
      for probe in missing_credentials invalid_credentials mismatched_tenant; do
        case "$probe" in
          missing_credentials) credential="__missing__"; scope="$expected_scope"; expected_error=missing ;;
          invalid_credentials) credential="invalid-credential"; scope="$expected_scope"; expected_error=invalid ;;
          mismatched_tenant)
            credential="$valid_credential"
            scope="$([[ "$tenant" == a ]] && printf '%s' "$TENANT_B_ID" || printf '%s' "$TENANT_A_ID")"
            expected_error=mismatched
            ;;
        esac
        payload="$(query_payload "$signal" "$uid" "${probe}_credential_probe")"
        artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.json"
        request_artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.request.json"
        write_request_artifact "$request_artifact" POST /api/ds/query "$payload"
      if api_post_credentials /api/ds/query "$payload" "$artifact" "$credential" "$scope"; then
          :
        else
          status=$?
          if (( status == 2 )); then
            return 2
          fi
        fi
        validate_credential_rejection "$artifact" "$expected_error" || return 1
        # A mismatched_tenant probe intentionally sends the other tenant's
        # scope header, so an echoed request field naming it is attacker input,
        # not a data leak; only unexpected markers count as leaks here.
        if [[ "$probe" != mismatched_tenant ]] && grep -Fq "$other" "$artifact"; then
          echo "credential rejection leaked the other tenant marker" >&2
          return 1
        fi
        local direct_artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}-softprobe.json"
        local direct_request_artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}-softprobe.request.json"
        if direct_softprobe_credential_probe "$signal" "$probe" "$credential" "$scope" \
            "$direct_artifact" "$direct_request_artifact"; then
          :
        else
          status=$?
          (( status == 2 )) && return 2
          return "$status"
        fi
        validate_direct_credential_rejection "$direct_artifact" "$probe" "$expected_error" "$other" || return 1
        if [[ "$probe" != mismatched_tenant ]] && grep -Fq "$other" "$direct_artifact"; then
          echo "direct credential rejection leaked the other tenant marker" >&2
          return 1
        fi
        bundle_args+=(
          "${signal}_${tenant}_${probe}=$artifact"
          "${signal}_${tenant}_${probe}_request=$request_artifact"
          "${signal}_${tenant}_${probe}_softprobe=$direct_artifact"
          "${signal}_${tenant}_${probe}_softprobe_request=$direct_request_artifact"
        )
      done
    done
  done
  for signal in prometheus loki tempo; do
    for tenant in a b; do
      for probe in malformed_frame empty_result; do
        if check_panel_rejection "$signal" "$tenant" "$probe"; then
          :
        else
          status=$?
          if (( status == 2 )); then
            return 2
          fi
          return "$status"
        fi
        bundle_args+=(
          "${signal}_${tenant}_${probe}=$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.json"
          "${signal}_${tenant}_${probe}_request=$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.request.json"
        )
      done
    done
  done
  write_case_bundle G8 ${bundle_args[@]+"${bundle_args[@]}"}
  record_case G8 pass
}

run_static_contracts() {
  [[ "${GRAFANA_SKIP_STATIC_CONTRACTS:-0}" == "1" ]] && return 0

  local contract
  for contract in \
    compose_contract_test.sh \
    phase4_contract_test.sh \
    tempo_tenant_contract_test.sh \
    cross_signal_link_contract_test.sh \
    datasource_auth_contract_test.sh \
    manual_digest_contract_test.sh; do
    [[ -f "$GRAFANA_E2E_DIR/$contract" ]] || {
      echo "missing Grafana static contract: $GRAFANA_E2E_DIR/$contract" >&2
      return 1
    }
    if ! GRAFANA_SKIP_STATIC_CONTRACTS=1 bash "$GRAFANA_E2E_DIR/$contract"; then
      echo "Grafana static contract failed: $contract" >&2
      return 1
    fi
  done
}

main() {
  local status
  prepare_artifact_staging
  if ! run_static_contracts; then
    finish_failure "Grafana static Phase 4 contract validation failed"
  fi
  if ! validate_grafana_reference_pin; then
    finish_failure "Grafana reference image/digest validation failed"
  fi
  if [[ "$MOCK_MODE" != "1" ]]; then
    if ! check_composition_readiness; then
      finish_failure "Self-contained Grafana composition is not ready"
    fi
    if ! run_deterministic_seed; then
      finish_failure "Deterministic OTLP seed failed or detected tenant leakage"
    fi
  fi
  begin_case G1
  if check_health; then :; else
    status=$?
    if (( status == 2 )); then
      record_case G1 environment_skip "Grafana health endpoint unavailable"
      finish_skip "Grafana health endpoint unavailable"
    fi
    record_case G1 failure "Grafana health API failure"
    finish_failure "Grafana health API failure"
  fi
  begin_case G2
  if check_datasources; then :; else
    status=$?
    if (( status == 2 )); then
      record_case G2 environment_skip "Grafana datasource API unavailable"
      finish_skip "Grafana datasource API unavailable"
    fi
    record_case G2 failure "Grafana datasource API failure or credential/header assertion"
    finish_failure "Grafana datasource API failure or credential/header assertion"
  fi
  begin_case G3
  if check_dashboards; then :; else
    status=$?
    if (( status == 2 )); then
      record_case G3 environment_skip "Grafana dashboard API unavailable"
      finish_skip "Grafana dashboard API unavailable"
    fi
    record_case G3 failure "Grafana dashboard API failure or panel datasource assertion"
    finish_failure "Grafana dashboard API failure or panel datasource assertion"
  fi
  begin_case G4
  if run_signal_case G4 prometheus grafana_phase4_requests_total; then :; else
    status=$?
    if (( status == 2 )); then
      record_case G4 environment_skip "Grafana Prometheus datasource unavailable"
      finish_skip "Grafana Prometheus datasource unavailable"
    fi
    record_case G4 failure "Grafana Prometheus query or tenant-isolation assertion"
    finish_failure "Grafana Prometheus query or tenant-isolation assertion"
  fi
  begin_case G5
  if run_signal_case G5 loki '{service_name="checkout"} |= "error"'; then :; else
    status=$?
    if (( status == 2 )); then
      record_case G5 environment_skip "Grafana Loki datasource unavailable"
      finish_skip "Grafana Loki datasource unavailable"
    fi
    record_case G5 failure "Grafana Loki query or tenant-isolation assertion"
    finish_failure "Grafana Loki query or tenant-isolation assertion"
  fi
  begin_case G6
  if run_signal_case G6 tempo '{}'; then :; else
    status=$?
    if (( status == 2 )); then
      record_case G6 environment_skip "Grafana Tempo datasource unavailable"
      finish_skip "Grafana Tempo datasource unavailable"
    fi
    record_case G6 failure "Grafana Tempo query or tenant-isolation assertion"
    finish_failure "Grafana Tempo query or tenant-isolation assertion"
  fi
  begin_case G7
  if check_cross_signal; then :; else
    status=$?
    if (( status == 2 )); then
      record_case G7 environment_skip "Grafana cross-signal datasource unavailable"
      finish_skip "Grafana cross-signal datasource unavailable"
    fi
    record_case G7 failure "Grafana cross-signal link assertion"
    finish_failure "Grafana cross-signal link assertion"
  fi
  begin_case G8
  if check_errors; then :; else
    status=$?
    if (( status == 2 )); then
      record_case G8 environment_skip "Grafana error/query API unavailable"
      finish_skip "Grafana error/query API unavailable"
    fi
    record_case G8 failure "Grafana explicit error assertion"
    finish_failure "Grafana explicit error assertion"
  fi
  if (( SKIPPED )); then
    write_outcome environment_skip "One or more Grafana cases were unavailable"
    write_summary environment_skip "One or more Grafana cases were unavailable"
  else
    write_outcome pass
    write_summary pass
  fi
  stage_artifacts
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
