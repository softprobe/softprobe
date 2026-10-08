#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
SCRIPT="$ROOT_DIR/scripts/grafana-system-smoke.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/grafana-redaction-test.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

run_bounded() {
    local seconds="$1"
    shift
    local child elapsed=0
    "$@" &
    child=$!
    while kill -0 "$child" 2>/dev/null; do
        if (( elapsed >= seconds )); then
            kill -TERM "$child" 2>/dev/null || true
            wait "$child" 2>/dev/null || true
            printf 'bounded Grafana mock test exceeded %ss\n' "$seconds" >&2
            return 124
        fi
        sleep 1
        ((elapsed += 1))
    done
    wait "$child"
}

export GRAFANA_ADMIN_USER="grafana-smoke-user"
export GRAFANA_ADMIN_PASSWORD="grafana-admin-password-123"
export SOFTPROBE_API_KEY="softprobe-global-api-key-456"
export SOFTPROBE_TENANT_A_API_KEY="softprobe-tenant-a-api-key-789"
export SOFTPROBE_TENANT_B_API_KEY="softprobe-tenant-b-api-key-012"
export SOFTPROBE_TENANT_A_TOKEN="softprobe-tenant-a-token-345"
export SOFTPROBE_TENANT_B_TOKEN="softprobe-tenant-b-token-678"

ci_dashboard_checks="$(CI=1 MOCK=1 ARTIFACT_DIR="$TMP_DIR/ci-defaults" bash -c 'source "$1"; printf "%s" "$GRAFANA_CHECK_DASHBOARD_QUERIES"' _ "$SCRIPT")"
if [[ "$ci_dashboard_checks" != "1" ]]; then
    printf 'expected CI Grafana smoke to enable dashboard/query assertions, got %q\n' "$ci_dashboard_checks" >&2
    exit 1
fi
local_dashboard_checks="$(env -u CI MOCK=1 ARTIFACT_DIR="$TMP_DIR/local-defaults" bash -c 'source "$1"; printf "%s" "$GRAFANA_CHECK_DASHBOARD_QUERIES"' _ "$SCRIPT")"
if [[ "$local_dashboard_checks" != "0" ]]; then
    printf 'expected local Grafana smoke default to remain opt-in, got %q\n' "$local_dashboard_checks" >&2
    exit 1
fi

sample="$TMP_DIR/sample-artifact.txt"
cat >"$sample" <<'EOF'
{"fixture_id":"grafana-redaction-fixture","authorization":"Bearer softprobe-tenant-a-token-345","api_key":"softprobe-tenant-a-api-key-789","password":"grafana-admin-password-123","nested":{"client_secret":"softprobe-global-api-key-456","tenant_token":"softprobe-tenant-b-token-678"},"url":"https://grafana.invalid/api/query?api_key=softprobe-global-api-key-456&access_token=softprobe-tenant-a-token-345&tenant_id=tenant-a","tenant_id":"tenant-a"}
Authorization: Bearer softprobe-tenant-b-token-678
query=https://grafana.invalid/api/query?password=grafana-admin-password-123&tenant_id=tenant-b
X-API-Key: softprobe-global-api-key-456
EOF

redacted="$TMP_DIR/redacted.txt"
ARTIFACT_DIR="$TMP_DIR/redaction-artifacts" bash -c 'source "$1"; redact < "$2"' _ "$SCRIPT" "$sample" >"$redacted"

python3 - "$redacted" <<'PY'
import pathlib
import sys

text = pathlib.Path(sys.argv[1]).read_text()
secrets = (
    "grafana-admin-password-123",
    "softprobe-global-api-key-456",
    "softprobe-tenant-a-api-key-789",
    "softprobe-tenant-b-api-key-012",
    "softprobe-tenant-a-token-345",
    "softprobe-tenant-b-token-678",
)
leaked = [secret for secret in secrets if secret in text]
if leaked:
    raise SystemExit(f"redaction leaked credential values: {', '.join(leaked)}")
for marker in ('grafana-redaction-fixture', '[REDACTED]'):
    if marker not in text:
        raise SystemExit(f"redaction did not preserve or emit expected marker: {marker}")
if '"tenant_id":"tenant-a"' not in text and '"tenant_id": "tenant-a"' not in text:
    raise SystemExit("redaction did not preserve the tenant-a marker")
if 'tenant_id=tenant-b' not in text:
    raise SystemExit("redaction did not preserve the tenant-b query marker")
PY

invalid_pin_dir="$TMP_DIR/invalid-pin"
set +e
MOCK=1 GRAFANA_REFERENCE_IMAGE="grafana/grafana:11.1.0" ARTIFACT_DIR="$invalid_pin_dir" \
    bash -c 'source "$1"; validate_grafana_reference_pin' _ "$SCRIPT"
invalid_pin_status=$?
set -e
if (( invalid_pin_status != 1 )); then
    printf 'expected invalid Grafana image pin to fail with exit 1, got exit %d\n' "$invalid_pin_status" >&2
    exit 1
fi

mock_dir="$TMP_DIR/mock"
mkdir -p "$mock_dir/.work"
printf 'stale artifact\n' > "$mock_dir/stale.txt"
printf 'stale evidence\n' > "$mock_dir/.work/stale.json"
set +e
export GRAFANA_SKIP_STATIC_CONTRACTS=1 MOCK=1 GRAFANA_CHECK_DASHBOARD_QUERIES=1 \
    GRAFANA_MOCK_PANEL_LIMIT=1 \
    GRAFANA_DASHBOARD_DIR="$ROOT_DIR/tests/compat/grafana/dashboards/compose" \
    GRAFANA_DASHBOARD_UIDS='compose-cross-signal compose-loki compose-prom compose-tempo' \
    ARTIFACT_DIR="$mock_dir"
run_bounded "${GRAFANA_MOCK_TIMEOUT_SECONDS:-300}" bash -x "$SCRIPT"
mock_status=$?
set -e
if (( mock_status != 0 )); then
    printf 'expected MOCK Grafana smoke to pass, got exit %d\n' "$mock_status" >&2
    exit 1
fi

python3 - "$mock_dir" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
for case_id in ("G1", "G2", "G3", "G4", "G5", "G6", "G7", "G8"):
    path = root / f"{case_id}.outcome.json"
    if not path.is_file():
        raise SystemExit(f"missing mock outcome artifact: {path}")
    outcome = json.loads(path.read_text()).get("outcome")
    if outcome != "pass":
        raise SystemExit(f"mock {case_id} was not a strict pass: {outcome!r}")

if json.loads((root / "outcome.json").read_text()).get("outcome") != "pass":
    raise SystemExit("mock overall outcome was not pass")
allowed = {"outcome.json", "summary.json", "seed-receipt.json", ".work"}
for name in root.iterdir():
    if name.name not in allowed and not (name.name.startswith("G") and name.name.endswith((".outcome.json", ".raw.json", ".normalized.json"))):
        raise SystemExit(f"unexpected non-allowlisted artifact survived staging: {name.name}")
if (root / "stale.txt").exists() or (root / ".work/stale.json").exists():
    raise SystemExit("stale artifact survived a fresh Grafana run")
unsupported = (root / ".work/G8-prometheus-a.json").read_text()
invalid = (root / ".work/G8-prometheus-invalid-datasource.json").read_text()
if "unsupported" not in unsupported or "datasource" not in invalid:
    raise SystemExit("mock G8 did not retain its required explicit failure evidence")
direct_missing = (root / ".work/G8-prometheus-a-missing_credentials-softprobe.json").read_text()
direct_mismatch = (root / ".work/G8-prometheus-a-mismatched_tenant-softprobe.json").read_text()
if '"errorSource":"softprobe"' not in direct_missing or '"probe":"missing_credentials"' not in direct_missing:
    raise SystemExit("mock G8 missing-credential probe did not retain direct Softprobe evidence")
if '"errorSource":"softprobe"' not in direct_mismatch or '"probe":"mismatched_tenant"' not in direct_mismatch:
    raise SystemExit("mock G8 tenant-mismatch probe did not retain direct Softprobe evidence")
if "sorted-json-response-envelope" not in (root / "G1.normalized.json").read_text():
    raise SystemExit("normalized evidence lost its normalization marker")

tempo = json.loads((root / ".work/G6-a-trace.json").read_text())
groups = tempo.get("batches") or tempo.get("resourceSpans")
if not isinstance(groups, list) or len(groups) < 2:
    raise SystemExit("mock Tempo trace did not preserve distinct ResourceSpans groups")
scope_groups = [scope for group in groups for scope in group.get("scopeSpans", [])]
spans = [span for scope in scope_groups for span in scope.get("spans", [])]
if len(scope_groups) < 2 or len(spans) < 2:
    raise SystemExit("mock Tempo trace did not preserve distinct ScopeSpans/spans")
if not all(span.get("status", {}).get("code") in {"STATUS_CODE_UNSET", "STATUS_CODE_OK", "STATUS_CODE_ERROR"} for span in spans):
    raise SystemExit("mock Tempo trace did not preserve wire status enum values")
if not all(group.get("resource", {}).get("attributes") for group in groups):
    raise SystemExit("mock Tempo trace did not preserve resource attributes")
if not all(scope.get("scope", {}).get("name") for scope in scope_groups):
    raise SystemExit("mock Tempo trace did not preserve instrumentation scopes")
if not any(span.get("parentSpanId") for span in spans):
    raise SystemExit("mock Tempo trace did not preserve parent span IDs")
if not any(span.get("events") for span in spans):
    raise SystemExit("mock Tempo trace did not preserve span events")
if not any(span.get("links") for span in spans):
    raise SystemExit("mock Tempo trace did not preserve span links")
PY

failure_dir="$TMP_DIR/mock-failure"
set +e
GRAFANA_SKIP_STATIC_CONTRACTS=1 MOCK=1 GRAFANA_MOCK_PANEL_LIMIT=1 ARTIFACT_DIR="$failure_dir" MOCK_FIXTURE_DIR="$TMP_DIR/missing-fixtures" \
run_bounded "${GRAFANA_MOCK_TIMEOUT_SECONDS:-300}" bash -x "$SCRIPT"
failure_status=$?
set -e
if (( failure_status != 1 )); then
    printf 'expected broken mock Grafana smoke to fail with exit 1, got exit %d\n' "$failure_status" >&2
    exit 1
fi
python3 - "$failure_dir/outcome.json" <<'PY'
import json
import pathlib
import sys

outcome = json.loads(pathlib.Path(sys.argv[1]).read_text())
if outcome.get("outcome") != "failure":
    raise SystemExit(f"broken mock was not recorded as failure: {outcome}")
PY

printf 'artifact redaction and strict mock G1-G8 regression: PASS\n'
