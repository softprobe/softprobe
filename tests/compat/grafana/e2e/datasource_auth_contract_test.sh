#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
HARNESS="$ROOT_DIR/scripts/grafana-system-smoke.sh"

python3 - "$HARNESS" <<'PY'
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()

# /api/ds/query is authenticated by Grafana's API boundary.  A tenant token
# must not replace that admin credential, or G8 only tests Grafana auth and
# never reaches the configured datasource.
api_start = text.index("api_request() {")
api_end = text.index("api_get()", api_start)
api_body = text[api_start:api_end]
if 'auth_args=(--header "Authorization: Bearer $credential")' in api_body:
    raise SystemExit(
        "G8 tenant credential is being used as Grafana Authorization for /api/ds/query"
    )
if 'auth_args=(--user "$GRAFANA_ADMIN_USER:$GRAFANA_ADMIN_PASSWORD")' not in api_body:
    raise SystemExit("/api/ds/query requests do not retain Grafana admin authentication")

validation_start = text.index("validate_credential_rejection() {")
validation_end = text.index("validate_direct_credential_rejection()", validation_start)
validation_body = text[validation_start:validation_end]
if 'validate_error_response "$artifact" "$expected_error"' not in validation_body:
    raise SystemExit("G8 credential rejection does not validate the protocol error body")
if 'errorSource' not in validation_body:
    raise SystemExit("G8 credential rejection does not validate the downstream error source")
if re.search(r'if \[\[ "\$code" == 2\* \]\]; then.*?elif \[\[ "\$code" != 401', validation_body, re.S):
    raise SystemExit("G8 accepts HTTP 401/403 without protocol-level error evidence")

errors_start = text.index("check_errors() {")
errors_end = text.index("run_static_contracts()", errors_start)
errors_body = text[errors_start:errors_end]
if errors_body.count('api_post /api/ds/query') < 1:
    raise SystemExit("G8 has no Grafana /api/ds/query datasource-auth probe")
if 'invalid_datasource_auth_probe' not in errors_body:
    raise SystemExit("G8 does not probe an invalid datasource credential through Grafana")
if 'validate_error_response "$artifact" datasource' not in errors_body:
    raise SystemExit("G8 invalid-datasource probe does not assert the protocol error")
PY

AUTH_TMP="$(mktemp -d "${TMPDIR:-/tmp}/grafana-auth-header-contract.XXXXXX")"
trap 'rm -rf "$AUTH_TMP"' EXIT
export MOCK=1 ARTIFACT_DIR="$AUTH_TMP" GRAFANA_SKIP_STATIC_CONTRACTS=1
# shellcheck disable=SC1090
source "$HARNESS"
missing_payload="$(query_payload prometheus softprobe-prom-a missing_credentials_credential_probe)"
mismatch_payload="$(query_payload prometheus softprobe-prom-a mismatched_tenant_credential_probe)"
mock_response_post /api/ds/query "$missing_payload" __missing__ "$TENANT_A_ID" > "$AUTH_TMP/missing.json"
mock_response_post /api/ds/query "$mismatch_payload" valid-tenant-a-key "$TENANT_B_ID" > "$AUTH_TMP/mismatch.json"
python3 - "$AUTH_TMP/missing.json" "$AUTH_TMP/mismatch.json" <<'PY'
import json
import pathlib
import sys

missing, mismatch = (json.loads(pathlib.Path(path).read_text()) for path in sys.argv[1:])
for name, obj in (("missing", missing), ("mismatch", mismatch)):
    result = obj.get("results", {}).get("A", {})
    headers = result.get("requestHeaders")
    if not isinstance(headers, dict):
        raise SystemExit(f"MOCK {name} response did not record datasource request headers")
    if "Authorization" not in headers or "X-Scope-OrgID" not in headers:
        raise SystemExit(f"MOCK {name} response did not record Authorization and X-Scope-OrgID")
if missing["results"]["A"]["requestHeaders"]["Authorization"]["present"]:
    raise SystemExit("MOCK missing-credential probe unexpectedly sent Authorization")
if not mismatch["results"]["A"]["requestHeaders"]["Authorization"]["present"]:
    raise SystemExit("MOCK mismatched-tenant probe did not send Authorization")
if mismatch["results"]["A"]["requestHeaders"]["X-Scope-OrgID"]["matchesTenant"]:
    raise SystemExit("MOCK mismatched-tenant probe accepted the wrong X-Scope-OrgID")
PY

printf 'Grafana G8 datasource-auth contract: PASS\n'
