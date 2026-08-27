#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/compat-release-evidence.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

COMPAT_CONFORMANCE_MODE=mock \
COMPAT_CONFORMANCE_OUT="$tmp_dir/mock" \
  "$ROOT_DIR/scripts/compat/conformance.sh" --mock --protocol prometheus --out "$tmp_dir/mock" >/dev/null

python3 - "$tmp_dir/mock" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])


def assert_false(path):
    value = json.loads(path.read_text())
    if not isinstance(value, dict) or value.get("release_evidence") is not False:
        raise SystemExit(f"{path} must set release_evidence=false")


assert_false(root / "versions.json")
assert_false(root / "outcome.json")
assert_false(root / "artifact-index.json")
for pattern in (
    "*/case.json",
    "*/case_provenance.json",
    "*/diff.json",
    "*/outcome.json",
):
    paths = sorted(root.glob(pattern))
    if not paths:
        raise SystemExit(f"mock run emitted no artifacts for {pattern}")
    for path in paths:
        assert_false(path)

for path in sorted(root.glob("**/execution-receipt.json")):
    receipt = json.loads(path.read_text())
    assert_false(path)
    if receipt.get("validation_only") is not True:
        raise SystemExit(f"{path} must set validation_only=true")
    cases = receipt.get("cases")
    if not isinstance(cases, list) or not cases:
        raise SystemExit(f"{path} must contain receipt case records")
    for index, case in enumerate(cases):
        if not isinstance(case, dict) or case.get("release_evidence") is not False:
            raise SystemExit(f"{path} cases[{index}] must set release_evidence=false")
        if case.get("validation_only") is not True:
            raise SystemExit(f"{path} cases[{index}] must set validation_only=true")

report = root / "report.jsonl"
for line in report.read_text().splitlines():
    if line.strip():
        value = json.loads(line)
        if value.get("release_evidence") is not False:
            raise SystemExit("report.jsonl must set release_evidence=false")
PY

CONFORMANCE_ROOT="$tmp_dir/consolidated"
mkdir -p "$CONFORMANCE_ROOT"
cat >"$CONFORMANCE_ROOT/report.json" <<'JSON'
{
  "mode": "mock",
  "release_evidence": false,
  "jobs": {},
  "required_jobs": [],
  "errors": [],
  "product_regressions": [],
  "unapproved_differences": []
}
JSON

# Non-release mock/drift reports remain structurally valid outside the gate,
# but the same report must be rejected by the release validator.
"$ROOT_DIR/scripts/compat/validate-artifacts.sh" --conformance-root "$CONFORMANCE_ROOT"
if "$ROOT_DIR/scripts/compat/validate-artifacts.sh" --conformance-root "$CONFORMANCE_ROOT" --release-gate >/dev/null 2>&1; then
    echo "mock consolidated report unexpectedly satisfied the release gate" >&2
    exit 1
fi

echo "mock release evidence contract: PASS"
