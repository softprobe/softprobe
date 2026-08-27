#!/usr/bin/env bash

set -euo pipefail

# Validates a conformance artifact set for completeness, consistency, and
# release-worthiness.  Fails closed: any missing required artifact, metadata
# mismatch, unknown or missing classification, credential leak, index
# inconsistency, or null/missing side rejects the set.  Mock and
# validation-only artifacts may validate as non-release but never satisfy
# release evidence.

usage() {
	cat >&2 <<'EOF'
Usage: validate-artifacts.sh --root DIR [--release-gate]
       validate-artifacts.sh --conformance-root DIR [--release-gate]

  --root DIR        artifact root containing artifact-index.json,
                    execution-receipt.json, and outcome.json
  --conformance-root DIR
                    consolidated compatibility report root containing
                    report.json and downloaded release evidence
  --release-gate    require the set to certify a release; mock, validation,
                    and drift artifacts never satisfy release evidence
EOF
	exit 2
}

ROOT=
CONFORMANCE_ROOT=
GATE=0

while [ "$#" -gt 0 ]; do
	case "$1" in
		--root)
			[ "$#" -ge 2 ] || usage
			ROOT=$2
			shift 2
			;;
		--conformance-root)
			[ "$#" -ge 2 ] || usage
			CONFORMANCE_ROOT=$2
			shift 2
			;;
		--release-gate)
			GATE=1
			shift
			;;
		*)
			usage
			;;
	esac
done

if [ -n "$ROOT" ] && [ -n "$CONFORMANCE_ROOT" ]; then
	echo "validate-artifacts: ERROR: --root and --conformance-root are mutually exclusive" >&2
	usage
fi

if [ -z "$ROOT" ] && [ -z "$CONFORMANCE_ROOT" ]; then
	echo "validate-artifacts: ERROR: --root DIR or --conformance-root DIR is required" >&2
	usage
fi

if [ -n "$CONFORMANCE_ROOT" ]; then
	if [ ! -d "$CONFORMANCE_ROOT" ]; then
		echo "validate-artifacts: ERROR: conformance root is not a directory: $CONFORMANCE_ROOT" >&2
		exit 2
	fi

	python3 - "$CONFORMANCE_ROOT" "$GATE" <<'PY'
import json
import os
import re
import sys
from urllib.parse import unquote
from pathlib import Path

root = Path(sys.argv[1])
release_gate = sys.argv[2] == "1"
errors = []


def error(path, message):
    errors.append("%s: %s" % (path, message))


BEARER = re.compile(r"\bBearer\s+([^\s,}]+)", re.IGNORECASE)
KV_SECRET = re.compile(
    r"\b(?:authorization|proxy[_-]?authorization|x-api-key|api-key|cookie|set-cookie|access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret|api[_-]?key|apikey|client[_-]?secret)[=:]\s*(\S+)",
    re.IGNORECASE,
)
URL_SECRET = re.compile(
    r"(?i)[?&](?:authorization|api[_-]?key|apikey|access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret|client[_-]?secret|session(?:[_-]?id)?)=([^&#\s\"'<]+)"
)
SECRET_KEY = re.compile(
    r"(?i)\b(?:authorization|proxy[_-]?authorization|x-api-key|api-key|cookie|set-cookie|password|passwd|secret|token|api[_-]?key|apikey|access[_-]?token|refresh[_-]?token|id[_-]?token|client[_-]?secret)\b"
)
PLACEHOLDER = re.compile(r"^\[?REDACTED\]?$", re.IGNORECASE)


def is_redacted(value):
    value = unquote(value.strip().strip("\"'[],;"))
    return bool(PLACEHOLDER.fullmatch(value) or re.fullmatch(r"Bearer\s+\[?REDACTED\]?", value, re.IGNORECASE))


def scan_string(value, path, where):
    for match in BEARER.finditer(value):
        token = match.group(1).strip("\"'[],")
        if not PLACEHOLDER.fullmatch(token):
            error(path, "credential leak: %s embeds an unredacted Bearer token" % where)
    for match in KV_SECRET.finditer(value):
        token = match.group(1).strip("\"'[],;")
        if not PLACEHOLDER.fullmatch(token):
            error(path, "credential leak: %s embeds an unredacted secret" % where)
    for match in URL_SECRET.finditer(value):
        if not is_redacted(match.group(1)):
            error(path, "credential leak: %s embeds an unredacted URL credential" % where)


def scan_value(value, path, where, depth=0):
    if depth > 64:
        error(path, "credential scan nesting exceeds the safety limit")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            location = "%s.%s" % (where, key)
            if isinstance(item, str):
                if SECRET_KEY.search(str(key)) and not is_redacted(item):
                    error(path, "credential leak: %s is not redacted" % location)
                scan_string(item, path, location)
            else:
                scan_value(item, path, location, depth + 1)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            scan_value(item, path, "%s[%d]" % (where, index), depth + 1)


json_values = {}
for file_path in sorted(path for path in root.rglob("*") if path.is_file()):
    relative = file_path.relative_to(root).as_posix()
    try:
        raw = file_path.read_bytes()
        text = raw.decode("utf-8")
    except OSError as exc:
        error(relative, "cannot read evidence: %s" % exc)
        continue
    except UnicodeDecodeError:
        error(relative, "unexpected binary artifact is not allowed")
        continue
    if b"\x00" in raw:
        error(relative, "unexpected binary artifact is not allowed")
        continue
    scan_string(text, relative, relative)
    if file_path.suffix == ".json":
        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            error(relative, "invalid JSON: %s" % exc)
            continue
        json_values[relative] = value
        scan_value(value, relative, relative)


report = json_values.get("report.json")
if report is None:
    error("report.json", "missing required consolidated report")
elif not isinstance(report, dict):
    error("report.json", "consolidated report must be a JSON object")


def nonempty(value):
    return value not in (None, False, 0, "", [], {})


if isinstance(report, dict):
    if release_gate and report.get("mode") != "real":
        error("report.json", "release gate requires a real consolidated report")
    if release_gate and report.get("release_evidence") is not True:
        error("report.json", "release gate requires release_evidence=true")
    for key in ("jobs", "required_jobs", "errors", "product_regressions", "unapproved_differences"):
        if key not in report:
            error("report.json", "missing required field '%s'" % key)

    jobs = report.get("jobs")
    required_jobs = report.get("required_jobs")
    if not isinstance(jobs, dict):
        error("report.json", "jobs must be an object")
        jobs = {}
    if not isinstance(required_jobs, list):
        error("report.json", "required_jobs must be an array")
        required_jobs = []
    elif release_gate and not required_jobs:
        error("report.json", "required_jobs must be a non-empty array for the release gate")
    for job_name in required_jobs:
        if not isinstance(job_name, str) or not job_name:
            error("report.json", "required_jobs entries must be non-empty strings")
        elif jobs.get(job_name) != "success":
            error("report.json", "required job %r is not successful" % job_name)

    for key in ("errors", "product_regressions", "infrastructure_skips"):
        if release_gate and nonempty(report.get(key)):
            error("report.json", "release gate rejects non-empty %s" % key)
    if release_gate and nonempty(report.get("unapproved_differences")):
        error("report.json", "release gate rejects unapproved_differences")

    expected = {}
    required = set(item for item in required_jobs if isinstance(item, str))
    if "prometheus-diff" in required:
        expected["prometheus-differential-evidence"] = ("raw.json", "normalized.json")
    if "loki-diff" in required:
        expected["loki-differential-evidence"] = ("raw.json", "normalized.json")
    if "tempo-diff" in required:
        expected["tempo-differential-evidence"] = ("raw.json", "normalized.json")
    if "grafana-system" in required:
        expected["grafana-system-real-evidence"] = (
            "outcome.json", "summary.json", "G1.outcome.json", "G2.outcome.json",
            "G3.outcome.json", "G4.outcome.json", "G5.outcome.json", "G6.outcome.json",
            "G7.outcome.json", "G8.outcome.json",
        )
    if "manifest-conformance" in required:
        for protocol in ("prometheus", "loki", "tempo"):
            expected["manifest-conformance-%s-real-evidence" % protocol] = ("report.jsonl", "versions.json")
    if "fast-pr" in required:
        expected["fast-pr-compatibility-evidence"] = ("report.txt",)

    for artifact_name, required_files in (expected.items() if release_gate else []):
        artifact_root = root / artifact_name
        if not artifact_root.is_dir():
            error(artifact_name, "missing required release evidence directory")
            continue
        files = [path for path in artifact_root.rglob("*") if path.is_file()]
        if not files:
            error(artifact_name, "required release evidence directory is empty")
            continue
        for required_file in required_files:
            matches = [path for path in artifact_root.rglob(required_file) if path.is_file()]
            if not matches:
                error(artifact_name, "missing required evidence file %s" % required_file)
            elif any(path.stat().st_size == 0 for path in matches):
                error(artifact_name, "required evidence file %s is empty" % required_file)


excluded_cases = {
    "prometheus-metadata-discovery",
    "tempo-search-span-selector",
    "tempo-search-tags",
    "tempo-tag-values-peer-service",
}

for relative, value in json_values.items():
    in_excluded_dir = any(case_id in relative for case_id in excluded_cases)
    values = value if isinstance(value, list) else [value]
    stack = list(values)
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            if "release_evidence" in item and not isinstance(item["release_evidence"], bool):
                error(relative, "release_evidence must be boolean")
            case_id = item.get("case_id")
            is_excluded = bool(
                in_excluded_dir
                or (case_id and case_id in excluded_cases)
                or item.get("conformance_exclusion")
                or bool(item.get("reason"))
                or item.get("classification") == "skipped"
                or item.get("outcome") == "conformance_exclusion"
            )
            if release_gate and item.get("release_evidence") is False and not is_excluded:
                error(relative, "release gate rejects release_evidence=false record")
            if release_gate and item.get("mode") in ("mock", "validation", "drift"):
                error(relative, "release gate rejects mode %r" % item.get("mode"))
            if release_gate and item.get("validation_only") is True and not is_excluded:
                error(relative, "release gate rejects validation-only record")
            if not is_excluded:
                stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)

if not release_gate:
    print("validate-artifacts: conformance evidence structurally valid (release gate not requested)")

for message in errors:
    print("validate-artifacts: %s" % message, file=sys.stderr)
if errors:
    print("validate-artifacts: FAILED with %d error(s)" % len(errors), file=sys.stderr)
    raise SystemExit(1)
PY
	exit 0
fi

if [ ! -d "$ROOT" ]; then
	echo "validate-artifacts: ERROR: artifact root is not a directory: $ROOT" >&2
	exit 2
fi

python3 - "$ROOT" "$GATE" <<'PY'
import json
import os
import re
import sys
from urllib.parse import unquote

root = sys.argv[1]
release_gate = sys.argv[2] == "1"

errors = []


def error(path, message):
    errors.append("%s: %s" % (path, message))


KNOWN_CLASSIFICATIONS = frozenset(
    [
        "pass",
        "drift",
        "product_regression",
        "infrastructure_failure",
        "environment_skip",
        "unsupported",
        "skipped",
        "mismatched_provenance",
        "receipt_validation_failure",
        "invalid_execution_receipt",
    ]
)

PLACEHOLDER = re.compile(r"^\[?REDACTED\]?$", re.IGNORECASE)
BEARER = re.compile(r"\bBearer\s+([^\s,}]+)", re.IGNORECASE)
KV_SECRET = re.compile(
    r"\b(?:authorization|proxy[_-]?authorization|x-api-key|api-key|cookie|set-cookie|access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret|api[_-]?key|apikey|client[_-]?secret)[=:]\s*(\S+)",
    re.IGNORECASE,
)
URL_SECRET = re.compile(
    r"(?i)[?&](?:authorization|api[_-]?key|apikey|access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret|client[_-]?secret|session(?:[_-]?id)?)=([^&#\s\"'<]+)"
)
SECRET_KEY = re.compile(
    r"(?i)\b(?:authorization|proxy[_-]?authorization|x-api-key|api-key|cookie|set-cookie|password|passwd|secret|token|api[_-]?key|apikey|access[_-]?token|refresh[_-]?token|id[_-]?token|client[_-]?secret)\b"
)

REQUIRED_CASE_FILES = [
    "case.json",
    "case_provenance.json",
    "request.raw.json",
    "request.normalized.json",
    "softprobe.raw.json",
    "softprobe.normalized.json",
    "reference.raw.json",
    "reference.normalized.json",
    "diff.json",
    "outcome.json",
]

CASE_META = ("run_id", "case_id", "protocol", "request_fingerprint")


def all_json_files():
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in sorted(filenames):
            if name.endswith(".json"):
                yield os.path.normpath(os.path.relpath(os.path.join(dirpath, name), root))


def all_regular_files():
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in sorted(filenames):
            rel = os.path.normpath(os.path.relpath(os.path.join(dirpath, name), root))
            if rel and os.path.isfile(os.path.join(root, rel)):
                yield rel


def load_json(rel):
    full = os.path.join(root, rel)
    try:
        with open(full, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        error(rel, "missing required artifact")
        return None
    except json.JSONDecodeError as exc:
        error(rel, "invalid JSON: %s" % exc)
        return None


def is_redacted(value):
    value = unquote(value.strip().strip("\"'[],;"))
    if re.match(r"(?i)^Bearer\s+\[?REDACTED\]?$", value):
        return True
    return bool(PLACEHOLDER.match(value))


def scan_string(value, path, where):
    for match in BEARER.finditer(value):
        token = match.group(1).strip("\"'[],")
        if not PLACEHOLDER.match(token):
            error(path, "credential leak: %s embeds a Bearer token that is not redacted" % where)
    for match in KV_SECRET.finditer(value):
        token = match.group(1).strip("\"'[],;")
        if not PLACEHOLDER.match(token):
            error(path, "credential leak: %s embeds a secret value that is not redacted: %s" % (where, match.group(0)))
    for match in URL_SECRET.finditer(value):
        if not is_redacted(match.group(1)):
            error(path, "credential leak: %s embeds an unredacted URL credential" % where)


def scan_value(value, path, where, depth=0):
    if depth > 64:
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, str):
                if SECRET_KEY.search(key):
                    if not is_redacted(item):
                        error(path, "credential leak: %s.%s=%r is not redacted" % (where, key, item))
                scan_string(item, path, "%s.%s" % (where, key))
            else:
                scan_value(item, path, "%s.%s" % (where, key), depth + 1)
    elif isinstance(value, list):
        for position, item in enumerate(value):
            scan_value(item, path, "%s[%d]" % (where, position), depth + 1)


def require_string(obj, path, key):
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        error(path, "field '%s' must be a non-empty string" % key)
        return None
    return value


def check_classification(obj, path, label):
    classification = obj.get("classification")
    if not isinstance(classification, str) or not classification:
        error(path, "%s is missing an explicit classification" % label)
        return
    if classification not in KNOWN_CLASSIFICATIONS:
        error(path, "unknown classification %r" % classification)


def load_case_file(cdir, name):
    rel = os.path.join(cdir, name)
    full = os.path.join(root, rel)
    try:
        with open(full, "r", encoding="utf-8") as handle:
            value = json.load(handle)
    except FileNotFoundError:
        error(rel, "missing required %s artifact" % name)
        return None
    except json.JSONDecodeError as exc:
        error(rel, "invalid JSON: %s" % exc)
        return None
    if not isinstance(value, dict):
        error(rel, "must be a non-null JSON object")
        return None
    return value


# --- artifact index ---------------------------------------------------------
index = load_json("artifact-index.json")
index_ok = isinstance(index, dict)
if index is not None and not index_ok:
    error("artifact-index.json", "must be a JSON object")

suite_run_id = None
suite_protocol = None
indexed = set()

if index_ok:
    suite_run_id = index.get("run_id")
    suite_protocol = index.get("protocol")
    if not isinstance(suite_run_id, str) or not suite_run_id:
        error("artifact-index.json", "run_id must be a non-empty string")
        suite_run_id = None
    if not isinstance(suite_protocol, str) or not suite_protocol:
        error("artifact-index.json", "protocol must be a non-empty string")
        suite_protocol = None
    if not isinstance(index.get("schema_version"), str) or not index["schema_version"]:
        error("artifact-index.json", "schema_version must be a non-empty string")
    if not isinstance(index.get("release_evidence"), bool):
        error("artifact-index.json", "release_evidence must be a boolean")
    artifacts = index.get("artifacts")
    if not isinstance(artifacts, list):
        error("artifact-index.json", "artifacts must be an array of relative paths")
        artifacts = []
    for entry in artifacts:
        if not isinstance(entry, str) or not entry:
            error("artifact-index.json", "artifacts entries must be non-empty relative paths")
            continue
        rel = os.path.normpath(entry)
        if os.path.isabs(entry) or rel == "." or rel.startswith(".."):
            error("artifact-index.json", "artifacts entry escapes the artifact root: %r" % entry)
            continue
        full = os.path.join(root, rel)
        if not os.path.isfile(full):
            error("artifact-index.json", "indexed artifact is missing: %s" % rel)
            continue
        indexed.add(rel)

# --- execution receipt ------------------------------------------------------
receipt = load_json("execution-receipt.json")
if isinstance(receipt, dict):
    if suite_run_id is not None and receipt.get("run_id") != suite_run_id:
        error("execution-receipt.json", "run_id %r does not match suite run_id %r" % (receipt.get("run_id"), suite_run_id))
    if suite_protocol is not None and receipt.get("protocol") != suite_protocol:
        error("execution-receipt.json", "protocol %r does not match suite protocol %r" % (receipt.get("protocol"), suite_protocol))
    if not isinstance(receipt.get("status"), str) or not receipt["status"]:
        error("execution-receipt.json", "status must be a non-empty string")
    for key in ("selected_case_ids", "executed_case_ids", "selected_fixture_ids"):
        if key in receipt and not isinstance(receipt[key], list):
            error("execution-receipt.json", "%s must be an array" % key)
    receipt_cases = receipt.get("cases")
    if not isinstance(receipt_cases, list):
        error("execution-receipt.json", "cases must be an array of per-case records")
        receipt_cases = []
else:
    if receipt is not None:
        error("execution-receipt.json", "must be a JSON object")
    receipt_cases = []

# --- suite outcome ----------------------------------------------------------
root_outcome = load_json("outcome.json")
root_outcome_ok = isinstance(root_outcome, dict)
if root_outcome is not None and not root_outcome_ok:
    error("outcome.json", "must be a JSON object")
if root_outcome_ok:
    if suite_run_id is not None and root_outcome.get("run_id") != suite_run_id:
        error("outcome.json", "run_id %r does not match suite run_id %r" % (root_outcome.get("run_id"), suite_run_id))
    if suite_protocol is not None and root_outcome.get("protocol") != suite_protocol:
        error("outcome.json", "protocol %r does not match suite protocol %r" % (root_outcome.get("protocol"), suite_protocol))
    if not isinstance(root_outcome.get("status"), str) or not root_outcome["status"]:
        error("outcome.json", "status must be a non-empty string")
    check_classification(root_outcome, "outcome.json", "suite outcome")
    if "release_evidence" in root_outcome and not isinstance(root_outcome["release_evidence"], bool):
        error("outcome.json", "release_evidence must be a boolean")

# --- scratch classification -------------------------------------------------
def is_runner_scratch(rel):
    # .differential/ holds the runner's internal scratch tree; its per-case
    # copies are re-ingested at the root, so it never validates directly.
    # suite/*/artifacts/ also holds raw intermediate artifacts.
    return (
        rel.startswith(".differential/")
        or "/.differential/" in rel
        or "/artifacts/" in rel
        or rel.startswith("artifacts/")
    )

# --- case directories -------------------------------------------------------
case_dirs = set()
for rel in indexed:
    if is_runner_scratch(rel):
        continue
    parent = os.path.dirname(rel)
    base = os.path.basename(rel)
    if parent and base == "case.json":
        case_dirs.add(parent)

case_meta = {}
case_outcomes = {}


def validate_case(cdir, suite_run_id, suite_protocol):
    rels = {name: os.path.join(cdir, name) for name in REQUIRED_CASE_FILES}
    values = {name: load_case_file(cdir, name) for name in REQUIRED_CASE_FILES}

    case = values["case.json"]
    if not isinstance(case, dict):
        return

    run_id = require_string(case, rels["case.json"], "run_id")
    case_id = require_string(case, rels["case.json"], "case_id")
    protocol = require_string(case, rels["case.json"], "protocol")
    fixture_id = require_string(case, rels["case.json"], "fixture_id")
    fingerprint = require_string(case, rels["case.json"], "request_fingerprint")
    if run_id is None or case_id is None or protocol is None or fixture_id is None or fingerprint is None:
        return

    if suite_run_id is not None and run_id != suite_run_id:
        error(rels["case.json"], "run_id %r does not match suite run_id %r" % (run_id, suite_run_id))
    if suite_protocol is not None and suite_protocol not in ("multi", "all") and protocol != suite_protocol:
        error(rels["case.json"], "protocol %r does not match suite protocol %r" % (protocol, suite_protocol))

    expected_meta = {
        "run_id": run_id,
        "case_id": case_id,
        "protocol": protocol,
        "request_fingerprint": fingerprint,
    }

    def check_meta(value, rel, label):
        for key in CASE_META:
            if value.get(key) != expected_meta[key]:
                error(rel, "%s %r does not match case %r" % (key, value.get(key), expected_meta[key]))

    provenance = values["case_provenance.json"]
    if isinstance(provenance, dict):
        check_meta(provenance, rels["case_provenance.json"], "case provenance")
        algorithm = provenance.get("fingerprint_algorithm")
        if not isinstance(algorithm, str) or not algorithm:
            error(rels["case_provenance.json"], "fingerprint_algorithm must be a non-empty string")

    for name in (
        "request.raw.json",
        "request.normalized.json",
        "softprobe.raw.json",
        "softprobe.normalized.json",
        "reference.raw.json",
        "reference.normalized.json",
    ):
        value = values[name]
        if isinstance(value, dict):
            check_meta(value, rels[name], name)

    diff = values["diff.json"]
    if isinstance(diff, dict):
        check_meta(diff, rels["diff.json"], "diff")
        check_classification(diff, rels["diff.json"], "diff")
        if "release_evidence" in diff and not isinstance(diff["release_evidence"], bool):
            error(rels["diff.json"], "release_evidence must be a boolean")

    outcome = values["outcome.json"]
    if isinstance(outcome, dict):
        case_outcomes[cdir] = outcome
        check_meta(outcome, rels["outcome.json"], "case outcome")
        if not isinstance(outcome.get("status"), str) or not outcome["status"]:
            error(rels["outcome.json"], "status must be a non-empty string")
        check_classification(outcome, rels["outcome.json"], "case outcome")
        if "release_evidence" in outcome and not isinstance(outcome["release_evidence"], bool):
            error(rels["outcome.json"], "release_evidence must be a boolean")
        evidence = outcome.get("evidence")
        if isinstance(evidence, dict):
            for reference in evidence.values():
                if not isinstance(reference, str) or not reference.strip():
                    error(rels["outcome.json"], "evidence pointer must be a non-null non-empty relative path")
                    continue
                if os.path.isabs(reference) or reference.startswith(os.sep):
                    error(rels["outcome.json"], "evidence pointer must stay inside the case directory: %s" % reference)
                    continue
                norm = os.path.normpath(reference)
                if norm == "." or norm.startswith(".."):
                    error(rels["outcome.json"], "evidence pointer must stay inside the case directory: %s" % reference)
                    continue
                target = os.path.normpath(os.path.join(cdir, norm))
                if not os.path.isfile(os.path.join(root, target)):
                    error(rels["outcome.json"], "evidence references missing artifact: %s" % reference)
                    continue
                if target not in indexed:
                    error(rels["outcome.json"], "evidence references unindexed artifact: %s" % reference)

    case_meta[case_id] = {"fixture_id": fixture_id, "request_fingerprint": fingerprint}


for cdir in sorted(case_dirs):
    validate_case(cdir, suite_run_id, suite_protocol)

# --- receipt per-case records ------------------------------------------------
recorded_case_ids = set()
for record_index, record in enumerate(receipt_cases):
    if not isinstance(record, dict):
        error("execution-receipt.json", "cases[%d] must be an object" % record_index)
        continue
    if suite_run_id is not None and record.get("run_id") != suite_run_id:
        error("execution-receipt.json", "cases[%d].run_id %r does not match suite run_id" % (record_index, record.get("run_id")))
    for key in ("case_id", "fixture_id", "request_fingerprint", "fingerprint", "fingerprint_algorithm"):
        if not isinstance(record.get(key), str) or not record[key]:
            error("execution-receipt.json", "cases[%d].%s must be a non-empty string" % (record_index, key))
    case_id = record.get("case_id")
    if not isinstance(case_id, str) or not case_id:
        continue
    if case_id in recorded_case_ids:
        error("execution-receipt.json", "cases[%d] duplicates the case record for %r" % (record_index, case_id))
        continue
    recorded_case_ids.add(case_id)
    meta = case_meta.get(case_id)
    if meta is not None:
        if record.get("fixture_id") != meta["fixture_id"]:
            error("execution-receipt.json", "cases[%d].fixture_id %r does not match case fixture_id %r" % (record_index, record.get("fixture_id"), meta["fixture_id"]))
        for key in ("request_fingerprint", "fingerprint"):
            if record.get(key) != meta["request_fingerprint"]:
                error("execution-receipt.json", "cases[%d].%s %r does not match case fingerprint" % (record_index, key, record.get(key)))

if isinstance(receipt, dict):
    selected_executed_ids = set()
    for key in ("selected_case_ids", "executed_case_ids"):
        entries = receipt.get(key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, str) or not entry:
                error("execution-receipt.json", "%s entries must be non-empty strings" % key)
            else:
                selected_executed_ids.add(entry)
    for case_id in sorted(selected_executed_ids):
        if case_id not in recorded_case_ids:
            error("execution-receipt.json", "no receipt case record for selected/executed case %r" % case_id)
    for case_id in sorted(recorded_case_ids):
        if case_id not in selected_executed_ids:
            error("execution-receipt.json", "receipt case record %r does not match any selected/executed case id" % case_id)

# --- index completeness -------------------------------------------------------
# The .differential/ subtree is the runner's internal scratch area; only its
# indexed copies (ingested into per-case dirs) are part of the evidence set.
for rel in all_regular_files():
    if rel == "artifact-index.json" or is_runner_scratch(rel):
        continue
    if rel not in indexed:
        error(rel, "evidence file is not listed in artifact-index.json")

# --- credential redaction ------------------------------------------------------
for rel in all_regular_files():
    if rel.endswith(".json"):
        value = load_json(rel)
        if isinstance(value, dict) or isinstance(value, list):
            scan_value(value, rel, rel.replace("/", "."))
        continue
    full = os.path.join(root, rel)
    try:
        with open(full, "rb") as handle:
            raw = handle.read()
        text = raw.decode("utf-8")
        if b"\x00" in raw:
            raise UnicodeError("NUL byte")
    except UnicodeError:
        error(rel, "unexpected binary artifact is not allowed")
        continue
    except OSError as exc:
        error(rel, "cannot read artifact for credential scan: %s" % exc)
        continue
    scan_string(text, rel, rel)

# --- release gate ----------------------------------------------------------------
if release_gate:
    if index_ok and index.get("release_evidence") is not True:
        error("artifact-index.json", "release gate: index does not assert release_evidence")
    if root_outcome_ok:
        if root_outcome.get("release_evidence") is not True:
            error("outcome.json", "release gate: suite outcome does not assert release_evidence")
        if root_outcome.get("status") != "pass":
            error("outcome.json", "release gate: suite status %r is not 'pass'" % root_outcome.get("status"))
        if root_outcome.get("classification") != "pass":
            error("outcome.json", "release gate: suite classification %r is not 'pass'" % root_outcome.get("classification"))
    for rel in all_json_files():
        value = load_json(rel)
        if not isinstance(value, dict):
            continue
        mode = value.get("mode")
        if mode in ("mock", "validation", "drift"):
            error(rel, "release gate: mode %r can never satisfy release evidence" % mode)
        if value.get("validation_only") is True:
            error(rel, "release gate: validation-only artifacts can never satisfy release evidence")
    for cdir in sorted(case_dirs):
        outcome = case_outcomes.get(cdir)
        if outcome is None:
            continue
        # Conformance exclusions (recorded reason, non-release) are the only
        # sanctioned way for a case to be skipped in a real-mode run.
        exclusion = outcome.get("reason") == "conformance_exclusion" and outcome.get("release_evidence") is False
        if exclusion:
            if outcome.get("status") != "skipped":
                error(os.path.join(cdir, "outcome.json"), "excluded case must be skipped with conformance_exclusion outcome")
            continue
        if outcome.get("release_evidence") is not True:
            error(os.path.join(cdir, "outcome.json"), "release gate: case outcome does not assert release_evidence")
        if outcome.get("status") != "pass":
            error(os.path.join(cdir, "outcome.json"), "release gate: case status %r is not 'pass'" % outcome.get("status"))

for message in errors:
    print("validate-artifacts: %s" % message, file=sys.stderr)
if errors:
    print("validate-artifacts: FAILED with %d error(s)" % len(errors), file=sys.stderr)
    raise SystemExit(1)
PY

exit 0
