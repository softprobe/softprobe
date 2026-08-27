#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
RUNNER="$ROOT_DIR/scripts/compat/conformance.sh"
WORKFLOW="$ROOT_DIR/.github/workflows/compatibility.yml"

assert_contains() {
	local needle=$1
	grep -Fq "$needle" "$RUNNER" || {
		echo "missing conformance drift contract: $needle" >&2
		exit 1
	}
}

assert_matches() {
	local pattern=$1
	grep -Eq "$pattern" "$RUNNER" || {
		echo "missing conformance drift pattern: $pattern" >&2
		exit 1
	}
}

assert_absent() {
	local needle=$1
	if grep -Fq "$needle" "$RUNNER"; then
		echo "obsolete conformance drift branch remains: $needle" >&2
		exit 1
	fi
}

assert_workflow_matches() {
	local pattern=$1
	grep -Eq "$pattern" "$WORKFLOW" || {
		echo "missing workflow drift pattern: $pattern" >&2
		exit 1
	}
}

assert_workflow_job_matches() {
	local job=$1
	local pattern=$2
	awk -v job="$job" '
		$0 == "  " job ":" { in_job = 1; print; next }
		in_job && $0 ~ /^  [A-Za-z0-9_-]+:/ { exit }
		in_job { print }
	' "$WORKFLOW" | grep -Eq -- "$pattern" || {
		echo "missing workflow drift pattern in $job: $pattern" >&2
		exit 1
	}
}

assert_matches 'protocol_env\+=[[:space:]]*\("PROMETHEUS_REFERENCE_IMAGE=\$DRIFT_CANDIDATE_REFERENCE"\)'
assert_matches 'protocol_env\+=[[:space:]]*\("LOKI_REFERENCE_IMAGE=\$DRIFT_CANDIDATE_REFERENCE"\)'
assert_matches 'protocol_env\+=[[:space:]]*\("TEMPO_REFERENCE_IMAGE=\$DRIFT_CANDIDATE_REFERENCE"\)'
assert_absent 'protocol_env+=("PROMETHEUS_REFERENCE_IMAGE=$DRIFT_CANDIDATE_IMAGE:$DRIFT_CANDIDATE_VERSION")'
assert_absent 'protocol_env+=("LOKI_REFERENCE_IMAGE=$DRIFT_CANDIDATE_IMAGE:$DRIFT_CANDIDATE_VERSION")'
assert_absent 'protocol_env+=("TEMPO_REFERENCE_IMAGE=$DRIFT_CANDIDATE_IMAGE:$DRIFT_CANDIDATE_VERSION")'
assert_contains '"candidate" => (ARGV[7] == "drift" ? {'
assert_contains '"image" => ARGV[8], "version" => ENV.fetch("DRIFT_CANDIDATE_VERSION"),'
assert_contains '"digest" => ENV.fetch("DRIFT_CANDIDATE_DIGEST")'
assert_contains 'DRIFT_CANDIDATE_REFERENCE'
assert_matches '"reference_image"[[:space:]]*=>[[:space:]]*\(ARGV\[8\].*ARGV\[8\]\)'
assert_absent 'candidate_reference_override_unsupported'
assert_absent '[ "$DRIFT" = true ] && [ "$protocol" != prometheus ]'

assert_workflow_matches 'drift_prometheus_digest:'
assert_workflow_matches 'drift_loki_digest:'
assert_workflow_matches 'drift_tempo_digest:'

assert_workflow_matches 'DRIFT_PROMETHEUS_DIGEST:[[:space:]]*\$\{\{[[:space:]]*inputs\.drift_prometheus_digest'
assert_workflow_matches 'DRIFT_LOKI_DIGEST:[[:space:]]*\$\{\{[[:space:]]*inputs\.drift_loki_digest'
assert_workflow_matches 'DRIFT_TEMPO_DIGEST:[[:space:]]*\$\{\{[[:space:]]*inputs\.drift_tempo_digest'
assert_workflow_job_matches 'drift-grafana' 'DRIFT_PROMETHEUS_DIGEST:[[:space:]]*\$\{\{[[:space:]]*inputs\.drift_prometheus_digest'
assert_workflow_job_matches 'drift-grafana' 'DRIFT_LOKI_DIGEST:[[:space:]]*\$\{\{[[:space:]]*inputs\.drift_loki_digest'
assert_workflow_job_matches 'drift-grafana' 'DRIFT_TEMPO_DIGEST:[[:space:]]*\$\{\{[[:space:]]*inputs\.drift_tempo_digest'
assert_workflow_job_matches 'drift-grafana' 'Create candidate reference manifest'

assert_workflow_job_matches 'drift-prometheus' 'DRIFT_CANDIDATE_DIGEST:[[:space:]]*\$\{\{[[:space:]]*inputs\.drift_prometheus_digest'
assert_workflow_job_matches 'drift-loki' 'DRIFT_CANDIDATE_DIGEST:[[:space:]]*\$\{\{[[:space:]]*inputs\.drift_loki_digest'
assert_workflow_job_matches 'drift-tempo' 'DRIFT_CANDIDATE_DIGEST:[[:space:]]*\$\{\{[[:space:]]*inputs\.drift_tempo_digest'

assert_workflow_job_matches 'drift-prometheus' '--candidate-digest[[:space:]]+"\$DRIFT_CANDIDATE_DIGEST"'
assert_workflow_job_matches 'drift-loki' '--candidate-digest[[:space:]]+"\$DRIFT_CANDIDATE_DIGEST"'
assert_workflow_job_matches 'drift-tempo' '--candidate-digest[[:space:]]+"\$DRIFT_CANDIDATE_DIGEST"'

assert_contains 'reference_image_for_protocol() {'
assert_contains 'ruby -ryaml - "$REFERENCE_MANIFEST_PATH" "$1" <<'\''RUBY'\'''
assert_contains 'image = reference.fetch("image")'
assert_contains 'digest = reference["digest"]'
assert_contains 'puts("#{image}@#{digest}")'
assert_matches '"image"[[:space:]]*=>[[:space:]]*image_reference'
assert_matches '"image_tag"[[:space:]]*=>[[:space:]]*image_reference'
assert_matches '"candidate"[[:space:]]*=>[[:space:]]*\(mode == "drift"'
assert_contains 'ENV.fetch("DRIFT_CANDIDATE_REFERENCE")'
assert_contains 'ENV.fetch("DRIFT_CANDIDATE_VERSION")'
assert_contains 'ENV.fetch("DRIFT_CANDIDATE_DIGEST")'

echo "conformance drift contract: PASS"
