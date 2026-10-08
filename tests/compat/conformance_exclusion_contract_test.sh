#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
MANIFEST="$ROOT_DIR/tests/compat/manifests/cases.v0.yaml"
REFERENCE_MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml"
CAPABILITY_MANIFEST="$ROOT_DIR/docs/compat/capability.v0.yaml"
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/compat-exclusion-contract.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

null_runner_manifest="$tmp_dir/null-runner.yaml"
cp "$MANIFEST" "$null_runner_manifest"
ruby -ryaml - "$null_runner_manifest" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
prometheus = document.fetch("cases").select { |entry| entry.fetch("protocol") == "prometheus" }
abort "expected eight Prometheus cases" unless prometheus.length == 8
prometheus.fetch(0)["runner_case_id"] = nil
prometheus.fetch(0)["conformance_exclusion"] = {
  "reason" => "runner does not expose this reference-only case",
  "release_evidence" => false
}
File.write(path, YAML.dump(document))
RUBY

mock_output="$tmp_dir/mock"
MANIFEST="$null_runner_manifest" \
COMPAT_REFERENCE_MANIFEST="$REFERENCE_MANIFEST" \
CAPABILITY_MANIFEST="$CAPABILITY_MANIFEST" \
  "$ROOT_DIR/scripts/compat/conformance.sh" --mock --protocol prometheus --out "$mock_output" >/dev/null

ruby -rjson - "$mock_output" <<'RUBY'
root = ARGV.fetch(0)
report = File.readlines(File.join(root, "report.jsonl"), chomp: true).reject(&:empty?).map { |line| JSON.parse(line) }
expected_case_ids = %w[
  prometheus-query-selector-instant
  prometheus-query-aggregation
  prometheus-query-rate-counter-instant
  prometheus-query-range-selector
  prometheus-labels-discovery
  prometheus-label-values-discovery
  prometheus-series-discovery
  prometheus-metadata-discovery
]
expected_runner_ids = [nil, "sum_by_job", "rate_counter", "range_selector", "labels", "label_values", "series", nil]
abort "unexpected Prometheus selection: #{report.map { |entry| entry["case_id"] }.inspect}" unless report.map { |entry| entry["case_id"] } == expected_case_ids
abort "nullable/non-null runner metadata drifted: #{report.map { |entry| entry["runner_case_id"] }.inspect}" unless report.map { |entry| entry["runner_case_id"] } == expected_runner_ids

excluded = report.fetch(0)
abort "excluded case did not remain skipped" unless excluded["status"] == "skipped" && excluded["outcome"] == "conformance_exclusion"
abort "excluded case was treated as release evidence" unless excluded["release_evidence"] == false
abort "excluded case lost its nullable runner metadata" unless JSON.parse(File.read(File.join(root, expected_case_ids.fetch(0), "case.json")))["runner_case_id"].nil?
abort "executable mock cases were not reported as pass" unless report.drop(1).reject { |entry| entry["case_id"] == "prometheus-metadata-discovery" }.all? { |entry| entry["status"] == "pass" && entry["runner_case_id"].is_a?(String) }
# prometheus-metadata-discovery carries a canonical conformance_exclusion
# (reference harness cannot serve metadata from preloaded blocks).
abort "canonically excluded case was not skipped" unless report.fetch(7)["status"] == "skipped" && report.fetch(7)["outcome"] == "conformance_exclusion"
RUBY

unknown_capability_manifest="$tmp_dir/unknown-capability.yaml"
cp "$MANIFEST" "$unknown_capability_manifest"
ruby -ryaml - "$unknown_capability_manifest" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("cases").first["unsupported_features"] = {
  "capability.not-in-canonical-manifest" => ["$.response.data"]
}
File.write(path, YAML.dump(document))
RUBY

set +e
unknown_output=$(MANIFEST="$unknown_capability_manifest" \
  COMPAT_REFERENCE_MANIFEST="$REFERENCE_MANIFEST" \
  CAPABILITY_MANIFEST="$CAPABILITY_MANIFEST" \
  "$ROOT_DIR/scripts/compat/conformance.sh" --mock --case prometheus-query-selector-instant --out "$tmp_dir/unknown-capability" 2>&1)
unknown_status=$?
set -e
test "$unknown_status" -eq 2
grep -Fq 'unsupported-feature entry references unknown capability' <<<"$unknown_output"

echo "conformance exclusion contract: PASS"
