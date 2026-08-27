#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)

real_plan=$(make --no-print-directory -n test-compat 2>&1)
grep -Fq 'scripts/compat/conformance.sh' <<<"$real_plan"
grep -Fq 'real) ;;' <<<"$real_plan"
grep -Fq 'mock) args+=(--mock)' <<<"$real_plan"

# The scheduled drift report must derive every release-evidence input before
# evaluating the gate.
ruby - "$ROOT_DIR/.github/workflows/compatibility.yml" <<'RUBY'
workflow = File.read(ARGV.fetch(0))
drift_report = workflow[/^  drift-report:.*?(?=^  [A-Za-z0-9_-]+:|\z)/m]
abort "missing drift-report workflow job" unless drift_report
release_index = drift_report.index("release_evidence =")
abort "missing drift release-evidence calculation" unless release_index
abort "drift report must remain non-release evidence" unless drift_report.include?('"release_evidence": False')
report_index = drift_report.index("report = {")
abort "drift report must calculate release evidence before serializing the report" unless report_index && release_index < report_index
RUBY

MANIFEST="$ROOT_DIR/tests/compat/manifests/cases.v0.yaml"
grep -Fq 'runner_case_id:' "$MANIFEST"
ruby -ryaml - "$MANIFEST" <<'RUBY'
document = YAML.load_file(ARGV.fetch(0))
release_cases = document.fetch("cases").select do |entry|
  %w[prometheus loki tempo].include?(entry.fetch("protocol")) && entry.fetch("evidence").fetch("retain")
end
release_cases.each do |entry|
  runner_case_id = entry["runner_case_id"]
  case_id = entry.fetch("id")
  # Conformance exclusions (with a recorded reason and release_evidence=false)
  # are allowed to skip differential execution; anything else must map 1:1 to
  # a protocol-runner case.
  excluded = entry["conformance_exclusion"].is_a?(Hash) && entry["conformance_exclusion"]["release_evidence"] == false
  abort "release-selected case lacks runner_case_id: #{case_id}" unless excluded || (runner_case_id.is_a?(String) && !runner_case_id.empty?)
end
executable = release_cases.select { |entry| entry["runner_case_id"].is_a?(String) && !entry["runner_case_id"].empty? }
runner_pairs = executable.map { |entry| [entry.fetch("id"), entry["runner_case_id"]] }
abort "release-selected runner_case_id mapping is not one-to-one" unless runner_pairs.map(&:last).uniq.length == runner_pairs.length
prometheus = executable.select { |entry| entry.fetch("protocol") == "prometheus" }
expected_prometheus = {
  "prometheus-query-selector-instant" => "selector_instant",
  "prometheus-query-aggregation" => "sum_by_job",
  "prometheus-query-rate-counter-instant" => "rate_counter",
  "prometheus-query-range-selector" => "range_selector",
  "prometheus-labels-discovery" => "labels",
  "prometheus-label-values-discovery" => "label_values",
  "prometheus-series-discovery" => "series"
}
actual_prometheus = prometheus.to_h { |entry| [entry.fetch("id"), entry["runner_case_id"]] }
abort "Prometheus release mapping drift: #{actual_prometheus.inspect}" unless actual_prometheus == expected_prometheus
metadata_entry = release_cases.find { |entry| entry.fetch("id") == "prometheus-metadata-discovery" }
abort "prometheus metadata case lost its conformance exclusion" unless metadata_entry && metadata_entry.dig("conformance_exclusion", "reason").is_a?(String)
RUBY
if grep -Fq "make test-prom-compat" "$ROOT_DIR/scripts/compat/conformance.sh"; then
	echo "conformance must not use the broad Prometheus compatibility suite" >&2
	exit 1
fi
grep -Fq 'COMPAT_CASE_IDS=' "$ROOT_DIR/scripts/compat/conformance.sh"
if grep -Fq 'COMPAT_CASE_ID=__suite__' "$ROOT_DIR/scripts/compat/conformance.sh"; then
	echo "conformance must not send the suite sentinel to protocol runners" >&2
	exit 1
fi

static_plan=$(make --no-print-directory -n test-grafana-static 2>&1)
grep -Fq 'GRAFANA_COMPOSE_IMAGE=' <<<"$static_plan"
system_plan=$(make --no-print-directory -n test-grafana-system 2>&1)
grep -Fq 'GRAFANA_COMPOSE_IMAGE=' <<<"$system_plan"

# otel_collector is a manual Grafana demo dependency, not a conformance
# oracle or CI pull.  Keep it out of the immutable reference-service gate
# until the manual demo is promoted into a reproducible CI lane.
if grep -R -n -E 'otel/opentelemetry-collector|otel_collector' \
	"$ROOT_DIR/.github/workflows" "$ROOT_DIR/Makefile" \
	"$ROOT_DIR/tests/compat/grafana/docker-compose.ci.yml" >/dev/null 2>&1; then
	echo "otel_collector unexpectedly entered the supported CI/reference pull set" >&2
	exit 1
fi

pin_output=$(make --no-print-directory check-compat-reference-pins)
grep -Fq 'prometheus:' <<<"$pin_output"
grep -Fq 'prometheus: prom/prometheus@sha256:f6639335d34a77d9d9db382b92eeb7fc00934be8eae81dbc03b31cfe90411a94' <<<"$pin_output"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/compat-target-test.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

protocol_label() {
	case "$1" in
		loki) printf '%s\n' Loki ;;
		tempo) printf '%s\n' Tempo ;;
		grafana) printf '%s\n' Grafana ;;
		*) printf '%s\n' "$1" ;;
	esac
}

# The Prometheus manifest contains eight cases; metadata carries a canonical
# conformance exclusion (reference cannot serve block-preloaded metadata).
# Exercise the selector path so discovery cases cannot be silently dropped and
# every mapped runner ID stays aligned with the manifest.
prometheus_selection_dir="$tmp_dir/prometheus-selection"
scripts/compat/conformance.sh --mock --protocol prometheus --out "$prometheus_selection_dir" >/dev/null
ruby -rjson - "$prometheus_selection_dir" <<'RUBY'
root = ARGV.fetch(0)
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
expected_runner_ids = %w[selector_instant sum_by_job rate_counter range_selector labels label_values series]
report = File.readlines(File.join(root, "report.jsonl"), chomp: true).map { |line| JSON.parse(line) }
actual_case_ids = report.map { |entry| entry.fetch("case_id") }
abort "Prometheus selector dropped manifest cases: #{actual_case_ids.inspect}" unless actual_case_ids == expected_case_ids
actual_runner_ids = report.map { |entry| entry["runner_case_id"] }.compact.uniq
abort "Prometheus runner mapping drifted: #{actual_runner_ids.inspect}" unless actual_runner_ids == expected_runner_ids
excluded = report.select { |entry| entry["outcome"] == "conformance_exclusion" }
abort "unexpected conformance exclusion set: #{excluded.map { |entry| entry["case_id"] }.inspect}" unless excluded.map { |entry| entry["case_id"] } == ["prometheus-metadata-discovery"]
RUBY

case_dir="$tmp_dir/prometheus-case-prometheus-query-selector-instant"
scripts/compat/conformance.sh --mock --case prometheus-query-selector-instant --out "$case_dir" >/dev/null
test "$(ruby -rjson -e 'puts File.readlines(File.join(ARGV.fetch(0), "report.jsonl"), chomp: true).reject(&:empty?).length' "$case_dir")" -eq 1

# Keep the exclusion path fail-closed: a temporary manifest that drops a
# runner mapping without declaring an explicit non-release exclusion must be
# rejected rather than silently treated as executable or reference-only.
cp "$MANIFEST" "$tmp_dir/missing-runner.yaml"
ruby -ryaml - "$tmp_dir/missing-runner.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("cases").first.delete("runner_case_id")
File.write(path, YAML.dump(document))
RUBY
set +e
missing_runner_output=$(MANIFEST="$tmp_dir/missing-runner.yaml" scripts/compat/conformance.sh --mock --protocol prometheus --out "$tmp_dir/missing-runner" 2>&1)
missing_runner_status=$?
set -e
test "$missing_runner_status" -eq 2
grep -Fq 'missing runner_case_id requires an explicit non-release conformance_exclusion reason' <<<"$missing_runner_output"

# An unknown capability must fail manifest validation before any allowlisted
# difference can be approved. Put the malformed entry on a selected case so
# this covers the per-case waiver path used by write_normalized_diff.
cp "$MANIFEST" "$tmp_dir/unknown-allowlist.yaml"
ruby -ryaml - "$tmp_dir/unknown-allowlist.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("cases").first["unsupported_features"] = {
  "capability.not-in-canonical-manifest" => ["$.response.data"]
}
File.write(path, YAML.dump(document))
RUBY
set +e
unknown_allowlist_output=$(MANIFEST="$tmp_dir/unknown-allowlist.yaml" scripts/compat/conformance.sh --mock --case prometheus-query-selector-instant --out "$tmp_dir/unknown-allowlist" 2>&1)
unknown_allowlist_status=$?
set -e
test "$unknown_allowlist_status" -eq 2
grep -Fq 'unsupported-feature entry references unknown capability' <<<"$unknown_allowlist_output"

# An unknown feature name must fail against the canonical capability registry,
# even when the capability ID itself is valid. This protects the approval path
# from silently accepting a waiver that is absent from docs/compat/capability.v0.yaml.
cp "$MANIFEST" "$tmp_dir/unknown-feature-allowlist.yaml"
ruby -ryaml - "$tmp_dir/unknown-feature-allowlist.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("cases").first["unsupported_features"] = {
  "capability" => "prometheus.query",
  "feature" => "feature.not-in-canonical-capability-manifest",
  "path" => "$.response.data"
}
File.write(path, YAML.dump(document))
RUBY
set +e
unknown_feature_output=$(MANIFEST="$tmp_dir/unknown-feature-allowlist.yaml" scripts/compat/conformance.sh --mock --case prometheus-query-selector-instant --out "$tmp_dir/unknown-feature-allowlist" 2>&1)
unknown_feature_status=$?
set -e
test "$unknown_feature_status" -eq 2
grep -Fq 'unsupported-feature "feature.not-in-canonical-capability-manifest" is not declared for capability prometheus.query' <<<"$unknown_feature_output"

cp docs/compat/references.v0.yaml "$tmp_dir/references.yaml"
ruby -ryaml - "$tmp_dir/references.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("references").fetch("prometheus")["tag"] = "v2.55.0"
File.write(path, YAML.dump(document))
RUBY
set +e
drift_output=$(COMPAT_REFERENCE_MANIFEST="$tmp_dir/references.yaml" make --no-print-directory check-compat-reference-pins 2>&1)
drift_status=$?
set -e
test "$drift_status" -ne 0
grep -Fq 'Prometheus reference drift' <<<"$drift_output"

cp docs/compat/references.v0.yaml "$tmp_dir/empty-tag.yaml"
ruby -ryaml - "$tmp_dir/empty-tag.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("references").fetch("prometheus")["tag"] = ""
File.write(path, YAML.dump(document))
RUBY
set +e
empty_tag_output=$(COMPAT_REFERENCE_MANIFEST="$tmp_dir/empty-tag.yaml" COMPAT_REFERENCE_CANONICAL_MANIFEST="$tmp_dir/empty-tag.yaml" COMPAT_REFERENCE_ALLOW_MANIFEST_OVERRIDE=1 make --no-print-directory check-compat-reference-pins 2>&1)
empty_tag_status=$?
set -e
test "$empty_tag_status" -ne 0
grep -Eq 'prometheus reference requires a non-empty image and tag|reference is missing a valid immutable sha256 digest' <<<"$empty_tag_output"

for protocol in loki tempo grafana; do
	cp docs/compat/references.v0.yaml "$tmp_dir/$protocol-drift.yaml"
	ruby -ryaml - "$tmp_dir/$protocol-drift.yaml" "$protocol" <<'RUBY'
path, protocol = ARGV
document = YAML.load_file(path)
document.fetch("references").fetch(protocol)["tag"] = "drift-version"
File.write(path, YAML.dump(document))
RUBY
	set +e
	metadata_drift_output=$(COMPAT_REFERENCE_MANIFEST="$tmp_dir/$protocol-drift.yaml" make --no-print-directory check-compat-reference-pins 2>&1)
	metadata_drift_status=$?
	set -e
	test "$metadata_drift_status" -ne 0
	grep -Fq "$(protocol_label "$protocol") reference drift from canonical manifest" <<<"$metadata_drift_output"
done

cp "$MANIFEST" "$tmp_dir/duplicate-runner.yaml"
ruby -ryaml - "$tmp_dir/duplicate-runner.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
prometheus = document.fetch("cases").select { |entry| entry.fetch("protocol") == "prometheus" }
prometheus.fetch(1)["runner_case_id"] = prometheus.fetch(0).fetch("runner_case_id")
File.write(path, YAML.dump(document))
RUBY
set +e
duplicate_output=$(MANIFEST="$tmp_dir/duplicate-runner.yaml" COMPAT_REFERENCE_MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml" scripts/compat/conformance.sh --mock --protocol prometheus --out "$tmp_dir/duplicate-runner" 2>&1)
duplicate_status=$?
set -e
test "$duplicate_status" -ne 0
grep -Fq 'duplicate runner_case_id' <<<"$duplicate_output"

set +e
sentinel_output=$(scripts/compat/conformance.sh --mock --case __suite__ --out "$tmp_dir/sentinel" 2>&1)
sentinel_status=$?
set -e
test "$sentinel_status" -ne 0
grep -Fq 'suite sentinel' <<<"$sentinel_output"

cp tests/compat/manifests/cases.v0.yaml "$tmp_dir/cases.yaml"
ruby -ryaml - "$tmp_dir/cases.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("cases").find { |entry| entry.fetch("protocol") == "prometheus" }.fetch("reference")["version"] = "v2.55.0"
File.write(path, YAML.dump(document))
RUBY
set +e
case_output=$(MANIFEST="$tmp_dir/cases.yaml" COMPAT_REFERENCE_MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml" COMPAT_CONFORMANCE_MODE=mock scripts/compat/conformance.sh --mock --protocol prometheus --out "$tmp_dir/mismatched-case" 2>&1)
case_status=$?
set -e
test "$case_status" -ne 0
grep -Fq 'reference version drift' <<<"$case_output"

for protocol in loki tempo; do
	cp tests/compat/manifests/cases.v0.yaml "$tmp_dir/$protocol-case.yaml"
	ruby -ryaml - "$tmp_dir/$protocol-case.yaml" "$protocol" <<'RUBY'
path, protocol = ARGV
document = YAML.load_file(path)
document.fetch("cases").find { |entry| entry.fetch("protocol") == protocol }.fetch("reference")["version"] = "drift-version"
File.write(path, YAML.dump(document))
RUBY
	set +e
	case_output=$(MANIFEST="$tmp_dir/$protocol-case.yaml" COMPAT_REFERENCE_MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml" scripts/compat/conformance.sh --mock --protocol "$protocol" --out "$tmp_dir/$protocol-mismatched-case" 2>&1)
	case_status=$?
	set -e
	test "$case_status" -ne 0
	grep -Fq 'reference version drift' <<<"$case_output"
done

cp tests/compat/manifests/cases.v0.yaml "$tmp_dir/metadata-drift.yaml"
ruby -ryaml - "$tmp_dir/metadata-drift.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("metadata").fetch("reference_pins").fetch("protocols").fetch("loki")["tag"] = "drift-version"
File.write(path, YAML.dump(document))
RUBY
set +e
metadata_case_output=$(MANIFEST="$tmp_dir/metadata-drift.yaml" COMPAT_REFERENCE_MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml" scripts/compat/conformance.sh --mock --protocol loki --out "$tmp_dir/metadata-mismatch" 2>&1)
metadata_case_status=$?
set -e
test "$metadata_case_status" -ne 0
grep -Fq 'metadata.reference_pins.protocols.loki.tag drift' <<<"$metadata_case_output"

cp tests/compat/manifests/cases.v0.yaml "$tmp_dir/metadata-cases.yaml"
ruby -ryaml - "$tmp_dir/metadata-cases.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("metadata").fetch("reference_pins").fetch("protocols").fetch("prometheus")["digest"] = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
File.write(path, YAML.dump(document))
RUBY
set +e
metadata_output=$(MANIFEST="$tmp_dir/metadata-cases.yaml" COMPAT_REFERENCE_MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml" COMPAT_CONFORMANCE_MODE=mock scripts/compat/conformance.sh --mock --protocol prometheus --out "$tmp_dir/mismatched-metadata" 2>&1)
metadata_status=$?
set -e
test "$metadata_status" -ne 0
grep -Fq 'metadata.reference_pins.protocols.prometheus.digest drift' <<<"$metadata_output"

COMPAT_CONFORMANCE_MODE=mock \
COMPAT_CONFORMANCE_OUT="$tmp_dir/mock" \
	make --no-print-directory test-compat >/dev/null

test "$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).fetch("mode")' "$tmp_dir/mock/versions.json")" = mock
grep -Fq 'not service-backed compatibility evidence' "$tmp_dir/mock/NOTICE.txt"
test -s "$tmp_dir/mock/artifact-index.json"
test -s "$tmp_dir/mock/execution-receipt.json"
test -s "$tmp_dir/mock/outcome.json"
ruby -rjson - "$tmp_dir/mock/execution-receipt.json" <<'RUBY'
receipt = JSON.parse(File.read(ARGV.fetch(0)))
selected = receipt.fetch("selected_case_ids")
runner = receipt.fetch("selected_runner_case_ids")
records = receipt.fetch("cases")
excluded_ids = %w[
  prometheus-metadata-discovery
  tempo-search-span-selector
  tempo-search-tags
  tempo-tag-values-peer-service
]
abort "selection receipt lost runner_case_id mapping" unless runner.zip(selected).all? { |id, case_id|
  if excluded_ids.include?(case_id)
    id.nil?
  else
    id.is_a?(String) && !id.empty?
  end
}
abort "selection receipt case/runner mapping length mismatch" unless selected.length == runner.length
records.each_with_index do |record, index|
  abort "selection receipt lost manifest case_id" unless record.fetch("case_id") == selected.fetch(index)
  abort "selection receipt lost runner_case_id" unless record.fetch("runner_case_id") == runner.fetch(index)
end
RUBY
scripts/compat/validate-artifacts.sh --root "$tmp_dir/mock"

set +e
invalid_output=$(COMPAT_CONFORMANCE_MODE=invalid make --no-print-directory test-compat 2>&1)
invalid_status=$?
set -e
test "$invalid_status" -eq 2
grep -Fq 'COMPAT_CONFORMANCE_MODE must be real or mock' <<<"$invalid_output"

echo "compatibility target regression: PASS"
