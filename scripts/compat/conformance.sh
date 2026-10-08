#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
SCRIPT_PATH=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/$(basename -- "${BASH_SOURCE[0]}")
ORIGINAL_ARGS=("$@")
MANIFEST=$(printenv MANIFEST 2>/dev/null || printf '%s' 'tests/compat/manifests/cases.v0.yaml')
REFERENCE_MANIFEST=$(printenv COMPAT_REFERENCE_MANIFEST 2>/dev/null || printf '%s' 'docs/compat/references.v0.yaml')
OUT=$(printenv OUT 2>/dev/null || printf '%s' 'target/compat/conformance')
RUN_ID=$(printenv RUN_ID 2>/dev/null || date -u +%Y%m%dT%H%M%SZ)
PROTOCOL_FILTER=
CASE_FILTER=
SHARD_INDEX=${COMPAT_CONFORMANCE_SHARD_INDEX:-}
SHARD_COUNT=${COMPAT_CONFORMANCE_SHARD_COUNT:-}
MOCK=false
DRIFT=false
CONFORMANCE_TOTAL_TIMEOUT_SECS=${COMPAT_CONFORMANCE_TOTAL_TIMEOUT_SECS:-3600}
PROTOCOL_TIMEOUT_SECS=${COMPAT_PROTOCOL_TIMEOUT_SECS:-900}
RUNNER_MAX_ATTEMPTS=${COMPAT_RUNNER_MAX_ATTEMPTS:-2}
RUNNER_RETRY_DELAY_SECS=${COMPAT_RUNNER_RETRY_DELAY_SECS:-2}
DRIFT_CANDIDATE_IMAGE=${DRIFT_CANDIDATE_IMAGE:-}
DRIFT_CANDIDATE_VERSION=${DRIFT_CANDIDATE_VERSION:-}
DRIFT_CANDIDATE_DIGEST=${DRIFT_CANDIDATE_DIGEST:-${COMPAT_DRIFT_CANDIDATE_DIGEST:-}}
DRIFT_BASELINE_IMAGE=${DRIFT_BASELINE_IMAGE:-}
DRIFT_BASELINE_VERSION=${DRIFT_BASELINE_VERSION:-}
TIMEOUT_ACTIVE=false

usage() {
	cat <<'EOF'
Usage: scripts/compat/conformance.sh [--mock|--drift] [--protocol PROTOCOL] [--case CASE_ID] [--out DIRECTORY]
       [--shard-index INDEX --shard-count COUNT]
       [--candidate-image IMAGE] [--candidate-version VERSION] [--candidate-digest DIGEST]
       [--baseline-image IMAGE] [--baseline-version VERSION]
EOF
}

while [ "$#" -gt 0 ]; do
	case "$1" in
		--mock) MOCK=true; shift ;;
		--drift) DRIFT=true; shift ;;
		--protocol)
			[ "$#" -ge 2 ] || { echo "ERROR: --protocol requires a value" >&2; exit 2; }
			PROTOCOL_FILTER=$2; shift 2 ;;
		--protocol=*) PROTOCOL_FILTER=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--case)
			[ "$#" -ge 2 ] || { echo "ERROR: --case requires a value" >&2; exit 2; }
			CASE_FILTER=$2; shift 2 ;;
		--case=*) CASE_FILTER=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--shard-index)
			[ "$#" -ge 2 ] || { echo "ERROR: --shard-index requires a value" >&2; exit 2; }
			SHARD_INDEX=$2; shift 2 ;;
		--shard-index=*) SHARD_INDEX=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--shard-count)
			[ "$#" -ge 2 ] || { echo "ERROR: --shard-count requires a value" >&2; exit 2; }
			SHARD_COUNT=$2; shift 2 ;;
		--shard-count=*) SHARD_COUNT=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--out)
			[ "$#" -ge 2 ] || { echo "ERROR: --out requires a directory" >&2; exit 2; }
			OUT=$2; shift 2 ;;
		--out=*) OUT=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--candidate-image)
			[ "$#" -ge 2 ] || { echo "ERROR: --candidate-image requires a value" >&2; exit 2; }
			DRIFT_CANDIDATE_IMAGE=$2; shift 2 ;;
		--candidate-image=*) DRIFT_CANDIDATE_IMAGE=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--candidate-version)
			[ "$#" -ge 2 ] || { echo "ERROR: --candidate-version requires a value" >&2; exit 2; }
			DRIFT_CANDIDATE_VERSION=$2; shift 2 ;;
		--candidate-version=*) DRIFT_CANDIDATE_VERSION=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--candidate-digest)
			[ "$#" -ge 2 ] || { echo "ERROR: --candidate-digest requires a value" >&2; exit 2; }
			DRIFT_CANDIDATE_DIGEST=$2; shift 2 ;;
		--candidate-digest=*) DRIFT_CANDIDATE_DIGEST=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--baseline-image)
			[ "$#" -ge 2 ] || { echo "ERROR: --baseline-image requires a value" >&2; exit 2; }
			DRIFT_BASELINE_IMAGE=$2; shift 2 ;;
		--baseline-image=*) DRIFT_BASELINE_IMAGE=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--baseline-version)
			[ "$#" -ge 2 ] || { echo "ERROR: --baseline-version requires a value" >&2; exit 2; }
			DRIFT_BASELINE_VERSION=$2; shift 2 ;;
		--baseline-version=*) DRIFT_BASELINE_VERSION=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--__timeout-active) TIMEOUT_ACTIVE=true; shift ;;
		--help|-h) usage; exit 0 ;;
		*) echo "ERROR: unknown option: $1" >&2; usage >&2; exit 2 ;;
		esac
done

if [ -z "$SHARD_INDEX" ] && [ -z "$SHARD_COUNT" ]; then
	:
elif [ -z "$SHARD_INDEX" ] || [ -z "$SHARD_COUNT" ]; then
	echo "ERROR: --shard-index and --shard-count must be supplied together" >&2
	exit 2
elif ! printf '%s' "$SHARD_INDEX" | grep -Eq '^[0-9]+$' || ! printf '%s' "$SHARD_COUNT" | grep -Eq '^[1-9][0-9]*$'; then
	echo "ERROR: shard index must be >= 0 and shard count must be a positive integer" >&2
	exit 2
elif [ "$SHARD_INDEX" -ge "$SHARD_COUNT" ]; then
	echo "ERROR: shard index must be less than shard count" >&2
	exit 2
fi

if [ "$TIMEOUT_ACTIVE" = false ]; then
	exec "$ROOT_DIR/scripts/compat/run-with-timeout" "$CONFORMANCE_TOTAL_TIMEOUT_SECS" \
		"$SCRIPT_PATH" --__timeout-active "${ORIGINAL_ARGS[@]}"
fi

if [ "$MOCK" = true ] && [ "$DRIFT" = true ]; then
	echo "ERROR: --mock and --drift are mutually exclusive" >&2
	exit 2
fi

canonical_image_reference() {
	local raw_image=$1
	local explicit_digest=$2
	local role=$3
	ruby -rjson - "$raw_image" "$explicit_digest" "$role" <<'RUBY'
raw_image, explicit_digest, role = ARGV
digest_pattern = /\Asha256:[0-9a-f]{64}\z/
abort "#{role} image is required" if raw_image.nil? || raw_image.empty?
abort "#{role} digest is invalid" unless explicit_digest.empty? || explicit_digest.match?(digest_pattern)

if raw_image.include?("@")
  image, embedded_digest = raw_image.split("@", 2)
  abort "#{role} image must contain one immutable digest" if image.empty? || embedded_digest.nil? || !embedded_digest.match?(digest_pattern)
  abort "#{role} digest disagrees with the image digest" unless explicit_digest.empty? || explicit_digest == embedded_digest
  digest = embedded_digest
else
  image = raw_image
  last_component = image.split("/").last
  abort "#{role} image must be an immutable digest-form reference" if explicit_digest.empty? || last_component.include?(":")
  digest = explicit_digest
end

abort "#{role} image must not contain a tag" if image.include?("@")
STDOUT.write(JSON.generate("image" => image, "digest" => digest, "reference" => "#{image}@#{digest}"))
RUBY
}

if [ "$DRIFT" = true ]; then
	for drift_value in DRIFT_CANDIDATE_IMAGE DRIFT_CANDIDATE_VERSION DRIFT_BASELINE_IMAGE DRIFT_BASELINE_VERSION; do
		[ -n "${!drift_value}" ] || { echo "ERROR: $drift_value is required in drift mode" >&2; exit 2; }
	done
	case "$DRIFT_CANDIDATE_VERSION" in
		""|latest|*latest*) echo "ERROR: candidate version must be immutable and non-latest" >&2; exit 2 ;;
	esac
	candidate_metadata=$(canonical_image_reference "$DRIFT_CANDIDATE_IMAGE" "$DRIFT_CANDIDATE_DIGEST" candidate)
	DRIFT_CANDIDATE_IMAGE=$(ruby -rjson -e 'puts JSON.parse(ARGV.fetch(0)).fetch("image")' "$candidate_metadata")
	DRIFT_CANDIDATE_DIGEST=$(ruby -rjson -e 'puts JSON.parse(ARGV.fetch(0)).fetch("digest")' "$candidate_metadata")
	DRIFT_CANDIDATE_REFERENCE=$(ruby -rjson -e 'puts JSON.parse(ARGV.fetch(0)).fetch("reference")' "$candidate_metadata")
	baseline_metadata=$(canonical_image_reference "$DRIFT_BASELINE_IMAGE" "" baseline)
	DRIFT_BASELINE_IMAGE=$(ruby -rjson -e 'puts JSON.parse(ARGV.fetch(0)).fetch("reference")' "$baseline_metadata")
	[ "$DRIFT_CANDIDATE_REFERENCE" != "$DRIFT_BASELINE_IMAGE" ] || [ "$DRIFT_CANDIDATE_VERSION" != "$DRIFT_BASELINE_VERSION" ] || {
		echo "ERROR: candidate reference must differ from the baseline reference" >&2
		exit 2
	}
	export DRIFT_CANDIDATE_IMAGE DRIFT_CANDIDATE_VERSION DRIFT_CANDIDATE_DIGEST DRIFT_CANDIDATE_REFERENCE DRIFT_BASELINE_IMAGE DRIFT_BASELINE_VERSION
fi

resolve_path() {
	case "$1" in
		/*) printf '%s\n' "$1" ;;
		*) printf '%s/%s\n' "$ROOT_DIR" "$1" ;;
	esac
}

MANIFEST_PATH=$(resolve_path "$MANIFEST")
REFERENCE_MANIFEST_PATH=$(resolve_path "$REFERENCE_MANIFEST")
CAPABILITY_MANIFEST_PATH=$(resolve_path "${CAPABILITY_MANIFEST:-docs/compat/capability.v0.yaml}")
OUT_PATH=$(resolve_path "$OUT")
TMP_BASE=$(printenv TMPDIR 2>/dev/null || printf '%s' /tmp)
TMP_DIR=$(mktemp -d "$TMP_BASE/compat-conformance.XXXXXX")
trap 'rm -rf "$TMP_DIR"' EXIT

[ -f "$MANIFEST_PATH" ] || { echo "ERROR: manifest does not exist: $MANIFEST_PATH" >&2; exit 2; }
[ -f "$REFERENCE_MANIFEST_PATH" ] || { echo "ERROR: reference manifest does not exist: $REFERENCE_MANIFEST_PATH" >&2; exit 2; }
[ -f "$CAPABILITY_MANIFEST_PATH" ] || { echo "ERROR: capability manifest does not exist: $CAPABILITY_MANIFEST_PATH" >&2; exit 2; }

if ! ruby -ryaml -rjson - "$MANIFEST_PATH" "$PROTOCOL_FILTER" "$CASE_FILTER" "$ROOT_DIR" "$REFERENCE_MANIFEST_PATH" "$CAPABILITY_MANIFEST_PATH" "$SHARD_INDEX" "$SHARD_COUNT" >"$TMP_DIR/selected.json" <<'RUBY'
manifest_path, protocol_filter, case_filter, repo_root, reference_manifest_path, capability_manifest_path, shard_index, shard_count = ARGV
begin
  document = YAML.safe_load(File.read(manifest_path), permitted_classes: [], permitted_symbols: [], aliases: false)
rescue StandardError => e
  warn "ERROR: invalid YAML manifest: #{e.message}"
  exit 2
end
begin
  reference_document = YAML.safe_load(File.read(reference_manifest_path), permitted_classes: [], permitted_symbols: [], aliases: false)
  canonical_references = reference_document.fetch("references")
rescue StandardError => e
  warn "ERROR: invalid reference manifest: #{e.message}"
  exit 2
end
begin
  capability_document = YAML.safe_load(File.read(capability_manifest_path), permitted_classes: [], permitted_symbols: [], aliases: false)
  canonical_capability_ids = capability_document.fetch("capability_ids")
  unless canonical_capability_ids.is_a?(Array) && canonical_capability_ids.all? { |id| id.is_a?(String) && !id.empty? } && canonical_capability_ids.uniq == canonical_capability_ids
    raise "capability_ids must be a unique sequence of non-empty strings"
  end
  canonical_unsupported_features = capability_document.fetch("unsupported_feature_ids")
  unless canonical_unsupported_features.is_a?(Hash)
    raise "unsupported_feature_ids must be a mapping"
  end
  canonical_unsupported_features.each do |capability_id, features|
    raise "unsupported_feature_ids references unknown capability #{capability_id.inspect}" unless canonical_capability_ids.include?(capability_id)
    unless features.is_a?(Array) && features.all? { |feature| feature.is_a?(String) && !feature.empty? } && features.uniq == features
      raise "unsupported_feature_ids.#{capability_id} must be a unique sequence of non-empty strings"
    end
  end
rescue StandardError => e
  warn "ERROR: invalid capability manifest: #{e.message}"
  exit 2
end

errors = []
tenant_isolation = document.is_a?(Hash) ? document.dig("metadata", "tenant_isolation") : nil
tenant_evidence_by_protocol = {}
unless tenant_isolation.is_a?(Hash)
  errors << "metadata.tenant_isolation must declare shared_helper and protocol contracts"
else
  shared_helper = tenant_isolation["shared_helper"]
  shared_helper_path = shared_helper.is_a?(String) ? File.expand_path(shared_helper, repo_root) : nil
  if !shared_helper.is_a?(String) || shared_helper.empty? || shared_helper.start_with?("/") || shared_helper.include?("..") || shared_helper_path.nil? || !shared_helper_path.start_with?(File.expand_path(repo_root) + "/") || !File.file?(shared_helper_path)
    errors << "metadata.tenant_isolation.shared_helper must be an existing repository-relative file"
  elsif !File.read(shared_helper_path).include?("authenticated_router")
    errors << "metadata.tenant_isolation.shared_helper must reference the shared authenticated tenant helper"
  end
  contracts = tenant_isolation["contracts"]
  unless contracts.is_a?(Hash)
    errors << "metadata.tenant_isolation.contracts must be a mapping"
  else
    %w[prometheus loki tempo].each do |protocol|
      contract = contracts[protocol]
      path = contract.is_a?(Hash) ? contract["path"] : nil
      command = contract.is_a?(Hash) ? contract["command"] : nil
      path_value = path.is_a?(String) ? File.expand_path(path, repo_root) : nil
      expected_command = "cargo test --lib compat::#{protocol}"
      if !path.is_a?(String) || path.empty? || path.start_with?("/") || path.include?("..") || path_value.nil? || !path_value.start_with?(File.expand_path(repo_root) + "/") || !File.file?(path_value)
        errors << "metadata.tenant_isolation.contracts.#{protocol}.path must be an existing repository-relative file"
      elsif !File.read(path_value).match?(/tenant/i)
        errors << "metadata.tenant_isolation.contracts.#{protocol}.path must contain tenant-isolation coverage"
      end
      unless command == expected_command
        errors << "metadata.tenant_isolation.contracts.#{protocol}.command must be #{expected_command.inspect}"
      end
      tenant_evidence_by_protocol[protocol] = {"shared_helper" => shared_helper, "contract_path" => path, "contract_command" => command}
    end
  end
end
%w[prometheus loki tempo grafana].each do |protocol|
  reference = canonical_references[protocol]
  unless reference.is_a?(Hash) &&
         reference["image"].is_a?(String) && !reference["image"].empty? &&
         reference["tag"].is_a?(String) && !reference["tag"].empty? &&
         reference["digest"].is_a?(String) && reference["digest"].match?(/\Asha256:[0-9a-f]{64}\z/)
    errors << "canonical #{protocol.capitalize} reference must contain an immutable sha256 digest"
  end
end
case_reference_pins = document.dig("metadata", "reference_pins", "protocols") if document.is_a?(Hash)
unless case_reference_pins.is_a?(Hash)
  errors << "metadata.reference_pins.protocols must declare compatibility reference pins"
else
  %w[prometheus loki tempo].each do |protocol|
    canonical = canonical_references[protocol]
    declared = case_reference_pins[protocol]
    if !declared.is_a?(Hash)
      errors << "metadata.reference_pins.protocols missing #{protocol}"
      next
    end
    %w[image tag digest].each do |field|
      errors << "metadata.reference_pins.protocols.#{protocol}.#{field} drift" unless declared[field] == canonical[field]
    end
  end
end
errors << "manifest root must be a mapping" unless document.is_a?(Hash)
cases = document.is_a?(Hash) ? document["cases"] : nil
errors << "manifest cases must be a non-empty sequence" unless cases.is_a?(Array) && !cases.empty?
errors << "manifest version must be compat.v0" unless document.is_a?(Hash) && document["version"] == "compat.v0"
required = %w[id protocol endpoint request fixture capability expected normalization reference evidence]
allowed_protocols = %w[prometheus loki tempo]
allowed_methods = %w[GET POST]
allowed_capability_statuses = %w[phase_1 supported supported_subset ignored unsupported_feature]
capability_ids = canonical_capability_ids
declared_features = canonical_unsupported_features
normalization_policies = {
  "prometheus" => "src/compat/prometheus/diff_normalize.rs::normalize_prom_response",
  "loki" => "tests/compat/support/loki.rs::normalize_loki_response",
  "tempo" => "tests/compat/support/tempo.rs::normalize_tempo_response"
}
seen = {}
runner_seen = {}
validated = []

safe_repo_relative_path = lambda do |path|
  path.is_a?(String) && !path.empty? &&
    !path.start_with?("/") && !path.match?(/\A[A-Za-z]:[\\\/]/) &&
    !path.include?("\\") && !path.include?("\0") &&
    path.split("/").none? { |part| part.empty? || part == "." || part == ".." }
end

existing_repo_file = lambda do |path|
  full_path = File.expand_path(path, repo_root)
  inside_repo = full_path == repo_root || full_path.start_with?(repo_root + File::SEPARATOR)
  inside_repo && File.file?(full_path)
end

validate_unsupported_allowlist = lambda do |value, prefix, expected_capability = nil|
  next if value.nil?
  unless value.is_a?(Array) || value.is_a?(Hash)
    errors << "#{prefix} unsupported-feature allowlist must be a sequence or mapping"
    next
  end
  entries = if value.is_a?(Hash) &&
               (value.key?("capability") || value.key?("capability_id") || value.key?("id"))
    [[nil, value]]
  elsif value.is_a?(Hash)
    value.to_a
  else
    value.map { |item| [nil, item] }
  end
  entries.each do |capability_key, item|
    capability_id = capability_key
    features = item
    if item.is_a?(Hash)
      capability_id ||= item["capability"] || item["capability_id"] || item["id"]
      features = item["features"] || item["feature"] || item["name"] || item["path"]
    end
    unless capability_id.is_a?(String) && capability_ids.include?(capability_id)
      errors << "#{prefix} unsupported-feature entry references unknown capability: #{capability_id.inspect}"
    end
    if expected_capability && capability_id != expected_capability
      errors << "#{prefix} unsupported-feature entry must target capability #{expected_capability}"
    end
    feature_values = features.is_a?(Array) ? features : [features]
    unless (features.is_a?(String) && !features.empty?) ||
           (features.is_a?(Array) && !features.empty? && features.all? { |feature| feature.is_a?(String) && !feature.empty? })
      errors << "#{prefix} unsupported-feature entry must contain a non-empty feature or features list"
      next
    end
    feature_values.each do |feature|
      if feature.include?("*") || feature.match?(/\A\s|\s\z/)
        errors << "#{prefix} unsupported-feature names must be exact declared feature names"
      elsif !declared_features.fetch(capability_id, []).include?(feature)
        errors << "#{prefix} unsupported-feature #{feature.inspect} is not declared for capability #{capability_id}"
      end
    end
    if item.is_a?(Hash) && (path = item["path"] || item["diff_path"])
      unless path.is_a?(String) && path.match?(/\A\$/) && !path.include?("*") &&
             !path.include?("..") && !path.match?(/(?:\.\*|\[\*\]|\.$)/)
        errors << "#{prefix} unsupported-feature diff paths must be exact, non-wildcard JSON paths"
      end
    end
  end
end

validate_unsupported_allowlist.call(document["unsupported_features"], "manifest") if document.is_a?(Hash)
validate_unsupported_allowlist.call(document["unsupported_feature_allowlist"], "manifest") if document.is_a?(Hash)

Array(cases).each_with_index do |entry, index|
  prefix = "case #{index + 1}"
  unless entry.is_a?(Hash)
    errors << "#{prefix} must be a mapping"
    next
  end
  missing = required.reject { |field| entry.key?(field) }
  errors << "#{prefix} missing required fields: #{missing.join(', ')}" unless missing.empty?
  id = entry["id"]
  safe_id = id.is_a?(String) && id.match?(/\A[A-Za-z0-9][A-Za-z0-9_.-]*\z/) && id != "." && id != ".."
  errors << "#{prefix} id must be a unique safe path component" unless safe_id
  if id.is_a?(String) && seen.key?(id)
    errors << "duplicate case id: #{id}"
  elsif safe_id
    seen[id] = true
  end
  protocol = entry["protocol"]
  if tenant_evidence_by_protocol.key?(protocol)
    entry["tenant_isolation_evidence"] = tenant_evidence_by_protocol.fetch(protocol)
  else
    errors << "#{prefix} must reference a validated tenant-isolation contract"
  end
  runner_case_id = entry["runner_case_id"]
  if runner_case_id.nil?
    exclusion = entry["conformance_exclusion"]
    unless exclusion.is_a?(Hash) && exclusion["reason"].is_a?(String) && !exclusion["reason"].empty? && exclusion["release_evidence"] == false
      errors << "#{prefix} missing runner_case_id requires an explicit non-release conformance_exclusion reason"
    end
  elsif !runner_case_id.is_a?(String) || !runner_case_id.match?(/\A[A-Za-z0-9][A-Za-z0-9_.-]*\z/) || runner_case_id == "__suite__"
    errors << "#{prefix} runner_case_id must be a safe non-sentinel ID"
  elsif runner_seen.key?([protocol, runner_case_id])
    errors << "duplicate runner_case_id for #{protocol}: #{runner_case_id}"
  else
    runner_seen[[protocol, runner_case_id]] = id
  end
  errors << "#{prefix} protocol must be one of #{allowed_protocols.join(', ')}" unless allowed_protocols.include?(protocol)
  endpoint = entry["endpoint"]
  unless endpoint.is_a?(Hash) && allowed_methods.include?(endpoint["method"]) &&
         endpoint["path"].is_a?(String) && endpoint["path"].start_with?("/")
    errors << "#{prefix} endpoint must contain a supported method (GET or POST) and absolute path"
  end
  request = entry["request"]
  if !request.is_a?(Hash) || !request["params"].is_a?(Hash)
    errors << "#{prefix} request must contain a params mapping"
  elsif request["params"].any? { |key, value| !key.is_a?(String) || !value.is_a?(String) }
    errors << "#{prefix} request.params keys and values must be strings"
  end
  fixture = entry["fixture"]
  if !fixture.is_a?(Hash) || !fixture["id"].is_a?(String) || fixture["id"].empty? ||
     !safe_repo_relative_path.call(fixture["path"])
    errors << "#{prefix} fixture.path must be a safe repository-relative path"
  elsif !existing_repo_file.call(fixture["path"])
    errors << "#{prefix} fixture.path does not exist in the repository: #{fixture["path"]}"
  end
  capability = entry["capability"]
  unless capability.is_a?(Hash) && capability["id"].is_a?(String) && !capability["id"].empty? &&
         allowed_capability_statuses.include?(capability["status"])
    errors << "#{prefix} capability.status must be one of: #{allowed_capability_statuses.join(', ')}"
  end
  if capability.is_a?(Hash) && capability["id"].is_a?(String) && !capability_ids.include?(capability["id"])
    errors << "#{prefix} unknown canonical capability: #{capability["id"]}"
  end
  expected = entry["expected"]
  unless expected.is_a?(Hash) && expected["status"].is_a?(Integer) && expected["status"].between?(100, 599) &&
         expected["envelope"].is_a?(String) && expected["envelope"].match?(/\A[A-Za-z0-9_.-]+\z/)
    errors << "#{prefix} expected must contain an HTTP status from 100..599 and a safe envelope"
  end
  normalization = entry["normalization"]
  unless normalization.is_a?(Hash) && normalization_policies[protocol] == normalization["policy"]
    errors << "#{prefix} normalization.policy is unsupported for #{protocol}"
  end
  reference = entry["reference"]
  unless reference.is_a?(Hash) && reference["service"].is_a?(String) && !reference["service"].empty? &&
         reference["version"].is_a?(String) && !reference["version"].empty?
    errors << "#{prefix} reference must contain service and version"
  else
    canonical = canonical_references[protocol]
    if !canonical.is_a?(Hash) || !canonical["tag"].is_a?(String) || canonical["tag"].empty?
      errors << "#{prefix} has no canonical reference pin for #{protocol}"
    else
      errors << "#{prefix} reference service drift: expected #{protocol}, got #{reference["service"]}" unless reference["service"] == protocol
      errors << "#{prefix} reference version drift: expected #{canonical["tag"]}, got #{reference["version"]}" unless reference["version"] == canonical["tag"]
    end
  end
  evidence = entry["evidence"]
  errors << "#{prefix} evidence must contain boolean retain" unless evidence.is_a?(Hash) && [true, false].include?(evidence["retain"])
  if evidence.is_a?(Hash) && evidence.key?("path")
    evidence_path = evidence["path"]
    if !safe_repo_relative_path.call(evidence_path)
      errors << "#{prefix} evidence.path must be a safe repository-relative path"
    elsif !existing_repo_file.call(evidence_path)
      errors << "#{prefix} evidence.path does not exist in the repository: #{evidence_path}"
    end
  end
  expected_capability = capability.is_a?(Hash) ? capability["id"] : nil
  validate_unsupported_allowlist.call(entry["unsupported_features"], prefix, expected_capability)
  validate_unsupported_allowlist.call(entry["unsupported_feature_allowlist"], prefix, expected_capability)
  validate_unsupported_allowlist.call(capability["unsupported_features"], "#{prefix} capability", expected_capability) if capability.is_a?(Hash)
  validated << entry if missing.empty? && safe_id && allowed_protocols.include?(protocol)
end

unless errors.empty?
  errors.each { |error| warn "ERROR: #{error}" }
  exit 2
end

manifest_filtered = validated.select do |entry|
  (protocol_filter.empty? || entry["protocol"] == protocol_filter) &&
    (case_filter.empty? || entry["id"] == case_filter)
end
if case_filter == "__suite__"
  errors << "case selector __suite__ is a reserved suite sentinel"
elsif !case_filter.empty? && manifest_filtered.empty?
  errors << "unknown manifest case selector: #{case_filter}"
end
unless errors.empty?
  errors.each { |error| warn "ERROR: #{error}" }
  exit 2
end
filtered = manifest_filtered
selected = if shard_index.empty?
  filtered
else
  filtered.each_with_index.select { |_entry, index| (index % shard_count.to_i) == shard_index.to_i }.map(&:first)
end
if selected.empty?
  filters = []
  filters << "protocol=#{protocol_filter}" unless protocol_filter.empty?
  filters << "case=#{case_filter}" unless case_filter.empty?
  filters << "shard=#{shard_index}/#{shard_count}" unless shard_index.empty?
  warn "ERROR: no cases selected#{filters.empty? ? '' : " (#{filters.join(', ')})"}"
  exit 2
end
selection = {
  "protocol" => (protocol_filter.empty? ? nil : protocol_filter),
  "case" => (case_filter.empty? ? nil : case_filter),
  "shard_index" => (shard_index.empty? ? nil : shard_index.to_i),
  "shard_count" => (shard_count.empty? ? nil : shard_count.to_i),
  "filtered_case_count" => filtered.length,
  "selected_case_count" => selected.length,
  "selected_case_ids" => selected.map { |entry| entry.fetch("id") },
  "selected_runner_case_ids" => selected.map { |entry| entry["runner_case_id"] }
}
STDOUT.write(JSON.generate("version" => document["version"], "selection" => selection, "cases" => selected))
RUBY
then
	exit 2
fi

write_json() {
	local destination=$1
	local json=$2
	python3 - "$destination" "$json" <<'PY'
import json
import re
import sys

destination, serialized = sys.argv[1:]
object = json.loads(serialized)
secret_key = re.compile(
    r"(?:authorization|proxy[_-]?authorization|token|password|passwd|secret|api[_-]?key|apikey|client[_-]?secret|cookie|set-cookie)",
    re.IGNORECASE,
)
bearer = re.compile(r"Bearer\s+[^\s]+", re.IGNORECASE)
header = re.compile(
    r"(\b(?:authorization|proxy[_-]?authorization|x-api-key|api-key|cookie|set-cookie)\s*[:=]\s*)[^\r\n]*",
    re.IGNORECASE,
)
query = re.compile(
    r"([?&](?:access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret|api[_-]?key|apikey|client[_-]?secret|session(?:[_-]?id)?)=)([^&#\s\"']+)",
    re.IGNORECASE,
)


def redact_text(value):
    value = bearer.sub("Bearer [REDACTED]", value)
    value = header.sub(r"\1[REDACTED]", value)
    return query.sub(r"\1[REDACTED]", value)


def redact(value, key=None):
    if key is not None and secret_key.search(str(key)):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {k: redact(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


with open(destination, "w") as file:
    file.write(json.dumps(redact(object), indent=2) + "\n")
PY
}

json_value() {
	local expression=$1
	local json=$2
	ruby -rjson -e '
  object = JSON.parse(ARGV.fetch(0))
  value = eval(ARGV.fetch(1))
  STDOUT.write(JSON.generate(value))
' "$json" "$expression"
}

canonical_json() {
	local json=$1
	ruby -rjson -e '
  def canonical(value)
    case value
    when Hash
      value.keys.map(&:to_s).sort.each_with_object({}) { |key, result| result[key] = canonical(value[key]) }
    when Array
      value.map { |item| canonical(item) }
    else
      value
    end
  end
  STDOUT.write(JSON.generate(canonical(JSON.parse(ARGV.fetch(0)))))
' "$json"
}

sha256_text() {
	local value=$1
	ruby -rdigest -e 'STDOUT.write(Digest::SHA256.hexdigest(ARGV.fetch(0)))' "$value"
}

mock_payload() {
	ruby -rjson -e '
  entry = JSON.parse(ARGV.fetch(0))
  STDOUT.write(JSON.generate(
    "case_id" => entry["id"],
    "protocol" => entry["protocol"],
    "endpoint" => entry["endpoint"],
    "request" => entry["request"],
    "expected" => entry["expected"],
    "fixture" => entry["fixture"]["id"],
    "response" => {
      "status" => entry["expected"]["status"],
      "envelope" => entry["expected"]["envelope"],
      "deterministic" => true
    }
  ))
' "$1"
}

reference_only_payload() {
	ruby -rjson -e '
  entry = JSON.parse(ARGV.fetch(0))
  exclusion = entry.fetch("conformance_exclusion")
  STDOUT.write(JSON.generate(
    "case_id" => entry.fetch("id"),
    "protocol" => entry.fetch("protocol"),
    "status" => "skipped",
    "reason" => exclusion.fetch("reason"),
    "release_evidence" => false
  ))
' "$1"
}

runner_for_protocol() {
	case "$1" in
		prometheus) printf '%s\n' 'make test-prom-diff' ;;
		loki) printf '%s\n' 'make test-loki-diff' ;;
		tempo) printf '%s\n' 'make test-tempo-diff' ;;
		*) return 1 ;;
	esac
}

classify_failure() {
	local output=$1
	if printf '%s' "$output" | ruby -e 'exit(ARGF.read.match?(/environment[_ ]skip|\bskip(?:ped)?\b|not run/i) ? 0 : 1)'; then
		printf '%s\n' environment_skip
	elif printf '%s' "$output" | ruby -e 'exit(ARGF.read.match?(/docker|image|unavailable|not found|no such file|cannot connect|connection refused|daemon|timed? ?out|timeout/i) ? 0 : 1)'; then
		printf '%s\n' infrastructure_failure
	else
		printf '%s\n' product_regression
	fi
}

is_retryable_readiness_failure() {
	local output=$1
	printf '%s' "$output" | ruby -e 'exit(ARGF.read.match?(/readiness|health check|connection refused|cannot connect to (?:the )?docker daemon|docker daemon is not running|service unavailable|temporarily unavailable|waiting for .* to become ready/i) ? 0 : 1)'
}

monotonic_seconds() {
	ruby -e 'printf "%.6f\n", Process.clock_gettime(Process::CLOCK_MONOTONIC)'
}

reference_image_for_protocol() {
	ruby -ryaml - "$REFERENCE_MANIFEST_PATH" "$1" <<'RUBY'
manifest = YAML.load_file(ARGV.fetch(0))
protocol = ARGV.fetch(1)
reference = manifest.fetch("references").fetch(protocol)
image = reference.fetch("image")
digest = reference["digest"]
unless digest.is_a?(String) && digest.match?(/\Asha256:[0-9a-f]{64}\z/)
  abort "#{protocol.capitalize} reference must use an immutable sha256 digest"
end
puts("#{image}@#{digest}")
RUBY
}

redact_file() {
	local source=$1
	local destination=$2
	ruby - "$source" "$destination" <<'RUBY'
def redact_text(value)
  value
    .gsub(/(?i)\bBearer\s+[^\s"']+/) { "Bearer [REDACTED]" }
    .gsub(/(?i)(["']?\b(?:access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret)\b["']?\s*[:=]\s*)(["']?)([^"'\s},\]]+)\2/) { "#{$1}#{$2}[REDACTED]#{$2}" }
    .gsub(/(?i)(\b(?:authorization|proxy[_-]?authorization|x-api-key|api-key|cookie|set-cookie)\s*[:=]\s*)[^\r\n]*/) { "#{$1}[REDACTED]" }
    .gsub(/(?i)([?&](?:access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret|api[_-]?key|apikey|client[_-]?secret|session(?:[_-]?id)?)=)([^&#\s"']+)/) { "#{$1}[REDACTED]" }
end
File.open(ARGV.fetch(1), "w") { |file| file.write(redact_text(File.read(ARGV.fetch(0)))) }
RUBY
}

copy_redacted_artifact() {
	local source=$1
	local destination=$2
	mkdir -p "$(dirname "$destination")"
	if ! ruby - "$source" <<'RUBY'
bytes = File.binread(ARGV.fetch(0))
text = bytes.force_encoding(Encoding::UTF_8)
abort "binary artifact" if bytes.include?("\x00") || !text.valid_encoding?
RUBY
	then
		echo "ERROR: refusing to copy unexpected binary artifact: $source" >&2
		return 2
	fi
	if [ "${source##*.}" = json ]; then
		write_json "$destination" "$(cat "$source")"
	else
		redact_file "$source" "$destination"
	fi
}

copy_first_artifact() {
	local destination=$1
	shift
	local source
	for source in "$@"; do
		if [ -f "$source" ]; then
			copy_redacted_artifact "$source" "$destination"
			return 0
		fi
	done
	return 1
}

write_case_provenance() {
	local case_dir=$1
	local case_json=$2
	local protocol=$3
	local suite_artifact_source=$4
	local evidence_source=$5
	local provenance_json

	provenance_json=$(ruby -rjson -rdigest - "$case_json" "$case_dir/request.raw.json" "$RUN_ID" "$protocol" \
		"$suite_artifact_source" "$evidence_source" <<'RUBY'
case_document = JSON.parse(File.read(ARGV.fetch(0)))
request_document = JSON.parse(File.read(ARGV.fetch(1)))
run_id, protocol, suite_artifact_source, evidence_source = ARGV.drop(2)

def canonical(value)
  case value
  when Hash
    value.keys.map(&:to_s).sort.each_with_object({}) { |key, result| result[key] = canonical(value[key]) }
  when Array
    value.map { |item| canonical(item) }
  else
    value
  end
end

canonical_request = canonical(request_document)
canonical_request_json = JSON.generate(canonical_request)
request_fingerprint = Digest::SHA256.hexdigest(canonical_request_json)
endpoint = case_document.fetch("endpoint")
fixture = case_document.fetch("fixture")
capability = case_document.fetch("capability")
expected = case_document.fetch("expected")
normalization = case_document.fetch("normalization")
reference = case_document.fetch("reference")

STDOUT.write(JSON.generate(
  "case_id" => case_document.fetch("id"),
  "protocol" => protocol,
  "runner_case_id" => case_document["runner_case_id"],
  "tenant_isolation_evidence" => case_document["tenant_isolation_evidence"],
  "endpoint" => { "method" => endpoint.fetch("method"), "path" => endpoint.fetch("path") },
  "canonical_request_json" => canonical_request,
  "canonical_request_json_text" => canonical_request_json,
  "request_fingerprint" => request_fingerprint,
  "request_fingerprint_algorithm" => "SHA-256",
  "request_sha256" => request_fingerprint,
  "fixture" => { "id" => fixture.fetch("id"), "path" => fixture.fetch("path") },
  "fixture_id" => fixture.fetch("id"),
  "fixture_path" => fixture.fetch("path"),
  "capability" => { "id" => capability.fetch("id"), "status" => capability.fetch("status") },
  "capability_id" => capability.fetch("id"),
  "capability_status" => capability.fetch("status"),
  "expected" => { "status" => expected.fetch("status"), "envelope" => expected.fetch("envelope") },
  "expected_status" => expected.fetch("status"),
  "expected_envelope" => expected.fetch("envelope"),
  "normalization" => { "policy" => normalization.fetch("policy") },
  "normalization_policy" => normalization.fetch("policy"),
  "reference" => { "service" => reference.fetch("service"), "version" => reference.fetch("version") },
  "reference_service" => reference.fetch("service"),
  "reference_version" => reference.fetch("version"),
  "run_id" => run_id,
  "suite_artifact_source" => suite_artifact_source,
  "case_artifact_source" => (evidence_source.empty? ? nil : evidence_source),
  "evidence" => {
    "provenance_path" => "case_provenance.json",
    "raw" => ["softprobe.raw.json", "reference.raw.json"],
    "normalized" => ["softprobe.normalized.json", "reference.normalized.json"]
  }
))
RUBY
	)
	if [ "${COMPAT_DRIFT_MODE:-}" != real ]; then
		provenance_json=$(ruby -rjson -e 'value = JSON.parse(STDIN.read); value["release_evidence"] = false; value["validation_only"] = true; puts JSON.generate(value)' <<<"$provenance_json")
	fi
	write_json "$case_dir/case_provenance.json" "$provenance_json"
}

validate_case_provenance() {
	local case_dir=$1
	if [ ! -f "$case_dir/case_provenance.json" ]; then
		printf '%s\n' 'infrastructure_failure|missing_provenance'
		return 0
	fi
	ruby -rjson -rdigest - "$case_dir" <<'RUBY'
case_dir = ARGV.fetch(0)
begin
  provenance = JSON.parse(File.read(File.join(case_dir, "case_provenance.json")))
  case_document = JSON.parse(File.read(File.join(case_dir, "case.json")))
  request_document = JSON.parse(File.read(File.join(case_dir, "request.raw.json")))
rescue Errno::ENOENT
  puts "infrastructure_failure|missing_provenance_input"
  exit
rescue JSON::ParserError, StandardError => e
  puts "product_regression|invalid_provenance:#{e.class}"
  exit
end

def canonical(value)
  case value
  when Hash
    value.keys.map(&:to_s).sort.each_with_object({}) { |key, result| result[key] = canonical(value[key]) }
  when Array
    value.map { |item| canonical(item) }
  else
    value
  end
end

canonical_request = canonical(request_document)
canonical_request_json = JSON.generate(canonical_request)
expected_fingerprint = Digest::SHA256.hexdigest(canonical_request_json)
errors = []
errors << "case_id" unless provenance["case_id"] == case_document["id"]
errors << "protocol" unless provenance["protocol"] == case_document["protocol"]
errors << "runner_case_id" unless provenance["runner_case_id"] == case_document["runner_case_id"]
errors << "tenant_isolation_evidence" unless provenance["tenant_isolation_evidence"] == case_document["tenant_isolation_evidence"]
errors << "canonical_request_json" unless provenance["canonical_request_json"] == canonical_request
errors << "canonical_request_json_text" unless provenance["canonical_request_json_text"] == canonical_request_json
errors << "request_fingerprint" unless provenance["request_fingerprint"] == expected_fingerprint
errors << "request_sha256" unless provenance["request_sha256"] == expected_fingerprint
errors << "request_fingerprint_algorithm" unless provenance["request_fingerprint_algorithm"] == "SHA-256"
errors << "evidence_link" unless provenance.dig("evidence", "provenance_path") == "case_provenance.json"
%w[softprobe.raw.json reference.raw.json softprobe.normalized.json reference.normalized.json].each do |name|
  errors << "evidence:#{name}" unless File.file?(File.join(case_dir, name))
end
if errors.empty?
  puts "pass|provenance_valid"
else
  puts "product_regression|mismatched_provenance:#{errors.join(',')}"
end
RUBY
}

validate_execution_receipt() {
	local receipt_path=$1
	local selected_cases_path=$2
	local protocol=$3
	local expected_run_id=$4
	local expected_runner_status=$5
	if [ ! -f "$receipt_path" ]; then
		printf '%s\n' 'infrastructure_failure|missing_execution_receipt'
		return 0
	fi
	ruby -rjson -rdigest - "$receipt_path" "$selected_cases_path" "$protocol" "$expected_run_id" "$expected_runner_status" <<'RUBY'
receipt_path, selected_path, protocol, expected_run_id, expected_runner_status = ARGV

begin
  receipt = JSON.parse(File.read(receipt_path))
  selected_document = JSON.parse(File.read(selected_path))
rescue Errno::ENOENT
  puts "infrastructure_failure|missing_execution_receipt_input"
  exit
rescue JSON::ParserError, StandardError => e
  puts "product_regression|invalid_execution_receipt:#{e.class}"
  exit
end

def canonical(value)
  case value
  when Hash
    value.keys.map(&:to_s).sort.each_with_object({}) { |key, result| result[key] = canonical(value[key]) }
  when Array
    value.map { |item| canonical(item) }
  else
    value
  end
end

def mismatch(errors, name, actual, expected)
  errors << name unless actual == expected
end

selected_cases = selected_document.fetch("cases")
expected_case_ids = selected_cases.map { |entry| entry.fetch("id") }
expected_fixture_ids = selected_cases.map { |entry| entry.fetch("fixture").fetch("id") }
expected_runner_case_ids = selected_cases.map { |entry| entry["runner_case_id"] }
errors = []
mismatch(errors, "run_id", receipt["run_id"], expected_run_id)
mismatch(errors, "protocol", receipt["protocol"], protocol)
mismatch(errors, "status", receipt["status"], expected_runner_status)
mismatch(errors, "selected_case_ids", receipt["selected_case_ids"], expected_case_ids)
mismatch(errors, "selected_fixture_ids", receipt["selected_fixture_ids"], expected_fixture_ids)
receipt_runner_case_ids = receipt["selected_runner_case_ids"]
if receipt_runner_case_ids.nil?
  receipt_runner_case_ids = Array(receipt["cases"]).map { |record| record.is_a?(Hash) ? (record["runner_case_id"] || record["source_id"]) : nil }
end
mismatch(errors, "selected_runner_case_ids", receipt_runner_case_ids, expected_runner_case_ids)

executed_case_ids = receipt["executed_case_ids"]
executed_fixture_ids = receipt["executed_fixture_ids"]
unless executed_case_ids.is_a?(Array) && executed_fixture_ids.is_a?(Array)
  errors << "executed_ids"
  executed_case_ids = []
  executed_fixture_ids = []
end
if executed_case_ids.uniq != executed_case_ids
  # Fixture ids may legitimately repeat across cases (shared fixtures);
  # only case identifiers must be unique.
  errors << "duplicate_executed_ids"
end
unless executed_case_ids.all? { |case_id| expected_case_ids.include?(case_id) }
  errors << "executed_case_ids"
end
expected_executed_fixture_ids = executed_case_ids.map do |case_id|
  index = expected_case_ids.index(case_id)
  index.nil? ? nil : expected_fixture_ids[index]
end
mismatch(errors, "executed_fixture_ids", executed_fixture_ids, expected_executed_fixture_ids)
if expected_runner_status == "pass" && executed_case_ids != expected_case_ids
  errors << "incomplete_execution"
end

receipt_cases = receipt["cases"]
unless receipt_cases.is_a?(Array)
  errors << "cases"
  receipt_cases = []
end
if receipt_cases.length != expected_case_ids.length
  errors << "case_count"
end

selected_cases.each_with_index do |entry, index|
  record = receipt_cases[index]
  unless record.is_a?(Hash)
    errors << "case_#{index}_record"
    next
  end
  case_id = entry.fetch("id")
  fixture_id = entry.fetch("fixture").fetch("id")
  runner_case_id = entry["runner_case_id"]
  endpoint = entry.fetch("endpoint")
  params = entry.fetch("request").fetch("params")
  expected_request = canonical(
    "method" => endpoint.fetch("method"),
    "path" => endpoint.fetch("path"),
    "params" => params
  )
  expected_text = JSON.generate(expected_request)
  expected_fingerprint = Digest::SHA256.hexdigest(expected_text)
  mismatch(errors, "case_#{index}_case_id", record["case_id"], case_id)
  mismatch(errors, "case_#{index}_runner_case_id", record["runner_case_id"] || record["source_id"], runner_case_id)
  mismatch(errors, "case_#{index}_tenant_isolation_evidence", record["tenant_isolation_evidence"], entry["tenant_isolation_evidence"])
  mismatch(errors, "case_#{index}_fixture_id", record["fixture_id"], fixture_id)
  mismatch(errors, "case_#{index}_canonical_request", record["canonical_request"], expected_request)
  mismatch(errors, "case_#{index}_canonical_text", record["canonical_text"], expected_text)
  mismatch(errors, "case_#{index}_canonical_request_json", record["canonical_request_json"], expected_text)
  mismatch(errors, "case_#{index}_fingerprint", record["fingerprint"], expected_fingerprint)
  mismatch(errors, "case_#{index}_request_fingerprint", record["request_fingerprint"], expected_fingerprint)
  mismatch(errors, "case_#{index}_nested_fingerprint", record.dig("fingerprints", "canonical_request"), expected_fingerprint)
  mismatch(errors, "case_#{index}_fingerprint_algorithm", record["fingerprint_algorithm"], "SHA-256")
end

if errors.empty?
  puts "pass|execution_receipt_valid"
else
  puts "product_regression|mismatched_execution_receipt:#{errors.join(',')}"
end
RUBY
}

self_check_provenance() {
	local self_dir="$TMP_DIR/provenance-self-check"
	local case_json='{"id":"provenance-self-check","protocol":"prometheus"}'
	local request_json='{"endpoint":{"method":"GET","path":"/api/v1/query"},"request":{"params":{"query":"up"}}}'
	local canonical_request
	local request_fingerprint
	local provenance_json
	local validation
	mkdir -p "$self_dir"
	write_json "$self_dir/case.json" "$case_json"
	write_json "$self_dir/request.raw.json" "$request_json"
	: >"$self_dir/softprobe.raw.json"
	: >"$self_dir/reference.raw.json"
	: >"$self_dir/softprobe.normalized.json"
	: >"$self_dir/reference.normalized.json"
	canonical_request=$(canonical_json "$request_json")
	request_fingerprint=$(sha256_text "$canonical_request")
	provenance_json=$(ruby -rjson -e '
  request = JSON.parse(ARGV.fetch(0))
  canonical = JSON.generate(request.keys.sort.each_with_object({}) { |key, result| result[key] = request[key] })
  fingerprint = ARGV.fetch(1)
  puts JSON.generate(
    "case_id" => "provenance-self-check", "protocol" => "prometheus",
    "canonical_request_json" => request, "canonical_request_json_text" => canonical,
    "request_fingerprint" => fingerprint, "request_fingerprint_algorithm" => "SHA-256",
    "request_sha256" => fingerprint,
    "evidence" => { "provenance_path" => "case_provenance.json" }
  )
' "$canonical_request" "$request_fingerprint")
	write_json "$self_dir/case_provenance.json" "$provenance_json"
	validation=$(validate_case_provenance "$self_dir")
	[ "$validation" = 'pass|provenance_valid' ] || { echo "self-check failed: valid provenance ($validation)" >&2; return 1; }
	validation=$(validate_case_provenance "$TMP_DIR/provenance-self-check-missing")
	[ "$validation" = 'infrastructure_failure|missing_provenance' ] || { echo "self-check failed: missing provenance ($validation)" >&2; return 1; }
	ruby -rjson -e '
  path = ARGV.fetch(0)
  document = JSON.parse(File.read(path))
  document["request_fingerprint"] = "0" * 64
  File.write(path, JSON.generate(document))
' "$self_dir/case_provenance.json"
	validation=$(validate_case_provenance "$self_dir")
	[ "$validation" = 'product_regression|mismatched_provenance:request_fingerprint' ] || { echo "self-check failed: mismatched provenance ($validation)" >&2; return 1; }
	echo 'provenance self-check: PASS (valid, missing, mismatched)'
}

self_check_receipt() {
	local self_dir="$TMP_DIR/receipt-self-check"
	local receipt_path="$self_dir/execution-receipt.json"
	local validation
	mkdir -p "$self_dir"
	write_json "$self_dir/selected.json" '{"version":"compat.v0","cases":[{"id":"receipt-case-a","runner_case_id":"runner-a","endpoint":{"method":"GET","path":"/api/v1/query"},"request":{"params":{"query":"up"}},"fixture":{"id":"fixture-a"}},{"id":"receipt-case-b","runner_case_id":"runner-b","endpoint":{"method":"GET","path":"/api/v1/labels"},"request":{"params":{}},"fixture":{"id":"fixture-b"}}]}'
	ruby -rjson -rdigest - "$self_dir/selected.json" "$receipt_path" <<'RUBY'
selected_path, receipt_path = ARGV
selected = JSON.parse(File.read(selected_path))
canonical = lambda do |value|
  case value
  when Hash
    value.keys.map(&:to_s).sort.each_with_object({}) { |key, result| result[key] = canonical.call(value[key]) }
  when Array
    value.map { |item| canonical.call(item) }
  else
    value
  end
end
records = selected.fetch("cases").map do |entry|
  endpoint = entry.fetch("endpoint")
  request = canonical.call("method" => endpoint.fetch("method"), "path" => endpoint.fetch("path"), "params" => entry.fetch("request").fetch("params"))
  text = JSON.generate(request)
  fingerprint = Digest::SHA256.hexdigest(text)
  {
    "case_id" => entry.fetch("id"),
    "runner_case_id" => entry["runner_case_id"],
    "source_id" => entry.fetch("id"),
    "fixture_id" => entry.fetch("fixture").fetch("id"),
    "canonical_request" => request,
    "canonical_text" => text,
    "canonical_request_json" => text,
    "fingerprint" => fingerprint,
    "request_fingerprint" => fingerprint,
    "fingerprints" => { "canonical_request" => fingerprint },
    "fingerprint_algorithm" => "SHA-256"
  }
end
File.write(receipt_path, JSON.generate(
  "run_id" => "receipt-self-check-run",
  "protocol" => "prometheus",
  "selected_case_ids" => selected.fetch("cases").map { |entry| entry.fetch("id") },
  "selected_runner_case_ids" => selected.fetch("cases").map { |entry| entry["runner_case_id"] },
  "executed_case_ids" => selected.fetch("cases").map { |entry| entry.fetch("id") },
  "selected_fixture_ids" => selected.fetch("cases").map { |entry| entry.fetch("fixture").fetch("id") },
  "executed_fixture_ids" => selected.fetch("cases").map { |entry| entry.fetch("fixture").fetch("id") },
  "status" => "pass",
  "outcome" => "pass",
  "cases" => records
))
RUBY
	validation=$(validate_execution_receipt "$receipt_path" "$self_dir/selected.json" prometheus receipt-self-check-run pass)
	[ "$validation" = 'pass|execution_receipt_valid' ] || { echo "self-check failed: valid receipt ($validation)" >&2; return 1; }
	validation=$(validate_execution_receipt "$self_dir/missing.json" "$self_dir/selected.json" prometheus receipt-self-check-run pass)
	[ "$validation" = 'infrastructure_failure|missing_execution_receipt' ] || { echo "self-check failed: missing receipt ($validation)" >&2; return 1; }
	ruby -rjson -e '
  path = ARGV.fetch(0)
  document = JSON.parse(File.read(path))
  document["cases"][0]["fingerprint"] = "0" * 64
  File.write(path, JSON.generate(document))
' "$receipt_path"
	validation=$(validate_execution_receipt "$receipt_path" "$self_dir/selected.json" prometheus receipt-self-check-run pass)
	case "$validation" in
		product_regression\|mismatched_execution_receipt:*fingerprint*) ;;
		*) echo "self-check failed: mismatched receipt ($validation)" >&2; return 1 ;;
	esac
	echo 'execution receipt self-check: PASS (valid, missing, mismatched)'
}

case_artifact_dir() {
	local protocol=$1
	local case_id=$2
	local runner_case_id=${3:-$case_id}
	local candidate
	for candidate in \
		"$RUN_ARTIFACT_DIR/$protocol/$runner_case_id" \
		"$RUN_ARTIFACT_DIR/$runner_case_id"; do
		if [ -d "$candidate" ]; then
			printf '%s\n' "$candidate"
			return 0
		fi
	done
	if [ "${RUN_PROTOCOL_CASE_COUNT:-0}" -eq 1 ] && [ -d "$RUN_ARTIFACT_DIR" ]; then
		printf '%s\n' "$RUN_ARTIFACT_DIR"
		return 0
	fi
	return 1
}

suite_artifact_dir() {
	local protocol=$1
	local candidate
	for candidate in \
		"$RUN_ARTIFACT_DIR/$protocol" \
		"$RUN_ARTIFACT_DIR/suite" \
		"$RUN_ARTIFACT_DIR"; do
		if [ -d "$candidate" ]; then
			printf '%s\n' "$candidate"
			return 0
		fi
	done
	return 1
}

extract_wrapper_side() {
	local source=$1
	local destination=$2
	shift 2
	local value
	value=$(ruby -rjson -e '
  object = JSON.parse(File.read(ARGV.fetch(0)))
  sides = ARGV.drop(1)
  value = sides.lazy.map { |side| object[side] }.find { |candidate| !candidate.nil? }
  exit 1 if value.nil?
  STDOUT.write(JSON.generate(value))
' "$source" "$@" 2>/dev/null) || return 1
	write_json "$destination" "$value"
}

ingest_from_source() {
	local source_dir=$1
	local case_dir=$2
	local copied=0

	if copy_first_artifact "$case_dir/softprobe.raw.json" \
		"$source_dir/softprobe.raw.json" "$source_dir/lake.raw.json" \
		"$source_dir/raw.softprobe.json" "$source_dir/raw.lake.json"; then
		copied=$((copied + 1))
	fi
	if copy_first_artifact "$case_dir/reference.raw.json" \
		"$source_dir/reference.raw.json" "$source_dir/oracle.raw.json" \
		"$source_dir/raw.reference.json" "$source_dir/raw.oracle.json"; then
		copied=$((copied + 1))
	fi
	if copy_first_artifact "$case_dir/softprobe.normalized.json" \
		"$source_dir/softprobe.normalized.json" "$source_dir/lake.normalized.json" \
		"$source_dir/normalized.softprobe.json" "$source_dir/normalized.lake.json"; then
		copied=$((copied + 1))
	fi
	if copy_first_artifact "$case_dir/reference.normalized.json" \
		"$source_dir/reference.normalized.json" "$source_dir/oracle.normalized.json" \
		"$source_dir/normalized.reference.json" "$source_dir/normalized.oracle.json"; then
		copied=$((copied + 1))
	fi
	# The shared lifecycle helper also emits raw/normalized wrappers. Split only
	# actual members; never invent a missing reference or Softprobe side.
	if [ ! -f "$case_dir/softprobe.raw.json" ] &&
		extract_wrapper_side "$source_dir/raw.json" "$case_dir/softprobe.raw.json" softprobe lake; then
		copied=$((copied + 1))
	fi
	if [ ! -f "$case_dir/reference.raw.json" ] &&
		extract_wrapper_side "$source_dir/raw.json" "$case_dir/reference.raw.json" reference oracle; then
		copied=$((copied + 1))
	fi
	if [ ! -f "$case_dir/softprobe.normalized.json" ] &&
		extract_wrapper_side "$source_dir/normalized.json" "$case_dir/softprobe.normalized.json" softprobe lake; then
		copied=$((copied + 1))
	fi
	if [ ! -f "$case_dir/reference.normalized.json" ] &&
		extract_wrapper_side "$source_dir/normalized.json" "$case_dir/reference.normalized.json" reference oracle; then
		copied=$((copied + 1))
	fi

	ARTIFACT_COUNT=$copied
	[ "$copied" -eq 4 ]
}

ingest_case_artifacts() {
	local protocol=$1
	local case_id=$2
	local case_dir=$3
	local case_json=$case_dir/case.json
	local source_dir
	local runner_case_id
	ARTIFACT_COUNT=0
	EVIDENCE_SCOPE=none
	EVIDENCE_SOURCE=

	runner_case_id=$(json_value 'object.fetch("runner_case_id")' "$(cat "$case_json")" | tr -d '"' 2>/dev/null || printf '%s' "$case_id")
	source_dir=$(case_artifact_dir "$protocol" "$case_id" "$runner_case_id" 2>/dev/null || true)
	if [ -n "$source_dir" ] && ingest_from_source "$source_dir" "$case_dir"; then
		EVIDENCE_SCOPE=case
		EVIDENCE_SOURCE=$source_dir
		return 0
	fi

	return 1
}

write_normalized_diff() {
	local case_dir=$1
	local case_json=$2
	local diff_json
diff_json=$(ruby -rjson - "$case_dir/softprobe.normalized.json" "$case_dir/reference.normalized.json" "$case_json" <<'RUBY'
softprobe = JSON.parse(File.read(ARGV.fetch(0)))
reference = JSON.parse(File.read(ARGV.fetch(1)))
manifest_case = JSON.parse(File.read(ARGV.fetch(2)))
allowlist = []
%w[unsupported_features unsupported_feature_allowlist].each do |key|
  value = manifest_case[key]
  allowlist.concat(value.is_a?(Array) ? value : [])
end
allowlist.concat(Array(manifest_case.dig("capability", "unsupported_features")))
allowlist = allowlist.each_with_object([]) do |item, paths|
  path = if item.is_a?(Hash)
    item["path"] || item["diff_path"]
  elsif item.is_a?(String)
    item
  end
  paths << path if path
end

def path_for_key(path, key)
  key.match?(/\A[A-Za-z_][A-Za-z0-9_]*\z/) ? "#{path}.#{key}" : "#{path}[#{key.inspect}]"
end

def approved_path?(path, allowlist)
  allowlist.include?(path)
end

def collect_differences(left, right, path, allowlist, differences)
  if left.is_a?(Hash) && right.is_a?(Hash)
    (left.keys | right.keys).map(&:to_s).sort.each do |key|
      collect_differences(left[key], right[key], path_for_key(path, key), allowlist, differences)
    end
  elsif left.is_a?(Array) && right.is_a?(Array)
    [left.length, right.length].max.times do |index|
      collect_differences(left[index], right[index], "#{path}[#{index}]", allowlist, differences)
    end
  elsif left != right
    differences << {
      "path" => path,
      "softprobe" => left,
      "reference" => right,
      "approved" => approved_path?(path, allowlist)
    }
  end
end

differences = []
collect_differences(softprobe, reference, "$", allowlist, differences)
unapproved = differences.reject { |difference| difference["approved"] }
puts JSON.generate(
  "equal" => unapproved.empty?,
  "differences" => differences,
  "approved_differences" => differences.count { |difference| difference["approved"] },
  "unapproved_differences" => unapproved.length,
  "classification" => (unapproved.empty? ? "pass" : "product_regression"),
  "release_evidence" => (ENV["COMPAT_DRIFT_MODE"] == "real" && unapproved.empty?),
  "validation_only" => (ENV["COMPAT_DRIFT_MODE"] != "real")
)
RUBY
	)
	equal=$(printf '%s' "$diff_json" | ruby -rjson -e 'puts JSON.parse(STDIN.read).fetch("equal")')
	write_json "$case_dir/diff.json" "$diff_json"
	[ "$equal" = true ]
}

canonicalize_case_artifacts() {
	local case_dir=$1
	local mode=$2
	ruby -rjson -rdigest - "$case_dir" "$mode" <<'RUBY'
case_dir, mode = ARGV
case_path = File.join(case_dir, "case.json")
manifest_case = JSON.parse(File.read(case_path))
outcome_path = File.join(case_dir, "outcome.json")
outcome = JSON.parse(File.read(outcome_path))
endpoint = manifest_case.fetch("endpoint")
request = manifest_case.fetch("request")
canonical = lambda do |value|
  case value
  when Hash
    value.keys.map(&:to_s).sort.each_with_object({}) { |key, result| result[key] = canonical.call(value[key]) }
  when Array
    value.map { |item| canonical.call(item) }
  else
    value
  end
end
canonical_request = canonical.call(
  "method" => endpoint.fetch("method"),
  "path" => endpoint.fetch("path"),
  "params" => request.fetch("params", {})
)
fingerprint = Digest::SHA256.hexdigest(JSON.generate(canonical_request))
release = mode == "real" &&
  outcome.fetch("status") == "pass" &&
  outcome.fetch("evidence_scope", "none") == "case" &&
  outcome.fetch("artifact_count", 0) == 4 &&
  outcome.fetch("provenance_status", "") == "pass" &&
  outcome.fetch("execution_receipt_status", "") == "pass"
meta = {
  "run_id" => ENV.fetch("RUN_ID"),
  "case_id" => manifest_case.fetch("id"),
  "protocol" => manifest_case.fetch("protocol"),
  "fixture_id" => manifest_case.fetch("fixture").fetch("id"),
  "runner_case_id" => manifest_case["runner_case_id"],
  "tenant_isolation_evidence" => manifest_case["tenant_isolation_evidence"],
  "conformance_exclusion" => manifest_case["conformance_exclusion"],
  "reason" => (manifest_case["conformance_exclusion"] ? "conformance_exclusion" : nil),
  "request_fingerprint" => fingerprint,
  "fingerprint_algorithm" => "SHA-256",
  "release_evidence" => release,
  "validation_only" => (mode != "real")
}
case_document = meta.merge(
  "schema_version" => "compat-case.v1",
  "endpoint" => endpoint,
  "request" => request,
  "canonical_request" => canonical_request,
  "capability" => manifest_case.fetch("capability"),
  "expected" => manifest_case.fetch("expected"),
  "normalization" => manifest_case.fetch("normalization"),
  "reference" => manifest_case.fetch("reference"),
  "fixture" => manifest_case.fetch("fixture")
)
File.write(case_path, JSON.pretty_generate(case_document) + "\n")

def read_json(path)
  JSON.parse(File.read(path))
end

def write_json(path, value)
  File.write(path, JSON.pretty_generate(value) + "\n")
end

%w[request.raw.json request.normalized.json softprobe.raw.json softprobe.normalized.json reference.raw.json reference.normalized.json diff.json].each do |name|
  path = File.join(case_dir, name)
  payload = read_json(path)
  artifact = meta.merge("schema_version" => "compat-case-artifact.v1", "artifact_kind" => name.delete_suffix(".json"), "payload" => payload)
  if name == "diff.json"
    artifact["classification"] = payload.fetch("classification")
    artifact["equal"] = payload.fetch("equal", true)
  end
  write_json(path, artifact)
end

provenance_path = File.join(case_dir, "case_provenance.json")
provenance = read_json(provenance_path)
write_json(provenance_path, provenance.merge(meta).merge("schema_version" => "compat-case-provenance.v1"))

write_json(outcome_path, outcome.merge(meta).merge("schema_version" => "compat-case-outcome.v1"))
RUBY
}

copy_artifact_tree() {
	local source_dir=$1
	local destination_dir=$2
	local source relative
	[ -d "$source_dir" ] || return 0
	while IFS= read -r -d '' source; do
		relative=${source#"$source_dir"/}
		copy_redacted_artifact "$source" "$destination_dir/$relative"
	done < <(find "$source_dir" -type f -print0)
}

run_protocol_target() {
	local protocol=$1
	local artifact_dir=$2
	local selected_cases=$3
	local command=$4
	if [ "${SELECTION_DEBUG:-0}" = "1" ]; then
		echo "SELECTION_DEBUG run_protocol selected_file=$selected_cases ids=$(ruby -rjson -e 'begin; puts JSON.parse(File.read(ARGV.fetch(0))).fetch("cases").select{|c| c["runner_case_id"].is_a?(String)}.map{|c| c["runner_case_id"]}.join(","); rescue => e; puts "RUBY_ERROR=#{e.message}"; end' "$selected_cases" 2>&1)" >&2
	fi
	local -a protocol_env
	case "$protocol" in
		prometheus)
			protocol_env=(
				"PROMETHEUS_DIFF_ARTIFACT_DIR=$artifact_dir"
				"PROMETHEUS_RAW_ARTIFACT=$artifact_dir/raw.json"
				"PROMETHEUS_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
				"PROMETHEUS_DIFF_RAW_ARTIFACT=$artifact_dir/raw.json"
				"PROMETHEUS_DIFF_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
			)
			if [ "$DRIFT" = true ]; then
				protocol_env+=("PROMETHEUS_REFERENCE_IMAGE=$DRIFT_CANDIDATE_REFERENCE")
			fi
			;;
		loki)
			protocol_env=(
				"LOKI_DIFF_ARTIFACT_DIR=$artifact_dir"
				"LOKI_RAW_ARTIFACT=$artifact_dir/raw.json"
				"LOKI_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
				"LOKI_DIFF_RAW_ARTIFACT=$artifact_dir/raw.json"
				"LOKI_DIFF_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
			)
			if [ "$DRIFT" = true ]; then
				protocol_env+=("LOKI_REFERENCE_IMAGE=$DRIFT_CANDIDATE_REFERENCE")
			fi
			;;
		tempo)
			protocol_env=(
				"TEMPO_DIFF_ARTIFACT_DIR=$artifact_dir"
				"TEMPO_RAW_ARTIFACT=$artifact_dir/raw.json"
				"TEMPO_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
				"TEMPO_DIFF_RAW_ARTIFACT=$artifact_dir/raw.json"
				"TEMPO_DIFF_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
			)
			if [ "$DRIFT" = true ]; then
				protocol_env+=("TEMPO_REFERENCE_IMAGE=$DRIFT_CANDIDATE_REFERENCE")
			fi
			;;
		*) return 2 ;;
	esac
	(
		cd "$ROOT_DIR"
		COMPAT_CASE_IDS_VALUE="$(ruby -rjson -e 'ids = JSON.parse(File.read(ARGV.fetch(0)))[String.new("cases")].select { |entry| entry[String.new("runner_case_id")].is_a?(String) }.map { |entry| entry[String.new("runner_case_id")] }; puts ids.join(String.new(","))' "$selected_cases")"
		env "${protocol_env[@]}" \
			"SOFTPROBE_COMPAT_ARTIFACT_DIR=$artifact_dir" \
			"COMPAT_PROTOCOL=$protocol" \
			"COMPAT_CASE_JSON=$selected_cases" \
			"COMPAT_CASE_IDS=$COMPAT_CASE_IDS_VALUE" \
			"COMPAT_CONFORMANCE_OUT=$artifact_dir" \
			"COMPAT_RUN_ID=$RUN_ID" \
			COMPAT_RUN_SCOPE=suite \
			"$ROOT_DIR/scripts/compat/run-with-timeout" "$PROTOCOL_TIMEOUT_SECS" bash -c "$command"
	)
}

self_check_drift() {
	local self_out="$OUT_PATH/drift-self-check"
	export COMPAT_DRIFT_MODE=drift
	rm -rf "$self_out"
	mkdir -p "$self_out"

	while IFS='|' read -r protocol baseline candidate; do
		local case_dir="$self_out/$protocol/drift-self-check"
		mkdir -p "$case_dir"
		write_json "$case_dir/case.json" "$(printf '{\"id\":\"drift-self-check\",\"protocol\":\"%s\",\"unsupported_features\":[],\"release_evidence\":false}' "$protocol")"
		write_json "$case_dir/request.raw.json" '{"request":{"params":{"query":"self-check"}}}'
		write_json "$case_dir/request.normalized.json" '{"request":{"params":{"query":"self-check"}}}'
		write_json "$case_dir/softprobe.raw.json" "$(printf '{\"reference\":\"%s\",\"value\":\"baseline\"}' "$baseline")"
		write_json "$case_dir/reference.raw.json" "$(printf '{\"reference\":\"%s\",\"value\":\"candidate\"}' "$candidate")"
		write_json "$case_dir/softprobe.normalized.json" '{"semantic_value":"baseline"}'
		write_json "$case_dir/reference.normalized.json" '{"semantic_value":"candidate"}'
		if write_normalized_diff "$case_dir" "$case_dir/case.json"; then
			echo "drift self-check unexpectedly reported equal semantics for $protocol" >&2
			return 1
		fi
		write_json "$case_dir/case_provenance.json" "$(printf '{\"mode\":\"drift\",\"protocol\":\"%s\",\"baseline\":\"%s\",\"candidate\":\"%s\",\"release_evidence\":false}' "$protocol" "$baseline" "$candidate")"
		if [ "$protocol" = prometheus ]; then
			write_json "$case_dir/outcome.json" "$(printf '{\"mode\":\"drift\",\"status\":\"drift\",\"classification\":\"drift\",\"review_status\":\"needs_review\",\"reference_image\":\"%s\",\"baseline\":{\"image\":\"%s\"},\"candidate\":{\"image\":\"%s\",\"version\":\"%s\",\"digest\":\"%s\"},\"release_evidence\":false}' "$candidate" "$baseline" "$candidate" "$DRIFT_CANDIDATE_VERSION" "$DRIFT_CANDIDATE_DIGEST")"
		else
			write_json "$case_dir/outcome.json" "$(printf '{\"mode\":\"drift\",\"status\":\"drift\",\"classification\":\"drift\",\"review_status\":\"needs_review\",\"baseline\":{\"image\":\"%s\"},\"candidate\":{\"image\":\"%s\",\"version\":\"%s\",\"digest\":\"%s\"},\"release_evidence\":false}' "$baseline" "$candidate" "$DRIFT_CANDIDATE_VERSION" "$DRIFT_CANDIDATE_DIGEST")"
		fi
done <<EOF
prometheus|$(reference_image_for_protocol prometheus)|prom/prometheus:v2.55.0
loki|$(reference_image_for_protocol loki)|grafana/loki:3.2.0
tempo|$(reference_image_for_protocol tempo)|grafana/tempo:2.7.0
EOF

	ruby -rjson - "$self_out" <<'RUBY'
root = ARGV.fetch(0)
%w[prometheus loki tempo].each do |protocol|
  path = File.join(root, protocol, "drift-self-check", "outcome.json")
  outcome = JSON.parse(File.read(path))
  abort "invalid drift classification for #{protocol}" unless outcome["classification"] == "drift" && outcome["review_status"] == "needs_review" && outcome["release_evidence"] == false
end
prometheus_root = File.join(root, "prometheus", "drift-self-check")
prometheus_outcome = JSON.parse(File.read(File.join(prometheus_root, "outcome.json")))
abort "missing Prometheus candidate metadata" unless prometheus_outcome["reference_image"] == "prom/prometheus:v2.55.0"
abort "Prometheus drift self-check emitted configuration failure placeholder" if File.exist?(File.join(prometheus_root, "configuration_failure.json"))
report = {
  "mode" => "drift",
  "release_evidence" => false,
  "classification" => "drift",
  "review_status" => "needs_review",
  "protocols" => %w[prometheus loki tempo]
}
File.write(File.join(root, "report.json"), JSON.pretty_generate(report) + "\n")
RUBY
	echo "drift self-check: PASS (Prometheus, Loki, Tempo)"
	echo "drift self-check artifacts: $self_out"
}

if [ "${COMPAT_CONFORMANCE_SELF_CHECK:-}" = provenance ]; then
	self_check_provenance
	exit 0
fi
if [ "${COMPAT_CONFORMANCE_SELF_CHECK:-}" = receipt ]; then
	self_check_receipt
	exit 0
fi
if [ "${COMPAT_CONFORMANCE_SELF_CHECK:-}" = drift ]; then
	self_check_drift
	exit 0
fi

mkdir -p "$OUT_PATH"
: >"$OUT_PATH/report.jsonl"
mode=$([ "$MOCK" = true ] && printf mock || { [ "$DRIFT" = true ] && printf drift || printf real; })
export COMPAT_DRIFT_MODE="$mode"
export RUN_ID
mkdir -p "$TMP_DIR/durations"
if [ "$MOCK" = true ]; then
	cat >"$OUT_PATH/NOTICE.txt" <<'EOF'
This artifact was produced by the explicit mock/fast harness selector.
Mock comparisons are manifest-shaped checks, not service-backed compatibility evidence.
EOF
fi

case_count=0
pass_count=0
product_regressions=0
drift_cases=0
infrastructure_failures=0
environment_skips=0
{
	echo "# Compatibility conformance"
	echo
	echo "- Run ID: $RUN_ID"
	echo "- Mode: $mode"
	if [ "$DRIFT" = true ]; then
		echo "- Candidate: $DRIFT_CANDIDATE_REFERENCE (version $DRIFT_CANDIDATE_VERSION)"
		echo "- Baseline: $DRIFT_BASELINE_IMAGE:$DRIFT_BASELINE_VERSION"
		echo "- Classification: semantic differences are drift; review_status=needs_review"
	fi
	echo "- Manifest: $MANIFEST"
	echo
	echo "| Case | Fixture | Protocol | Status |"
	echo "| --- | --- | --- | --- |"
} >"$OUT_PATH/summary.md"

if [ "$MOCK" != true ]; then
	while IFS= read -r protocol; do
		[ -n "$protocol" ] || continue

		RUN_ARTIFACT_DIR="$OUT_PATH/.differential/$protocol/$RUN_ID"
		suite_dir="$OUT_PATH/suite/$protocol"
		mkdir -p "$RUN_ARTIFACT_DIR" "$suite_dir/artifacts"
		runner_command=$(printenv COMPAT_RUNNER_CMD 2>/dev/null || true)
		if [ -z "$runner_command" ]; then
			runner_command=$(runner_for_protocol "$protocol" || true)
		fi
		printf '%s\n' "$runner_command" >"$suite_dir/command.txt"
			protocol_cases="$TMP_DIR/$protocol.selected.json"
			ruby -rjson -e '
  document = JSON.parse(File.read(ARGV.fetch(0)))
  protocol = ARGV.fetch(1)
  selected = document.fetch("cases").select { |entry| entry.fetch("protocol") == protocol && entry["runner_case_id"].is_a?(String) }
  STDOUT.write(JSON.generate("version" => document.fetch("version"), "cases" => selected))
' "$TMP_DIR/selected.json" "$protocol" >"$protocol_cases"
			runner_case_count=$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).fetch("cases").length' "$protocol_cases")
			stdout_file="$TMP_DIR/$protocol.stdout"
			stderr_file="$TMP_DIR/$protocol.stderr"
		runner_exit_code=127
			runner_started=$(monotonic_seconds)
			if [ "$runner_case_count" -eq 0 ]; then
				runner_command="reference-only selection; no differential runner invoked"
				runner_exit_code=0
				printf '%s\n' 'reference-only selection has no executable runner cases' >"$stderr_file"
				: >"$stdout_file"
			elif [ -n "$runner_command" ]; then
			runner_attempt=1
			while :; do
				: >"$stdout_file"
				: >"$stderr_file"
				set +e
				run_protocol_target "$protocol" "$RUN_ARTIFACT_DIR" "$protocol_cases" "$runner_command" \
					>"$stdout_file" 2>"$stderr_file"
				runner_exit_code=$?
				set -e
				# Persist raw runner output immediately so an abort during
				# validation can never hide the runner's own diagnostics.
				cp -f "$stdout_file" "$suite_dir/runner.stdout.raw" 2>/dev/null || true
				cp -f "$stderr_file" "$suite_dir/runner.stderr.raw" 2>/dev/null || true
				if [ "$runner_exit_code" -eq 0 ] || [ "$runner_attempt" -ge "$RUNNER_MAX_ATTEMPTS" ]; then
					break
				fi
				attempt_output=$(cat "$stdout_file" "$stderr_file")
				if ! is_retryable_readiness_failure "$attempt_output"; then
					break
				fi
				runner_attempt=$((runner_attempt + 1))
				sleep "$RUNNER_RETRY_DELAY_SECS"
			done
		else
			printf '%s\n' "no differential target configured for protocol $protocol" >"$stderr_file"
			: >"$stdout_file"
		fi
		runner_finished=$(monotonic_seconds)
		runner_duration=$(ruby -e 'printf "%.6f\n", ARGV[1].to_f - ARGV[0].to_f' "$runner_started" "$runner_finished")
		printf '%s\n' "$runner_duration" >"$TMP_DIR/durations/$protocol"
		if [ "$runner_exit_code" -eq 0 ]; then
			protocol_status=pass
		else
			protocol_output=$(cat "$stdout_file" "$stderr_file")
			if [ "$DRIFT" = true ]; then
				protocol_status=infrastructure_failure
			else
				protocol_status=$(classify_failure "$protocol_output")
				if [ "$runner_exit_code" -eq 127 ] && [ -z "$runner_command" ]; then
					protocol_status=infrastructure_failure
				fi
			fi
		fi
		receipt_validation_status=not_applicable
		receipt_validation_reason=drift_mode
		if [ "$DRIFT" != true ] && [ "$runner_case_count" -gt 0 ]; then
# The Rust runners cannot know the manifest's shared tenant-isolation
# contract metadata; inject it so receipt validation compares like-for-like.
ruby -rjson - "$RUN_ARTIFACT_DIR/execution-receipt.json" "$protocol_cases" <<'RUBY'
receipt_path = ARGV.fetch(0)
cases = JSON.parse(File.read(ARGV.fetch(1))).fetch("cases")
receipt = JSON.parse(File.read(receipt_path))
expected = cases.each_with_object({}) { |entry, map| map[entry.fetch("id")] = entry["tenant_isolation_evidence"] }
runners = cases.each_with_object({}) { |entry, map| map[entry.fetch("id")] = entry["runner_case_id"] }
Array(receipt["cases"]).each do |record|
  evidence = expected[record["case_id"]]
  record["tenant_isolation_evidence"] = evidence if evidence && !record.key?("tenant_isolation_evidence")
  record["runner_case_id"] = runners[record["case_id"]] if runners.key?(record["case_id"]) && !record.key?("runner_case_id")
end
File.write(receipt_path, JSON.pretty_generate(receipt) + "\n")
RUBY
				expected_receipt_status=$([ "$runner_exit_code" -eq 0 ] && printf '%s' pass || printf '%s' failure)
				receipt_validation=$(validate_execution_receipt \
					"$RUN_ARTIFACT_DIR/execution-receipt.json" "$protocol_cases" "$protocol" "$RUN_ID" "$expected_receipt_status")
				receipt_validation_status=${receipt_validation%%|*}
				receipt_validation_reason=${receipt_validation#*|}
				if [ "$receipt_validation_status" != pass ] && [ "$protocol_status" = pass ]; then
					protocol_status=$receipt_validation_status
				fi
		elif [ "$DRIFT" = true ]; then
			receipt_validation_status=not_applicable
			receipt_validation_reason=drift_mode
		else
			receipt_validation_status=not_applicable
			receipt_validation_reason=reference_only_selection
		fi
		redact_file "$stdout_file" "$suite_dir/runner.stdout"
		redact_file "$stderr_file" "$suite_dir/runner.stderr"
		candidate_reference_image=
		if [ "$DRIFT" = true ]; then
			candidate_reference_image="$DRIFT_CANDIDATE_REFERENCE"
		fi
		suite_outcome=$(ruby -rjson -e '
  puts JSON.generate(
    "run_id" => ARGV[0], "protocol" => ARGV[1], "scope" => "suite",
    "status" => ARGV[2], "runner_command" => ARGV[3],
    "runner_exit_code" => Integer(ARGV[4]),
    "artifact_dir" => ARGV[5],
    "selected_cases" => JSON.parse(ARGV[6]).fetch("cases").map { |entry| entry.fetch("id") },
    "selected_case_records" => JSON.parse(ARGV[6]).fetch("cases").map { |entry| {
      "case_id" => entry.fetch("id"), "fixture_id" => entry.fetch("fixture").fetch("id")
    } },
	    "mode" => ARGV[7],
	    "classification" => (ARGV[2] == "drift" ? "drift" : ARGV[2]),
	    "review_status" => (ARGV[2] == "drift" ? "needs_review" : "not_required"),
		    "release_evidence" => (ARGV[7] == "real" && ARGV[2] == "pass" && ARGV[9] == "pass"),
		    "validation_only" => (ARGV[7] != "real"),
	    "reference_image" => (ARGV[8].empty? ? nil : ARGV[8]),
	    "candidate" => (ARGV[7] == "drift" ? {
	      "image" => ARGV[8], "version" => ENV.fetch("DRIFT_CANDIDATE_VERSION"),
	      "digest" => ENV.fetch("DRIFT_CANDIDATE_DIGEST")
	    } : nil),
	    "execution_receipt_path" => (ARGV[9] == "not_applicable" ? nil : "execution-receipt.json"),
	    "execution_receipt_status" => ARGV[9],
	    "execution_receipt_reason" => ARGV[10]
	  )
' "$RUN_ID" "$protocol" "$protocol_status" "$runner_command" "$runner_exit_code" \
			"$RUN_ARTIFACT_DIR" "$(cat "$protocol_cases")" "$mode" "$candidate_reference_image" "$receipt_validation_status" "$receipt_validation_reason")
		write_json "$suite_dir/outcome.json" "$suite_outcome"
		if [ -n "$(find "$RUN_ARTIFACT_DIR" -type f -print -quit 2>/dev/null)" ]; then
			copy_artifact_tree "$RUN_ARTIFACT_DIR" "$suite_dir/artifacts"
		fi
done < <(ruby -rjson -e 'JSON.parse(File.read(ARGV.fetch(0))).fetch("cases").map { |entry| entry.fetch("protocol") }.uniq.each { |protocol| puts protocol }' "$TMP_DIR/selected.json")
fi

while IFS= read -r case_id; do
	[ -n "$case_id" ] || continue
	case_json=$(ruby -rjson -e '
  document = JSON.parse(File.read(ARGV.fetch(0)))
  entry = document.fetch("cases").find { |item| item["id"] == ARGV.fetch(1) }
  abort "case not found" unless entry
  STDOUT.write(JSON.generate(entry))
' "$TMP_DIR/selected.json" "$case_id")
	protocol=$(json_value 'object.fetch("protocol")' "$case_json")
	protocol=$(printf '%s' "$protocol" | tr -d '"')
	fixture_id=$(json_value 'object.fetch("fixture").fetch("id")' "$case_json")
	fixture_id=$(printf '%s' "$fixture_id" | tr -d '"')
	reference_service=$(json_value 'object.fetch("reference").fetch("service")' "$case_json")
	reference_service=$(printf '%s' "$reference_service" | tr -d '"')
	reference_version=$(json_value 'object.fetch("reference").fetch("version")' "$case_json")
	reference_version=$(printf '%s' "$reference_version" | tr -d '"')
	reference_image=$(reference_image_for_protocol "$protocol")
	if [ "$DRIFT" = true ]; then
		reference_image="$DRIFT_CANDIDATE_REFERENCE"
		reference_version="$DRIFT_CANDIDATE_VERSION"
	fi
	if [ "$mode" != real ]; then
		case_json=$(ruby -rjson -e 'value = JSON.parse(STDIN.read); value["release_evidence"] = false; value["validation_only"] = true; puts JSON.generate(value)' <<<"$case_json")
	fi
	case_dir="$OUT_PATH/$case_id"
	mkdir -p "$case_dir"
	write_json "$case_dir/case.json" "$case_json"
	request_json=$(json_value '{ "endpoint" => object.fetch("endpoint"), "request" => object.fetch("request") }' "$case_json")
	write_json "$case_dir/request.raw.json" "$request_json"
	write_json "$case_dir/request.normalized.json" "$request_json"

	status=pass
	runner_exit_code=0
	runner_duration=0.000000
	runner_command=
	reason=mock_deterministic_payload
	artifact_count=0
	evidence_scope=none
	evidence_source=
		receipt_validation_status=not_applicable
		receipt_validation_reason=mock_mode
		runner_case_id=$(json_value 'object["runner_case_id"]' "$case_json")
		if [ "$runner_case_id" = null ]; then
			status=skipped
			reason=conformance_exclusion
			payload=$(reference_only_payload "$case_json")
			write_json "$case_dir/softprobe.raw.json" "$payload"
			write_json "$case_dir/softprobe.normalized.json" "$payload"
			write_json "$case_dir/reference.raw.json" "$payload"
			write_json "$case_dir/reference.normalized.json" "$payload"
			diff_json='{"equal":true,"differences":[],"classification":"skipped","release_evidence":false}'
			write_json "$case_dir/diff.json" "$diff_json"
		elif [ "$MOCK" = true ]; then
		payload=$(mock_payload "$case_json")
		write_json "$case_dir/softprobe.raw.json" "$payload"
		write_json "$case_dir/softprobe.normalized.json" "$payload"
		write_json "$case_dir/reference.raw.json" "$payload"
		write_json "$case_dir/reference.normalized.json" "$payload"
		diff_json='{"equal":true,"differences":[],"classification":"pass","release_evidence":false}'
		write_json "$case_dir/diff.json" "$diff_json"
	else
		RUN_ARTIFACT_DIR="$OUT_PATH/.differential/$protocol/$RUN_ID"
		RUN_PROTOCOL_CASE_COUNT=$(ruby -rjson -e '
  document = JSON.parse(File.read(ARGV.fetch(0)))
  puts document.fetch("cases").count { |entry| entry.fetch("protocol") == ARGV.fetch(1) }
' "$TMP_DIR/selected.json" "$protocol")
		protocol_status=$(cat "$OUT_PATH/suite/$protocol/outcome.json" | ruby -rjson -e 'puts JSON.parse(STDIN.read).fetch("status")')
		runner_command=$(cat "$OUT_PATH/suite/$protocol/command.txt")
		runner_exit_code=$(cat "$OUT_PATH/suite/$protocol/outcome.json" | ruby -rjson -e 'puts JSON.parse(STDIN.read).fetch("runner_exit_code")')
			runner_duration=$(cat "$TMP_DIR/durations/$protocol" 2>/dev/null || printf '%s' 0.000000)
			status=$protocol_status
			reason="suite_$protocol_status"
			receipt_validation_status=$(cat "$OUT_PATH/suite/$protocol/outcome.json" | ruby -rjson -e 'puts JSON.parse(STDIN.read).fetch("execution_receipt_status")')
			receipt_validation_reason=$(cat "$OUT_PATH/suite/$protocol/outcome.json" | ruby -rjson -e 'puts JSON.parse(STDIN.read).fetch("execution_receipt_reason")')
		if ingest_case_artifacts "$protocol" "$case_id" "$case_dir"; then
			artifact_count=$ARTIFACT_COUNT
			evidence_scope=$EVIDENCE_SCOPE
			evidence_source=$EVIDENCE_SOURCE
		else
			artifact_count=$ARTIFACT_COUNT
			if [ "$status" = pass ]; then
				status=infrastructure_failure
				reason=missing_required_evidence
			fi
		fi
		if [ "$status" = pass ] || { [ "$DRIFT" = true ] && [ "$status" = drift ]; }; then
			if ! write_normalized_diff "$case_dir" "$case_dir/case.json"; then
				if [ "$DRIFT" = true ]; then
					status=drift
					reason=semantic_candidate_difference
				else
					status=product_regression
					reason=normalized_mismatch
				fi
			fi
		fi
	fi

	if [ "$mode" = real ] && [ -f "$OUT_PATH/suite/$protocol/outcome.json" ]; then
		copy_redacted_artifact "$OUT_PATH/suite/$protocol/outcome.json" "$case_dir/suite.evidence.json"
	fi

	suite_artifact_source="$OUT_PATH/suite/$protocol"
	[ "$MOCK" = true ] && suite_artifact_source=mock
	write_case_provenance "$case_dir" "$case_dir/case.json" "$protocol" "$suite_artifact_source" "$evidence_source"
	provenance_validation=$(validate_case_provenance "$case_dir")
	provenance_status=${provenance_validation%%|*}
	provenance_reason=${provenance_validation#*|}
	if [ "$MOCK" != true ] && [ "$provenance_status" != pass ] && { [ "$status" = pass ] || [ "$DRIFT" = true ] && [ "$status" = drift ]; }; then
		if [ "$DRIFT" = true ]; then
			status=infrastructure_failure
			reason=provenance_failure
		else
			status=$provenance_status
			reason=$provenance_reason
		fi
	fi
	classification=$status
	review_status=not_required
	if [ "$DRIFT" = true ] && [ "$status" = drift ]; then
		classification=drift
		review_status=needs_review
	fi

outcome_json=$(ruby -rjson -e '
  case_document = JSON.parse(ARGV.fetch(22))
  endpoint = case_document.fetch("endpoint")
  request = case_document.fetch("request")
  release = ARGV[3] == "real" && ARGV[4] == "pass" && ARGV[9] == "case" && Integer(ARGV[8]) == 4 && ARGV[16] == "pass" && ARGV[20] == "pass"
  STDOUT.write(JSON.generate("run_id" => ARGV[0], "case_id" => ARGV[1],
                              "runner_case_id" => case_document["runner_case_id"], "protocol" => ARGV[2],
                              "tenant_isolation_evidence" => case_document["tenant_isolation_evidence"],
                              "mode" => ARGV[3], "status" => ARGV[4], "runner_command" => ARGV[5],
                              "runner_exit_code" => Integer(ARGV[6]), "reason" => ARGV[7],
                              "artifact_count" => Integer(ARGV[8]),
                              "evidence_scope" => ARGV[9], "evidence_source" => ARGV[10],
                              "provenance_path" => "case_provenance.json",
	                              "provenance_status" => ARGV[16], "provenance_reason" => ARGV[17],
	                              "classification" => ARGV[18], "review_status" => ARGV[19],
	                              "execution_receipt_status" => ARGV[20], "execution_receipt_reason" => ARGV[21],
                              "release_evidence" => release,
                              "validation_only" => (ARGV[3] != "real"),
                              "endpoint" => endpoint,
                              "query" => request.fetch("params", {})["query"],
                              "capability" => case_document.fetch("capability"),
                              "conformance_exclusion" => case_document["conformance_exclusion"],
                              "normalization" => case_document.fetch("normalization"),
                              "fixture_id" => ARGV[11], "reference" => {
                                "service" => ARGV[12], "version" => ARGV[13], "image" => ARGV[14]
                              }, "candidate" => (ARGV[3] == "drift" ? {
                                "image" => ARGV[14], "version" => ARGV[13],
                                "digest" => ENV.fetch("DRIFT_CANDIDATE_DIGEST")
                              } : nil), "runner_duration_seconds" => Float(ARGV[15])))
' "$RUN_ID" "$case_id" "$protocol" "$mode" "$status" "$runner_command" "$runner_exit_code" "$reason" "$artifact_count" "$evidence_scope" "$evidence_source" "$fixture_id" "$reference_service" "$reference_version" "$reference_image" "$runner_duration" "$provenance_status" "$provenance_reason" "$classification" "$review_status" "$receipt_validation_status" "$receipt_validation_reason" "$case_json")
	write_json "$case_dir/outcome.json" "$outcome_json"
	canonicalize_case_artifacts "$case_dir" "$mode"
report_line=$(ruby -rjson -e '
  case_document = JSON.parse(ARGV.fetch(10))
  endpoint = case_document.fetch("endpoint")
  request = case_document.fetch("request")
  release = ARGV[5] == "real" && ARGV[4] == "pass" && ARGV[15] == "case" && Integer(ARGV[14]) == 4 && ARGV[16] == "pass" && ARGV[17] == "pass"
  payload = {
    "run_id" => ARGV[0],
    "case_id" => ARGV[1],
    "runner_case_id" => case_document["runner_case_id"],
    "tenant_isolation_evidence" => case_document["tenant_isolation_evidence"],
    "protocol" => ARGV[2],
    "fixture_id" => ARGV[3],
    "status" => ARGV[4],
    "mode" => ARGV[5],
    "reference_version" => ARGV[6],
    "outcome" => ARGV[7],
    "classification" => ARGV[8],
    "review_status" => ARGV[9],
    "release_evidence" => release,
    "validation_only" => (ARGV[5] != "real"),
    "endpoint" => endpoint,
    "query" => request.fetch("params", {})["query"],
    "capability" => case_document.fetch("capability"),
    "normalization" => case_document.fetch("normalization"),
    "reference" => {
      "service" => case_document.fetch("reference").fetch("service"),
      "version" => ARGV[6],
      "image" => ARGV[11]
    },
    "candidate" => (ARGV[5] == "drift" ? {
      "image" => ARGV[11],
      "version" => ARGV[6],
      "digest" => ENV.fetch("DRIFT_CANDIDATE_DIGEST")
    } : nil),
    "provenance_path" => "case_provenance.json",
    "duration_seconds" => Float(ARGV[12]),
    "runner_duration_seconds" => Float(ARGV[12]),
    "runner_exit_code" => Integer(ARGV[13]),
    "artifact_count" => Integer(ARGV[14]),
    "evidence_scope" => ARGV[15],
    "provenance_status" => ARGV[16]
  }
  puts JSON.generate(payload)
' "$RUN_ID" "$case_id" "$protocol" "$fixture_id" "$status" "$mode" "$reference_version" "$reason" "$classification" "$review_status" "$case_json" "$reference_image" "$runner_duration" "$runner_exit_code" "$artifact_count" "$evidence_scope" "$provenance_status" "$receipt_validation_status" "$receipt_validation_reason")
	printf '%s\n' "$report_line" >>"$OUT_PATH/report.jsonl"
	printf '| %s | %s | %s | %s |\n' "$case_id" "$fixture_id" "$protocol" "$status" >>"$OUT_PATH/summary.md"
	case_count=$((case_count + 1))
	case "$status" in
		pass) pass_count=$((pass_count + 1)) ;;
		drift) drift_cases=$((drift_cases + 1)) ;;
		product_regression) product_regressions=$((product_regressions + 1)) ;;
		infrastructure_failure) infrastructure_failures=$((infrastructure_failures + 1)) ;;
		environment_skip) environment_skips=$((environment_skips + 1)) ;;
	esac
done < <(ruby -rjson -e 'JSON.parse(File.read(ARGV.fetch(0))).fetch("cases").each { |entry| puts entry.fetch("id") }' "$TMP_DIR/selected.json")

write_json "$OUT_PATH/versions.json" "$(ruby -ryaml -rjson - "$TMP_DIR/selected.json" "$TMP_DIR/durations" "$OUT_PATH/suite" "$OUT_PATH" "$RUN_ID" "$MANIFEST_PATH" "$mode" "$REFERENCE_MANIFEST_PATH" <<'RUBY'
selected_path, duration_dir, suite_dir, out_path, run_id, manifest_path, mode, reference_manifest_path = ARGV
document = JSON.parse(File.read(selected_path))
cases = document.fetch("cases")
selection = document.fetch("selection")
references = YAML.load_file(reference_manifest_path).fetch("references")
protocols = cases.map { |entry| entry.fetch("protocol") }.uniq
runner_records = protocols.to_h do |protocol|
  command_path = File.join(suite_dir, protocol, "command.txt")
  duration_path = File.join(duration_dir, protocol)
  command = File.file?(command_path) ? File.read(command_path).strip : nil
  duration = File.file?(duration_path) ? File.read(duration_path).to_f : 0.0
  [protocol, { "command" => command, "duration_seconds" => duration }]
end
reference_records = cases.map do |entry|
  reference = entry.fetch("reference")
  protocol = entry.fetch("protocol")
  reference_pin = references.fetch(protocol)
  image = reference_pin.fetch("image")
  digest = reference_pin.fetch("digest")
  abort "#{protocol} reference must use an immutable sha256 digest" unless digest.match?(/\Asha256:[0-9a-f]{64}\z/)
  image_reference = "#{image}@#{digest}"
  record = {
    "case_id" => entry.fetch("id"),
    "fixture_id" => entry.fetch("fixture").fetch("id"),
    "protocol" => protocol,
    "service" => reference.fetch("service"),
    "version" => reference.fetch("version"),
    "image" => image_reference,
    "image_tag" => image_reference,
    "conformance_exclusion" => entry["conformance_exclusion"],
    "reason" => (entry["conformance_exclusion"] ? "conformance_exclusion" : nil),
    "release_evidence" => (mode == "real" && (entry["conformance_exclusion"] ? false : (File.file?(File.join(out_path, entry.fetch("id"), "outcome.json")) && JSON.parse(File.read(File.join(out_path, entry.fetch("id"), "outcome.json")))["release_evidence"] == true))),
    "validation_only" => (mode != "real")
  }
  if mode == "drift"
    record["baseline"] = { "image" => ENV.fetch("DRIFT_BASELINE_IMAGE"), "version" => ENV.fetch("DRIFT_BASELINE_VERSION") }
    record["candidate"] = {
      "image" => ENV.fetch("DRIFT_CANDIDATE_REFERENCE"),
      "version" => ENV.fetch("DRIFT_CANDIDATE_VERSION"),
      "digest" => ENV.fetch("DRIFT_CANDIDATE_DIGEST")
    }
  end
  record
end
case_release_evidence = cases.reject { |entry| entry["conformance_exclusion"] }.all? do |entry|
  outcome_path = File.join(out_path, entry.fetch("id"), "outcome.json")
  File.file?(outcome_path) && JSON.parse(File.read(outcome_path))["release_evidence"] == true
end
release_evidence = mode == "real" && case_release_evidence
puts JSON.generate(
  "run_id" => run_id,
  "manifest" => manifest_path,
  "mode" => mode,
  "ruby" => RUBY_DESCRIPTION,
  "selected_protocols" => protocols,
  "selected_case_ids" => cases.map { |entry| entry.fetch("id") },
  "selected_fixture_ids" => cases.map { |entry| entry.fetch("fixture").fetch("id") },
  "selection" => selection,
  "references" => reference_records,
  "runners" => runner_records,
  "release_evidence" => release_evidence,
  "validation_only" => (mode != "real"),
  "candidate" => (mode == "drift" ? {
    "image" => ENV.fetch("DRIFT_CANDIDATE_REFERENCE"),
    "version" => ENV.fetch("DRIFT_CANDIDATE_VERSION"),
    "digest" => ENV.fetch("DRIFT_CANDIDATE_DIGEST")
  } : nil),
  "baseline" => (mode == "drift" ? { "image" => ENV.fetch("DRIFT_BASELINE_IMAGE"), "version" => ENV.fetch("DRIFT_BASELINE_VERSION") } : nil)
)
RUBY
)"

ruby -rjson - "$OUT_PATH" "$TMP_DIR/selected.json" "$RUN_ID" "$mode" <<'RUBY'
out_path, selected_path, run_id, mode = ARGV
selected = JSON.parse(File.read(selected_path))
cases = selected.fetch("cases")
protocols = cases.map { |entry| entry.fetch("protocol") }.uniq
protocol = protocols.length == 1 ? protocols.fetch(0) : "multi"
case_ids = cases.map { |entry| entry.fetch("id") }
fixture_ids = cases.map { |entry| entry.fetch("fixture").fetch("id") }
case_records = cases.map do |entry|
  case_id = entry.fetch("id")
  case_document = JSON.parse(File.read(File.join(out_path, case_id, "case.json")))
  outcome = JSON.parse(File.read(File.join(out_path, case_id, "outcome.json")))
  {
    "run_id" => run_id,
    "case_id" => case_id,
    "runner_case_id" => entry["runner_case_id"],
    "tenant_isolation_evidence" => entry["tenant_isolation_evidence"],
    "fixture_id" => entry.fetch("fixture").fetch("id"),
    "request_fingerprint" => case_document.fetch("request_fingerprint"),
    "fingerprint" => case_document.fetch("request_fingerprint"),
    "fingerprint_algorithm" => "SHA-256",
    "status" => outcome.fetch("status"),
    "outcome" => outcome.fetch("status"),
    "reason" => outcome.fetch("reason", "completed"),
    "release_evidence" => outcome.fetch("release_evidence", false),
    "validation_only" => outcome.fetch("validation_only", mode != "real")
  }
end
statuses = case_records.map { |record| record.fetch("status") }
# Conformance-excluded cases are skipped by design; they keep the run honest
# without blocking the release gate, provided everything else passes.
non_pass = statuses.reject { |status| status == "pass" || status == "skipped" }
suite_status = non_pass.empty? ? "pass" : (non_pass.first || "infrastructure_failure")
classification = %w[pass drift product_regression infrastructure_failure environment_skip unsupported skipped].include?(suite_status) ? suite_status : "infrastructure_failure"
evidence_bearing = case_records.reject { |record| record.fetch("status") == "skipped" }
release = mode == "real" && suite_status == "pass" && evidence_bearing.all? { |record| record.fetch("release_evidence") == true }
write = lambda do |name, value|
  File.write(File.join(out_path, name), JSON.pretty_generate(value) + "\n")
end
write.call("outcome.json", {
  "schema_version" => "compat-suite-outcome.v1",
  "run_id" => run_id,
  "protocol" => protocol,
  "scope" => "suite",
  "mode" => mode,
  "status" => suite_status,
  "classification" => classification,
  "selected_case_ids" => case_ids,
  "selected_fixture_ids" => fixture_ids,
  "release_evidence" => release,
  "validation_only" => (mode != "real")
})
write.call("execution-receipt.json", {
  "schema_version" => "compat-execution-receipt.v1",
  "run_id" => run_id,
  "protocol" => protocol,
  "status" => suite_status,
  "outcome" => suite_status,
  "selected_case_ids" => case_ids,
  "selected_runner_case_ids" => cases.map { |entry| entry["runner_case_id"] },
  "executed_case_ids" => case_ids,
  "executed_runner_case_ids" => cases.map { |entry| entry["runner_case_id"] },
  "selected_fixture_ids" => fixture_ids,
  "executed_fixture_ids" => fixture_ids,
  "release_evidence" => release,
  "validation_only" => (mode != "real"),
  "cases" => case_records
})
artifacts = Dir.glob(File.join(out_path, "**", "*")).select { |path| File.file?(path) }.map { |path| path.delete_prefix(out_path + "/") }.reject { |path| path == "artifact-index.json" }.sort
write.call("artifact-index.json", {
  "schema_version" => "compat-artifact-index.v1",
  "run_id" => run_id,
  "protocol" => protocol,
  "mode" => mode,
  "release_evidence" => release,
  "validation_only" => (mode != "real"),
  "selected_case_ids" => case_ids,
  "selected_fixture_ids" => fixture_ids,
  "artifacts" => artifacts
})
RUBY

"$ROOT_DIR/scripts/compat/validate-artifacts.sh" --root "$OUT_PATH"

{
	echo
	echo "Cases: $case_count"
	echo "Pass: $pass_count"
	echo "Drift cases: $drift_cases"
	echo "Infrastructure failures: $infrastructure_failures"
	echo "Product regressions: $product_regressions"
	echo "Environment skips: $environment_skips"
} >>"$OUT_PATH/summary.md"

if [ "$product_regressions" -gt 0 ] || [ "$infrastructure_failures" -gt 0 ] || [ "$environment_skips" -gt 0 ]; then
	exit 1
fi
exit 0
