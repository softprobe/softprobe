#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml"
MAKEFILE="$ROOT_DIR/Makefile"

for service in prometheus loki tempo grafana; do
	grep -Eq "^  ${service}:" "$MANIFEST"
	digest=$(ruby -ryaml -e '
	manifest = YAML.load_file(ARGV.fetch(0))
	reference = manifest.fetch("references").fetch(ARGV.fetch(1))
	%w[image tag digest].each do |field|
		value = reference[field]
		abort "#{ARGV.fetch(1)} must declare #{field}" unless value.is_a?(String) && !value.empty?
	end
	puts reference.fetch("digest")
' "$MANIFEST" "$service") || {
		echo "${service} must declare image, tag, and digest fields in its manifest mapping" >&2
		exit 1
	}
	[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || {
		echo "${service} must declare a canonical immutable sha256 digest" >&2
		exit 1
	}
done

ruby -ryaml -e '
	reference = YAML.load_file(ARGV.fetch(0)).fetch("references").fetch("otel_collector")
	abort "otel_collector must be explicitly non-runtime" unless reference["runtime"] == false
	abort "otel_collector non-runtime reference must not declare a digest" if reference.key?("digest")
' "$MANIFEST"

# Regression guard: the canonical manifest currently uses nested mappings;
# keep the contract valid for that inline YAML shape as well as block style.
ruby -ryaml -e '
	reference = YAML.load_file(ARGV.fetch(0)).fetch("references").fetch("prometheus")
	abort "inline reference mapping regression" unless reference.is_a?(Hash) && reference.fetch("image") == "prom/prometheus"
' "$MANIFEST"

grep -Fq 'PROMETHEUS_REFERENCE_DIGEST ?=' "$MAKEFILE"
grep -Fq 'LOKI_REFERENCE_DIGEST ?=' "$MAKEFILE"
grep -Fq 'TEMPO_REFERENCE_DIGEST ?=' "$MAKEFILE"
grep -Fq 'GRAFANA_REFERENCE_DIGEST ?=' "$MAKEFILE"

for service in prometheus loki tempo grafana; do
	grep -Fq "\"${service}|\$\$${service}_manifest|\$\$${service}_digest_manifest" "$MAKEFILE" || {
		echo "missing immutable image@digest check for ${service}" >&2
		exit 1
	}
done

grep -Fq '[[ "$$manifest" == *@"$$digest" ]]' "$MAKEFILE"
grep -Fq 'canonical = YAML.load_file' "$MAKEFILE"

if grep -Eq "tag:[[:space:]]+[\"']?(latest|main|nightly)[\"']?[[:space:]]*\$" "$MANIFEST"; then
	echo "mutable latest-style reference is not allowed" >&2
	exit 1
fi

echo "reference manifest immutable-pin contract: PASS"
