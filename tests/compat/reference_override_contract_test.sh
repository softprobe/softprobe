#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
LOKI="$ROOT_DIR/tests/compat/support/loki.rs"
TEMPO="$ROOT_DIR/tests/compat/support/tempo.rs"

assert_contains() {
	local file=$1
	local needle=$2
	grep -Fq "$needle" "$file" || {
		echo "missing reference override contract in ${file#$ROOT_DIR/}: $needle" >&2
		exit 1
	}
}

assert_contains "$LOKI" 'std::env::var("LOKI_REFERENCE_IMAGE")'
assert_contains "$TEMPO" 'std::env::var("TEMPO_REFERENCE_IMAGE")'

# Empty overrides fall back to the manifest-derived image.
assert_contains "$LOKI" '.filter(|image| !image.trim().is_empty())'
assert_contains "$TEMPO" '.filter(|image| !image.trim().is_empty())'
assert_contains "$LOKI" 'unwrap_or_else(|| manifest_reference_image(&manifest, "loki"))'
assert_contains "$TEMPO" 'unwrap_or_else(|| manifest_reference_image(&manifest, "tempo"))'
assert_contains "$LOKI" 'reference["digest"]'
assert_contains "$LOKI" 'format!("{}@{}", image, digest)'
assert_contains "$TEMPO" 'manifest_reference_image(&manifest, "tempo")'

# Drift candidates must be immutable digests or explicit non-latest tags.
assert_contains "$LOKI" 'candidate_reference_image("LOKI_REFERENCE_IMAGE", &image)'
assert_contains "$TEMPO" 'candidate_reference_image("TEMPO_REFERENCE_IMAGE", &image)'
assert_contains "$LOKI" 'candidate image must not use the latest tag'
assert_contains "$LOKI" '@sha256:'
assert_contains "$TEMPO" 'candidate_reference_image'

echo "reference override contract: PASS"
