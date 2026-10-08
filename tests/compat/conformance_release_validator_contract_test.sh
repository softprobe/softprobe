#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
VALIDATOR="$ROOT_DIR/scripts/compat/validate-artifacts.sh"
WORKFLOW="$ROOT_DIR/.github/workflows/compatibility.yml"

grep -Fq -- '--conformance-root DIR' "$VALIDATOR"
grep -Fq -- '--root DIR' "$VALIDATOR"
grep -Fq -- 'validate-artifacts.sh --conformance-root target/compat/release-gate --release-gate' "$WORKFLOW"
grep -Fq -- 'report.json' "$VALIDATOR"
grep -Fq -- 'product_regressions' "$VALIDATOR"
grep -Fq -- 'unapproved_differences' "$VALIDATOR"
grep -Fq -- 'validation_only' "$VALIDATOR"
grep -Fq -- 'credential leak' "$VALIDATOR"
grep -Fq -- 'release_evidence' "$WORKFLOW"
grep -Fq -- 'release_evidence' "$VALIDATOR"

echo "conformance release-validator contract: PASS"
