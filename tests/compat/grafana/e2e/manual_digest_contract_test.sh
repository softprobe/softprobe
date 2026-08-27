#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/compat/grafana/docker-compose.manual.yml"
LAUNCHER="$ROOT_DIR/scripts/grafana-manual-up.sh"
MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml"

python3 - "$COMPOSE_FILE" "$LAUNCHER" "$MANIFEST" <<'PY'
import pathlib
import re
import sys

compose, launcher, manifest = map(pathlib.Path, sys.argv[1:])
compose_text = compose.read_text()
launcher_text = launcher.read_text()
manifest_text = manifest.read_text()

match = re.search(
    r"(?ms)^\s+grafana:\s*\n\s+image:\s*([^\s#]+)\s*\n\s+tag:\s*[\"']?([^\s\"']+).*?\n\s+digest:\s*[\"']?(sha256:[0-9a-fA-F]{64})",
    manifest_text,
)
if not match:
    raise SystemExit("canonical Grafana manifest entry is missing an immutable digest")
image, tag, digest = match.groups()
immutable_image = f"{image}@{digest}"

if re.search(r"image:\s*\$\{GRAFANA_REFERENCE_IMAGE:-[^}]*:[^}]+\}", compose_text):
    raise SystemExit("manual Grafana compose still permits a mutable tag fallback")
if "GRAFANA_COMPOSE_IMAGE" not in compose_text:
    raise SystemExit("manual Grafana compose is not wired to the canonical immutable image")
if re.search(r"image:\s*[^\n@]*:[0-9][^\s]*\s*$", compose_text, re.M):
    raise SystemExit("manual Grafana compose contains a tag-only image")
if "GRAFANA_COMPOSE_IMAGE" not in launcher_text:
    raise SystemExit("manual Grafana launcher does not pass the manifest-derived image")
if immutable_image not in (compose_text + launcher_text) and "GRAFANA_COMPOSE_IMAGE:?" not in compose_text:
    raise SystemExit(f"manual Grafana path does not require canonical image {immutable_image}")
PY

printf 'Grafana manual immutable-reference contract: PASS\n'
