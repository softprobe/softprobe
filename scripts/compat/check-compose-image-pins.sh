#!/usr/bin/env bash

set -euo pipefail

COMPOSE_FILE=${1:-}
if [ -z "$COMPOSE_FILE" ]; then
	echo "usage: check-compose-image-pins.sh COMPOSE_FILE" >&2
	exit 2
fi

if [ ! -f "$COMPOSE_FILE" ]; then
	echo "compose file does not exist: $COMPOSE_FILE" >&2
	exit 2
fi

python3 - "$COMPOSE_FILE" "${SOFTPROBE_BUILDER_IMAGE:-}" "${GRAFANA_COMPOSE_IMAGE:-}" <<'PY'
import re
import sys

compose_path, builder_override, grafana_override = sys.argv[1:]
digest = re.compile(r"@sha256:[0-9a-fA-F]{64}$")
errors = []

for line_number, line in enumerate(open(compose_path, encoding="utf-8"), 1):
    match = re.match(r"^\s+image:\s*(.*?)\s*$", line)
    if not match:
        continue
    image = match.group(1)
    if image.startswith("${GRAFANA_COMPOSE_IMAGE:?"):
        if not digest.search(grafana_override):
            errors.append(
                f"line {line_number}: Grafana image is not immutable: "
                f"{grafana_override or '<unset>'}"
            )
        continue
    if image.startswith("${SOFTPROBE_BUILDER_IMAGE:-"):
        default = image.removeprefix("${SOFTPROBE_BUILDER_IMAGE:-").removesuffix("}")
        resolved = builder_override or default
        if not digest.search(resolved):
            errors.append(f"line {line_number}: builder image is not immutable: {resolved}")
        continue
    # Dev DuckLake catalog for manual stack — not part of references.v0.yaml compat pins.
    if "postgres" in image or "GRAFANA_PG_IMAGE" in image:
        continue
    if not digest.search(image):
        errors.append(f"line {line_number}: image is not immutable: {image}")

if errors:
    print("\n".join(errors), file=sys.stderr)
    raise SystemExit(1)
PY

echo "compose image pin contract: PASS"
