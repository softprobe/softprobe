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
# Postgres is a dev catalog pin (not part of references.v0.yaml); Grafana/WireMock must be digest-pinned.
for line in compose_text.splitlines():
    stripped = line.strip()
    if not stripped.startswith("image:"):
        continue
    if "postgres" in stripped or "GRAFANA_PG_IMAGE" in stripped:
        continue
    if "@" not in stripped and re.search(r":[^\s]+", stripped):
        raise SystemExit(f"manual Grafana compose contains a tag-only image: {stripped}")
if "GRAFANA_COMPOSE_IMAGE" not in launcher_text:
    raise SystemExit("manual Grafana launcher does not pass the manifest-derived image")
if immutable_image not in (compose_text + launcher_text) and "GRAFANA_COMPOSE_IMAGE:?" not in compose_text:
    raise SystemExit(f"manual Grafana path does not require canonical image {immutable_image}")

# Manual stack is single-tenant (auth-mock → local-dev-tenant). Loki/Tempo/
# tenant-Prom datasources expand SOFTPROBE_TENANT_* at provision time; empty
# values regress Explore to "Authentication to data source failed".
# A≡B is intentional — dual-tenant lives in docker-compose.ci.yml only.
required_env = {
    "SOFTPROBE_API_KEY": "local-dev-key",
    "SOFTPROBE_TENANT_A_API_KEY": "local-dev-key",
    "SOFTPROBE_TENANT_B_API_KEY": "local-dev-key",
    "SOFTPROBE_TENANT_A_ID": "local-dev-tenant",
    "SOFTPROBE_TENANT_B_ID": "local-dev-tenant",
}
for key, value in required_env.items():
    if not re.search(rf"(?m)^\s*{re.escape(key)}:\s*{re.escape(value)}\s*$", compose_text):
        raise SystemExit(
            f"manual Grafana compose must set {key}: {value} "
            "(single-tenant provisioned datasources)"
        )
PY

printf 'Grafana manual immutable-reference contract: PASS\n'
