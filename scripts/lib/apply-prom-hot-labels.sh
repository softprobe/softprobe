#!/usr/bin/env bash
# Apply the canonical metrics Prom hot-label telemetry_columns manifest.
# Usage: apply_prom_hot_labels <base_url> <bearer_token>
# Relies on ROOT being set to the thelake repo root (caller sets it).

apply_prom_hot_labels() {
  local base_url="${1:?base_url}"
  local token="${2:?bearer_token}"
  local manifest="${ROOT}/docs/promotion/metrics-prom-hot-labels.yaml"
  if [[ ! -f "$manifest" ]]; then
    echo "ERROR: missing hot-label manifest: $manifest" >&2
    return 1
  fi
  local yaml
  yaml="$(cat "$manifest")"
  local payload
  payload="$(MANIFEST_YAML="$yaml" python3 - <<'PY'
import json, os
print(json.dumps({"manifestYaml": os.environ["MANIFEST_YAML"]}))
PY
)"
  echo "==> applying metrics Prom hot-label promotion"
  local resp http
  http="$(curl -sS -o /tmp/thelake-prom-hot-labels-apply.json -w '%{http_code}' \
    -X POST "${base_url%/}/v1/promotions/apply" \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json" \
    -d "$payload" || true)"
  if [[ "$http" != "200" && "$http" != "201" ]]; then
    echo "ERROR: promotions/apply returned HTTP ${http:-curl-fail}" >&2
    cat /tmp/thelake-prom-hot-labels-apply.json >&2 || true
    return 1
  fi
  echo "==> hot-label promotion applied (HTTP $http)"
}
