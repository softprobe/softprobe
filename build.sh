#!/usr/bin/env bash
# Image tag/push for `make publish-docker`. Bits must already be in dist/
# (built by `make build-release` — same path as CI). This script only packages
# and pushes; it never compiles Rust.
#
# Usage (via Make):
#   make publish-docker TAG=v1.2.3
#   make publish-docker TAG=v1.2.3-rc.1 TAG_LATEST=0
#   PRINT_TAGS=1 ./build.sh v1.2.3
#   PRINT_BUILDX_ARGS=1 ./build.sh v1.2.3
set -euo pipefail

cd "$(dirname "$0")"

TAG="${1:-latest}"
TAG_LATEST="${TAG_LATEST:-1}"
AR_IMAGE="us-central1-docker.pkg.dev/cs-poc-sasxbttlzroculpau4u6e2l/softprobe/splake"
CACHE_REF="${AR_IMAGE}:buildcache"
FALLBACK_BUILDER_NAME="thelake-builder"
DIST_DIR="${DIST_DIR:-dist}"

image_tags=("${AR_IMAGE}:${TAG}")
case "${TAG_LATEST}" in
  0|false|FALSE|no|NO) ;;
  *)
    if [[ "${TAG}" != "latest" ]]; then
      image_tags+=("${AR_IMAGE}:latest")
    fi
    ;;
esac

if [[ "${PRINT_TAGS:-0}" == "1" ]]; then
  printf '%s\n' "${image_tags[@]}"
  exit 0
fi

docker_tags=()
for t in "${image_tags[@]}"; do
  docker_tags+=(-t "${t}")
done

ensure_cache_builder() {
  local name="" driver=""
  echo "Resolving Buildx builder (CI=${CI:-false})..."
  set +e
  name="$(docker buildx inspect 2>/dev/null | awk '/^Name:/{print $2; exit}' | tr -d '\r')"
  driver="$(docker buildx inspect 2>/dev/null | awk '/^Driver:/{print $2; exit}' | tr -d '\r')"
  set -e
  echo "Current builder name=${name:-<none>} driver=${driver:-<none>}"

  if [[ "${driver}" == "docker-container" || "${driver}" == "kubernetes" || "${driver}" == "remote" ]]; then
    BUILDER_NAME="${name}"
    return 0
  fi

  set +e
  if docker buildx inspect "${FALLBACK_BUILDER_NAME}" >/dev/null 2>&1; then
    name="${FALLBACK_BUILDER_NAME}"
  else
    name="$(
      docker buildx ls 2>/dev/null | awk '
        /docker-container/ && $0 !~ /\\_/ {
          n=$1; gsub(/\*$/, "", n); print n; exit
        }' | tr -d '\r'
    )"
  fi
  set -e

  if [[ -n "${name}" ]]; then
    BUILDER_NAME="${name}"
    docker buildx use "${BUILDER_NAME}"
    docker buildx inspect --bootstrap "${BUILDER_NAME}" >/dev/null 2>&1 || true
    return 0
  fi

  if [[ "${CI:-}" == "true" ]]; then
    echo "error: no docker-container Buildx builder in CI; setup-buildx-action must run first" >&2
    docker buildx ls >&2 || true
    exit 1
  fi

  echo "Creating Buildx builder ${FALLBACK_BUILDER_NAME}..."
  docker buildx create --name "${FALLBACK_BUILDER_NAME}" --driver docker-container --use
  docker buildx inspect --bootstrap >/dev/null 2>&1 || true
  name="$(docker buildx inspect | awk '/^Name:/{print $2; exit}' | tr -d '\r')"
  driver="$(docker buildx inspect | awk '/^Driver:/{print $2; exit}' | tr -d '\r')"
  BUILDER_NAME="${name}"
  if [[ "${driver}" != "docker-container" ]]; then
    echo "error: builder ${BUILDER_NAME} driver '${driver}'; need docker-container" >&2
    exit 1
  fi
}

require_dist() {
  test -x "${DIST_DIR}/softprobe-runtime" || {
    echo "error: missing ${DIST_DIR}/softprobe-runtime — run: make build-release" >&2
    exit 1
  }
  test -f "${DIST_DIR}/libduckdb.so" || {
    echo "error: missing ${DIST_DIR}/libduckdb.so — run linux build-release (TARGET_PLATFORM=linux/amd64 on Mac)" >&2
    exit 1
  }
  test -f "${DIST_DIR}/config.yaml" || {
    echo "error: missing ${DIST_DIR}/config.yaml" >&2
    exit 1
  }
}

BUILDER_NAME="${FALLBACK_BUILDER_NAME}"
if [[ "${PRINT_BUILDX_ARGS:-0}" != "1" ]]; then
  require_dist
  ensure_cache_builder
fi

# Thin image: registry cache helps base layers only (no cargo cook in Dockerfile).
buildx_args=(
  --builder "${BUILDER_NAME}"
  --platform linux/amd64
  --cache-from "type=registry,ref=${CACHE_REF},ignore-error=true"
  --cache-to "type=registry,ref=${CACHE_REF},mode=max"
  --push
  "${docker_tags[@]}"
  .
)

if [[ "${PRINT_BUILDX_ARGS:-0}" == "1" ]]; then
  printf '%s\n' "${buildx_args[@]}"
  exit 0
fi

echo "Packaging ${AR_IMAGE}:${TAG} from ${DIST_DIR}/ (linux/amd64)${image_tags[1]:+ + :latest}"
docker buildx build "${buildx_args[@]}"
echo "Pushed ${image_tags[*]}"
