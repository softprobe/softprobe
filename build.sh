#!/usr/bin/env bash
set -euo pipefail
# Image build/push implementation for `make publish-docker`.
# Prefer the Make target (CI release.yml and local both use it).
#
# Usage (via Make):
#   make publish-docker TAG=v1.2.3
#   make publish-docker TAG=v1.2.3-rc.1 TAG_LATEST=0
#   PRINT_TAGS=1 ./build.sh v1.2.3         # print planned product tags only
#   PRINT_BUILDX_ARGS=1 ./build.sh v1.2.3  # print buildx argv (no docker)
#
# TAG defaults to latest. When TAG is not "latest" and TAG_LATEST is unset/true,
# also pushes :latest.
#
# BuildKit registry cache (Artifact Registry): intermediate layers including
# cargo-chef cook are stored at ${AR_IMAGE}:buildcache (not a runtime image —
# do not deploy this tag). Ephemeral GHA runners have no local layer cache;
# without registry cache every release cold-compiles dependencies (~7m).
# --cache-from miss is fine (ignore-error=true). --cache-to write failure
# fails the whole publish (fail-fast: release needs push rights on :buildcache).
# Registry cache export requires docker-container (or equivalent) Buildx driver.

cd "$(dirname "$0")"

TAG="${1:-latest}"
TAG_LATEST="${TAG_LATEST:-1}"
AR_IMAGE="us-central1-docker.pkg.dev/cs-poc-sasxbttlzroculpau4u6e2l/softprobe/splake"
# Separate mutable tag: must not collide with product :latest / release tags.
CACHE_REF="${AR_IMAGE}:buildcache"
# Fallback name when the current builder cannot export registry cache (typical
# local Docker Desktop default driver). CI usually already has a
# docker-container builder from setup-buildx-action — reuse that instead of
# creating a second one (create can exit 255 on GHA).
FALLBACK_BUILDER_NAME="thelake-builder"

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

# Resolve builder before assembling argv so --builder matches what we select.
ensure_cache_builder() {
  local name="" driver=""
  echo "Resolving Buildx builder for registry cache (CI=${CI:-false})..."
  # pipefail + a failed `buildx inspect` would abort before any message; probe softly.
  set +e
  name="$(docker buildx inspect 2>/dev/null | awk '/^Name:/{print $2; exit}' | tr -d '\r')"
  driver="$(docker buildx inspect 2>/dev/null | awk '/^Driver:/{print $2; exit}' | tr -d '\r')"
  set -e
  echo "Current builder name=${name:-<none>} driver=${driver:-<none>}"

  if [[ "${driver}" == "docker-container" || "${driver}" == "kubernetes" || "${driver}" == "remote" ]]; then
    BUILDER_NAME="${name}"
    echo "Using current Buildx builder ${BUILDER_NAME} (driver=${driver})"
    return 0
  fi

  # Prefer an existing docker-container builder (setup-buildx on GHA, or prior local).
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
    echo "Using existing docker-container builder ${BUILDER_NAME}"
    docker buildx use "${BUILDER_NAME}"
    docker buildx inspect --bootstrap "${BUILDER_NAME}" >/dev/null 2>&1 || true
    return 0
  fi

  if [[ "${CI:-}" == "true" ]]; then
    echo "error: no docker-container Buildx builder available in CI; setup-buildx-action must run first" >&2
    docker buildx ls >&2 || true
    exit 1
  fi

  echo "Creating Buildx builder ${FALLBACK_BUILDER_NAME} (docker-container; required for registry cache)..."
  docker buildx create --name "${FALLBACK_BUILDER_NAME}" --driver docker-container --use
  docker buildx inspect --bootstrap >/dev/null 2>&1 || true
  name="$(docker buildx inspect | awk '/^Name:/{print $2; exit}' | tr -d '\r')"
  driver="$(docker buildx inspect | awk '/^Driver:/{print $2; exit}' | tr -d '\r')"
  BUILDER_NAME="${name}"
  echo "Builder ${BUILDER_NAME} driver=${driver}"
  if [[ "${driver}" != "docker-container" ]]; then
    echo "error: builder ${BUILDER_NAME} uses driver '${driver}'; need docker-container for --cache-to type=registry" >&2
    docker buildx ls >&2 || true
    exit 1
  fi
}

BUILDER_NAME="${FALLBACK_BUILDER_NAME}"
if [[ "${PRINT_BUILDX_ARGS:-0}" != "1" ]]; then
  ensure_cache_builder
fi

buildx_args=(
  --builder "${BUILDER_NAME}"
  --platform linux/amd64
  # ignore-error: missing :buildcache must not fail the publish (BuildKit
  # otherwise marks the whole build failed after a successful image+cache push).
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

echo "Building ${AR_IMAGE}:${TAG} (linux/amd64)${image_tags[1]:+ + :latest}"
echo "BuildKit cache: ${CACHE_REF} (builder ${BUILDER_NAME})"
docker buildx build "${buildx_args[@]}"
echo "Pushed ${image_tags[*]}"
