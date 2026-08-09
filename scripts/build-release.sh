#!/usr/bin/env bash
# Canonical release build for softprobe-runtime.
# Host → dist/{softprobe-runtime,libduckdb.*,config.yaml}
#
# cargo chef cook blanks *.rs (Docker recovers via COPY). On the host we
# snapshot sources before cook and restore before the real cargo build.
#
# Linux docker images on a non-linux/amd64 host:
#   TARGET_PLATFORM=linux/amd64 ./scripts/build-release.sh
#
# Usage:
#   ./scripts/build-release.sh
#   TARGET_PLATFORM=linux/amd64 ./scripts/build-release.sh
#   IN_LINUX_BUILDER=1 ./scripts/build-release.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export DUCKDB_DOWNLOAD_LIB="${DUCKDB_DOWNLOAD_LIB:-1}"
RECIPE_PATH="${RECIPE_PATH:-recipe.json}"
DIST_DIR="${DIST_DIR:-dist}"
BIN_NAME=softprobe-runtime
SRC_SNAPSHOT=""

host_needs_linux_builder() {
  if [[ "${IN_LINUX_BUILDER:-0}" == "1" ]]; then
    return 1
  fi
  if [[ "${TARGET_PLATFORM:-}" == "linux/amd64" || "${FORCE_LINUX_BUILDER:-0}" == "1" ]]; then
    return 0
  fi
  return 1
}

run_in_linux_builder() {
  echo "Building linux/amd64 bits via Docker (same script body)..."
  local image="${LINUX_BUILDER_IMAGE:-rust:1-bookworm}"
  docker run --rm \
    --platform linux/amd64 \
    -v "${ROOT}:/app" \
    -w /app \
    -e IN_LINUX_BUILDER=1 \
    -e DUCKDB_DOWNLOAD_LIB \
    -e CARGO_HOME=/app/.cargo-linux \
    -e RECIPE_PATH \
    -e DIST_DIR \
    "${image}" \
    bash -lc '
      set -euo pipefail
      apt-get update -qq
      apt-get install -y -qq pkg-config libssl-dev protobuf-compiler clang mold cmake build-essential >/dev/null
      ./scripts/build-release.sh
    '
}

ensure_cargo_chef() {
  if command -v cargo-chef >/dev/null 2>&1; then
    return 0
  fi
  echo "Installing cargo-chef..."
  cargo install cargo-chef --locked
}

snapshot_sources() {
  SRC_SNAPSHOT="$(mktemp -t thelake-src.XXXXXX.tar)"
  # Include paths cook/prepare may rewrite; keep build scripts out of the tarball churn.
  tar cf "${SRC_SNAPSHOT}" \
    Cargo.toml Cargo.lock rust-toolchain.toml \
    src tests \
    2>/dev/null || tar cf "${SRC_SNAPSHOT}" Cargo.toml Cargo.lock src tests
  echo "Source snapshot: ${SRC_SNAPSHOT}"
}

restore_sources() {
  if [[ -n "${SRC_SNAPSHOT}" && -f "${SRC_SNAPSHOT}" ]]; then
    echo "Restoring sources blanked by cargo chef cook..."
    tar xf "${SRC_SNAPSHOT}" -C "${ROOT}"
    rm -f "${SRC_SNAPSHOT}"
    SRC_SNAPSHOT=""
  fi
}

cleanup() {
  restore_sources || true
}
trap cleanup EXIT

stage_dist() {
  mkdir -p "${DIST_DIR}"
  local bin="target/release/${BIN_NAME}"
  test -x "${bin}" || { echo "error: missing ${bin}" >&2; exit 1; }

  local duckdb_so=""
  duckdb_so="$(find target/duckdb-download -type f -name 'libduckdb.so*' -print -quit 2>/dev/null || true)"
  if [[ -z "${duckdb_so}" ]]; then
    duckdb_so="$(find target/duckdb-download -type f -name 'libduckdb.dylib*' -print -quit 2>/dev/null || true)"
  fi
  test -n "${duckdb_so}" || { echo "error: libduckdb not found under target/duckdb-download" >&2; exit 1; }

  sh scripts/assert-duckdb-version.sh Cargo.lock "${duckdb_so}"

  cp -f "${bin}" "${DIST_DIR}/${BIN_NAME}"
  rm -f "${DIST_DIR}/libduckdb.so" "${DIST_DIR}/libduckdb.dylib"
  if [[ "${duckdb_so}" == *.dylib* ]]; then
    cp -f "${duckdb_so}" "${DIST_DIR}/libduckdb.dylib"
  else
    cp -f "${duckdb_so}" "${DIST_DIR}/libduckdb.so"
  fi
  cp -f config.yaml "${DIST_DIR}/config.yaml"
  echo "Staged ${DIST_DIR}/ (bin + duckdb + config.yaml)"
}

build_on_host() {
  ensure_cargo_chef
  snapshot_sources
  echo "cargo chef prepare..."
  cargo chef prepare --recipe-path "${RECIPE_PATH}"
  echo "cargo chef cook --release --locked..."
  cargo chef cook --release --locked --recipe-path "${RECIPE_PATH}"
  restore_sources
  trap - EXIT
  echo "cargo build --release --locked --bin ${BIN_NAME}..."
  cargo build --release --locked --bin "${BIN_NAME}"
  stage_dist
}

if host_needs_linux_builder; then
  run_in_linux_builder
  exit 0
fi

build_on_host
