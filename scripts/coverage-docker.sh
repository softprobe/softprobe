#!/usr/bin/env bash
# Reproducible Linux `make coverage-lib` in Docker (mold + LLVM + clang; avoids macOS llvm-cov flakiness).
# Usage: from repo root, ./softprobe-runtime/scripts/coverage-docker.sh
# Or:    docker run --rm -v "$PWD":/work -w /work/softprobe-runtime rust:1.85-bookworm \
#            bash -c 'apt-get update -qq && apt-get install -y -qq make build-essential clang mold llvm-19 llvm-19-dev pkg-config libssl-dev && ... make coverage-lib'
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
exec docker run --rm \
  -e DUCKDB_DOWNLOAD_LIB=1 \
  -v "${REPO_ROOT}":/work \
  -w /work/softprobe-runtime \
  rust:1.85-bookworm \
  bash -c '
    set -euo pipefail
    export PATH="/usr/local/cargo/bin:${PATH}"
    apt-get update -qq
    apt-get install -y -qq make build-essential clang mold llvm-19 llvm-19-dev pkg-config libssl-dev
    rustup component add llvm-tools-preview
    cargo install cargo-llvm-cov --locked --version 0.6.16 --quiet
    make coverage-lib
  '
