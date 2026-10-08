#!/usr/bin/env bash
# Run cargo tests with process isolation without re-invoking cargo per test.
#
# 1) cargo test --no-run once (capture Executable path from cargo output)
# 2) exec that binary once per filter with --exact --test-threads=1
#
# Usage:
#   ./scripts/run-isolated-cargo-tests.sh --features integration-e2e --test tests --list-prefix integration::
#   ./scripts/run-isolated-cargo-tests.sh --features integration-e2e --test integration_perf --tests performance::perf_union_read_latency
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CARGO_ARGS=()
LIST_PREFIX=""
EXPLICIT_TESTS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release)
      CARGO_ARGS+=("$1")
      shift
      ;;
    --features|--test)
      CARGO_ARGS+=("$1" "$2")
      shift 2
      ;;
    --list-prefix)
      LIST_PREFIX="$2"
      shift 2
      ;;
    --tests)
      shift
      while [[ $# -gt 0 && "$1" != --* ]]; do
        EXPLICIT_TESTS+=("$1")
        shift
      done
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

test_name=""
i=0
while [[ $i -lt ${#CARGO_ARGS[@]} ]]; do
  if [[ "${CARGO_ARGS[$i]}" == "--test" ]]; then
    test_name="${CARGO_ARGS[$((i + 1))]}"
    break
  fi
  i=$((i + 1))
done
if [[ -z "${test_name}" ]]; then
  echo "error: --test <name> is required" >&2
  exit 1
fi

echo "Compiling tests once: cargo test --no-run ${CARGO_ARGS[*]}..."
# Capture the Executable line for this --test target (feature-aware hash).
build_log="$(mktemp)"
trap 'rm -f "${build_log}"' EXIT
set +e
cargo test --no-run "${CARGO_ARGS[@]}" >"${build_log}" 2>&1
cargo_rc=$?
set -e
cat "${build_log}"
if [[ "${cargo_rc}" -ne 0 ]]; then
  exit "${cargo_rc}"
fi

# Example: Executable tests/tests.rs (/abs/or/rel/target/debug/deps/tests-abc123)
TEST_BIN="$(
  awk -v name="${test_name}" '
    $1 == "Executable" && match($0, /\([^)]+\)/) {
      bin = substr($0, RSTART + 1, RLENGTH - 2)
      n = split(bin, parts, "/")
      base = parts[n]
      if (index(base, name "-") == 1) {
        print bin
        found = 1
        exit
      }
    }
    END { if (!found) exit 1 }
  ' "${build_log}"
)" || true

TARGET_DIR="${CARGO_TARGET_DIR:-target}"

if [[ -z "${TEST_BIN}" || ! -x "${TEST_BIN}" ]]; then
  # Fallback: newest executable matching deps/<name>-*
  TEST_BIN="$(
    find "${TARGET_DIR}" -type f -path "*/deps/${test_name}-*" ! -name "*.d" ! -name "*.rlib" ! -name "*.rmeta" 2>/dev/null \
      | while read -r p; do
          if [[ -x "$p" ]]; then
            echo "$(stat -f '%m' "$p" 2>/dev/null || stat -c '%Y' "$p" 2>/dev/null) $p"
          fi
        done \
      | sort -nr \
      | head -1 \
      | awk '{print $2}'
  )"
fi

if [[ -z "${TEST_BIN}" || ! -x "${TEST_BIN}" ]]; then
  echo "error: could not find executable test binary for --test ${test_name}" >&2
  exit 1
fi
echo "Using test binary: ${TEST_BIN}"

# libduckdb is dynamically linked; the harness needs the download dir on the loader path.
DUCKDB_LIB_DIR="$(find "${TARGET_DIR}/duckdb-download" -type f \( -name 'libduckdb.so*' -o -name 'libduckdb.dylib*' \) -print -quit 2>/dev/null | xargs dirname 2>/dev/null || true)"
if [[ -n "${DUCKDB_LIB_DIR}" ]]; then
  case "$(uname -s)" in
    Darwin) export DYLD_LIBRARY_PATH="${DUCKDB_LIB_DIR}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}" ;;
    *) export LD_LIBRARY_PATH="${DUCKDB_LIB_DIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" ;;
  esac
  echo "DuckDB loader path: ${DUCKDB_LIB_DIR}"
fi

TESTS=()
if [[ ${#EXPLICIT_TESTS[@]} -gt 0 ]]; then
  TESTS=("${EXPLICIT_TESTS[@]}")
else
  if [[ -z "${LIST_PREFIX}" ]]; then
    echo "error: provide --list-prefix or --tests" >&2
    exit 2
  fi
  while IFS= read -r name; do
    [[ -n "${name}" ]] || continue
    TESTS+=("${name}")
  done < <("${TEST_BIN}" --list 2>/dev/null | awk -v pfx="${LIST_PREFIX}" '
    $0 ~ ("^" pfx) {
      name=$1
      sub(/:$/, "", name)
      print name
    }')
fi

if [[ ${#TESTS[@]} -eq 0 ]]; then
  echo "error: no tests matched (binary=${TEST_BIN}, prefix=${LIST_PREFIX:-explicit})" >&2
  "${TEST_BIN}" --list 2>/dev/null | head -40 >&2 || true
  exit 1
fi

echo "Running ${#TESTS[@]} isolated test process(es)..."
failed=0
for t in "${TESTS[@]}"; do
  echo "🧪 ${t}"
  if ! "${TEST_BIN}" "${t}" --exact --test-threads=1 --nocapture; then
    failed=1
    break
  fi
done

if [[ "${failed}" -ne 0 ]]; then
  exit 1
fi
echo "✅ isolated suite completed (${#TESTS[@]} tests)"
