#!/bin/sh
# Fail the build if the libduckdb being packaged is not the engine version
# Cargo.lock resolved to.
#
# Why this exists: `scripts/build-release.sh` stages libduckdb with
# `find -print -quit` under target/duckdb-download. A stale download cache
# can ship an engine that differs from what the build resolved -- silently,
# under a tag claiming otherwise. DuckDB 1.5.2 crashes on empty-array VARIANT
# values and invalidates the whole database (2026-08-03 outage), so "which
# engine is actually packaged" is not something to leave to chance.
#
# The duckdb crate encodes the engine version in its own: 1.1<mm><pp>.<n>,
# e.g. 1.10505.0 -> DuckDB 1.5.5. `DUCKDB_DOWNLOAD_LIB=1` then places the
# library under .../duckdb-download/<target-triple>/<version>/, which is what
# this checks -- NOT the library's contents: libduckdb embeds a whole list of
# historical version strings for storage compatibility, so grepping it returns
# v1.0.0 and friends, not the real version.
set -eu

LOCKFILE="${1:?usage: assert-duckdb-version <Cargo.lock> <libduckdb path>}"
SOPATH="${2:?usage: assert-duckdb-version <Cargo.lock> <libduckdb path>}"

crate_version="$(
    awk '/^name = "duckdb"$/ { want = 1; next }
         want && /^version = / { gsub(/[",]/, "", $3); print $3; exit }' "$LOCKFILE"
)"
[ -n "$crate_version" ] || { echo "FATAL: no duckdb crate entry in $LOCKFILE" >&2; exit 1; }

# 1.<major><mm><pp>.<n>; major is not hardcoded so a DuckDB 2.x crate
# (1.20500.0 -> 2.5.0) derives correctly instead of failing the build.
major="$(echo "$crate_version" | sed -n 's/^1\.\([0-9]\)[0-9][0-9][0-9][0-9]\..*$/\1/p')"
minor="$(echo "$crate_version" | sed -n 's/^1\.[0-9]\([0-9][0-9]\)[0-9][0-9]\..*$/\1/p' | sed 's/^0//')"
patch="$(echo "$crate_version" | sed -n 's/^1\.[0-9][0-9][0-9]\([0-9][0-9]\)\..*$/\1/p' | sed 's/^0//')"
[ -n "$major" ] && [ -n "$minor" ] && [ -n "$patch" ] || {
    echo "FATAL: cannot derive engine version from duckdb crate '$crate_version'" >&2
    exit 1
}
want="${major}.${minor}.${patch}"

# .../duckdb-download/<triple>/<version>/libduckdb.so
got="$(echo "$SOPATH" | sed -n 's#.*/duckdb-download/[^/][^/]*/\([0-9][0-9.]*\)/.*#\1#p')"

if [ -z "$got" ]; then
    # Library came from somewhere without a version in its path (a vendored
    # copy, a system install). --locked already pins the crate, so warn rather
    # than block -- a check that cannot run must not become a build failure.
    echo "WARNING: cannot verify engine version from path '$SOPATH'" >&2
    echo "         (Cargo.lock expects DuckDB $want via duckdb $crate_version)" >&2
    exit 0
fi

if [ "$got" != "$want" ]; then
    echo "FATAL: engine mismatch -- Cargo.lock resolved duckdb $crate_version (expects $want)," >&2
    echo "       but the library being packaged is $got ($SOPATH)." >&2
    echo "       Refusing to ship an image whose engine is not the one it was built against." >&2
    exit 1
fi

echo "duckdb crate $crate_version -> engine $got (matches lockfile)"
