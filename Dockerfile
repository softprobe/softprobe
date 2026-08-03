FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

# Native deps required by transitive crates during cargo-chef cook/build.
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    clang \
    mold \
    cmake \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ENV DUCKDB_DOWNLOAD_LIB=1
# Must be present before `cook`: otherwise cook uses the image default Rust and
# `cargo build` (after COPY) switches to rust-toolchain.toml → second full compile.
COPY --from=planner /app/rust-toolchain.toml rust-toolchain.toml
COPY --from=planner /app/recipe.json recipe.json
# --locked on both: Cargo.lock is committed and cargo honours it, so this is not
# about drift on a normal build. What it buys is turning "someone raised the
# DuckDB floor in Cargo.toml but did not regenerate the lock" from a silent
# lock rewrite into a loud build failure. That floor exists because DuckDB
# 1.5.2 crashes on empty-array VARIANT values and invalidates the whole
# database -- it took production down on 2026-08-03.
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked --bin softprobe-runtime
# Take the library from the versioned download directory, and only from there.
#
# libduckdb-sys writes the library to BOTH target/<profile>/deps/ (no version in
# the path) and target/duckdb-download/<triple>/<version>/. A `find` across the
# whole tree returns whichever readdir yields first -- in practice the
# versionless copy, since cargo creates target/release/ before the build script
# creates duckdb-download/. Selecting from the versioned directory makes the
# engine version structurally present in the path, so the assertion below can
# actually compare something instead of silently skipping.
#
# Assert BEFORE the copy, on the source path: the destination has no version
# segment, so asserting on it can only ever take the "cannot verify" branch.
#
# Both the script and Cargo.lock arrive with `COPY . .` above, so no separate
# COPY is needed -- and the lock read here is the real one, not the version
# cargo-chef mangles into the cook workdir.
RUN mkdir -p /opt/duckdb-lib \
    && DUCKDB_SO_PATH="$(find /app/target/duckdb-download -type f -name 'libduckdb.so*' -print -quit)" \
    && test -n "$DUCKDB_SO_PATH" \
    && sh scripts/assert-duckdb-version.sh Cargo.lock "$DUCKDB_SO_PATH" \
    && cp "$DUCKDB_SO_PATH" /opt/duckdb-lib/libduckdb.so

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/softprobe-runtime /app/softprobe-runtime
COPY --from=builder /opt/duckdb-lib/libduckdb.so /usr/local/lib/libduckdb.so
COPY config.yaml /app/config.yaml

RUN useradd -m -u 1000 softprobe && \
    chown -R softprobe:softprobe /app

USER softprobe

EXPOSE 8080
EXPOSE 4317
EXPOSE 4318

ENV RUST_LOG=info
ENV CONFIG_FILE=/app/config.yaml
ENV LD_LIBRARY_PATH=/usr/local/lib

CMD ["/app/softprobe-runtime"]
