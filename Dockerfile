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
COPY --from=planner /app/Cargo.lock Cargo.lock
# --locked on both: without it the resolver picks versions at build time, so the
# engine inside the image is decided by *when* you build, not by the lockfile.
# That silently defeats digest-pinned deploys (same tag, different engine) and
# would have let a rebuild drift off the DuckDB floor that Cargo.toml sets --
# 1.5.2 is the version that took production down on 2026-08-03.
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
RUN cargo build --release --locked --bin softprobe-runtime
# Assert the shipped engine matches the lockfile. `find -print -quit` takes
# whatever .so appears first with nothing tying it to the resolved crate, so a
# stale download cache could ship a crashing engine under a "fixed" tag.
# The duckdb crate encodes the engine version as 1.1<mm><pp>.<n>, e.g.
# 1.10505.0 -> DuckDB v1.5.5.
COPY --from=planner /app/scripts/assert-duckdb-version.sh /usr/local/bin/assert-duckdb-version
RUN mkdir -p /opt/duckdb-lib \
    && DUCKDB_SO_PATH="$(find /app /root/.cargo -type f \( -name 'libduckdb.so' -o -name 'libduckdb.so.*' \) -print -quit)" \
    && test -n "$DUCKDB_SO_PATH" \
    && cp "$DUCKDB_SO_PATH" /opt/duckdb-lib/libduckdb.so \
    && sh /usr/local/bin/assert-duckdb-version Cargo.lock /opt/duckdb-lib/libduckdb.so

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
