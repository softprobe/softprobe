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
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin softprobe-runtime
RUN mkdir -p /opt/duckdb-lib \
    && DUCKDB_SO_PATH="$(find /app /root/.cargo -type f \( -name 'libduckdb.so' -o -name 'libduckdb.so.*' \) -print -quit)" \
    && test -n "$DUCKDB_SO_PATH" \
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
