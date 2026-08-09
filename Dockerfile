# Packaging-only image. Product bits are built on the host (or Linux builder)
# via `make build-release` → dist/{softprobe-runtime,libduckdb.so,config.yaml}.
# Never run cargo / cargo-chef in this Dockerfile.
FROM debian:trixie-slim AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY dist/softprobe-runtime /app/softprobe-runtime
COPY dist/libduckdb.so /usr/local/lib/libduckdb.so
COPY dist/config.yaml /app/config.yaml

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
