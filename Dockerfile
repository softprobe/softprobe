# Runtime-only image: build binary natively, then copy it in.
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Buildx sets TARGETARCH per platform (amd64/arm64).
# Override BINARY_PATH for custom builds when needed.
ARG TARGETARCH
ARG BINARY_PATH=target/linux-${TARGETARCH}/softprobe-runtime
COPY ${BINARY_PATH} /app/softprobe-runtime
COPY config.yaml /app/config.yaml

RUN useradd -m -u 1000 softprobe && \
    chown -R softprobe:softprobe /app

USER softprobe

EXPOSE 8080
EXPOSE 4317
EXPOSE 4318

ENV RUST_LOG=info
ENV CONFIG_FILE=/app/config.yaml

CMD ["/app/softprobe-runtime"]
