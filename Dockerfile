# Multi-stage build for Rust OTLP Backend
# Stage 1: Build
# Use nightly for edition2024 support required by apache-avro
FROM rust:latest AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build for release
RUN cargo build --release --features iceberg_catalog

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/softprobe-otlp-backend /app/softprobe-otlp-backend

# Create a non-root user
RUN useradd -m -u 1000 softprobe && \
    chown -R softprobe:softprobe /app

USER softprobe

# Expose the service port
EXPOSE 4317

# Set default environment variables
ENV RUST_LOG=info
# ENV SERVER__PORT=8080
# ENV SERVER__HOST=0.0.0.0

# Run the binary
CMD ["/app/softprobe-otlp-backend", "--config", "/app/config.yaml"]
