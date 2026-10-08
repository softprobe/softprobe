# OpenTelemetry Istio Wasm Module

## Purpose

This module is a Rust Proxy-Wasm HTTP tracing integration for Istio. It captures
configured request and response data, builds OpenTelemetry spans, propagates
W3C trace context, and exports traces to the configured Softprobe OTLP endpoint.

It is an instrumentation source in the Softprobe agent observability and
evaluation project. It does not implement the telemetry backend or evaluation
engine; those responsibilities belong to the Softprobe runtime and the
agent/evaluation components in this repository.

## Module boundaries

- Keep the crate, vendored OpenTelemetry protobuf definitions, Envoy test
  harness, Istio manifests, and local development scripts in this directory.
- Keep build and deploy commands usable through the repository-root Makefile.
- Preserve trace-context propagation and filtering behavior when changing the
  instrumentation.
- Treat captured headers and bodies as telemetry data. Keep collection rules
  explicit for deployments that should not capture all traffic.
- Do not add another telemetry storage or evaluation implementation here.

## Build and local validation

Use the repository root or this module directory for the existing Make targets:

```bash
make build
make integration-test
```

The Wasm binary is written to the repository-root `target/wasm32-unknown-unknown/release/` directory.
