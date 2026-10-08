# OpenTelemetry Istio module use cases

The Istio Wasm integration adds HTTP-level trace data for services running in
an Istio mesh. It complements application instrumentation by observing service
requests at the proxy boundary.

## Agent workflow traces

When an agent service calls tools or APIs through the mesh, the module can add
request and response context to the service trace. With trace context
propagated end to end, those spans can help connect an agent operation to its
network calls and downstream services.

## Evaluation evidence

The module can contribute trace evidence to an evaluation workflow—for example,
which services an agent contacted, which requests failed, and how calls were
ordered. Evaluation scoring and agent-specific semantics remain in the agent
observability and evaluation components; this module exports network spans.

## Filtering

Use `collectionRules` to select the hosts and paths that should produce telemetry.
The bundled quickstart manifests match broad traffic patterns, so review and
narrow those rules before using them on a workload with unrelated or sensitive
traffic.
