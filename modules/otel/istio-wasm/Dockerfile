FROM scratch
ARG WASM_SHA256
LABEL wasm.sha256=${WASM_SHA256}
COPY target/wasm32-unknown-unknown/release/sp_istio_agent.wasm /plugin.wasm
