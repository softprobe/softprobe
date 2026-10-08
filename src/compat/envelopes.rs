//! Protocol-native response envelopes for compatibility stubs and adapters.
//!
//! Softprobe stable codes (`unsupported_feature`, …) remain discoverable in the
//! protocol error message (and Tempo's `softprobe_code` field). Prometheus uses
//! `errorType: "execution"` for unsupported features.

use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::tenant::ProtocolScope;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

/// Map a compatibility error into the wire envelope for `protocol`.
///
/// Softprobe stable codes remain discoverable:
/// - Prometheus/Loki: prefixed in the `error` string (`unsupported_feature: …`)
/// - Tempo: `softprobe_code` field
///
/// Protocol HTTP handlers MUST return [`error_response`], not
/// [`CompatError`](crate::compat::errors::CompatError)'s Softprobe-internal
/// `IntoResponse` (reserved for non-protocol Softprobe APIs).
pub fn error_envelope(protocol: ProtocolScope, err: &CompatError) -> Value {
    // Prefer stable code + feature detail without doubling "unsupported feature".
    let detail = err
        .message
        .strip_prefix("unsupported feature: ")
        .unwrap_or(err.message.as_str());
    let message = format!("{}: {}", err.code.as_str(), detail);
    match protocol {
        ProtocolScope::Prometheus => json!({
            "status": "error",
            "errorType": prometheus_error_type(err.code),
            "error": message,
        }),
        ProtocolScope::Loki => json!({
            "status": "error",
            "error": message,
        }),
        ProtocolScope::Tempo => json!({
            "message": message,
            "softprobe_code": err.code.as_str(),
        }),
    }
}

fn prometheus_error_type(code: CompatErrorCode) -> &'static str {
    match code {
        CompatErrorCode::UnsupportedFeature => "execution",
        // Prometheus native types: timeout | canceled | execution | bad_data | unavailable.
        // Map authz/tenant failures to bad_data (invalid request for this tenant context).
        CompatErrorCode::BadRequest
        | CompatErrorCode::LimitExceeded
        | CompatErrorCode::Unauthorized
        | CompatErrorCode::Forbidden => "bad_data",
    }
}

/// HTTP response with protocol-native JSON body.
pub fn error_response(protocol: ProtocolScope, err: CompatError) -> Response {
    (err.code.http_status(), Json(error_envelope(protocol, &err))).into_response()
}

/// Target success envelope shapes for Phase 1+ (fixtures / docs).
pub fn success_envelope_minimal(protocol: ProtocolScope) -> Value {
    match protocol {
        ProtocolScope::Prometheus => json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": []
            }
        }),
        ProtocolScope::Loki => json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": []
            }
        }),
        ProtocolScope::Tempo => json!({
            "batches": []
        }),
    }
}

/// Softprobe code substring used in protocol error messages for contract tests.
pub fn softprobe_code_in_message(body: &Value, code: &str) -> bool {
    body.get("error")
        .and_then(|v| v.as_str())
        .map(|s| s.starts_with(code))
        .unwrap_or(false)
        || body.get("softprobe_code").and_then(|v| v.as_str()) == Some(code)
        || body
            .get("message")
            .and_then(|v| v.as_str())
            .map(|s| s.starts_with(code))
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_unsupported_uses_execution_error_type() {
        let err = CompatError::unsupported("prometheus_api");
        let body = error_envelope(ProtocolScope::Prometheus, &err);
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "execution");
        assert!(body["error"]
            .as_str()
            .unwrap()
            .starts_with("unsupported_feature:"));
    }

    #[test]
    fn loki_and_tempo_carry_stable_code() {
        let err = CompatError::unsupported("loki_api");
        let loki = error_envelope(ProtocolScope::Loki, &err);
        assert!(softprobe_code_in_message(&loki, "unsupported_feature"));
        let tempo = error_envelope(ProtocolScope::Tempo, &err);
        assert_eq!(tempo["softprobe_code"], "unsupported_feature");
    }
}
