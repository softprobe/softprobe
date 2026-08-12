//! Stable compatibility error classes (never silent approximation).
//!
//! For **protocol** HTTP responses (Prometheus/Loki/Tempo), use
//! [`crate::compat::envelopes::error_response`]. The `IntoResponse` impl below
//! is Softprobe-internal JSON only and must not be used for compat stubs.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use serde_json::json;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatErrorCode {
    UnsupportedFeature,
    Unauthorized,
    Forbidden,
    BadRequest,
    LimitExceeded,
}

impl CompatErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedFeature => "unsupported_feature",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::BadRequest => "bad_request",
            Self::LimitExceeded => "limit_exceeded",
        }
    }

    pub fn http_status(self) -> StatusCode {
        match self {
            Self::UnsupportedFeature => StatusCode::NOT_IMPLEMENTED,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::LimitExceeded => StatusCode::BAD_REQUEST,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatError {
    pub code: CompatErrorCode,
    pub message: String,
}

impl CompatError {
    pub fn new(code: CompatErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn unsupported(feature: impl Into<String>) -> Self {
        Self::new(
            CompatErrorCode::UnsupportedFeature,
            format!("unsupported feature: {}", feature.into()),
        )
    }

    pub fn to_json(&self) -> serde_json::Value {
        json!({
            "status": "error",
            "error": {
                "code": self.code.as_str(),
                "message": self.message,
            }
        })
    }
}

impl IntoResponse for CompatError {
    fn into_response(self) -> Response {
        (self.code.http_status(), Json(self.to_json())).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_feature_is_501_with_stable_code() {
        let err = CompatError::unsupported("promql: absent");
        assert_eq!(err.code, CompatErrorCode::UnsupportedFeature);
        assert_eq!(err.code.http_status(), StatusCode::NOT_IMPLEMENTED);
        let body = err.to_json();
        assert_eq!(body["error"]["code"], "unsupported_feature");
    }
}
