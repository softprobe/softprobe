//! OTLP/gRPC trace ingest (`TraceService/Export`) — mirrors HTTP `/v1/traces` behavior.
//!
//! Requires gRPC metadata `authorization: Bearer <api_key>` (same as HTTP).
//! Listens on `OTEL_GRPC_PORT` (default **4317**) unless the process sets `SOFTPROBE_GRPC_DISABLE=1`.

use crate::api::ingestion::traces::process_traces;
use crate::api::AppState;
use crate::runtime_api::parse_bearer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost::Message;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// gRPC [`TraceService`] implementation; exposed for `integration-e2e` tests that assert OTLP/gRPC parity with HTTP ingest.
pub struct GrpcTraceService {
    pub state: AppState,
}

#[tonic::async_trait]
impl TraceService for GrpcTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let metadata = request.metadata().clone();
        let inner = request.into_inner();
        let body_size = inner.encoded_len();

        let auth_val = metadata.get("authorization").and_then(|v| v.to_str().ok());
        let token = auth_val
            .and_then(parse_bearer)
            .ok_or_else(|| Status::unauthenticated("missing or invalid authorization metadata"))?;
        let control_plane = self
            .state
            .engines
            .control_plane()
            .ok_or_else(|| Status::internal("gRPC ingest requires control-plane runtime"))?;
        let tenant = control_plane
            .resolver
            .resolve(&token)
            .await
            .map_err(|_| Status::permission_denied("tenant resolution failed"))?;
        process_traces(
            self.state.clone(),
            inner,
            body_size,
            Some(tenant.tenant_id.clone()),
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

pub async fn run_trace_grpc_server(
    addr: std::net::SocketAddr,
    state: AppState,
) -> anyhow::Result<()> {
    if state.engines.control_plane().is_none() {
        anyhow::bail!("gRPC ingest requires control-plane runtime");
    }
    let svc = GrpcTraceService { state };
    Server::builder()
        .add_service(TraceServiceServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn grpc_export_empty_trace_without_auth_is_unauthenticated() {
        let (_r, state, _t) = crate::test_support::local_router_and_state()
            .await
            .expect("state");
        assert!(state.engines.control_plane().is_none());
        let svc = GrpcTraceService { state };
        let req = Request::new(ExportTraceServiceRequest::default());
        let got = TraceService::export(&svc, req).await;
        let err = got.expect_err("gRPC export should fail without auth");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        assert!(
            err.message()
                .contains("missing or invalid authorization metadata"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn grpc_export_with_auth_without_control_plane_is_internal_error() {
        let (_r, state, _t) = crate::test_support::local_router_and_state()
            .await
            .expect("state");
        assert!(state.engines.control_plane().is_none());
        let svc = GrpcTraceService { state };
        let mut req = Request::new(ExportTraceServiceRequest::default());
        req.metadata_mut()
            .insert("authorization", "Bearer test-key".parse().unwrap());
        let got = TraceService::export(&svc, req).await;
        let err = got.expect_err("gRPC export needs control-plane auth wiring");
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("control-plane"), "{err}");
    }
}
