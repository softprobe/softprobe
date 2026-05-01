//! OTLP/gRPC trace ingest (`TraceService/Export`) — mirrors HTTP `/v1/traces` behavior.
//!
//! When [`AppState::hosted`] is set, requires gRPC metadata `authorization: Bearer <api_key>` (same as HTTP).
//! Listens on `OTEL_GRPC_PORT` (default **4317**) unless the process sets `SOFTPROBE_GRPC_DISABLE=1`.

use crate::api::ingestion::traces::process_traces;
use crate::api::AppState;
use crate::hosted::{hosted_export_trace_request, parse_bearer};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost::Message;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

struct GrpcTraceService {
    state: AppState,
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

        if self.state.hosted.is_some() {
            let auth_val = metadata.get("authorization").and_then(|v| v.to_str().ok());
            let token = auth_val.and_then(parse_bearer).ok_or_else(|| {
                Status::unauthenticated("missing or invalid authorization metadata")
            })?;
            let hosted = self
                .state
                .hosted
                .as_ref()
                .ok_or_else(|| Status::failed_precondition("hosted mode not configured"))?;
            let tenant = hosted
                .resolver
                .resolve(&token)
                .await
                .map_err(|_| Status::permission_denied("tenant resolution failed"))?;
            hosted_export_trace_request(self.state.clone(), &tenant, inner)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            return Ok(Response::new(ExportTraceServiceResponse::default()));
        }

        process_traces(self.state.clone(), inner, body_size)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

pub async fn run_trace_grpc_server(
    addr: std::net::SocketAddr,
    state: AppState,
) -> anyhow::Result<()> {
    let svc = GrpcTraceService { state };
    Server::builder()
        .add_service(TraceServiceServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}
