//! Shared authenticated router testkit for compatibility contracts.

use axum::middleware::from_fn_with_state;
use axum::routing::post;
use axum::Router;
use softprobe_runtime::api::ingestion::traces::ingest_traces;
use softprobe_runtime::api::{create_router, AppState, ControlPlaneRuntime};
use softprobe_runtime::authn::Resolver;
use softprobe_runtime::config::Config;
use softprobe_runtime::runtime_api::{runtime_auth_middleware, runtime_control_routes};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Build the same auth/lifecycle router used by the production compatibility path.
pub async fn authenticated_router(
    config: Arc<Config>,
    tenant_id: &str,
    auth_success: bool,
) -> (Router, AppState, MockServer) {
    authenticated_router_with_expected_token(config, tenant_id, auth_success, None).await
}

/// Build an authenticated router whose control-plane mock only accepts `token`.
///
/// This keeps tests honest about the production Bearer-token boundary: a request
/// with a different token never receives the tenant identity from the mock.
pub async fn authenticated_router_with_token(
    config: Arc<Config>,
    tenant_id: &str,
    token: &str,
) -> (Router, AppState, MockServer) {
    authenticated_router_with_expected_token(config, tenant_id, true, Some(token)).await
}

async fn authenticated_router_with_expected_token(
    config: Arc<Config>,
    tenant_id: &str,
    auth_success: bool,
    expected_token: Option<&str>,
) -> (Router, AppState, MockServer) {
    let mock = MockServer::start().await;
    let body = if auth_success {
        serde_json::json!({
            "success": true,
            "data": {"tenantId": tenant_id, "resources": []}
        })
    } else {
        serde_json::json!({"success": false})
    };
    let request = Mock::given(method("POST")).and(path("/"));
    let request = match expected_token {
        Some(token) => request.and(body_json(serde_json::json!({ "apiKey": token }))),
        None => request,
    };
    request
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&mock)
        .await;

    let control = ControlPlaneRuntime {
        resolver: Resolver::new(format!("{}/", mock.uri()), Duration::from_secs(60)),
    };
    let (router, state) = create_router(config, post(ingest_traces), Some(control))
        .await
        .expect("authenticated test router");
    let router = router
        .merge(runtime_control_routes().with_state(state.clone()))
        .layer(from_fn_with_state(state.clone(), runtime_auth_middleware));
    (router, state, mock)
}

/// A production-shaped Axum server backed by the token-aware router above.
/// The server is bound to an ephemeral local port and is shut down when the
/// returned guard is dropped.
pub struct AuthenticatedServer {
    pub base_url: String,
    pub state: AppState,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
    _auth_mock: MockServer,
}

impl Drop for AuthenticatedServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

pub async fn start_authenticated_server(
    config: Arc<Config>,
    tenant_id: &str,
    token: &str,
) -> AuthenticatedServer {
    let (router, state, auth_mock) =
        authenticated_router_with_token(config, tenant_id, token).await;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind authenticated test server");
    let address = listener.local_addr().expect("authenticated server address");
    let (shutdown, signal) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = signal.await;
            })
            .await
            .expect("authenticated test server");
    });

    AuthenticatedServer {
        base_url: format!("http://{address}"),
        state,
        shutdown: Some(shutdown),
        task,
        _auth_mock: auth_mock,
    }
}
