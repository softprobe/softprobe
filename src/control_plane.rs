//! Shared control-plane wiring (auth resolver, Redis sessions).

use crate::authn;
use crate::session_redis::RedisStore;
use std::sync::Arc;

#[derive(Clone)]
pub struct ControlPlaneRuntime {
    pub resolver: authn::Resolver,
    pub session_store: Arc<tokio::sync::Mutex<RedisStore>>,
}
