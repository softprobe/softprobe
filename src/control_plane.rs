//! Shared control-plane wiring (auth resolver).

use crate::authn;

#[derive(Clone)]
pub struct ControlPlaneRuntime {
    pub resolver: authn::Resolver,
}
