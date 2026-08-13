//! Shared helpers for compatibility contract tests.

pub mod prometheus;
pub mod prometheus_oracle;
pub mod promqltest;

use softprobe_runtime::compat::capability::{
    parse_capability_yaml, CapabilityManifest, EMBEDDED_CAPABILITY_V0,
};
use softprobe_runtime::compat::stubs::declared_compat_probe_paths;

pub fn load_embedded_capability() -> CapabilityManifest {
    parse_capability_yaml(EMBEDDED_CAPABILITY_V0).expect("embedded capability.v0")
}

pub fn all_compat_probe_paths() -> &'static [(&'static str, &'static str)] {
    declared_compat_probe_paths()
}

#[test]
fn support_helpers_load_manifest_and_probe_paths() {
    let m = load_embedded_capability();
    assert_eq!(m.version, "compat.v0");
    assert!(!all_compat_probe_paths().is_empty());
}
