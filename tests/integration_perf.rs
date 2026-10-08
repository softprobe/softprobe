//! DuckDB-heavy performance tests run in a **separate** test binary so a fresh process avoids
//! libduckdb global state accumulating after many prior integration tests (intermittent SIGSEGV on
//! macOS when everything shares one `tests` binary).

#![allow(dead_code)] // `util` is shared with `tests.rs`; only a subset is used here.

mod util;

#[path = "integration/performance.rs"]
mod performance;
