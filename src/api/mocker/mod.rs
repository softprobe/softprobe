//! Mocker aggregation/list SQL recipes (Phase 1 of
//! `backend/docs/thelake-telemetry-mocker-migration-plan.md`).
//!
//! No HTTP routes are wired here yet — Phase 1 is thelake-only ("no backend API cutover"). The
//! backend read facade (Phase 5/6) will call these SQL-compiling functions the same way
//! `api/llm/query.rs`'s `compile_*` functions are called from that module's handlers today.

pub mod query;
