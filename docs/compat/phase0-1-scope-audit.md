# Phase 0/1 scope audit

This is a read-only audit of the working tree against `origin/v0.2`, based on
the bounded inspection performed on 2026-08-15. No tests were run and no
GitHub issue was modified during this audit.

The baseline is pinned to `origin/v0.2 = 5e01c3475dee99239ba48c8e901d4f5d7c52aece`.

Authoritative issue context: [parent issue #25](https://github.com/softprobe/thelake/issues/25),
[Loki phase #29](https://github.com/softprobe/thelake/issues/29),
[Tempo phase #31](https://github.com/softprobe/thelake/issues/31),
[Grafana phase #27](https://github.com/softprobe/thelake/issues/27), and
[conformance phase #28](https://github.com/softprobe/thelake/issues/28).

## Baseline and scope

The branch is `codex/compatibility-project`, based on `origin/v0.2`. Phase 0/1
Prometheus behavior is the baseline. The current diff also contains Phase 2/3
protocol implementation, Phase 4 Grafana assets, and Phase 5 harness assets.
Those phase-specific additions are not Phase 0/1 changes and must not be
counted as baseline work.

The following modified pre-existing production, storage, or integration files
are the Phase 0/1-era files that require explicit scope justification:

| File | Minimal Phase 2–5 necessity | Exact regression/compatibility evidence | Scope boundary |
|---|---|---|---|
| `src/api/ingestion/traces.rs` | Required for Tempo response fidelity: preserves OTLP instrumentation scope and span-link metadata on the canonical write path. | `src/models/span.rs::tests::otlp_scope_and_link_carriers_keep_key_value_shape`; Tempo contract suite in `tests/compat/tempo/mod.rs`. | Encode only the two retained Tempo fidelity carriers; no new ingestion protocol or write API. |
| `src/api/llm/query.rs` | Required after trace/log storage moved to nanosecond precision; existing LLM/session predicates must not truncate compatibility fixture times. | `src/api/llm/query.rs::tests::span_query_predicates_use_timestamp_ns`; `tests/integration/http_api.rs::timestamp_ns_span_queries_work_through_http_paths`. | Timestamp SQL literals for span/session paths only; metrics remain unchanged. |
| `src/api/sql_support.rs` | Shared timestamp literals and cursor bounds must preserve nanoseconds for Loki/Tempo and existing telemetry queries. | `src/api/sql_support.rs::tests::timestamp_ns_literal_preserves_nanoseconds`; telemetry and HTTP regression tests. | Timestamp formatting and bounds only; no protocol-specific SQL handler. |
| `src/api/telemetry.rs` | Shared trace/log search and detail paths must read the nanosecond columns used by Phase 2/3; the local SQL literal helper is consolidated. | `src/api/telemetry.rs::tests::bounded_log_details_use_timestamp_ns_without_changing_other_tables`; `src/api/telemetry.rs::tests::timestamp_filter_uses_timestamp_ns_for_trace_search`. | Trace/log predicates and helper reuse only; metrics retain their existing timestamp type. |
| `src/models/span.rs` | Required to retain instrumentation scope and links for Tempo projection without changing the public canonical model shape. | `src/models/span.rs::tests::otlp_scope_and_link_carriers_keep_key_value_shape`; Tempo fixture/contract coverage in `tests/compat/tempo/mod.rs`. | Internal persistence carriers only; arbitrary instrumentation-scope fields remain explicitly unsupported. |
| `src/storage/schema/tables.rs` | Required for Loki/Tempo nanosecond fidelity and trace fidelity columns. | `src/storage/schema/tables.rs::tests::logs_timestamps_use_nanosecond_contract`; `tests/integration/http_api.rs`. | Logs and traces only; metrics schema is not migrated. |
| `src/storage/schema/arrow.rs` | Required to write/read nanosecond log/trace timestamps and retained trace metadata. | `traces_round_trip_nanosecond_timestamps_and_metadata_columns`; `logs_round_trip_nanosecond_timestamps`; `logs_preserve_distinct_timestamps_within_one_microsecond`. | Arrow conversion for logs/traces and retained metadata only. |
| `src/storage/schema/variant.rs` | Required to expose trace fidelity columns and preserve their VARIANT types during materialization. | `encodes_hot_keys_with_stable_json_types`; `variant_sql_helpers_cast_nested_fields`; `parse_projected_json_value_objects_and_leaves_plain_text`; `encode_attributes_json_rehydrates_only_tagged_nested`; Tempo fidelity assertions in `tests/compat/tempo/mod.rs::tempo_phase3_trace_responses_preserve_otlp_fidelity_and_ordering`. | Trace fidelity columns and migration shape only. |
| `src/storage/ducklake/util.rs` | Required to upgrade existing v0.2 tables safely to `TIMESTAMP_NS` and add trace fidelity columns without silent truncation. | Helpers are exercised through `src/storage/ducklake/writer.rs::migrates_existing_microsecond_log_columns_without_truncating_history`, `migrates_existing_microsecond_trace_columns_without_losing_epoch_values`, and `refuses_unsupported_log_timestamp_schema_before_ddl`. | Refuse unknown schemas; migrate only required trace/log columns. |
| `src/storage/ducklake/writer.rs` | Required to invoke safe migrations before writing compatibility data to existing tables. | `migrates_existing_microsecond_log_columns_without_truncating_history`; `migrates_existing_microsecond_trace_columns_without_losing_epoch_values`; `refuses_unsupported_log_timestamp_schema_before_ddl`. | Migration orchestration for traces/logs only; no destructive reset. |
| `tests/integration/http_api.rs` | Minimal regression proving existing HTTP/session/telemetry paths survive nanosecond trace storage changes. | `tests/integration/http_api.rs::timestamp_ns_span_queries_work_through_http_paths`. | Existing HTTP paths only; no new compatibility endpoint. |
| `tests/compat_phase0.rs` | Must absorb the move from authenticated Loki/Tempo stubs to live handlers while retaining Phase 0 route/auth/tenant checks. | The file itself, including `tempo_query_tenant_id_param_does_not_override_auth`, plus `tests/compat/loki/mod.rs` and `tests/compat/tempo/mod.rs`. | Route expectation updates and shared-auth reuse only; preserve unrelated Phase 0 assertions. |
| `tests/compat/support/mod.rs` | Shared compatibility testkit/lifecycle/auth/artifact helpers are reused by Phases 2–5 and remove duplicated Phase 0/1 setup. | `support_helpers_load_manifest_and_probe_paths`; `shared_selector_parser_combines_ids_and_ignores_suite_sentinel`; `shared_selector_rejects_unknown_and_non_differential_cases`; `shared_execution_receipt_is_written_and_validated`; `manifest_descriptor_fixture_provenance_is_preserved_in_receipt`; consumers include `tests/compat_phase0.rs`, `tests/compat/loki/mod.rs`, and `tests/compat/tempo/mod.rs`. | One shared implementation for setup, auth, normalization, and artifacts. |
| `tests/compat/support/prometheus_oracle.rs` | Phase 1 Prometheus oracle consumes the shared lifecycle and exposes state for the shared harness, avoiding a second reference-service implementation. | `reference_image_defaults_to_pinned_image`; `reference_image_uses_non_empty_environment_override`; `tests/integration/prometheus/diff.rs::manifest_selector_filters_prometheus_differential_requests`; `selectors_combine_and_suite_sentinel_is_ignored`; `selectors_reject_unknown_and_non_differential_manifest_cases`; `absent_selector_preserves_all_prometheus_differential_requests`. | Lifecycle reuse only; preserve Prometheus fixture and oracle semantics. |

This is the complete Phase 0/1-era production/integration list identified in
the bounded audit. The named tests above are the local regression and
compatibility evidence for the listed changes.

## Modified files that are not Phase 0/1 baseline changes

These pre-existing production files are Phase 2/3 implementation or route
wiring and should be reviewed under issues #29 and #31, not described as
Phase 0/1 work:

- `src/api/mod.rs` — merges live Loki and Tempo routers.
- `src/compat/mod.rs` — exports Loki and Tempo modules.
- `src/compat/backends/logs.rs` and `src/compat/backends/traces.rs` — typed query backends.
- `src/compat/backends/mod.rs` — backend wiring.
- `src/compat/projection/loki.rs` and `src/compat/projection/tempo.rs` — protocol projections.
- `src/compat/stubs.rs` — removes old Loki/Tempo stub route registrations.
- `src/compat/prometheus/mod.rs` — exports shared Prometheus parameter helpers; retain only if the shared harness consumes them.

New files under `src/compat/loki/`, `src/compat/tempo/`, and
`src/compat/backends/ducklake_*` are also Phase 2/3 files, not baseline
changes.

## Files to exclude or revert before the PR

The following user-owned untracked presentation/marketing material is outside
the compatibility project and must remain untouched and uncommitted:

- `Softprobe-AI-Observability-China-Bank-v1.pptx`
- `Softprobe-AI-Observability-China-Bank-v1.pptx.inspect.ndjson`
- `Softprobe-AI-Observability-China-Bank-v1/`
- `marketing/`

Before the PR, exclude any unrelated edits in those paths and any Phase 0/1
change that cannot be tied to the rationale and test evidence above. In
particular, `docs/compat/phase1-prometheus.md`,
`docs/compat/capability.v0.yaml`, `docs/compat/matrix.md`, and
`docs/compat/references.v0.yaml` are documentation/manifest changes, not
production or integration changes; retain only lines required to describe the
implemented Phase 2/3 subset and pinned references.

Do not revert the nanosecond/storage or OTLP-fidelity changes solely because
they touch shared Phase 0/1 code: they are necessary cross-cutting fixes, but
they must be retained only with passing regression evidence. Phase 4/5 files
under `scripts/`, `.github/workflows/`, `tests/compat/grafana/`,
`tests/compat/manifests/`, and the new compatibility documentation must be
evaluated against issues #27 and #28, not included in a baseline exception.

## Evidence status

This document records static evidence only. No tests, formatting, build, or
full-suite validation were run. The GitHub checklists in issues #25, #29, #31,
#27, and #28 were not changed. No Phase 0/1 change should be called complete
until the named tests have been run and recorded.

Historical scope evidence is recorded in [issue #26 comment 5273679712](https://github.com/softprobe/thelake/issues/26#issuecomment-5273679712), [issue #26 comment 5273324662](https://github.com/softprobe/thelake/issues/26#issuecomment-5273324662), and [issue #30 comment 5274096708](https://github.com/softprobe/thelake/issues/30#issuecomment-5274096708). Fresh Prometheus sentinel evidence is recorded in [issue #25 comment 5306075775](https://github.com/softprobe/thelake/issues/25#issuecomment-5306075775).

Remaining limitation: no direct regression test upgrades an existing trace
table missing `resource_attributes`, `instrumentation_scope`, and `links`.
