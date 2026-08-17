# metrics-layout 20260815T174353Z

- binary_profile: `dev`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `1`
- pass/fail: **34/49** pass, **15** fail
- fixture_hash: `35ff9865becdcd18`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_series:part=1,sort=261; metric_hist_samples:part=1,sort=261; metric_posti |
| AC-D3 | True | None | cargo: test compat::backends::postings_resolve::tests::prom_backend_is_ducklake_ |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=9 after=10 sender_alive=True |
| AC-Q1 | True | 9.973321000870783 | repeats=20 codes={200} series=1 |
| AC-Q2 | False | 2.9782659985357895 | series=0 points=0 |
| AC-Q3 | True | 278.01942599762697 | series=1 |
| AC-Q4 | True | 130.58596599876182 | http=400 ms=131 body_snip={"error": "limit_exceeded: series count 10001 exceeds  |
| AC-Q5 | False | 2.174738001485821 | series=0 want_J=10 |
| AC-Q6 | True | 3.4371470028418116 | repeats=20 |
| AC-Q7 | True | None | cargo: test compat::backends::postings_resolve::tests::resolve_and_samples_sql_u |
| AC-Q8 | True | 531.4624449965777 | all 15 exprs ok |
| AC-Q9 | False | None | maintenance-under-load measure not fully automated in harness yet |
| AC-H1 | True | 542.1517740032868 | hist_rows=1200 sample_rows=0 |
| AC-H2 | True | None | cargo: test compat::backends::grain::tests::hist_selector_uses_hist_table_short_ |
| AC-C1 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_pod_values_differ_b |
| AC-C2 | True | None | count=15000 want=15000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_dead_pod_absent_fro |
| AC-W1 | True | None | 180d http=200 365d http=200 unit=True |
| AC-W2 | True | None | 30d accept |
| AC-W3 | False | 251.5965100028552 | series=0 want=10 |
| AC-W4 | True | 238.65963400021428 | http=400 ms=239 |
| AC-W5 | False | 683.3104899997124 | points=0 |
| AC-W6 | True | 1.7029090013238601 | tall_days_loaded=30 |
| AC-N1 | True | None | cargo: test config::tests::default_max_snapshot_age_seconds_is_one_hour ... ok |
| AC-N2 | True | None | cargo: test compaction::executor::tests::expire_snapshots_sql_honors_seconds ... |
| AC-N3 | False | None | F-snap (120s commits) not run in this harness iteration |
| AC-N4 | False | None | depends on F-snap / expiry pass |
| AC-N5 | True | None | cargo: test compaction::executor::tests::cleanup_old_files_sql_honors_seconds .. |
| AC-F1 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F2 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F3 | True | None | cargo: test compaction::twcs::tests::twcs_partition_key_is_record_date_only ...  |
| AC-F4 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F5 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F6 | True | None | cargo: test compaction::twcs::tests::twcs_merge_does_not_cross_record_date ... o |
| AC-S1 | True | None | cargo: test storage::ducklake::metrics_layout_write::tests::skinny_samples_small |
| AC-S2 | False | None | downsample trigger not automated; raw_count=202375 |
| AC-S3 | False | None | grafana-manual-up.sh must build release binary |
| AC-M1 | True | None | cargo: test compaction::executor::tests::maintenance_tables_include_metric_famil |
| AC-M2 | False | None | second maintenance watermark pass not automated |
| AC-G0 | True | None | sha=a8924bb95c43b562e4312af309d2a5e80c103185 hb_before=2 hb_after=5 hb_alive=Tru |
| AC-G1 | True | 9.973321000870783 | T-Q1 layout_tall 30m; greptime_codes={200} |
| AC-G2 | True | 2.9782659985357895 | T-Q2 layout_tall 30d; greptime_codes={200} |
| AC-G3 | False | 278.01942599762697 | AC-G3 ratio 17.38 > R=10.0 (soft=278.0ms greptime=16.0ms) — escalate per §4.4 |
| AC-G4 | True | 3.4371470028418116 | T-Q6 __name__/values; greptime_codes={200} |
| AC-G5 | True | 2.174738001485821 | T-Q5 collapse 30d; greptime_codes={200} |
| AC-G6 | True | None | ingest_path=/v1/otlp/v1/metrics (must be OTLP /v1/otlp/v1/metrics, not remote_wr |

## Blockers
- pr_floor skipped F-collapse-90d full load (wall-clock); AC-W3 expected fail
- prom 1h probe points=0 (want>=600)
- prom collapse probe series=0 want>=10
- AC-G3 ratio 17.38 > R=10.0 (soft=278.0ms greptime=16.0ms) — escalate per §4.4
