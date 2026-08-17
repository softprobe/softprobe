# metrics-layout 20260815T173908Z

- binary_profile: `dev`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `1`
- pass/fail: **28/49** pass, **21** fail
- fixture_hash: `bb5b49d27d203084`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_hist_samples:part=1,sort=262; metric_series:part=1,sort=262; metric_posti |
| AC-D3 | True | None | cargo: test compat::backends::postings_resolve::tests::prom_backend_is_ducklake_ |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=10 after=11 sender_alive=True |
| AC-Q1 | True | 8.616432001872454 | repeats=20 codes={200} series=1 |
| AC-Q2 | False | 1.6421879990957677 | series=0 points=0 |
| AC-Q3 | True | 225.32181599672185 | series=1 |
| AC-Q4 | True | 125.47474099847022 | http=400 ms=125 body_snip={"error": "limit_exceeded: series count 10001 exceeds  |
| AC-Q5 | False | 2.028857998084277 | series=0 want_J=10 |
| AC-Q6 | True | 3.6524070019368082 | repeats=20 |
| AC-Q7 | True | None | cargo: test compat::backends::postings_resolve::tests::resolve_and_samples_sql_u |
| AC-Q8 | True | 485.6496190041071 | all 15 exprs ok |
| AC-Q9 | False | None | maintenance-under-load measure not fully automated in harness yet |
| AC-H1 | True | 593.7113260006299 | hist_rows=1200 sample_rows=0 |
| AC-H2 | True | None | cargo: test compat::backends::grain::tests::hist_selector_uses_hist_table_short_ |
| AC-C1 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_pod_values_differ_b |
| AC-C2 | True | None | count=15000 want=15000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_dead_pod_absent_fro |
| AC-W1 | True | None | 180d http=200 365d http=200 unit=True |
| AC-W2 | True | None | 30d accept |
| AC-W3 | False | 342.0042959987768 | series=0 want=10 |
| AC-W4 | True | 223.28689799905987 | http=400 ms=223 |
| AC-W5 | False | 725.2790109996567 | points=0 |
| AC-W6 | True | 7.26116300211288 | tall_days_loaded=30 |
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
| AC-S2 | False | None | downsample trigger not automated; raw_count=202376 |
| AC-S3 | False | None | grafana-manual-up.sh must build release binary |
| AC-M1 | True | None | cargo: test compaction::executor::tests::maintenance_tables_include_metric_famil |
| AC-M2 | False | None | second maintenance watermark pass not automated |
| AC-G0 | False | None | GREPTIME_URL=http://127.0.0.1:14000 not healthy |
| AC-G1 | False | None | GREPTIME_URL=http://127.0.0.1:14000 not healthy |
| AC-G2 | False | None | GREPTIME_URL=http://127.0.0.1:14000 not healthy |
| AC-G3 | False | None | GREPTIME_URL=http://127.0.0.1:14000 not healthy |
| AC-G4 | False | None | GREPTIME_URL=http://127.0.0.1:14000 not healthy |
| AC-G5 | False | None | GREPTIME_URL=http://127.0.0.1:14000 not healthy |
| AC-G6 | False | None | GREPTIME_URL=http://127.0.0.1:14000 not healthy |

## Blockers
- pr_floor skipped F-collapse-90d full load (wall-clock); AC-W3 expected fail
- prom 1h probe points=0 (want>=600)
- prom collapse probe series=0 want>=10
- GREPTIME_URL=http://127.0.0.1:14000 not healthy
