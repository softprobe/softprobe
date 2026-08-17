# metrics-layout 20260815T221216Z

- binary_profile: `dev`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `0`
- pass/fail: **40/49** pass, **9** fail
- fixture_hash: `44fc891c53824b90`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_series:part=1,sort=520; metric_hist_samples:part=1,sort=520; metric_sampl |
| AC-D3 | True | None | cargo: test compat::backends::postings_resolve::tests::prom_backend_is_ducklake_ |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=9 after=10 sender_alive=True |
| AC-Q1 | True | 17.171655999845825 | repeats=20 codes={200} series=1 |
| AC-Q2 | True | 17.830204000347294 | series=1 points=721 |
| AC-Q3 | True | 575.1971410063561 | series=1 |
| AC-Q4 | True | 2.7866369928233325 | http=400 ms=3 body_snip={"error": "limit_exceeded: series count 15000 exceeds ma |
| AC-Q5 | True | 37.003698002081364 | series=10 want_J=10 |
| AC-Q6 | True | 4.230277001624927 | repeats=20 |
| AC-Q7 | True | None | cargo: test compat::backends::postings_resolve::tests::resolve_and_samples_sql_u |
| AC-Q8 | True | 842.6590339950053 | all 15 exprs ok |
| AC-Q9 | True | 28.285794003750198 | p95=28.285794003750198 codes={200} maint=metric_samples:set=200:ok ms=81;metric_ |
| AC-H1 | False | 97.70487299829256 | hist_rows=0 sample_rows=0 |
| AC-H2 | True | None | cargo: test compat::backends::grain::tests::hist_selector_uses_hist_table_short_ |
| AC-C1 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_pod_values_differ_b |
| AC-C2 | True | None | count=15000 want=15000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_dead_pod_absent_fro |
| AC-W1 | True | None | 180d http=200 365d http=200 unit=True |
| AC-W2 | True | None | 30d accept |
| AC-W3 | True | 287.77267500117887 | series=10 want=10 |
| AC-W4 | True | 1672.525558999041 | http=400 ms=1673 |
| AC-W5 | True | 2177.5046609982383 | points=721 want>=672 |
| AC-W6 | True | 6.020505010383204 | tall_days_loaded=30 |
| AC-N1 | True | None | cargo: test config::tests::default_max_snapshot_age_seconds_is_one_hour ... ok |
| AC-N2 | True | None | cargo: test compaction::executor::tests::expire_snapshots_sql_honors_seconds ... |
| AC-N3 | False | None | snaps=452 before=1064 old_gt_A+I=0 bar=81 expire_http=200/200 commits=120 expire |
| AC-N4 | True | None | samples_before_snap=12 pre_expire=132 after=132 |
| AC-N5 | True | None | cargo: test compaction::executor::tests::cleanup_old_files_sql_honors_seconds .. |
| AC-F1 | True | None | closed_files=2 limit=4 stats=[('2026-08-13', 1, 10131272), ('2026-08-14', 1, 987 |
| AC-F2 | True | None | bytes_before=21096415 median=9872435.0 sizes=[9872435, 10131272] pre=True |
| AC-F3 | True | None | cargo: test compaction::twcs::tests::twcs_partition_key_is_record_date_only ...  |
| AC-F4 | True | None | today=2026-08-15 files=2 stats=[('2026-08-15', 2, 70702)] |
| AC-F5 | True | None | precondition_met=True; metric_postings:files=2/4; metric_series:files=2/4; metri |
| AC-F6 | True | None | cargo: test compaction::twcs::tests::twcs_merge_does_not_cross_record_date ... o |
| AC-S1 | True | None | cargo: test storage::ducklake::metrics_layout_write::tests::skinny_samples_small |
| AC-S2 | True | None | raw_before=2185088 raw_after_downsample=2185088 |
| AC-S3 | True | None | grafana-manual-up.sh must build release binary |
| AC-M1 | True | None | cargo: test compaction::executor::tests::maintenance_tables_include_metric_famil |
| AC-M2 | True | None | first={'metric_samples': 2185088, 'metric_samples_5m': 175953, 'metric_samples_1 |
| AC-G0 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G1 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G2 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G3 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G4 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G5 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G6 | False | None | COMPARE_GREPTIME=0 — G9 not run |

## Blockers
- pr_floor skipped F-collapse-90d full load (wall-clock); AC-W3 expected fail
