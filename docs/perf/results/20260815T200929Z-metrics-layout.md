# metrics-layout 20260815T200929Z

- binary_profile: `dev`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `0`
- pass/fail: **41/49** pass, **8** fail
- fixture_hash: `d70c39d6cd88cfc1`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_samples:part=1,sort=426; metric_postings:part=1,sort=426; metric_series:p |
| AC-D3 | True | None | cargo: test compat::backends::postings_resolve::tests::prom_backend_is_ducklake_ |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=34 after=35 sender_alive=True |
| AC-Q1 | True | 13.006056993617676 | repeats=20 codes={200} series=1 |
| AC-Q2 | True | 4.209916005493142 | series=1 points=721 |
| AC-Q3 | True | 381.7343139962759 | series=1 |
| AC-Q4 | True | 4.259856010321528 | http=400 ms=4 body_snip={"error": "limit_exceeded: series count 15000 exceeds ma |
| AC-Q5 | True | 36.64146400114987 | series=10 want_J=10 |
| AC-Q6 | True | 11.881338999955915 | repeats=20 |
| AC-Q7 | True | None | cargo: test compat::backends::postings_resolve::tests::resolve_and_samples_sql_u |
| AC-Q8 | True | 1307.8955770033645 | all 15 exprs ok |
| AC-Q9 | True | 13.447877005091868 | p95=13.447877005091868 codes={200} maint=metric_samples:set=200:ok ms=114;metric |
| AC-H1 | True | 551.669402004336 | hist_rows=1200 sample_rows=0 |
| AC-H2 | True | None | cargo: test compat::backends::grain::tests::hist_selector_uses_hist_table_short_ |
| AC-C1 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_pod_values_differ_b |
| AC-C2 | True | None | count=15000 want=15000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_dead_pod_absent_fro |
| AC-W1 | True | None | 180d http=200 365d http=200 unit=True |
| AC-W2 | True | None | 30d accept |
| AC-W3 | True | 324.80418900377117 | series=10 want=10 |
| AC-W4 | True | 1361.9644639984472 | http=400 ms=1362 |
| AC-W5 | True | 1830.189865999273 | points=721 want>=672 |
| AC-W6 | True | 4.881335000391118 | tall_days_loaded=30 |
| AC-N1 | True | None | cargo: test config::tests::default_max_snapshot_age_seconds_is_one_hour ... ok |
| AC-N2 | True | None | cargo: test compaction::executor::tests::expire_snapshots_sql_honors_seconds ... |
| AC-N3 | False | None | snaps=451 before=4531 old_gt_A+I=0 bar=81 expire_http=200/200 commits=120 expire |
| AC-N4 | True | None | samples_before_snap=2185011 pre_expire=2185131 after=2185131 |
| AC-N5 | True | None | cargo: test compaction::executor::tests::cleanup_old_files_sql_honors_seconds .. |
| AC-F1 | True | None | closed_files=2 limit=4 stats=[('2026-08-13', 1, 10131276), ('2026-08-14', 1, 987 |
| AC-F2 | True | None | bytes_before=21096415 median=9872464.0 sizes=[9872464, 10131276] pre=True |
| AC-F3 | True | None | cargo: test compaction::twcs::tests::twcs_partition_key_is_record_date_only ...  |
| AC-F4 | True | None | today=2026-08-15 files=3 stats=[('2026-08-15', 3, 69775)] |
| AC-F5 | True | None | precondition_met=True; metric_postings:files=2/4; metric_series:files=2/4; metri |
| AC-F6 | True | None | cargo: test compaction::twcs::tests::twcs_merge_does_not_cross_record_date ... o |
| AC-S1 | True | None | cargo: test storage::ducklake::metrics_layout_write::tests::skinny_samples_small |
| AC-S2 | True | None | raw_before=2184993 raw_after_downsample=2184993 |
| AC-S3 | True | None | grafana-manual-up.sh must build release binary |
| AC-M1 | True | None | cargo: test compaction::executor::tests::maintenance_tables_include_metric_famil |
| AC-M2 | True | None | first={'metric_samples': 2184993, 'metric_samples_5m': 175953, 'metric_samples_1 |
| AC-G0 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G1 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G2 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G3 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G4 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G5 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G6 | False | None | COMPARE_GREPTIME=0 — G9 not run |

## Blockers
- pr_floor skipped F-collapse-90d full load (wall-clock); AC-W3 expected fail
