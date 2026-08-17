# metrics-layout 20260815T184552Z

- binary_profile: `dev`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `0`
- pass/fail: **35/49** pass, **14** fail
- fixture_hash: `685ccfb43b204d6f`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_hist_samples:part=1,sort=261; metric_postings:part=1,sort=261; metric_ser |
| AC-D3 | True | None | cargo: test compat::backends::postings_resolve::tests::prom_backend_is_ducklake_ |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=9 after=10 sender_alive=True |
| AC-Q1 | True | 2.3938880040077493 | repeats=20 codes={200} series=1 |
| AC-Q2 | True | 5.443754998850636 | series=1 points=721 |
| AC-Q3 | True | 304.78890399535885 | series=1 |
| AC-Q4 | True | 2.8982670046389103 | http=400 ms=3 body_snip={"error": "limit_exceeded: series count 15000 exceeds ma |
| AC-Q5 | True | 43.14441000315128 | series=10 want_J=10 |
| AC-Q6 | True | 13.627867003378924 | repeats=20 |
| AC-Q7 | True | None | cargo: test compat::backends::postings_resolve::tests::resolve_and_samples_sql_u |
| AC-Q8 | True | 522.6567610006896 | all 15 exprs ok |
| AC-Q9 | True | 9.845130000030622 | p95=9.845130000030622 codes={200} maint=metric_samples:set=200:ok ms=6;metric_sa |
| AC-H1 | True | 281.5384960049414 | hist_rows=1200 sample_rows=0 |
| AC-H2 | True | None | cargo: test compat::backends::grain::tests::hist_selector_uses_hist_table_short_ |
| AC-C1 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_pod_values_differ_b |
| AC-C2 | True | None | count=15000 want=15000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_dead_pod_absent_fro |
| AC-W1 | True | None | 180d http=200 365d http=200 unit=True |
| AC-W2 | True | None | 30d accept |
| AC-W3 | True | 437.86962000012863 | series=10 want=10 |
| AC-W4 | True | 1054.6227419981733 | http=400 ms=1055 |
| AC-W5 | False | 267.91606999904616 | points=721 |
| AC-W6 | True | 7.006772997556254 | tall_days_loaded=30 |
| AC-N1 | True | None | cargo: test config::tests::default_max_snapshot_age_seconds_is_one_hour ... ok |
| AC-N2 | True | None | cargo: test compaction::executor::tests::expire_snapshots_sql_honors_seconds ... |
| AC-N3 | False | None | snaps=649 old_gt_A+I=0 bar=81 expire_http=200 commits=120 cols=[] |
| AC-N4 | False | None | samples_before_snap=202380 pre_expire=202561 after=202562 |
| AC-N5 | True | None | cargo: test compaction::executor::tests::cleanup_old_files_sql_honors_seconds .. |
| AC-F1 | False | None | F-files days missing |
| AC-F2 | False | None | F-files days missing |
| AC-F3 | True | None | cargo: test compaction::twcs::tests::twcs_partition_key_is_record_date_only ...  |
| AC-F4 | False | None | F-files days missing |
| AC-F5 | False | None | F-files days missing |
| AC-F6 | True | None | cargo: test compaction::twcs::tests::twcs_merge_does_not_cross_record_date ... o |
| AC-S1 | True | None | cargo: test storage::ducklake::metrics_layout_write::tests::skinny_samples_small |
| AC-S2 | True | None | raw_before=202368 raw_after_downsample=202368 |
| AC-S3 | True | None | grafana-manual-up.sh must build release binary |
| AC-M1 | True | None | cargo: test compaction::executor::tests::maintenance_tables_include_metric_famil |
| AC-M2 | True | None | first={'metric_samples': 202368, 'metric_samples_5m': 167683, 'metric_samples_1h |
| AC-G0 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G1 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G2 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G3 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G4 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G5 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G6 | False | None | COMPARE_GREPTIME=0 — G9 not run |

## Blockers
- pr_floor skipped F-collapse-90d full load (wall-clock); AC-W3 expected fail
- pr_floor deferred F-files load (wall-clock); AC-F* expected fail
