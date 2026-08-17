# metrics-layout 20260815T192424Z

- binary_profile: `dev`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `0`
- pass/fail: **39/49** pass, **10** fail
- fixture_hash: `352fd3c281a79108`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_samples:part=1,sort=409; metric_series:part=1,sort=409; metric_hist_sampl |
| AC-D3 | True | None | cargo: test compat::backends::postings_resolve::tests::prom_backend_is_ducklake_ |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=33 after=34 sender_alive=True |
| AC-Q1 | True | 6.969643000047654 | repeats=20 codes={200} series=1 |
| AC-Q2 | True | 11.550628987606615 | series=1 points=721 |
| AC-Q3 | True | 478.3860550087411 | series=1 |
| AC-Q4 | True | 8.69355199392885 | http=400 ms=9 body_snip={"error": "limit_exceeded: series count 15000 exceeds ma |
| AC-Q5 | True | 32.775508996564895 | series=10 want_J=10 |
| AC-Q6 | True | 4.210845989291556 | repeats=20 |
| AC-Q7 | True | None | cargo: test compat::backends::postings_resolve::tests::resolve_and_samples_sql_u |
| AC-Q8 | True | 939.0632470021956 | all 15 exprs ok |
| AC-Q9 | True | 20.134070990025066 | p95=20.134070990025066 codes={200} maint=metric_samples:set=200:ok ms=66;metric_ |
| AC-H1 | True | 459.4451430020854 | hist_rows=1200 sample_rows=0 |
| AC-H2 | True | None | cargo: test compat::backends::grain::tests::hist_selector_uses_hist_table_short_ |
| AC-C1 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_pod_values_differ_b |
| AC-C2 | True | None | count=15000 want=15000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_dead_pod_absent_fro |
| AC-W1 | True | None | 180d http=200 365d http=200 unit=True |
| AC-W2 | True | None | 30d accept |
| AC-W3 | True | 386.9140549941221 | series=10 want=10 |
| AC-W4 | True | 1467.6727240002947 | http=400 ms=1468 |
| AC-W5 | False | 1785.8044750028057 | points=721 |
| AC-W6 | True | 6.5540539944777265 | tall_days_loaded=30 |
| AC-N1 | True | None | cargo: test config::tests::default_max_snapshot_age_seconds_is_one_hour ... ok |
| AC-N2 | True | None | cargo: test compaction::executor::tests::expire_snapshots_sql_honors_seconds ... |
| AC-N3 | False | None | snaps=452 before=4395 old_gt_A+I=0 bar=81 expire_http=200/200 commits=120 cols=[ |
| AC-N4 | True | None | samples_before_snap=1785010 pre_expire=1785130 after=1785130 |
| AC-N5 | True | None | cargo: test compaction::executor::tests::cleanup_old_files_sql_honors_seconds .. |
| AC-F1 | True | None | closed_files=2 limit=4 stats=[('2026-08-13', 1, 8093849), ('2026-08-14', 1, 7816 |
| AC-F2 | False | None | bytes_before=16864584 median=7816954.0 sizes=[8093849, 7816954] pre=True |
| AC-F3 | True | None | cargo: test compaction::twcs::tests::twcs_partition_key_is_record_date_only ...  |
| AC-F4 | True | None | today=2026-08-15 files=2 stats=[('2026-08-15', 2, 69166)] |
| AC-F5 | True | None | precondition_met=True; metric_postings:files=2/4; metric_series:files=2/4; metri |
| AC-F6 | True | None | cargo: test compaction::twcs::tests::twcs_merge_does_not_cross_record_date ... o |
| AC-S1 | True | None | cargo: test storage::ducklake::metrics_layout_write::tests::skinny_samples_small |
| AC-S2 | True | None | raw_before=1784992 raw_after_downsample=1784992 |
| AC-S3 | True | None | grafana-manual-up.sh must build release binary |
| AC-M1 | True | None | cargo: test compaction::executor::tests::maintenance_tables_include_metric_famil |
| AC-M2 | True | None | first={'metric_samples': 1784992, 'metric_samples_5m': 174608, 'metric_samples_1 |
| AC-G0 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G1 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G2 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G3 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G4 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G5 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G6 | False | None | COMPARE_GREPTIME=0 — G9 not run |

## Blockers
- pr_floor skipped F-collapse-90d full load (wall-clock); AC-W3 expected fail
