# metrics-layout 20260816T004506Z

- binary_profile: `release`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `1`
- pass/fail: **49/49** pass, **0** fail
- fixture_hash: `2a6d0e0d4ff6bf0d`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_postings:part=1,sort=1; metric_hist_samples:part=1,sort=1; metric_series: |
| AC-D3 | True | None | cargo: test compat::backends::postings_resolve::tests::prom_backend_is_ducklake_ |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=181 after=183 sender_alive=True |
| AC-Q1 | True | 1.3907990069128573 | repeats=20 codes={200} series=1 |
| AC-Q2 | True | 2.2959280031500384 | series=1 points=721 |
| AC-Q3 | True | 0.7189900061348453 | series=1 |
| AC-Q4 | True | 0.6842490111012012 | http=400 ms=1 body_snip={"error": "limit_exceeded: series count 15000 exceeds ma |
| AC-Q5 | True | 10.66185999661684 | series=10 want_J=10 |
| AC-Q6 | True | 0.8594590035500005 | repeats=20 |
| AC-Q7 | True | None | cargo: test compat::backends::postings_resolve::tests::resolve_and_samples_sql_u |
| AC-Q8 | True | 582.944451001822 | all 15 exprs ok |
| AC-Q9 | True | 1.9826790085062385 | p95=1.9826790085062385 codes={200} maint=metric_samples:set=200:ok ms=7;metric_s |
| AC-H1 | True | 309.9319890025072 | hist_rows=2400 sample_rows=0 series=10 prom=layout_latency_count prom_series=10  |
| AC-H2 | True | None | cargo: test compat::backends::grain::tests::hist_selector_uses_hist_table_short_ |
| AC-C1 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_pod_values_differ_b |
| AC-C2 | True | None | count=15000 want=15000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_dead_pod_absent_fro |
| AC-W1 | True | None | 180d http=200 365d http=200 unit=True |
| AC-W2 | True | None | 30d accept |
| AC-W3 | True | 86.09838200209197 | series=10 want=10 |
| AC-W4 | True | 943.9702239906183 | http=400 ms=944 |
| AC-W5 | True | 350.58354200737085 | points=721 want>=672 |
| AC-W6 | True | 0.937868986511603 | tall_days_loaded=30 |
| AC-N1 | True | None | cargo: test config::tests::default_max_snapshot_age_seconds_is_one_hour ... ok |
| AC-N2 | True | None | cargo: test compaction::executor::tests::expire_snapshots_sql_honors_seconds ... |
| AC-N3 | True | None | snaps=56 before=121 old_gt_A+I=0 bar=80 expire_http=200/200 commits=120 expired_ |
| AC-N4 | True | None | samples_before_snap=181 pre_expire=301 after=301 |
| AC-N5 | True | None | cargo: test compaction::executor::tests::cleanup_old_files_sql_honors_seconds .. |
| AC-F1 | True | None | closed_files=2 limit=4 stats=[('2026-08-14', 1, 10131305), ('2026-08-15', 1, 987 |
| AC-F2 | True | None | bytes_before=21096512 median=9872499.0 sizes=[9872499, 10131305] pre=True |
| AC-F3 | True | None | cargo: test compaction::twcs::tests::twcs_partition_key_is_record_date_only ...  |
| AC-F4 | True | None | today=2026-08-16 files=2 stats=[('2026-08-16', 2, 74650)] |
| AC-F5 | True | None | precondition_met=True; metric_postings:files=2/4; metric_series:files=2/4; metri |
| AC-F6 | True | None | cargo: test compaction::twcs::tests::twcs_merge_does_not_cross_record_date ... o |
| AC-S1 | True | None | cargo: test storage::ducklake::metrics_layout_write::tests::skinny_samples_small |
| AC-S2 | True | None | raw_before=2185260 raw_after_downsample=2185260 |
| AC-S3 | True | None | grafana-manual-up.sh must build release binary |
| AC-M1 | True | None | cargo: test compaction::executor::tests::maintenance_tables_include_metric_famil |
| AC-M2 | True | None | first={'metric_samples': 2185260, 'metric_samples_5m': 175756, 'metric_samples_1 |
| AC-G0 | True | None | sha=a8924bb95c43b562e4312af309d2a5e80c103185 hb_before=3 hb_after=5 hb_alive=Tru |
| AC-G1 | True | 1.3907990069128573 | T-Q1 layout_tall 30m; greptime_codes={200} |
| AC-G2 | True | 2.2959280031500384 | T-Q2 layout_tall 30d; greptime_codes={200} |
| AC-G3 | True | 0.7189900061348453 | T-Q3 wide resolve; greptime_codes={200} |
| AC-G4 | True | 0.8594590035500005 | T-Q6 __name__/values; greptime_names=30 greptime_codes={200} |
| AC-G5 | True | 10.66185999661684 | T-Q5 collapse 30d; greptime_codes={200} |
| AC-G6 | True | None | ingest_path=/v1/otlp/v1/metrics (must be OTLP /v1/otlp/v1/metrics, not remote_wr |

## Blockers
- pr_floor skipped F-collapse-90d full load (wall-clock); AC-W3 expected fail
