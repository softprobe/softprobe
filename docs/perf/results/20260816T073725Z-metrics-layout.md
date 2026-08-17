# metrics-layout 20260816T073725Z

- binary_profile: `release`
- fixture_profile: `release_full`
- COMPARE_GREPTIME: `1`
- pass/fail: **47/49** pass, **2** fail
- fixture_hash: `2b83e7844e120ec8`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_series:part=1,sort=1; metric_samples:part=1,sort=1; metric_hist_samples:p |
| AC-D3 | True | None | cargo: test compat::backends::postings_resolve::tests::prom_backend_is_ducklake_ |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=179 after=180 sender_alive=True |
| AC-Q1 | True | 2.6290980022167787 | repeats=20 codes={200} series=1 |
| AC-Q2 | True | 4.37218599836342 | series=1 points=721 |
| AC-Q3 | True | 3.0567270005121827 | series=1 |
| AC-Q4 | True | 887.3131020081928 | http=400 ms=887 body_snip={"error": "limit_exceeded: series count 100000 exceeds |
| AC-Q5 | True | 77.72671899874695 | series=50 want_J=50 |
| AC-Q6 | True | 5.8328039885964245 | repeats=20 |
| AC-Q7 | True | None | cargo: test compat::backends::postings_resolve::tests::resolve_and_samples_sql_u |
| AC-Q8 | True | 1988.7065549992258 | all 15 exprs ok |
| AC-Q9 | True | 4.790254999534227 | p95=4.790254999534227 codes={200} maint=metric_samples:set=200:ok ms=14;metric_s |
| AC-H1 | True | 810.95844200172 | hist_rows=2400 sample_rows=0 series=10 prom=layout_latency_count prom_series=10  |
| AC-H2 | True | None | cargo: test compat::backends::grain::tests::hist_selector_uses_hist_table_short_ |
| AC-C1 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_pod_values_differ_b |
| AC-C2 | True | None | count=100000 want=100000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_dead_pod_absent_fro |
| AC-W1 | True | None | 180d http=200 365d http=200 unit=True |
| AC-W2 | True | None | 30d accept |
| AC-W3 | False | 490.9648670000024 | series=0 want=50 |
| AC-W4 | False | 3276.9729669962544 | http=200 ms=3277 |
| AC-W5 | True | 612.9472850007005 | points=2161 want>=1800 |
| AC-W6 | True | 13.438787995255552 | tall_days_loaded=180 |
| AC-N1 | True | None | cargo: test config::tests::default_max_snapshot_age_seconds_is_one_hour ... ok |
| AC-N2 | True | None | cargo: test compaction::executor::tests::expire_snapshots_sql_honors_seconds ... |
| AC-N3 | True | None | snaps=56 before=121 old_gt_A+I=0 bar=80 expire_http=200/200 commits=120 expired_ |
| AC-N4 | True | None | samples_before_snap=179 pre_expire=299 after=299 |
| AC-N5 | True | None | cargo: test compaction::executor::tests::cleanup_old_files_sql_honors_seconds .. |
| AC-F1 | True | None | closed_files=2 limit=4 stats=[('2026-08-14', 1, 10131826), ('2026-08-15', 1, 987 |
| AC-F2 | True | None | bytes_before=21096512 median=9872532.0 sizes=[10131826, 9872532] pre=True |
| AC-F3 | True | None | cargo: test compaction::twcs::tests::twcs_partition_key_is_record_date_only ...  |
| AC-F4 | True | None | today=2026-08-16 files=5 stats=[('2026-08-16', 5, 76710)] |
| AC-F5 | True | None | precondition_met=True; metric_postings:files=2/4; metric_series:files=2/4; metri |
| AC-F6 | True | None | cargo: test compaction::twcs::tests::twcs_merge_does_not_cross_record_date ... o |
| AC-S1 | True | None | cargo: test storage::ducklake::metrics_layout_write::tests::skinny_samples_small |
| AC-S2 | True | None | raw_before=3227858 raw_after_downsample=3227858 |
| AC-S3 | True | None | grafana-manual-up.sh must build release binary |
| AC-M1 | True | None | cargo: test compaction::executor::tests::maintenance_tables_include_metric_famil |
| AC-M2 | True | None | first={'metric_samples': 3227887, 'metric_samples_5m': 268127, 'metric_samples_1 |
| AC-G0 | True | None | sha=a8924bb95c43b562e4312af309d2a5e80c103185 hb_before=7 hb_after=10 hb_alive=Tr |
| AC-G1 | True | 2.6290980022167787 | T-Q1 layout_tall 30m; greptime_codes={200} |
| AC-G2 | True | 4.37218599836342 | T-Q2 layout_tall 30d; greptime_codes={200} |
| AC-G3 | True | 3.0567270005121827 | T-Q3 wide resolve; greptime_codes={200} |
| AC-G4 | True | 5.8328039885964245 | T-Q6 __name__/values; greptime_names=30 greptime_codes={200} |
| AC-G5 | True | 77.72671899874695 | T-Q5 collapse 30d; greptime_codes={200} |
| AC-G6 | True | None | ingest_path=/v1/otlp/v1/metrics (must be OTLP /v1/otlp/v1/metrics, not remote_wr |
