# metrics-layout 20260818T045403Z

- binary_profile: `release`
- fixture_profile: `release_full`
- COMPARE_GREPTIME: `1`
- pass/fail: **56/56** pass, **0** fail
- fixture_hash: `2bbaeda33dfb0ac2`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_postings:part=1,sort=1; metric_samples:part=1,sort=1; metric_hist_samples |
| AC-D3 | True | None | cargo: test compat::backends::postings_resolve::tests::prom_backend_is_ducklake_ |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=279 after=280 sender_alive=True |
| AC-Q1 | True | 6.311474018730223 | repeats=20 codes={200} series=1 |
| AC-Q2 | True | 8.051702985540032 | series=1 points=721 |
| AC-Q3 | True | 12.033428996801376 | series=1 |
| AC-Q4 | True | 1232.0670639746822 | http=400 ms=1232 body_snip={"error": "limit_exceeded: series count 100000 exceed |
| AC-Q5 | True | 71.21374498819932 | series=50 want_J=50 |
| AC-Q6 | True | 6.350974028464407 | repeats=20 |
| AC-Q7 | True | None | cargo: test compat::backends::postings_resolve::tests::resolve_and_samples_sql_u |
| AC-Q8 | True | 2900.591571989935 | all 15 exprs ok |
| AC-Q9 | True | 1455.4655519896187 | p95=1455.4655519896187 codes={200} maint=metric_samples:set=200:ok ms=67;metric_ |
| AC-H1 | True | 1699.6484190458432 | hist_rows=2400 sample_rows=0 series=10 prom=layout_latency_count prom_series=10  |
| AC-H2 | True | None | cargo: test compat::backends::grain::tests::hist_selector_always_uses_hist_table |
| AC-H3 | True | 1077.8457370470278 | range_s=10800 series=10 points=900 hist_rows=2400 p95=1077.8457370470278 |
| AC-H4 | True | 1175.45910802437 | range_s=86400 series=10 points=300 hist_rows=2400 p95=1175.45910802437 |
| AC-H5 | True | None | cargo: test compat::backends::postings_resolve::tests::hist_prom_sql_uses_hist_t |
| AC-H6 | True | None | cargo: test compat::backends::grain::tests::window_series_type_grain_matrix ...  |
| AC-C1 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_pod_values_differ_b |
| AC-C2 | True | None | count=100000 want=100000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_dead_pod_absent_fro |
| AC-W1 | True | None | 180d http=200 365d http=200 unit=True |
| AC-W2 | True | None | 30d accept |
| AC-W3 | True | 1083.7961309589446 | series=50 want=50 |
| AC-W4 | True | 158.4618459455669 | http=400 ms=158 body_snip={"error": "limit_exceeded: series count 10001 exceeds  |
| AC-W5 | True | 1415.9250890370458 | points=2161 want>=1800 |
| AC-W6 | True | 12.201278994325548 | tall_days_loaded=180 |
| AC-N1 | True | None | cargo: test config::tests::default_max_snapshot_age_seconds_is_one_minute ... ok |
| AC-N2 | True | None | cargo: test compaction::executor::tests::expire_snapshots_sql_honors_seconds ... |
| AC-N3 | True | None | snaps=56 before=121 old_gt_A+I=0 bar=80 expire_http=200/200 commits=120 expired_ |
| AC-N4 | True | None | samples_before_snap=280 pre_expire=400 after=400 |
| AC-N5 | True | None | cargo: test compaction::executor::tests::cleanup_old_files_sql_honors_seconds .. |
| AC-N6 | True | None | snaps=32 older=0 expire=200:ok ms=427 |
| AC-F1 | True | None | closed_files=2 limit=4 stats=[('2026-08-16', 1, 10131950), ('2026-08-17', 1, 987 |
| AC-F2 | True | None | bytes_before=21095942 median=9872418.0 sizes=[9872418, 10131950] pre=True |
| AC-F3 | True | None | cargo: test compaction::twcs::tests::twcs_partition_key_is_record_date_only ...  |
| AC-F4 | True | None | today=2026-08-18 files=6 stats=[('2026-08-18', 6, 78933)] |
| AC-F5 | True | None | precondition_met=True; metric_postings:files=2/4; metric_series:files=2/4; metri |
| AC-F6 | True | None | cargo: test compaction::twcs::tests::twcs_merge_does_not_cross_record_date ... o |
| AC-F7 | True | None | unit_inlining_default_0=True; metric_samples:rows=3227985 parquet_files=190; met |
| AC-F8 | True | None | metric_samples@2026-08-16:files=1 bytes=10131950 median=10131950.0 bar=True; met |
| AC-S1 | True | None | cargo: test storage::ducklake::metrics_layout_write::tests::skinny_samples_small |
| AC-S2 | True | None | raw_before=3227959 raw_after_downsample=3227959 |
| AC-S3 | True | None | grafana-manual-up.sh must build release binary |
| AC-M1 | True | None | cargo: test compaction::executor::tests::maintenance_tables_include_metric_famil |
| AC-M2 | True | None | first={'metric_samples': 3227978, 'metric_samples_5m': 268125, 'metric_samples_1 |
| AC-G0 | True | None | sha=a8924bb95c43b562e4312af309d2a5e80c103185 hb_before=13 hb_after=23 hb_alive=T |
| AC-G1 | True | 6.311474018730223 | T-Q1 layout_tall 30m; greptime_codes={200} |
| AC-G2 | True | 8.051702985540032 | T-Q2 layout_tall 30d; greptime_codes={200} |
| AC-G3 | True | 12.033428996801376 | T-Q3 wide resolve; greptime_codes={200} |
| AC-G4 | True | 6.350974028464407 | T-Q6 __name__/values; greptime_names=30 greptime_codes={200} |
| AC-G5 | True | 71.21374498819932 | T-Q5 collapse 30d; greptime_codes={200} |
| AC-G6 | True | None | ingest_path=/v1/otlp/v1/metrics (must be OTLP /v1/otlp/v1/metrics, not remote_wr |
