# metrics-layout 20260816T084205Z

- binary_profile: `release`
- fixture_profile: `release_full`
- COMPARE_GREPTIME: `1`
- pass/fail: **49/49** pass, **0** fail
- fixture_hash: `2b83e7844e120ec8`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_samples:part=1,sort=1; metric_postings:part=1,sort=1; metric_series:part= |
| AC-D3 | True | None | cargo: test compat::backends::postings_resolve::tests::prom_backend_is_ducklake_ |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=129 after=131 sender_alive=True |
| AC-Q1 | True | 0.7733890088275075 | repeats=20 codes={200} series=1 |
| AC-Q2 | True | 0.9229999996023253 | series=1 points=721 |
| AC-Q3 | True | 4.847546006203629 | series=1 |
| AC-Q4 | True | 521.2134800094645 | http=400 ms=521 body_snip={"error": "limit_exceeded: series count 100000 exceeds |
| AC-Q5 | True | 24.007197993341833 | series=50 want_J=50 |
| AC-Q6 | True | 2.7786470018327236 | repeats=20 |
| AC-Q7 | True | None | cargo: test compat::backends::postings_resolve::tests::resolve_and_samples_sql_u |
| AC-Q8 | True | 941.5078220044961 | all 15 exprs ok |
| AC-Q9 | True | 13.432118008495308 | p95=13.432118008495308 codes={200} maint=metric_samples:set=200:ok ms=11;metric_ |
| AC-H1 | True | 589.0526580042206 | hist_rows=2400 sample_rows=0 series=10 prom=layout_latency_count prom_series=10  |
| AC-H2 | True | None | cargo: test compat::backends::grain::tests::hist_selector_uses_hist_table_short_ |
| AC-C1 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_pod_values_differ_b |
| AC-C2 | True | None | count=100000 want=100000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_dead_pod_absent_fro |
| AC-W1 | True | None | 180d http=200 365d http=200 unit=True |
| AC-W2 | True | None | 30d accept |
| AC-W3 | True | 396.9128219905542 | series=50 want=50 |
| AC-W4 | True | 101.00346899707802 | http=400 ms=101 body_snip={"error": "limit_exceeded: series count 10001 exceeds  |
| AC-W5 | True | 397.22229199833237 | points=2161 want>=1800 |
| AC-W6 | True | 2.942256993264891 | tall_days_loaded=180 |
| AC-N1 | True | None | cargo: test config::tests::default_max_snapshot_age_seconds_is_one_hour ... ok |
| AC-N2 | True | None | cargo: test compaction::executor::tests::expire_snapshots_sql_honors_seconds ... |
| AC-N3 | True | None | snaps=56 before=121 old_gt_A+I=0 bar=80 expire_http=200/200 commits=120 expired_ |
| AC-N4 | True | None | samples_before_snap=129 pre_expire=249 after=249 |
| AC-N5 | True | None | cargo: test compaction::executor::tests::cleanup_old_files_sql_honors_seconds .. |
| AC-F1 | True | None | closed_files=2 limit=4 stats=[('2026-08-14', 1, 10131805), ('2026-08-15', 1, 987 |
| AC-F2 | True | None | bytes_before=21096512 median=9872537.0 sizes=[9872537, 10131805] pre=True |
| AC-F3 | True | None | cargo: test compaction::twcs::tests::twcs_partition_key_is_record_date_only ...  |
| AC-F4 | True | None | today=2026-08-16 files=4 stats=[('2026-08-16', 4, 74941)] |
| AC-F5 | True | None | precondition_met=True; metric_postings:files=2/4; metric_series:files=2/4; metri |
| AC-F6 | True | None | cargo: test compaction::twcs::tests::twcs_merge_does_not_cross_record_date ... o |
| AC-S1 | True | None | cargo: test storage::ducklake::metrics_layout_write::tests::skinny_samples_small |
| AC-S2 | True | None | raw_before=3227808 raw_after_downsample=3227808 |
| AC-S3 | True | None | grafana-manual-up.sh must build release binary |
| AC-M1 | True | None | cargo: test compaction::executor::tests::maintenance_tables_include_metric_famil |
| AC-M2 | True | None | first={'metric_samples': 3227824, 'metric_samples_5m': 268145, 'metric_samples_1 |
| AC-G0 | True | None | sha=a8924bb95c43b562e4312af309d2a5e80c103185 hb_before=8 hb_after=12 hb_alive=Tr |
| AC-G1 | True | 0.7733890088275075 | T-Q1 layout_tall 30m; greptime_codes={200} |
| AC-G2 | True | 0.9229999996023253 | T-Q2 layout_tall 30d; greptime_codes={200} |
| AC-G3 | True | 4.847546006203629 | T-Q3 wide resolve; greptime_codes={200} |
| AC-G4 | True | 2.7786470018327236 | T-Q6 __name__/values; greptime_names=30 greptime_codes={200} |
| AC-G5 | True | 24.007197993341833 | T-Q5 collapse 30d; greptime_codes={200} |
| AC-G6 | True | None | ingest_path=/v1/otlp/v1/metrics (must be OTLP /v1/otlp/v1/metrics, not remote_wr |
