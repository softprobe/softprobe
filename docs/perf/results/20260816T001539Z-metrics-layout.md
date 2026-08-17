# metrics-layout 20260816T001539Z

- binary_profile: `release`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `1`
- pass/fail: **46/49** pass, **3** fail
- fixture_hash: `daebb913e835e3f7`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_hist_samples:part=1,sort=1; metric_samples:part=1,sort=1; metric_postings |
| AC-D3 | True | None | cargo: test compat::backends::postings_resolve::tests::prom_backend_is_ducklake_ |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=86 after=87 sender_alive=True |
| AC-Q1 | True | 2.768467995338142 | repeats=20 codes={200} series=1 |
| AC-Q2 | True | 1.867487997515127 | series=1 points=721 |
| AC-Q3 | True | 616.5565089904703 | series=1 |
| AC-Q4 | True | 0.7828890084056184 | http=400 ms=1 body_snip={"error": "limit_exceeded: series count 15000 exceeds ma |
| AC-Q5 | True | 4.455265996512026 | series=10 want_J=10 |
| AC-Q6 | True | 6.705003004753962 | repeats=20 |
| AC-Q7 | True | None | cargo: test compat::backends::postings_resolve::tests::resolve_and_samples_sql_u |
| AC-Q8 | True | 595.1086660061264 | all 15 exprs ok |
| AC-Q9 | True | 4.008456991869025 | p95=4.008456991869025 codes={200} maint=metric_samples:set=200:ok ms=5;metric_sa |
| AC-H1 | True | 297.2597790067084 | hist_rows=2400 sample_rows=0 series=10 prom=layout_latency_count prom_series=10  |
| AC-H2 | True | None | cargo: test compat::backends::grain::tests::hist_selector_uses_hist_table_short_ |
| AC-C1 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_pod_values_differ_b |
| AC-C2 | True | None | count=15000 want=15000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | True | None | cargo: test compat::backends::postings_resolve::tests::churn_dead_pod_absent_fro |
| AC-W1 | True | None | 180d http=200 365d http=200 unit=True |
| AC-W2 | True | None | 30d accept |
| AC-W3 | True | 71.64437399478629 | series=10 want=10 |
| AC-W4 | True | 1007.2852309967857 | http=400 ms=1007 |
| AC-W5 | True | 233.89010700338986 | points=721 want>=672 |
| AC-W6 | True | 0.793598999734968 | tall_days_loaded=30 |
| AC-N1 | True | None | cargo: test config::tests::default_max_snapshot_age_seconds_is_one_hour ... ok |
| AC-N2 | True | None | cargo: test compaction::executor::tests::expire_snapshots_sql_honors_seconds ... |
| AC-N3 | True | None | snaps=56 before=121 old_gt_A+I=0 bar=80 expire_http=200/200 commits=120 expired_ |
| AC-N4 | True | None | samples_before_snap=85 pre_expire=205 after=205 |
| AC-N5 | True | None | cargo: test compaction::executor::tests::cleanup_old_files_sql_honors_seconds .. |
| AC-F1 | True | None | closed_files=2 limit=4 stats=[('2026-08-14', 1, 10131325), ('2026-08-15', 1, 987 |
| AC-F2 | True | None | bytes_before=21096512 median=9872496.0 sizes=[9872496, 10131325] pre=True |
| AC-F3 | True | None | cargo: test compaction::twcs::tests::twcs_partition_key_is_record_date_only ...  |
| AC-F4 | True | None | today=2026-08-16 files=2 stats=[('2026-08-16', 2, 72873)] |
| AC-F5 | True | None | precondition_met=True; metric_postings:files=2/4; metric_series:files=2/4; metri |
| AC-F6 | True | None | cargo: test compaction::twcs::tests::twcs_merge_does_not_cross_record_date ... o |
| AC-S1 | True | None | cargo: test storage::ducklake::metrics_layout_write::tests::skinny_samples_small |
| AC-S2 | True | None | raw_before=2185164 raw_after_downsample=2185164 |
| AC-S3 | True | None | grafana-manual-up.sh must build release binary |
| AC-M1 | True | None | cargo: test compaction::executor::tests::maintenance_tables_include_metric_famil |
| AC-M2 | False | None | first={'metric_samples': 2185164, 'metric_samples_5m': 175690, 'metric_samples_1 |
| AC-G0 | True | None | sha=a8924bb95c43b562e4312af309d2a5e80c103185 hb_before=3 hb_after=5 hb_alive=Tru |
| AC-G1 | True | 2.768467995338142 | T-Q1 layout_tall 30m; greptime_codes={200} |
| AC-G2 | True | 1.867487997515127 | T-Q2 layout_tall 30d; greptime_codes={200} |
| AC-G3 | False | 616.5565089904703 | AC-G3 ratio 34.23 > R=10.0 (soft=616.6ms greptime=18.0ms) — escalate per §4.4 |
| AC-G4 | False | 6.705003004753962 | AC-G4 ratio 12.76 > R=10.0 (soft=6.7ms greptime=0.5ms) — escalate per §4.4 |
| AC-G5 | True | 4.455265996512026 | T-Q5 collapse 30d; greptime_codes={200} |
| AC-G6 | True | None | ingest_path=/v1/otlp/v1/metrics (must be OTLP /v1/otlp/v1/metrics, not remote_wr |

## Blockers
- pr_floor skipped F-collapse-90d full load (wall-clock); AC-W3 expected fail
- AC-G3 ratio 34.23 > R=10.0 (soft=616.6ms greptime=18.0ms) — escalate per §4.4
- AC-G4 ratio 12.76 > R=10.0 (soft=6.7ms greptime=0.5ms) — escalate per §4.4
