# metrics-layout 20260815T171957Z

- binary_profile: `dev`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `0`
- pass/fail: **15/49** pass, **34** fail
- fixture_hash: `bb5b49d27d203084`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_postings:part=1,sort=254; metric_hist_samples:part=1,sort=254; metric_ser |
| AC-D3 | False | None | cargo: filter=prom_backend_is_ducklake_no_sidecar_writers not found in cargo out |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=2 after=3 sender_alive=True |
| AC-Q1 | True | 8.376851998036727 | repeats=20 codes={200} series=1 |
| AC-Q2 | False | 463.500985999417 | series=0 points=0 |
| AC-Q3 | True | 322.6486109997495 | series=1 |
| AC-Q4 | True | 114.60832000011578 | http=400 ms=115 body_snip={"error": "limit_exceeded: series count 10001 exceeds  |
| AC-Q5 | False | 153.75629300251603 | series=0 want_J=10 |
| AC-Q6 | True | 3.629345999797806 | repeats=20 |
| AC-Q7 | False | None | cargo: filter=resolve_and_samples_sql_uses_postings_not_fat not found in cargo o |
| AC-Q8 | True | 642.857834995084 | all 15 exprs ok |
| AC-Q9 | False | None | maintenance-under-load measure not fully automated in harness yet |
| AC-H1 | True | 495.1348360045813 | hist_rows=1200 sample_rows=0 |
| AC-H2 | False | None | cargo: filter=hist_selector_uses_hist_table_short_range not found in cargo outpu |
| AC-C1 | False | None | cargo: filter=churn_pod_values_differ_by_record_date not found in cargo output |
| AC-C2 | True | None | count=15000 want=15000 |
| AC-C3 | True | None | tall query_range series isolation |
| AC-C4 | False | None | cargo: filter=churn_dead_pod_absent_from_today_postings not found in cargo outpu |
| AC-W1 | False | None | 180d http=200 365d http=200 unit=False |
| AC-W2 | True | None | 30d accept |
| AC-W3 | False | 211.8594870044035 | series=0 want=10 |
| AC-W4 | True | 135.31956000224454 | http=400 ms=135 |
| AC-W5 | False | 359.6722160000354 | points=0 |
| AC-W6 | True | 1.6334589963662438 | tall_days_loaded=30 |
| AC-N1 | False | None | cargo: filter=default_max_snapshot_age_seconds_is_one_hour not found in cargo ou |
| AC-N2 | False | None | cargo: filter=expire_snapshots_sql_honors_seconds not found in cargo output |
| AC-N3 | False | None | F-snap (120s commits) not run in this harness iteration |
| AC-N4 | False | None | depends on F-snap / expiry pass |
| AC-N5 | False | None | cargo: filter=cleanup_old_files_sql_honors_seconds not found in cargo output |
| AC-F1 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F2 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F3 | False | None | cargo: filter=twcs_partition_key_is_record_date_only not found in cargo output |
| AC-F4 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F5 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F6 | False | None | cargo: filter=twcs_merge_does_not_cross_record_date not found in cargo output |
| AC-S1 | False | None | cargo: filter=skinny_samples_smaller_than_fat_and_no_variant not found in cargo  |
| AC-S2 | False | None | downsample trigger not automated; raw_count=202368 |
| AC-S3 | False | None | grafana-manual-up.sh must build release binary |
| AC-M1 | False | None | cargo: filter=maintenance_tables_include_metric_family not found in cargo output |
| AC-M2 | False | None | second maintenance watermark pass not automated |
| AC-G0 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G1 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G2 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G3 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G4 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G5 | False | None | COMPARE_GREPTIME=0 — G9 not run |
| AC-G6 | False | None | COMPARE_GREPTIME=0 — G9 not run |

## Blockers
- pr_floor skipped F-collapse-90d full load (wall-clock); AC-W3 expected fail
