# metrics-layout 20260815T063834Z

- binary_profile: `dev`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `1`
- pass/fail: **5/49** pass, **44** fail
- fixture_hash: `d0ef4d6ef8093efc`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | False | None | tables=[] http=200 |
| AC-D2 | False | None | metric_samples:part=0,sort=0; metric_series:part=0,sort=0; metric_postings:part= |
| AC-D3 | False | None | cargo: filter=prom_backend_is_ducklake_no_sidecar_writers not found in cargo out |
| AC-D4 | False | None | union_http=500 rows=None committed_http=500 |
| AC-Q0 | False | None | hb before=0 after=0 sender_alive=True |
| AC-Q1 | True | 9.559000998706324 | repeats=20 codes={200} |
| AC-Q2 | False | 8.183062000171049 | series=0 points=0 |
| AC-Q3 | False | 7.3182439991796855 | series=0 |
| AC-Q4 | False | 5.810734000988305 | http=200 ms=6 body_snip={"data": {"result": [], "resulttype": "matrix"}, "status |
| AC-Q5 | False | 7.036104001599597 | series=0 want_J=10 |
| AC-Q6 | True | 5.2523650010698475 | repeats=20 |
| AC-Q7 | False | None | cargo: filter=resolve_and_samples_sql_uses_postings_not_fat not found in cargo o |
| AC-Q8 | True | 26.03704500143067 | all 15 exprs ok |
| AC-Q9 | False | None | maintenance-under-load measure not fully automated in harness yet |
| AC-H1 | False | 5.662334999215091 | hist_rows=0 sample_rows=0 |
| AC-H2 | False | None | cargo: filter=hist_selector_uses_hist_table_short_range not found in cargo outpu |
| AC-C1 | False | None | cargo: filter=churn_pod_values_differ_by_record_date not found in cargo output |
| AC-C2 | False | None | count=0 want=15000 |
| AC-C3 | False | None | tall query_range series isolation |
| AC-C4 | False | None | cargo: filter=churn_dead_pod_absent_from_today_postings not found in cargo outpu |
| AC-W1 | False | None | 180d http=200 365d http=200 unit=False |
| AC-W2 | True | None | 30d accept |
| AC-W3 | False | 9.20161100293626 | series=0 want=10 |
| AC-W4 | False | 6.242094001208898 | http=200 ms=6 |
| AC-W5 | False | 7.466062997991685 | points=0 |
| AC-W6 | True | 3.216146997147007 | tall_days_loaded=30 |
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
| AC-S2 | False | None | downsample trigger not automated; raw_count=-1 |
| AC-S3 | False | None | grafana-manual-up.sh must build release binary |
| AC-M1 | False | None | cargo: filter=maintenance_tables_include_metric_family not found in cargo output |
| AC-M2 | False | None | second maintenance watermark pass not automated |
| AC-G0 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G1 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G2 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G3 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G4 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G5 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G6 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |

## Blockers
- F-tall ingest HTTP 400 at batch 0
- F-wide ingest HTTP 400 at 0
- Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb95c43b562e4312af309d2a5e80c103185. Set GREPTIME_BIN or GREPTIME_URL to enable G9.
