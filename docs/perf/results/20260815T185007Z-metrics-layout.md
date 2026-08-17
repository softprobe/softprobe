# metrics-layout 20260815T185007Z

- binary_profile: `release`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `1`
- pass/fail: **18/49** pass, **31** fail
- fixture_hash: `6dba1fb666d94a2e`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_postings:part=1,sort=153; metric_samples:part=1,sort=153; metric_hist_sam |
| AC-D3 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-D4 | False | None | union_http=200 rows=0 committed_http=200 |
| AC-Q0 | True | None | hb before=2 after=4 sender_alive=True |
| AC-Q1 | False | 3.729476004082244 | repeats=20 codes={200} series=0 |
| AC-Q2 | False | 863.6039470002288 | series=0 points=0 |
| AC-Q3 | True | 150.02603799803182 | series=1 |
| AC-Q4 | True | 1.053649997629691 | http=400 ms=1 body_snip={"error": "limit_exceeded: series count 15000 exceeds ma |
| AC-Q5 | False | 93.80904199497309 | series=0 want_J=10 |
| AC-Q6 | True | 0.7033689980744384 | repeats=20 |
| AC-Q7 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-Q8 | True | 170.46942900196882 | all 15 exprs ok |
| AC-Q9 | True | 8.210561005398631 | p95=8.210561005398631 codes={200} maint=metric_samples:set=200:ok ms=27;metric_s |
| AC-H1 | False | 40.57153200119501 | hist_rows=0 sample_rows=0 |
| AC-H2 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-C1 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-C2 | True | None | count=15000 want=15000 |
| AC-C3 | False | None | tall query_range series isolation |
| AC-C4 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-W1 | False | None | 180d http=200 365d http=200 unit=False |
| AC-W2 | True | None | 30d accept |
| AC-W3 | False | 32.58688899950357 | series=0 want=10 |
| AC-W4 | True | 802.6260939950589 | http=400 ms=803 |
| AC-W5 | False | 1.2970589959877543 | points=0 |
| AC-W6 | True | 0.5826590058859438 | tall_days_loaded=30 |
| AC-N1 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-N2 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-N3 | False | None | snaps=718 old_gt_A+I=0 bar=81 expire_http=200 commits=120 cols=[] |
| AC-N4 | False | None | samples_before_snap=15011 pre_expire=15195 after=15196 |
| AC-N5 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-F1 | False | None | F-files days missing |
| AC-F2 | False | None | F-files days missing |
| AC-F3 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-F4 | False | None | F-files days missing |
| AC-F5 | False | None | F-files days missing |
| AC-F6 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-S1 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-S2 | False | None | raw_before=-1 raw_after_downsample=15011 |
| AC-S3 | True | None | grafana-manual-up.sh must build release binary |
| AC-M1 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-M2 | True | None | first={'metric_samples': 15011, 'metric_samples_5m': 15000, 'metric_samples_1h': |
| AC-G0 | False | None | sha=a8924bb95c43b562e4312af309d2a5e80c103185 hb_before=3 hb_after=5 hb_alive=Tru |
| AC-G1 | True | 3.729476004082244 | T-Q1 layout_tall 30m; greptime_codes={200} |
| AC-G2 | False | 863.6039470002288 | AC-G2 ratio 1088.86 > R=10.0 (soft=863.6ms greptime=0.8ms) — escalate per §4.4 |
| AC-G3 | True | 150.02603799803182 | T-Q3 wide resolve; greptime_codes={200} |
| AC-G4 | True | 0.7033689980744384 | T-Q6 __name__/values; greptime_codes={200} |
| AC-G5 | False | 93.80904199497309 | AC-G5 ratio 26.46 > R=10.0 (soft=93.8ms greptime=3.5ms) — escalate per §4.4 |
| AC-G6 | True | None | ingest_path=/v1/otlp/v1/metrics (must be OTLP /v1/otlp/v1/metrics, not remote_wr |

## Blockers
- LAYOUT_G3_SCOPED=1 skipped tall/collapse/files; G3 timing only
- AC-G2 ratio 1088.86 > R=10.0 (soft=863.6ms greptime=0.8ms) — escalate per §4.4
- AC-G5 ratio 26.46 > R=10.0 (soft=93.8ms greptime=3.5ms) — escalate per §4.4
