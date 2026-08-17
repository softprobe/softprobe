# metrics-layout 20260815T094333Z

- binary_profile: `dev`
- fixture_profile: `pr_floor`
- COMPARE_GREPTIME: `1`
- pass/fail: **9/49** pass, **40** fail
- fixture_hash: `fd2ba951482c5680`

| AC | pass | p95_ms | notes |
|----|------|--------|-------|
| AC-D1 | True | None | tables=['metric_hist_samples', 'metric_postings', 'metric_samples', 'metric_seri |
| AC-D2 | True | None | metric_series:part=2,sort=3108; metric_samples:part=2,sort=3019; metric_hist_sam |
| AC-D3 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-D4 | True | None | union_http=200 rows=1 committed_http=200 |
| AC-Q0 | True | None | hb before=349 after=351 sender_alive=True |
| AC-Q1 | False | 2.5600869994377717 | repeats=20 codes={200} series=0 |
| AC-Q2 | False | 127.39669300208334 | series=0 points=0 |
| AC-Q3 | False | 144.7159980016295 | series=0 |
| AC-Q4 | False | 149.48714400088647 | http=200 ms=149 body_snip={"data": {"result": [], "resulttype": "matrix"}, "stat |
| AC-Q5 | False | 153.90549899893813 | series=0 want_J=10 |
| AC-Q6 | True | 2.6721280009951442 | repeats=20 |
| AC-Q7 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-Q8 | True | 259.08605299991905 | all 15 exprs ok |
| AC-Q9 | False | None | maintenance-under-load measure not fully automated in harness yet |
| AC-H1 | True | 139.60996300011175 | hist_rows=1200 sample_rows=0 |
| AC-H2 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-C1 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-C2 | False | None | count=0 want=2000 |
| AC-C3 | False | None | tall query_range series isolation |
| AC-C4 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-W1 | False | None | 180d http=200 365d http=200 unit=False |
| AC-W2 | True | None | 30d accept |
| AC-W3 | False | 143.5334289999446 | series=0 want=10 |
| AC-W4 | False | 65.12777999887476 | http=200 ms=65 |
| AC-W5 | False | 65.922939000302 | points=0 |
| AC-W6 | True | 2.4104680014715996 | tall_days_loaded=30 |
| AC-N1 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-N2 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-N3 | False | None | F-snap (120s commits) not run in this harness iteration |
| AC-N4 | False | None | depends on F-snap / expiry pass |
| AC-N5 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-F1 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F2 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F3 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-F4 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F5 | False | None | F-files maintenance size bars not run in this harness iteration |
| AC-F6 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-S1 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-S2 | False | None | downsample trigger not automated; raw_count=54113 |
| AC-S3 | False | None | grafana-manual-up.sh must build release binary |
| AC-M1 | False | None | LAYOUT_SKIP_UNIT=1 |
| AC-M2 | False | None | second maintenance watermark pass not automated |
| AC-G0 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G1 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G2 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G3 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G4 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G5 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |
| AC-G6 | False | None | Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb |

## Blockers
- pr_floor skipped F-collapse-90d full load (wall-clock); AC-W3 expected fail
- Greptime binary/URL not available in this environment. Pinned source SHA=a8924bb95c43b562e4312af309d2a5e80c103185. Set GREPTIME_BIN or GREPTIME_URL to enable G9.
