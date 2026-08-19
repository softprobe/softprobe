# Metrics layout — AC gap board

**SoT:** `docs/metrics-timeseries-layout.md`  
**Updated:** 2026-08-18 04:54Z

**Ready for verification:** **yes**  
**JSON:** `docs/perf/results/20260818T045403Z-metrics-layout.json`  
**Pass:** **56/56** (`binary_profile=release`, `fixture_profile=release_full`, `COMPARE_GREPTIME=1`)  
**Validator:** `OK (ready)`

## Gate evidence

| Gate | Result |
|------|--------|
| `make test` | pass (2026-08-18 03:54Z) |
| `make test-perf` metrics-layout `release_full` + `COMPARE_GREPTIME=1` | pass 56/56, 3521s |

G9 Greptime SHA `a8924bb95c43b562e4312af309d2a5e80c103185`. Softprobe HEAD recorded as `3f07e35` (working tree also had uncommitted maintenance/orphan-delete changes compiled into the release binary used by this run).
