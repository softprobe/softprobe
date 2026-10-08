#!/usr/bin/env python3
"""Metrics-layout machine gate harness (§10.3).

Emits docs/perf/results/<stamp>-metrics-layout.{json,md} with all 53 AC ids.
Does not invent passes: every AC row comes from a real check in this process
or a cargo unit test exit code recorded here.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from importlib.util import module_from_spec, spec_from_file_location

_spec = spec_from_file_location(
    "validate_metrics_layout_results",
    SCRIPTS / "validate-metrics-layout-results.py",
)
assert _spec and _spec.loader
_vmod = module_from_spec(_spec)
_spec.loader.exec_module(_vmod)
REQUIRED_AC_IDS = _vmod.REQUIRED_AC_IDS
validate_schema = _vmod.validate_schema

LAYOUT_FIXTURE_SEED = int(os.environ.get("LAYOUT_FIXTURE_SEED", "42"))
GOLD_EXPRS = [
    "sum by (job) (rate(http_server_request_duration_count[5m]))",
    "sum by (job) (rate(traces_span_metrics_calls[5m]))",
    "sum by (job) (rate(rpc_server_call_duration_count[5m]))",
    "sum by (job) (rate(http_client_request_duration_count[5m]))",
    "sum by (category) (rate(demo_ad_served_total[5m]))",
    "sum(rate(demo_cart_add_item_latency_count[5m]))",
    "sum(rate(demo_payment_transactions[5m]))",
    "sum(rate(demo_shipping_items_shipped[5m]))",
    "sum(rate(demo_exchange_conversions_counter[5m]))",
    "sum(rate(quotes[5m]))",
    "k6_vus",
    "sum(rate(k6_iterations[5m]))",
    "sum(k6_http_req_failed_total)",
    "topk(8, avg by (container_name) (container_cpu_utilization))",
    "topk(8, avg by (container_name) (container_memory_percent))",
]

# Fixed "now" for backdated fixtures (UTC).
EVAL_END_S = 1_700_000_000
# Tenant DuckLake schema (Postgres catalog). Unqualified names miss the table.
LAYOUT_SQL_SCHEMA = os.environ.get(
    "LAYOUT_SQL_SCHEMA", "softprobe.metrics_layout_local_dev_tenant"
)

def qtable(name: str) -> str:
    return f"{LAYOUT_SQL_SCHEMA}.{name}"



@dataclass
class AcRow:
    pass_: bool | None = None
    p95_ms: float | None = None
    softprobe_p95_ms: float | None = None
    greptime_p95_ms: float | None = None
    ratio: float | None = None
    fixture_scale: dict[str, Any] = field(default_factory=dict)
    explain_ok: bool | None = None
    notes: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "pass": bool(self.pass_) if self.pass_ is not None else False,
            "p95_ms": self.p95_ms,
            "softprobe_p95_ms": self.softprobe_p95_ms,
            "greptime_p95_ms": self.greptime_p95_ms,
            "ratio": self.ratio,
            "fixture_scale": self.fixture_scale,
            "explain_ok": self.explain_ok,
            "notes": self.notes,
        }


def profile_scales(fixture_profile: str) -> dict[str, Any]:
    if fixture_profile == "release_full":
        return {
            "wide_n": 100_000,
            # AC-Q5/W3 assert series count = J; collapse is sum-by(job) so I only
            # multiplies raw OTLP/DuckLake cost. I=1 keeps release_full hours not days.
            "collapse_j": 50,
            "collapse_i": 1,
            "collapse_days": 30,
            "collapse_90d_days": 90,
            "tall_days": 180,
            "tall_step_s": 15,
            "q_tall_short_repeats": 20,
            "q_discover_repeats": 20,
            "q_long_repeats": 5,
            "gold_repeats": 5,
        }
    # pr_floor
    return {
        "wide_n": 15_000,
        "collapse_j": 10,
        "collapse_i": 20,
        "collapse_days": 30,
        "collapse_90d_days": 90,
        "tall_days": 30,
        "tall_step_s": 60,  # pr_floor wall-clock; release_full uses 15s
        "q_tall_short_repeats": 20,
        "q_discover_repeats": 20,
        "q_long_repeats": 5,
        "gold_repeats": 5,
    }


def pct(xs: list[float], p: float) -> float | None:
    if not xs:
        return None
    ys = sorted(xs)
    i = min(len(ys) - 1, max(0, int(round((p / 100.0) * (len(ys) - 1)))))
    return ys[i]


class HttpClient:
    def __init__(self, base: str, token: str, timeout: float = 35.0):
        self.base = base.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _req(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        form: dict[str, str] | None = None,
    ) -> tuple[int, bytes, float]:
        url = f"{self.base}{path}"
        data = body
        hdrs = {"Authorization": f"Bearer {self.token}"}
        if headers:
            hdrs.update(headers)
        if form is not None:
            data = urllib.parse.urlencode(form).encode()
            hdrs["Content-Type"] = "application/x-www-form-urlencoded"
        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                payload = resp.read()
                ms = (time.perf_counter() - t0) * 1000.0
                return resp.status, payload, ms
        except urllib.error.HTTPError as e:
            ms = (time.perf_counter() - t0) * 1000.0
            return e.code, e.read() if e.fp else b"", ms
        except Exception as e:
            ms = (time.perf_counter() - t0) * 1000.0
            return 0, str(e).encode(), ms

    def ready(self) -> bool:
        code, _, _ = self._req("GET", "/ready")
        return code == 200

    def post_otlp_json(self, export_obj: dict[str, Any]) -> tuple[int, float]:
        body = json.dumps(export_obj).encode()
        code, _, ms = self._req(
            "POST",
            "/v1/metrics",
            body=body,
            headers={"Content-Type": "application/json"},
        )
        return code, ms

    def sql(self, sql: str) -> tuple[int, dict[str, Any], float]:
        body = json.dumps({"sql": sql}).encode()
        code, payload, ms = self._req(
            "POST",
            "/v1/query/sql",
            body=body,
            headers={"Content-Type": "application/json"},
        )
        try:
            doc = json.loads(payload.decode() or "{}")
        except json.JSONDecodeError:
            doc = {"error": payload.decode(errors="replace")}
        return code, doc, ms

    def query_range(
        self, query: str, start: float, end: float, step: str
    ) -> tuple[int, dict[str, Any], float]:
        code, payload, ms = self._req(
            "GET",
            "/api/v1/query_range?"
            + urllib.parse.urlencode(
                {"query": query, "start": str(start), "end": str(end), "step": step}
            ),
        )
        try:
            doc = json.loads(payload.decode() or "{}")
        except json.JSONDecodeError:
            doc = {"error": payload.decode(errors="replace")}
        return code, doc, ms

    def label_values(self, label: str = "__name__") -> tuple[int, dict[str, Any], float]:
        code, payload, ms = self._req("GET", f"/api/v1/label/{label}/values")
        try:
            doc = json.loads(payload.decode() or "{}")
        except json.JSONDecodeError:
            doc = {"error": payload.decode(errors="replace")}
        return code, doc, ms


def gauge_export(
    name: str,
    series: list[tuple[dict[str, str], list[tuple[int, float]]]],
) -> dict[str, Any]:
    """Build OTLP JSON ExportMetricsServiceRequest for gauges."""
    resource_metrics = []
    for labels, points in series:
        attrs = [{"key": k, "value": {"stringValue": v}} for k, v in labels.items()]
        # Prefer resource attrs for job/instance when present.
        resource_attrs = []
        point_attrs = []
        for a in attrs:
            if a["key"] in ("service.name", "service.instance.id", "job", "instance"):
                key = a["key"]
                if key == "job":
                    key = "service.name"
                if key == "instance":
                    key = "service.instance.id"
                resource_attrs.append({"key": key, "value": a["value"]})
            else:
                point_attrs.append(a)
        data_points = []
        for ts_ns, val in points:
            data_points.append(
                {
                    "attributes": point_attrs,
                    "timeUnixNano": str(ts_ns),
                    "asDouble": val,
                }
            )
        resource_metrics.append(
            {
                "resource": {"attributes": resource_attrs},
                "scopeMetrics": [
                    {
                        "metrics": [
                            {
                                "name": name,
                                "unit": "1",
                                "gauge": {"dataPoints": data_points},
                            }
                        ]
                    }
                ],
            }
        )
    return {"resourceMetrics": resource_metrics}


def sum_export(
    name: str,
    labels: dict[str, str],
    points: list[tuple[int, float]],
) -> dict[str, Any]:
    resource_attrs = []
    point_attrs = []
    for k, v in labels.items():
        item = {"key": k, "value": {"stringValue": v}}
        if k in ("job", "service.name"):
            resource_attrs.append(
                {"key": "service.name", "value": {"stringValue": v}}
            )
        elif k in ("instance", "service.instance.id"):
            resource_attrs.append(
                {"key": "service.instance.id", "value": {"stringValue": v}}
            )
        else:
            point_attrs.append(item)
    data_points = [
        {
            "attributes": point_attrs,
            "timeUnixNano": str(ts),
            "asDouble": val,
            "startTimeUnixNano": "0",
        }
        for ts, val in points
    ]
    return {
        "resourceMetrics": [
            {
                "resource": {"attributes": resource_attrs},
                "scopeMetrics": [
                    {
                        "metrics": [
                            {
                                "name": name,
                                "unit": "1",
                                "sum": {
                                    "dataPoints": data_points,
                                    "aggregationTemporality": 2,
                                    "isMonotonic": True,
                                },
                            }
                        ]
                    }
                ],
            }
        ]
    }


def hist_export(
    name: str,
    labels: dict[str, str],
    points: list[tuple[int, list[int], int, float]],
    bounds: list[float],
) -> dict[str, Any]:
    """points: (ts_ns, bucket_counts, count, sum)."""
    resource_attrs = [
        {"key": "service.name", "value": {"stringValue": labels.get("job", "hist")}}
    ]
    if "instance" in labels:
        resource_attrs.append(
            {
                "key": "service.instance.id",
                "value": {"stringValue": labels["instance"]},
            }
        )
    point_attrs = [
        {"key": k, "value": {"stringValue": v}}
        for k, v in labels.items()
        if k not in ("job", "instance")
    ]
    data_points = []
    for ts, buckets, count, sm in points:
        data_points.append(
            {
                "attributes": point_attrs,
                "timeUnixNano": str(ts),
                "count": str(count),
                "sum": sm,
                "bucketCounts": [str(c) for c in buckets],
                "explicitBounds": bounds,
            }
        )
    return {
        "resourceMetrics": [
            {
                "resource": {"attributes": resource_attrs},
                "scopeMetrics": [
                    {
                        "metrics": [
                            {
                                "name": name,
                                "unit": "s",
                                "histogram": {
                                    "dataPoints": data_points,
                                    "aggregationTemporality": 2,
                                },
                            }
                        ]
                    }
                ],
            }
        ]
    }


def run_cargo_units(
    mapping: list[tuple[str, str]], profile_flag: str
) -> dict[str, tuple[bool, str]]:
    """Run each cargo unit filter separately (cargo accepts one TESTNAME)."""
    env = os.environ.copy()
    cache_root = Path(os.environ.get("THELAKE_CACHE_ROOT", Path.home() / ".cache" / "thelake"))
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", cache_root / "target"))
    env["CARGO_TARGET_DIR"] = str(target_dir)
    env.setdefault("CARGO_HOME", str(cache_root / "cargo"))
    env.setdefault("DUCKDB_DOWNLOAD_LIB", "1")
    duck_so = next(target_dir.glob("duckdb-download/**/libduckdb.so*"), None)
    if duck_so is not None:
        libdir = str(duck_so.parent)
        prev = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = f"{libdir}:{prev}" if prev else libdir

    results: dict[str, tuple[bool, str]] = {}
    for ac_id, filt in mapping:
        cmd = ["cargo", "test"]
        if profile_flag:
            cmd.append(profile_flag)
        cmd.extend(["--lib", filt, "--", "--test-threads=1"])
        out = ""
        try:
            p = subprocess.run(
                cmd,
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                timeout=600,
                env=env,
            )
            out = p.stdout + "\n" + p.stderr
        except Exception as e:
            results[ac_id] = (False, str(e))
            continue

        ok = False
        note = f"filter={filt} not found in cargo output"
        for line in out.splitlines():
            if filt in line and ("ok" in line or "FAILED" in line or "failed" in line):
                ok = " ... ok" in line or line.strip().endswith("ok")
                if "FAILED" in line or line.strip().endswith("FAILED"):
                    ok = False
                note = line.strip()[:240]
                break
        if "error: linking" in out or "could not compile" in out:
            note = "cargo compile/link failed: " + out[-300:].replace("\n", " ")
            ok = False
        # cargo summary line when filter matches exactly one test
        if not ok and "test result: ok." in out and "0 passed" not in out:
            # e.g. "1 passed; 0 failed"
            for line in out.splitlines():
                if line.startswith("test result:") and "passed" in line:
                    try:
                        n = int(line.split("passed")[0].split()[-1])
                        if n >= 1 and "0 failed" in line:
                            ok = True
                            note = line.strip()[:240]
                    except ValueError:
                        pass
        results[ac_id] = (ok, note)
    return results


def run_cargo_unit(filter_name: str, profile_flag: str) -> tuple[bool, str]:
    return run_cargo_units([("_", filter_name)], profile_flag)


def fixture_bin() -> Path:
    env_bin = os.environ.get("LAYOUT_OTLP_FIXTURE_BIN", "").strip()
    if env_bin:
        return Path(env_bin)
    target = Path(
        os.environ.get(
            "CARGO_TARGET_DIR", Path.home() / ".cache" / "thelake" / "target"
        )
    )
    profile = "release" if os.environ.get("CARGO_PROFILE_FLAG", "").strip() == "--release" else "debug"
    return target / profile / "layout_otlp_fixture"


def post_fixture_cmds(
    cmds: list[dict[str, Any]],
    client_base: str,
    token: str,
    *,
    metrics_path: str = "/v1/metrics",
    headers: list[str] | None = None,
) -> None:
    bin_path = fixture_bin()
    if not bin_path.is_file():
        raise SystemExit(f"missing {bin_path}; build with cargo build --bin layout_otlp_fixture")
    payload = "\n".join(json.dumps(c) for c in cmds) + "\n"
    env = os.environ.copy()
    target = Path(os.environ.get("CARGO_TARGET_DIR", Path.home() / ".cache" / "thelake" / "target"))
    duck_so = next(target.glob("duckdb-download/**/libduckdb.so*"), None)
    if duck_so is not None:
        libdir = str(duck_so.parent)
        prev = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = f"{libdir}:{prev}" if prev else libdir
    argv = [
        str(bin_path),
        "--url",
        client_base,
        "--token",
        token,
        "--metrics-path",
        metrics_path,
    ]
    for h in headers or []:
        argv.extend(["--header", h])
    p = subprocess.run(
        argv,
        input=payload,
        text=True,
        capture_output=True,
        env=env,
        # release_full F-collapse job posts can take many minutes under DuckLake.
        timeout=int(os.environ.get("LAYOUT_OTLP_SUBPROCESS_TIMEOUT_SECS", "7200")),
    )
    if p.returncode != 0:
        raise SystemExit(
            f"layout_otlp_fixture failed: {p.stderr[-800:] or p.stdout[-800:]}"
        )


GREPTIME_OTLP_PATH = "/v1/otlp/v1/metrics"
GREPTIME_NO_TRANSLATION_HEADER = (
    "x-greptime-otlp-metric-translation-strategy:NoTranslation"
)
G9_RATIO_R = 10.0


class GreptimeClient:
    """HTTP client for Greptime standalone (PromQL + health; no Softprobe auth)."""

    def __init__(self, base: str, timeout: float = 35.0):
        self.base = base.rstrip("/")
        self.timeout = timeout

    def _req(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        form: dict[str, str] | None = None,
    ) -> tuple[int, bytes, float]:
        url = f"{self.base}{path}"
        data = body
        hdrs: dict[str, str] = {}
        if headers:
            hdrs.update(headers)
        if form is not None:
            data = urllib.parse.urlencode(form).encode()
            hdrs["Content-Type"] = "application/x-www-form-urlencoded"
        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                payload = resp.read()
                ms = (time.perf_counter() - t0) * 1000.0
                return resp.status, payload, ms
        except urllib.error.HTTPError as e:
            ms = (time.perf_counter() - t0) * 1000.0
            return e.code, e.read() if e.fp else b"", ms
        except Exception as e:
            ms = (time.perf_counter() - t0) * 1000.0
            return 0, str(e).encode(), ms

    def ready(self) -> bool:
        code, _, _ = self._req("GET", "/health")
        return code == 200

    def query_range(
        self, query: str, start: float, end: float, step: str
    ) -> tuple[int, dict[str, Any], float]:
        code, payload, ms = self._req(
            "GET",
            "/v1/prometheus/api/v1/query_range?"
            + urllib.parse.urlencode(
                {"query": query, "start": str(start), "end": str(end), "step": step}
            ),
        )
        try:
            doc = json.loads(payload.decode() or "{}")
        except json.JSONDecodeError:
            doc = {"error": payload.decode(errors="replace")}
        return code, doc, ms

    def label_values(self, label: str = "__name__") -> tuple[int, dict[str, Any], float]:
        code, payload, ms = self._req(
            "GET", f"/v1/prometheus/api/v1/label/{label}/values"
        )
        try:
            doc = json.loads(payload.decode() or "{}")
        except json.JSONDecodeError:
            doc = {"error": payload.decode(errors="replace")}
        return code, doc, ms

    def sql(self, sql: str) -> tuple[int, dict[str, Any], float]:
        code, payload, ms = self._req(
            "GET",
            "/v1/sql?" + urllib.parse.urlencode({"db": "public", "sql": sql}),
        )
        try:
            doc = json.loads(payload.decode() or "{}")
        except json.JSONDecodeError:
            doc = {"error": payload.decode(errors="replace")}
        return code, doc, ms

    def heartbeat_sample_count(self) -> int:
        # Metric-engine table name matches OTLP metric name under NoTranslation.
        code, doc, _ = self.sql("SELECT count(*) AS c FROM layout_ingest_heartbeat")
        if code != 200:
            return -1
        try:
            outputs = doc.get("output") or []
            if not outputs:
                # Some builds wrap records differently
                rows = doc.get("rows") or []
                if rows:
                    return int(rows[0][0])
                return -1
            records = outputs[0].get("records") or {}
            rows = records.get("rows") or []
            if not rows:
                return 0
            return int(rows[0][0])
        except (IndexError, KeyError, TypeError, ValueError):
            return -1


def greptime_http_base() -> str:
    return os.environ.get("GREPTIME_URL", "http://127.0.0.1:14000").rstrip("/")


def greptime_bind_addrs() -> dict[str, str]:
    """Derive bind addresses from GREPTIME_URL / overrides."""
    base = greptime_http_base()
    parsed = urllib.parse.urlparse(base)
    host = parsed.hostname or "127.0.0.1"
    http_port = parsed.port or 14000
    return {
        "http": f"{host}:{http_port}",
        "grpc": os.environ.get("GREPTIME_GRPC_ADDR", f"{host}:{http_port + 1}"),
        "mysql": os.environ.get("GREPTIME_MYSQL_ADDR", f"{host}:{http_port + 2}"),
        "postgres": os.environ.get("GREPTIME_POSTGRES_ADDR", f"{host}:{http_port + 3}"),
    }


class Harness:
    def __init__(self) -> None:
        self.base = os.environ.get("SOFTPROBE_URL", "http://127.0.0.1:18091")
        self.token = os.environ.get("SOFTPROBE_API_KEY", "local-dev-key")
        self.fixture_profile = os.environ.get("METRICS_LAYOUT_PROFILE", "pr_floor")
        if self.fixture_profile not in ("pr_floor", "release_full"):
            raise SystemExit(f"bad METRICS_LAYOUT_PROFILE={self.fixture_profile}")
        self.compare_greptime = os.environ.get("COMPARE_GREPTIME", "0") == "1"
        profile_flag = os.environ.get("CARGO_PROFILE_FLAG", "").strip()
        self.binary_profile = (
            "release" if profile_flag == "--release" else "dev"
        )
        self.scales = profile_scales(self.fixture_profile)
        self.client = HttpClient(self.base, self.token)
        self.acs: dict[str, AcRow] = {i: AcRow(pass_=False, notes="not measured") for i in REQUIRED_AC_IDS}
        self.preconditions: dict[str, Any] = {
            "AC-F2_bytes_before_merge": 0,
            "AC-F5_precondition_met": False,
            "sender_alive": False,
            "greptime_sender_alive": False,
        }
        self.fixture_parts: list[str] = []
        self.greptime_sha = os.environ.get("GREPTIME_GIT_SHA", "missing")
        self.blocker_notes: list[str] = []
        self._otlp_dest: dict[str, Any] | None = None
        self._greptime_proc: subprocess.Popen[Any] | None = None
        self._greptime_hb_proc: subprocess.Popen[Any] | None = None
        self._greptime_owned = False
        self._greptime_ingest_path = ""
        self._raw_before_downsample = -1
        self._ladder_after_first: dict[str, int] = {}
        self._f_files_closed_days: list[str] = []
        self._f_files_today: str = ""
        cat, schema = LAYOUT_SQL_SCHEMA.split(".", 1) if "." in LAYOUT_SQL_SCHEMA else (
            LAYOUT_SQL_SCHEMA,
            "main",
        )
        self.ducklake_catalog = cat
        self.ducklake_schema = schema

    def post_cmds(self, cmds: list[dict[str, Any]]) -> None:
        dest = self._otlp_dest or {
            "base": self.base,
            "token": self.token,
            "metrics_path": "/v1/metrics",
            "headers": [],
        }
        post_fixture_cmds(
            cmds,
            dest["base"],
            dest["token"],
            metrics_path=dest["metrics_path"],
            headers=list(dest.get("headers") or []),
        )

    def mark(self, ac_id: str, **kwargs: Any) -> None:
        row = self.acs[ac_id]
        for k, v in kwargs.items():
            if k == "pass":
                row.pass_ = v
            elif hasattr(row, k):
                setattr(row, k, v)
            elif k == "pass_":
                row.pass_ = v

    def heartbeat_count(self) -> int:
        code, doc, _ = self.client.sql(
            f"SELECT count(*) FROM {qtable('metric_samples')} sm "
            f"JOIN {qtable('metric_series')} s ON sm.series_id = s.series_id "
            f"AND sm.record_date = s.record_date "
            f"WHERE s.metric_name = 'layout_ingest_heartbeat'"
        )
        if code != 200 or not doc.get("rows"):
            return 0
        try:
            return int(doc["rows"][0][0])
        except (IndexError, TypeError, ValueError):
            return 0

    def sql_scalar(self, sql: str, timeout: float | None = None) -> int:
        old = self.client.timeout
        if timeout is not None:
            self.client.timeout = timeout
        try:
            code, doc, _ = self.client.sql(sql)
        finally:
            self.client.timeout = old
        if code != 200:
            return -1
        try:
            return int((doc.get("rows") or [[0]])[0][0] or 0)
        except (IndexError, TypeError, ValueError):
            return -1

    def sql_exec(self, sql: str, timeout: float = 300.0) -> tuple[int, str]:
        old = self.client.timeout
        self.client.timeout = timeout
        try:
            code, doc, ms = self.client.sql(sql.strip())
        finally:
            self.client.timeout = old
        note = str(doc.get("error") or doc.get("message") or f"ok ms={ms:.0f}")
        return code, note

    def meta(self) -> str:
        return f"__ducklake_metadata_{self.ducklake_catalog}"

    def live_partition_stats(
        self, table: str, days: list[str] | None = None
    ) -> list[tuple[str, int, int]]:
        """Return (record_date, live_file_count, total_bytes) for live files."""
        meta = self.meta()
        day_filter = ""
        if days:
            lit = ", ".join(f"'{d}'" for d in days)
            day_filter = f" AND CAST(fp.partition_value AS VARCHAR) IN ({lit})"
        sql = (
            f"SELECT CAST(fp.partition_value AS VARCHAR) AS record_date, "
            f"count(*)::BIGINT AS live_file_count, "
            f"coalesce(sum(df.file_size_bytes), 0)::BIGINT AS total_bytes "
            f"FROM {meta}.ducklake_data_file df "
            f"JOIN {meta}.ducklake_table t ON df.table_id = t.table_id "
            f"JOIN {meta}.ducklake_file_partition_value fp "
            f"  ON fp.data_file_id = df.data_file_id AND fp.table_id = t.table_id "
            f"WHERE t.table_name = '{table}' AND t.end_snapshot IS NULL "
            f"  AND df.end_snapshot IS NULL{day_filter} "
            f"GROUP BY 1 ORDER BY 1"
        )
        code, doc, _ = self.client.sql(sql)
        out: list[tuple[str, int, int]] = []
        if code != 200:
            return out
        for row in doc.get("rows") or []:
            if not row or len(row) < 3:
                continue
            out.append((str(row[0]), int(row[1] or 0), int(row[2] or 0)))
        return out

    def live_file_sizes(self, table: str, days: list[str]) -> list[int]:
        meta = self.meta()
        lit = ", ".join(f"'{d}'" for d in days)
        sql = (
            f"SELECT df.file_size_bytes::BIGINT "
            f"FROM {meta}.ducklake_data_file df "
            f"JOIN {meta}.ducklake_table t ON df.table_id = t.table_id "
            f"JOIN {meta}.ducklake_file_partition_value fp "
            f"  ON fp.data_file_id = df.data_file_id AND fp.table_id = t.table_id "
            f"WHERE t.table_name = '{table}' AND t.end_snapshot IS NULL "
            f"  AND df.end_snapshot IS NULL "
            f"  AND CAST(fp.partition_value AS VARCHAR) IN ({lit})"
        )
        code, doc, _ = self.client.sql(sql)
        sizes: list[int] = []
        if code != 200:
            return sizes
        for row in doc.get("rows") or []:
            if row:
                sizes.append(int(row[0] or 0))
        return sizes

    def force_twcs_merge(
        self,
        tables: list[str] | None = None,
        *,
        max_compacted_files: int = 32,
        max_file_size_bytes: int = 8 * 1024 * 1024,
        waves: int = 8,
    ) -> list[str]:
        """Force bounded ducklake_merge_adjacent_files waves (Softprobe TWCS)."""
        tables = tables or [
            "metric_samples",
            "metric_postings",
            "metric_series",
            "metric_hist_samples",
        ]
        cat = self.ducklake_catalog
        schema = self.ducklake_schema
        notes: list[str] = []
        for table in tables:
            scope = f"schema => '{schema}', table_name => '{table}'"
            set_sql = f"CALL {cat}.set_option('target_file_size', '64MB', {scope})"
            c1, n1 = self.sql_exec(set_sql, timeout=60.0)
            notes.append(f"{table}:set={c1}:{n1[:80]}")
            merge_sql = (
                f"CALL ducklake_merge_adjacent_files('{cat}', '{table}', "
                f"schema => '{schema}', max_compacted_files => {int(max_compacted_files)}, "
                f"max_file_size => {int(max_file_size_bytes)})"
            )
            for wave in range(1, int(waves) + 1):
                c2, n2 = self.sql_exec(merge_sql, timeout=300.0)
                notes.append(f"{table}:merge{wave}={c2}:{n2[:60]}")
                if c2 not in (200, 201):
                    break
        return notes

    def force_expire_snapshots(self, age_seconds: int) -> tuple[int, str]:
        sql = (
            f"CALL ducklake_expire_snapshots('{self.ducklake_catalog}', "
            f"older_than => now() - INTERVAL '{int(age_seconds)} seconds')"
        )
        return self.sql_exec(sql, timeout=120.0)

    def ladder_counts(self) -> dict[str, int]:
        return {
            "metric_samples": self.sql_scalar(
                f"SELECT count(*) FROM {qtable('metric_samples')}"
            ),
            "metric_samples_5m": self.sql_scalar(
                f"SELECT count(*) FROM {qtable('metric_samples_5m')}"
            ),
            "metric_samples_1h": self.sql_scalar(
                f"SELECT count(*) FROM {qtable('metric_samples_1h')}"
            ),
            "metric_collapse_job_1h": self.sql_scalar(
                f"SELECT count(*) FROM {qtable('metric_collapse_job_1h')}"
            ),
        }

    def run_downsample_pass(self) -> list[str]:
        """One incremental ladder pass (same SQL as materialize_query_grains)."""
        notes: list[str] = []
        # Reuse materialize steps without re-printing the tall/collapse probes.
        cat = LAYOUT_SQL_SCHEMA
        sqls = [
            (
                "5m",
                f"""
INSERT INTO {cat}.metric_samples_5m
  (series_id, window_ts, record_date, count, sum, min, max, last, last_ts)
SELECT
  series_id,
  time_bucket(INTERVAL '5 minutes', timestamp) AS window_ts,
  CAST(time_bucket(INTERVAL '5 minutes', timestamp) AS DATE) AS record_date,
  count(*)::UBIGINT AS count,
  sum(value) AS sum,
  min(value) AS min,
  max(value) AS max,
  arg_max(value, timestamp) AS last,
  max(timestamp) AS last_ts
FROM {cat}.metric_samples
WHERE timestamp < now() - INTERVAL '2 hours'
  AND time_bucket(INTERVAL '5 minutes', timestamp) < date_trunc('hour', now())
  AND time_bucket(INTERVAL '5 minutes', timestamp) >
      (SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM {cat}.metric_samples_5m)
GROUP BY series_id, time_bucket(INTERVAL '5 minutes', timestamp)
""",
            ),
            (
                "1h_from_raw",
                f"""
INSERT INTO {cat}.metric_samples_1h
  (series_id, window_ts, record_date, count, sum, min, max, last, last_ts)
SELECT
  series_id,
  time_bucket(INTERVAL '1 hour', timestamp) AS window_ts,
  CAST(time_bucket(INTERVAL '1 hour', timestamp) AS DATE) AS record_date,
  count(*)::UBIGINT AS count,
  sum(value) AS sum,
  min(value) AS min,
  max(value) AS max,
  arg_max(value, timestamp) AS last,
  max(timestamp) AS last_ts
FROM {cat}.metric_samples
WHERE timestamp < now() - INTERVAL '24 hours'
  AND time_bucket(INTERVAL '1 hour', timestamp) >
      (SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM {cat}.metric_samples_1h)
GROUP BY series_id, time_bucket(INTERVAL '1 hour', timestamp)
""",
            ),
            (
                "collapse_from_1h",
                f"""
INSERT INTO {cat}.metric_collapse_job_1h
  (metric_name, job, window_ts, record_date, count, sum, min, max, last)
SELECT
  s.metric_name,
  p.label_value AS job,
  h.window_ts,
  h.record_date,
  sum(h.count)::UBIGINT AS count,
  sum(h.sum) AS sum,
  min(h.min) AS min,
  max(h.max) AS max,
  sum(h.last) AS last
FROM {cat}.metric_samples_1h h
JOIN {cat}.metric_series s
  ON h.series_id = s.series_id AND h.record_date = s.record_date
JOIN {cat}.metric_postings p
  ON p.series_id = h.series_id AND p.record_date = h.record_date
 AND p.label_name = 'job'
WHERE h.window_ts >
      (SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM {cat}.metric_collapse_job_1h)
GROUP BY s.metric_name, p.label_value, h.window_ts, h.record_date
""",
            ),
        ]
        for label, sql in sqls:
            code, note = self.sql_exec(sql, timeout=300.0)
            notes.append(f"{label}={code}:{note[:60]}")
        return notes

    def load_f_tall(self) -> None:
        days = int(self.scales["tall_days"])
        step = int(self.scales["tall_step_s"])
        n_points = (days * 86400) // step
        batch = 500
        end_ns = EVAL_END_S * 1_000_000_000
        print(f"==> F-tall layout_tall days={days} step={step}s points={n_points}")
        for start in range(0, n_points, batch):
            pts = []
            for i in range(start, min(start + batch, n_points)):
                ts = end_ns - (n_points - 1 - i) * step * 1_000_000_000
                pts.append([ts, 10.0 + (i % 100)])
            self.post_cmds(
                [{"name": "layout_tall", "kind": "gauge",
                  "labels": {"job": "tall", "instance": "t0"}, "points": pts}]
            )
        self.fixture_parts.append(f"tall:{days}d@{step}s")

    def load_f_wide(self) -> None:
        n = int(self.scales["wide_n"])
        print(f"==> F-wide layout_wide N={n}")
        end_ns = EVAL_END_S * 1_000_000_000
        batch = 500
        for start in range(0, n, batch):
            cmds = []
            for i in range(start, min(start + batch, n)):
                cmds.append({
                    "name": "layout_wide", "kind": "gauge",
                    "labels": {"job": "wide", "instance": f"i-{i}"},
                    "points": [[end_ns, float(i % 97)]],
                })
            self.post_cmds(cmds)
        self.fixture_parts.append(f"wide:{n}")

    def load_f_churn(self) -> None:
        print("==> F-churn")
        today = EVAL_END_S
        older = today - 2 * 86400
        cmds = []
        for day_s, pod in ((older, "p1"), (today, "p2")):
            cmds.append({
                "name": "layout_churn", "kind": "gauge",
                "labels": {"job": "churn", "instance": "c0", "pod": pod},
                "points": [[day_s * 1_000_000_000, 1.0]],
            })
        self.post_cmds(cmds)
        self.fixture_parts.append("churn")

    def load_f_collapse(self, days: int, tag: str) -> None:
        j = int(self.scales["collapse_j"])
        i_n = int(self.scales["collapse_i"])
        print(f"==> F-collapse {tag} J={j} I={i_n} days={days}", flush=True)
        end_ns = EVAL_END_S * 1_000_000_000
        hours = days * 24
        # Thin I (release_full I=1): larger point chunks + multi-job posts cut HTTP
        # round-trips. Wide I still posts one job at a time and mid-load TWCS-merges
        # so DuckLake file count stays bounded (maintenance off in layout compose).
        thin = i_n <= 2
        pts_per_cmd = 720 if thin else 240
        jobs_per_post = 10 if thin else 1
        merge_every = 0 if thin else (5 if self.fixture_profile == "release_full" else 0)
        # Prefer fewer, larger OTLP flushes for thin collapse (override via env).
        if thin and "LAYOUT_OTLP_FLUSH_EVERY" not in os.environ:
            os.environ["LAYOUT_OTLP_FLUSH_EVERY"] = "500"
        pending: list[dict[str, Any]] = []
        jobs_in_pending = 0
        for job_i in range(j):
            for inst_i in range(i_n):
                pts = [
                    [
                        end_ns - (hours - 1 - h) * 3600 * 1_000_000_000,
                        float(10 + h + job_i),
                    ]
                    for h in range(hours)
                ]
                for c in range(0, len(pts), pts_per_cmd):
                    pending.append({
                        "name": "layout_http", "kind": "sum",
                        "labels": {
                            "job": f"job-{job_i}",
                            "instance": f"inst-{inst_i}",
                        },
                        "points": pts[c:c + pts_per_cmd],
                    })
            jobs_in_pending += 1
            if jobs_in_pending >= jobs_per_post or job_i + 1 == j:
                self.post_cmds(pending)
                pending = []
                jobs_in_pending = 0
            if job_i == 0 or (job_i + 1) % 5 == 0 or job_i + 1 == j:
                print(f"==> F-collapse {tag} job {job_i + 1}/{j}", flush=True)
            if merge_every and (job_i + 1) % merge_every == 0:
                notes = self.force_twcs_merge(
                    ["metric_samples", "metric_series", "metric_postings"],
                    max_compacted_files=256,
                    waves=12,
                )
                print(
                    f"==> F-collapse {tag} mid-load merge after job {job_i + 1}: "
                    f"{';'.join(notes[:8])}",
                    flush=True,
                )
        self.fixture_parts.append(f"collapse-{tag}:{j}x{i_n}x{days}d")

    def count_layout_latency_rows(self) -> tuple[int, int, int, str]:
        """Return (hist_n, sample_n, series_n, diag) for AC-H1 / F-hist verify.

        F-hist lands on EVAL_END's UTC day; F-files hist bulk uses recent closed days.
        Restricting to that partition + series_id IN (layout_latency…) avoids full
        scans of MiB-scale metric_hist_samples (prior timeouts → hist_rows=0).
        """
        cat = LAYOUT_SQL_SCHEMA
        eval_day = datetime.fromtimestamp(EVAL_END_S, tz=timezone.utc).date().isoformat()
        series_sql = (
            f"SELECT count(*) FROM {cat}.metric_series "
            f"WHERE metric_name LIKE 'layout_latency%' "
            f"AND CAST(record_date AS VARCHAR) = '{eval_day}'"
        )
        series_n = self.sql_scalar(series_sql, timeout=60.0)
        hist_sql = (
            f"SELECT count(*) FROM {cat}.metric_hist_samples h "
            f"WHERE CAST(h.record_date AS VARCHAR) = '{eval_day}' "
            f"AND h.series_id IN ("
            f"  SELECT s.series_id FROM {cat}.metric_series s "
            f"  WHERE s.metric_name LIKE 'layout_latency%' "
            f"  AND CAST(s.record_date AS VARCHAR) = '{eval_day}'"
            f")"
        )
        samp_sql = (
            f"SELECT count(*) FROM {cat}.metric_samples sm "
            f"WHERE CAST(sm.record_date AS VARCHAR) = '{eval_day}' "
            f"AND sm.series_id IN ("
            f"  SELECT s.series_id FROM {cat}.metric_series s "
            f"  WHERE s.metric_name LIKE 'layout_latency%' "
            f"  AND CAST(s.record_date AS VARCHAR) = '{eval_day}'"
            f")"
        )
        hist_n = self.sql_scalar(hist_sql, timeout=120.0)
        samp_n = self.sql_scalar(samp_sql, timeout=60.0)
        diag = (
            f"day={eval_day} series={series_n} hist={hist_n} samples={samp_n}"
        )
        # sql_scalar returns -1 on HTTP/SQL failure — surface as hard fail (not 0).
        if hist_n < 0 or samp_n < 0 or series_n < 0:
            return 0, -1 if samp_n < 0 else max(samp_n, 0), max(series_n, 0), diag + " sql_err"
        return hist_n, samp_n, series_n, diag

    def load_f_hist(self, *, verify: bool = True) -> None:
        print("==> F-hist")
        bounds = [0.005, 0.01, 0.025, 0.05, 0.1]
        end_ns = EVAL_END_S * 1_000_000_000
        # One series per OTLP commit: smaller txns retry cleanly under DuckLake conflicts.
        for s in range(10):
            pts = [[end_ns - (119 - i) * 15 * 1_000_000_000, [1, 1, 1, 1, 1, 1], 6, 0.12]
                   for i in range(120)]
            self.post_cmds([{
                "name": "layout_latency", "kind": "histogram",
                "labels": {"job": "hist", "instance": f"h-{s}"},
                "bounds": bounds, "points": pts,
            }])
        if "hist" not in self.fixture_parts:
            self.fixture_parts.append("hist")
        if verify:
            hist_n, samp_n, series_n, diag = self.count_layout_latency_rows()
            print(f"==> F-hist verify {diag}")
            if hist_n < 1 or series_n < 1 or samp_n != 0:
                raise SystemExit(
                    f"F-hist verify failed after load: {diag} "
                    f"(want hist>0 series>0 samples==0)"
                )

    def load_f_gold(self) -> None:
        print("==> F-gold")
        end_ns = EVAL_END_S * 1_000_000_000
        step, n = 60, 12

        def rising(base: float = 10.0):
            return [[end_ns - (n - 1 - i) * step * 1_000_000_000, base + i * 5.0] for i in range(n)]

        cmds = []
        bounds = [0.01, 0.05, 0.1]
        for name, job in [
            ("http.server.request.duration", "frontend"),
            ("rpc.server.call.duration", "productcatalog"),
            ("http.client.request.duration", "frontend"),
            ("demo.cart.add.item.latency", "cart"),
        ]:
            pts = [[end_ns - (n - 1 - i) * step * 1_000_000_000, [2, 2, 2, 2], 8, 0.4] for i in range(n)]
            cmds.append({"name": name, "kind": "histogram", "labels": {"job": job},
                         "bounds": bounds, "points": pts})
        for name, labels in [
            ("traces.span.metrics.calls", {"job": "frontend"}),
            ("demo.ad.served.total", {"category": "ads"}),
            ("demo.payment.transactions", {"job": "payment"}),
            ("demo.shipping.items.shipped", {"job": "shipping"}),
            ("demo.exchange.conversions.counter", {"job": "currency"}),
            ("quotes", {"job": "quote"}),
            ("k6.iterations", {"job": "k6"}),
            ("k6.http.req.failed.total", {"job": "k6"}),
        ]:
            cmds.append({"name": name, "kind": "sum", "labels": labels, "points": rising()})
        cmds.append({"name": "k6.vus", "kind": "gauge",
                     "labels": {"job": "k6", "instance": "k0"}, "points": rising(40)})
        for metric, cname in [
            ("container.cpu.utilization", "frontend"),
            ("container.memory.percent", "frontend"),
            ("container.cpu.utilization", "checkout"),
            ("container.memory.percent", "checkout"),
        ]:
            cmds.append({"name": metric, "kind": "gauge",
                         "labels": {"job": "node", "instance": cname, "container_name": cname},
                         "points": rising(0.5)})
        self.post_cmds(cmds)
        self.fixture_parts.append("gold")

    def load_f_sql(self) -> None:
        print("==> F-sql")
        end_ns = EVAL_END_S * 1_000_000_000
        self.post_cmds([{
            "name": "layout_sql_gauge", "kind": "gauge",
            "labels": {"job": "sql", "instance": "s0"},
            "points": [[end_ns, 3.14]],
        }])
        self.fixture_parts.append("sql")

    def load_f_files(self) -> None:
        """≥30 OTLP batches into two closed days + today; closed samples ≥16 MiB."""
        today = datetime.now(timezone.utc).date()
        d1 = today - timedelta(days=2)
        d2 = today - timedelta(days=1)
        self._f_files_closed_days = [d1.isoformat(), d2.isoformat()]
        self._f_files_today = today.isoformat()
        target = int(os.environ.get("LAYOUT_FFILES_TARGET_BYTES", str(20 * 1024 * 1024)))
        idx_target = int(os.environ.get("LAYOUT_FFILES_INDEX_BYTES", str(8 * 1024 * 1024)))
        points_per = int(os.environ.get("LAYOUT_FFILES_POINTS_PER_BATCH", "25000"))
        # Cap extra sample top-up so the gate stays within PERF_LAYOUT_GOAL_SECS.
        max_batches = int(os.environ.get("LAYOUT_FFILES_MAX_BATCHES", "100"))

        print(
            f"==> F-files closed={self._f_files_closed_days} today={self._f_files_today} "
            f"target_bytes={target}"
        )

        def day_ns(d: date, i: int) -> int:
            # Spread points across the UTC day so record_date matches.
            base = int(
                datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()
            )
            return (base + (i % 86000)) * 1_000_000_000

        batch_i = 0
        # Closed-day sample bulk (≥30 batches total across days+today).
        for day in (d1, d2):
            for _ in range(20):
                pts = [
                    [day_ns(day, batch_i * points_per + j), float((j + batch_i) % 97)]
                    for j in range(points_per)
                ]
                self.post_cmds([{
                    "name": "layout_files",
                    "kind": "gauge",
                    "labels": {
                        "job": "files",
                        "instance": f"f-{batch_i}",
                        "pad": ("x" * 256) + str(batch_i),
                    },
                    "points": pts,
                }])
                batch_i += 1
                if batch_i % 5 == 0:
                    closed_bytes = sum(
                        b for _, _, b in self.live_partition_stats(
                            "metric_samples", self._f_files_closed_days
                        )
                    )
                    print(f"==> F-files progress batches={batch_i} closed_bytes={closed_bytes}")
                    if closed_bytes >= target:
                        break
            closed_bytes = sum(
                b for _, _, b in self.live_partition_stats(
                    "metric_samples", self._f_files_closed_days
                )
            )
            if closed_bytes >= target:
                break

        # Today open-day batches (enough files to exercise AC-F4 soft cap).
        for _ in range(12):
            pts = [
                [day_ns(today, batch_i * 500 + j), float(j)]
                for j in range(500)
            ]
            self.post_cmds([{
                "name": "layout_files",
                "kind": "gauge",
                "labels": {"job": "files", "instance": f"today-{batch_i}"},
                "points": pts,
            }])
            batch_i += 1

        # Index bulk for AC-F5 (≥8 MiB each family before merge).
        # High-entropy pads defeat Parquet RLE on repeated chars.
        def fat_pad(seed: int, n: int = 2048) -> str:
            out = []
            x = (seed * 1103515245 + 12345) & 0x7FFFFFFF
            for _ in range(n):
                x = (x * 1103515245 + 12345) & 0x7FFFFFFF
                out.append(chr(33 + (x % 94)))
            return "".join(out)

        series_n = 0
        while series_n < 12000:
            cmds = []
            for k in range(100):
                idx = series_n + k
                day = d1 if idx % 2 == 0 else d2
                cmds.append({
                    "name": "layout_files_idx",
                    "kind": "gauge",
                    "labels": {
                        "job": f"idx-{idx % 40}",
                        "instance": f"ix-{idx}",
                        "pad": fat_pad(idx, 4096),
                        "pod": f"pod-{idx}",
                        "region": f"r-{idx % 16}",
                        "zone": fat_pad(idx + 99, 1024),
                    },
                    "points": [[day_ns(day, idx), float(idx % 13)]],
                })
            self.post_cmds(cmds)
            series_n += 100
            stats = {
                t: sum(b for _, _, b in self.live_partition_stats(t, self._f_files_closed_days))
                for t in ("metric_series", "metric_postings")
            }
            if series_n % 500 == 0:
                print(f"==> F-files index series={series_n} bytes={stats}")
            if all(v >= idx_target for v in stats.values()):
                break

        hist_batches = 0
        bounds = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
        while hist_batches < 80:
            day = d1 if hist_batches % 2 == 0 else d2
            # Dense hist commits: grow metric_hist_samples (labels land on series).
            cmds = []
            for s in range(40):
                pts = [
                    [
                        day_ns(day, hist_batches * 40000 + s * 500 + j),
                        [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
                        22,
                        1.2,
                    ]
                    for j in range(500)
                ]
                cmds.append({
                    "name": "layout_files_hist",
                    "kind": "histogram",
                    "labels": {
                        "job": "files-hist",
                        "instance": f"fh-{hist_batches}-{s}",
                        "pad": fat_pad(hist_batches * 100 + s, 512),
                    },
                    "bounds": bounds,
                    "points": pts,
                })
            self.post_cmds(cmds)
            hist_batches += 1
            hist_bytes = sum(
                b for _, _, b in self.live_partition_stats(
                    "metric_hist_samples", self._f_files_closed_days
                )
            )
            if hist_batches % 5 == 0:
                print(f"==> F-files hist batches={hist_batches} bytes={hist_bytes}")
            if hist_bytes >= idx_target:
                break

        closed_bytes = sum(
            b for _, _, b in self.live_partition_stats(
                "metric_samples", self._f_files_closed_days
            )
        )
        # Keep loading samples until precondition met or hard cap.
        while closed_bytes < target and batch_i < max_batches:
            day = d1 if batch_i % 2 == 0 else d2
            pts = [
                [day_ns(day, batch_i * points_per + j), float(j % 50)]
                for j in range(points_per)
            ]
            self.post_cmds([{
                "name": "layout_files",
                "kind": "gauge",
                "labels": {"job": "files", "instance": f"extra-{batch_i}"},
                "points": pts,
            }])
            batch_i += 1
            closed_bytes = sum(
                b for _, _, b in self.live_partition_stats(
                    "metric_samples", self._f_files_closed_days
                )
            )
            print(f"==> F-files extra batch={batch_i} closed_bytes={closed_bytes}")

        self.preconditions["AC-F2_bytes_before_merge"] = int(closed_bytes)
        idx_bytes = {
            t: sum(b for _, _, b in self.live_partition_stats(t, self._f_files_closed_days))
            for t in ("metric_series", "metric_postings", "metric_hist_samples")
        }
        self.preconditions["AC-F5_precondition_met"] = all(
            v >= idx_target for v in idx_bytes.values()
        )
        print(
            f"==> F-files done batches={batch_i} closed_sample_bytes={closed_bytes} "
            f"idx={idx_bytes} f5_pre={self.preconditions['AC-F5_precondition_met']}"
        )
        self.fixture_parts.append(
            f"files:{batch_i}b:{closed_bytes}B:{','.join(self._f_files_closed_days)}"
        )

    def load_fixtures(self) -> None:
        # Order: wide/tall/collapse are heavy; gold/hist/churn/sql smaller.
        # Pause heartbeat while loading to avoid DuckLake commit storms (503).
        self.pause_heartbeat()
        try:
            g3_scoped = os.environ.get("LAYOUT_G3_SCOPED", "0") == "1"
            if g3_scoped:
                # AC-G3 re-measure: only F-wide (skip tall/collapse/files wall-clock).
                print("==> LAYOUT_G3_SCOPED=1 — load F-wide only")
                self.load_f_wide()
                self.fixture_parts.append("g3_scoped:wide_only")
                self.blocker_notes.append(
                    "LAYOUT_G3_SCOPED=1 skipped tall/collapse/files; G3 timing only"
                )
                self.verify_fixture_counts(wide_only=True)
                return
            # F-snap first: empty-ish catalog so expiry can meet ≤ ceil(A/C)+20 (AC-N3).
            # Later tall/wide/files pin begin_snapshots and block count-bar expiry.
            self.run_f_snap(manage_heartbeat=False)
            self.load_f_sql()
            self.load_f_churn()
            self.load_f_gold()
            # F-hist → layout_latency in metric_hist_samples (AC-H1); must run before measure.
            self.load_f_hist()
            self.load_f_tall()
            self.load_f_wide()
            if self.fixture_profile == "release_full":
                # 90d span subsumes 30d for AC-Q5; avoid double-ingest of the overlap.
                # I=1 raw seed + materialize_query_grains fills metric_collapse_job_1h
                # (same SQL as collapse.rs) so AC-W3 sees J series over 90d honestly.
                self.load_f_collapse(int(self.scales["collapse_90d_days"]), "90d")
                self.fixture_parts.append(
                    f"collapse-30d:covered_by_90d_I={self.scales['collapse_i']}"
                )
            else:
                self.load_f_collapse(int(self.scales["collapse_days"]), "30d")
                # pr_floor: skip full 90d collapse wall-clock; AC-W3 stays fail until release_full.
                self.fixture_parts.append("collapse-90d:skipped_pr_floor")
                self.blocker_notes.append(
                    "pr_floor skipped F-collapse-90d full load (wall-clock); AC-W3 expected fail"
                )
            # F-files required for AC-F1/F2/F4/F5 (both profiles).
            self.load_f_files()
            # Re-assert F-hist after heavy F-files hist bulk so AC-H1 still sees
            # layout_latency (EVAL_END day) without relying on a full-table JOIN.
            self.load_f_hist(verify=True)
            self.verify_fixture_counts()
            self._raw_before_downsample = self.sql_scalar(
                f"SELECT count(*) FROM {qtable('metric_samples')}"
            )
            self.materialize_query_grains()
            self._ladder_after_first = self.ladder_counts()
        finally:
            self.resume_heartbeat()

    def pause_heartbeat(self) -> None:
        pid_file = Path(
            os.environ.get(
                "LAYOUT_SENDER_PID_FILE",
                "/tmp/thelake-metrics-layout/heartbeat.pid",
            )
        )
        if not pid_file.is_file():
            return
        try:
            pid = int(pid_file.read_text().strip())
        except (OSError, ValueError):
            return
        print(f"==> pausing heartbeat sender pid={pid}")
        try:
            os.kill(pid, 15)
        except OSError:
            return
        for _ in range(40):
            try:
                os.kill(pid, 0)
            except OSError:
                break
            time.sleep(0.1)
        try:
            os.kill(pid, 9)
        except OSError:
            pass
        try:
            pid_file.unlink(missing_ok=True)
        except OSError:
            pass
        os.environ["LAYOUT_SENDER_ALIVE"] = "0"

    def resume_heartbeat(self) -> None:
        bin_path = fixture_bin()
        if not bin_path.is_file():
            print("==> WARN: cannot resume heartbeat; fixture bin missing", file=sys.stderr)
            return
        pid_file = Path(
            os.environ.get(
                "LAYOUT_SENDER_PID_FILE",
                "/tmp/thelake-metrics-layout/heartbeat.pid",
            )
        )
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        target = Path(
            os.environ.get("CARGO_TARGET_DIR", Path.home() / ".cache" / "thelake" / "target")
        )
        duck_so = next(target.glob("duckdb-download/**/libduckdb.so*"), None)
        if duck_so is not None:
            libdir = str(duck_so.parent)
            prev = env.get("LD_LIBRARY_PATH", "")
            env["LD_LIBRARY_PATH"] = f"{libdir}:{prev}" if prev else libdir
        print("==> resuming heartbeat sender")
        proc = subprocess.Popen(
            [str(bin_path), "--url", self.base, "--token", self.token, "--heartbeat-secs", "1"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        pid_file.write_text(str(proc.pid) + "\n", encoding="utf-8")
        os.environ["LAYOUT_SENDER_ALIVE"] = "1"
        time.sleep(1.5)

    def verify_fixture_counts(self, *, wide_only: bool = False) -> None:
        want_wide = int(self.scales["wide_n"])
        code, doc, _ = self.client.sql(
            f"SELECT count(*) FROM {qtable('metric_series')} "
            f"WHERE metric_name = 'layout_wide'"
        )
        wide = int((doc.get("rows") or [[0]])[0][0] or 0) if code == 200 else -1
        if wide_only:
            print(f"==> fixture verify layout_wide={wide} want={want_wide} (g3_scoped)")
            if wide < want_wide:
                raise SystemExit(
                    f"fixture verify failed: layout_wide={wide} want>={want_wide} "
                    f"(http={code})"
                )
            return
        code2, doc2, _ = self.client.sql(
            f"SELECT count(*) FROM {qtable('metric_series')} "
            f"WHERE metric_name = 'layout_tall'"
        )
        tall = int((doc2.get("rows") or [[0]])[0][0] or 0) if code2 == 200 else -1
        print(f"==> fixture verify layout_wide={wide} want={want_wide} layout_tall={tall}")
        if wide < want_wide or tall < 1:
            raise SystemExit(
                f"fixture verify failed: layout_wide={wide} want>={want_wide} "
                f"layout_tall={tall} (http wide={code} tall={code2})"
            )

    def materialize_query_grains(self) -> None:
        """Fill 5m/1h/collapse so long-window ACs see points (AC-Q2/Q5/W5).

        Uses the same closed-hour + watermark ladder as Softprobe maintenance (§7.2).
        SQL-API DML is wrapped in BEGIN…COMMIT by the runtime so Prom workers see
        catalog snapshots (orphan parquet alone is not enough).
        """
        cat = LAYOUT_SQL_SCHEMA
        print(f"==> materialize 5m/1h/collapse into {cat}")
        # Match src/compaction/downsample.rs + collapse.rs (EVAL_END fixtures << now()).
        sqls = [
            (
                "5m",
                f"""
INSERT INTO {cat}.metric_samples_5m
  (series_id, window_ts, record_date, count, sum, min, max, last, last_ts)
SELECT
  series_id,
  time_bucket(INTERVAL '5 minutes', timestamp) AS window_ts,
  CAST(time_bucket(INTERVAL '5 minutes', timestamp) AS DATE) AS record_date,
  count(*)::UBIGINT AS count,
  sum(value) AS sum,
  min(value) AS min,
  max(value) AS max,
  arg_max(value, timestamp) AS last,
  max(timestamp) AS last_ts
FROM {cat}.metric_samples
WHERE timestamp < now() - INTERVAL '2 hours'
  AND time_bucket(INTERVAL '5 minutes', timestamp) < date_trunc('hour', now())
  AND time_bucket(INTERVAL '5 minutes', timestamp) >
      (SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM {cat}.metric_samples_5m)
GROUP BY series_id, time_bucket(INTERVAL '5 minutes', timestamp)
""",
            ),
            (
                "1h_from_raw",
                f"""
INSERT INTO {cat}.metric_samples_1h
  (series_id, window_ts, record_date, count, sum, min, max, last, last_ts)
SELECT
  series_id,
  time_bucket(INTERVAL '1 hour', timestamp) AS window_ts,
  CAST(time_bucket(INTERVAL '1 hour', timestamp) AS DATE) AS record_date,
  count(*)::UBIGINT AS count,
  sum(value) AS sum,
  min(value) AS min,
  max(value) AS max,
  arg_max(value, timestamp) AS last,
  max(timestamp) AS last_ts
FROM {cat}.metric_samples
WHERE timestamp < now() - INTERVAL '24 hours'
  AND time_bucket(INTERVAL '1 hour', timestamp) >
      (SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM {cat}.metric_samples_1h)
GROUP BY series_id, time_bucket(INTERVAL '1 hour', timestamp)
""",
            ),
            (
                "collapse_from_1h",
                f"""
INSERT INTO {cat}.metric_collapse_job_1h
  (metric_name, job, window_ts, record_date, count, sum, min, max, last)
SELECT
  s.metric_name,
  p.label_value AS job,
  h.window_ts,
  h.record_date,
  sum(h.count)::UBIGINT AS count,
  sum(h.sum) AS sum,
  min(h.min) AS min,
  max(h.max) AS max,
  sum(h.last) AS last
FROM {cat}.metric_samples_1h h
JOIN {cat}.metric_series s
  ON h.series_id = s.series_id AND h.record_date = s.record_date
JOIN {cat}.metric_postings p
  ON p.series_id = h.series_id AND p.record_date = h.record_date
 AND p.label_name = 'job'
WHERE h.window_ts >
      (SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM {cat}.metric_collapse_job_1h)
GROUP BY s.metric_name, p.label_value, h.window_ts, h.record_date
""",
            ),
            (
                "collapse_from_raw",
                f"""
INSERT INTO {cat}.metric_collapse_job_1h
  (metric_name, job, window_ts, record_date, count, sum, min, max, last)
SELECT
  s.metric_name,
  p.label_value AS job,
  time_bucket(INTERVAL '1 hour', sm.timestamp) AS window_ts,
  CAST(time_bucket(INTERVAL '1 hour', sm.timestamp) AS DATE) AS record_date,
  count(*)::UBIGINT AS count,
  sum(sm.value) AS sum,
  min(sm.value) AS min,
  max(sm.value) AS max,
  arg_max(sm.value, sm.timestamp) AS last
FROM {cat}.metric_samples sm
JOIN {cat}.metric_series s
  ON sm.series_id = s.series_id AND sm.record_date = s.record_date
JOIN {cat}.metric_postings p
  ON p.series_id = sm.series_id AND p.record_date = sm.record_date
 AND p.label_name = 'job'
WHERE sm.timestamp < now() - INTERVAL '24 hours'
  AND time_bucket(INTERVAL '1 hour', sm.timestamp) >
      (SELECT coalesce(max(window_ts), TIMESTAMPTZ '-infinity') FROM {cat}.metric_collapse_job_1h)
GROUP BY s.metric_name, p.label_value, time_bucket(INTERVAL '1 hour', sm.timestamp)
""",
            ),
        ]
        for label, sql in sqls:
            old_timeout = self.client.timeout
            self.client.timeout = 300.0
            try:
                code, doc, ms = self.client.sql(sql.strip())
            finally:
                self.client.timeout = old_timeout
            note = doc.get("error") or doc.get("message") or f"rows={doc.get('row_count')}"
            print(f"==> grain step {label} http={code} ms={ms:.0f} {note}")
            if code not in (200, 201) and "already exists" not in str(note).lower():
                self.blocker_notes.append(f"materialize {label} http={code}: {note}")

        # Catalog-visible counts (must be >0 after COMMIT).
        for label, table in (
            ("samples_5m", "metric_samples_5m"),
            ("samples_1h", "metric_samples_1h"),
            ("collapse_1h", "metric_collapse_job_1h"),
        ):
            c, d, _ = self.client.sql(f"SELECT count(*) FROM {cat}.{table}")
            n = int((d.get("rows") or [[0]])[0][0] or 0) if c == 200 else -1
            print(f"==> grain verify {label} count={n} http={c}")
            if n <= 0:
                self.blocker_notes.append(f"materialize left {table} count={n}")

        # Metric-shaped proof for Q2 / Q5 at EVAL_END (pr_floor 30d).
        c, d, _ = self.client.sql(
            f"SELECT count(*) FROM {cat}.metric_samples_1h h "
            f"JOIN {cat}.metric_series s ON h.series_id = s.series_id "
            f"AND h.record_date = s.record_date WHERE s.metric_name = 'layout_tall'"
        )
        tall_1h = int((d.get("rows") or [[0]])[0][0] or 0) if c == 200 else -1
        c2, d2, _ = self.client.sql(
            f"SELECT count(DISTINCT job) FROM {cat}.metric_collapse_job_1h "
            f"WHERE metric_name = 'layout_http'"
        )
        collapse_j = int((d2.get("rows") or [[0]])[0][0] or 0) if c2 == 200 else -1
        print(f"==> grain shaped tall_1h={tall_1h} collapse_jobs={collapse_j}")
        want_tall = max(600, int(self.scales["tall_days"]) * 24 - 48)
        if tall_1h < want_tall:
            self.blocker_notes.append(
                f"tall 1h catalog rows={tall_1h} want>={want_tall}"
            )
        want_j = int(self.scales["collapse_j"])
        if collapse_j < want_j:
            self.blocker_notes.append(
                f"collapse jobs={collapse_j} want>={want_j}"
            )

        end = float(EVAL_END_S)
        code, doc, ms = self.client.query_range(
            "layout_tall", end - 30 * 86400, end, "1h"
        )
        pts = 0
        if doc.get("data", {}).get("result"):
            pts = sum(len(r.get("values") or []) for r in doc["data"]["result"])
        print(f"==> grain prom probe layout_tall 30d points={pts} http={code} ms={ms:.0f}")
        if pts < 600:
            self.blocker_notes.append(f"prom 1h probe points={pts} (want>=600)")
        code2, doc2, ms2 = self.client.query_range(
            "sum by (job) (rate(layout_http[5m]))", end - 30 * 86400, end, "1h"
        )
        sn = len((doc2.get("data") or {}).get("result") or [])
        print(
            f"==> grain prom probe collapse series={sn} want={want_j} "
            f"http={code2} ms={ms2:.0f}"
        )
        if sn < want_j:
            self.blocker_notes.append(f"prom collapse probe series={sn} want>={want_j}")

    def measure_unit_acs(self) -> None:
        profile_flag = os.environ.get("CARGO_PROFILE_FLAG", "").strip()
        mapping = [
            ("AC-D3", "prom_backend_is_ducklake_no_sidecar_writers"),
            ("AC-N1", "default_max_snapshot_age_seconds_is_one_minute"),
            ("AC-N2", "expire_snapshots_sql_honors_seconds"),
            ("AC-N5", "cleanup_old_files_sql_honors_seconds"),
            ("AC-F3", "twcs_partition_key_is_record_date_only"),
            ("AC-F6", "twcs_merge_does_not_cross_record_date"),
            ("AC-F7", "default_data_inlining_row_limit_is_zero"),
            ("AC-F8", "closed_day_file_bar_allows_two_only_over_target"),
            ("AC-M1", "maintenance_tables_include_metric_family"),
            ("AC-W1", "max_query_range_is_unlimited"),
            ("AC-Q7", "resolve_and_samples_sql_uses_postings_not_fat"),
            ("AC-H2", "hist_selector_always_uses_hist_table"),
            ("AC-H5", "hist_prom_sql_uses_hist_table_for_mid_and_long_windows"),
            ("AC-H6", "window_series_type_grain_matrix"),
            ("AC-C1", "churn_pod_values_differ_by_record_date"),
            ("AC-C4", "churn_dead_pod_absent_from_today_postings"),
            ("AC-S1", "skinny_samples_smaller_than_fat_and_no_variant"),
        ]
        if os.environ.get("LAYOUT_SKIP_UNIT", "0") == "1":
            for ac_id, _ in mapping:
                self.mark(ac_id, pass_=False, notes="LAYOUT_SKIP_UNIT=1")
            print("==> unit cargo batch SKIPPED (LAYOUT_SKIP_UNIT=1)")
        else:
            print(f"==> unit cargo batch ({len(mapping)} filters)")
            results = run_cargo_units(mapping, profile_flag)
            for ac_id, (ok, note) in results.items():
                self.mark(ac_id, pass_=ok, notes=f"cargo: {note[:200]}")

        # AC-S3: grafana-manual-up must build release
        up = (ROOT / "scripts" / "grafana-manual-up.sh").read_text(encoding="utf-8")
        uses_release = (
            "build-release" in up
            or "cargo build --release" in up
            or "cargo build -q --release" in up
            or "CARGO_PROFILE_FLAG=--release" in up
            or ("--release" in up and "cargo build" in up)
        )
        debug_bin = (
            "target/debug/softprobe-runtime" in up
            or (
                "cargo build" in up
                and "--release" not in up
                and "build-release" not in up
            )
        )
        self.mark(
            "AC-S3",
            pass_=uses_release and not debug_bin,
            notes="grafana-manual-up.sh must build release binary",
        )

    def measure_catalog_acs(self) -> None:
        # AC-D1 tables exist in tenant DuckLake metadata
        code, doc, _ = self.client.sql(
            "SELECT t.table_name FROM __ducklake_metadata_softprobe.ducklake_table t "
            "WHERE t.end_snapshot IS NULL AND t.table_name IN ("
            "'metric_series','metric_postings','metric_samples','metric_hist_samples')"
        )
        names = set()
        if code != 200:
            # catalog alias may differ — try information_schema / show tables
            code2, doc2, _ = self.client.sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name IN ("
                "'metric_series','metric_postings','metric_samples','metric_hist_samples')"
            )
            code, doc = code2, doc2
        if code == 200:
            for row in doc.get("rows") or []:
                if row:
                    names.add(str(row[0]))
        need = {
            "metric_series",
            "metric_postings",
            "metric_samples",
            "metric_hist_samples",
        }
        self.mark(
            "AC-D1",
            pass_=need.issubset(names),
            notes=f"tables={sorted(names)} http={code}",
        )

        # AC-D2 partition + sort info non-empty for each
        ok_d2 = True
        notes_d2 = []
        for t in need:
            c1, d1, _ = self.client.sql(
                "SELECT count(*) FROM __ducklake_metadata_softprobe.ducklake_partition_info info "
                "JOIN __ducklake_metadata_softprobe.ducklake_table t ON info.table_id = t.table_id "
                f"WHERE t.table_name = '{t}' AND t.end_snapshot IS NULL"
            )
            c2, d2, _ = self.client.sql(
                "SELECT count(*) FROM __ducklake_metadata_softprobe.ducklake_sort_info info "
                "JOIN __ducklake_metadata_softprobe.ducklake_table t ON info.table_id = t.table_id "
                f"WHERE t.table_name = '{t}' AND t.end_snapshot IS NULL"
            )
            n1 = int((d1.get("rows") or [[0]])[0][0] or 0) if c1 == 200 else 0
            n2 = int((d2.get("rows") or [[0]])[0][0] or 0) if c2 == 200 else 0
            if n1 < 1 or n2 < 1:
                ok_d2 = False
            notes_d2.append(f"{t}:part={n1},sort={n2}")
        self.mark("AC-D2", pass_=ok_d2, notes="; ".join(notes_d2))

        # AC-D4 union_metrics / committed_metrics
        c_u, d_u, _ = self.client.sql(
            "SELECT metric_name, value FROM union_metrics "
            "WHERE metric_name = 'layout_sql_gauge' LIMIT 5"
        )
        c_c, d_c, _ = self.client.sql(
            "SELECT metric_name, value FROM committed_metrics "
            "WHERE metric_name = 'layout_sql_gauge' LIMIT 5"
        )
        ok_d4 = (
            c_u == 200
            and c_c == 200
            and (d_u.get("row_count") or 0) >= 1
            and (d_c.get("row_count") or 0) >= 1
        )
        self.mark(
            "AC-D4",
            pass_=ok_d4,
            notes=f"union_http={c_u} rows={d_u.get('row_count')} committed_http={c_c}",
        )

    def measure_query_acs(self) -> None:
        end = float(EVAL_END_S)
        scales = self.scales
        j = int(scales["collapse_j"])

        # AC-Q0: heartbeat progress during measures
        before = self.heartbeat_count()
        time.sleep(2.5)
        after = self.heartbeat_count()
        sender_alive = os.environ.get("LAYOUT_SENDER_ALIVE", "0") == "1"
        q0_ok = sender_alive and after >= before + 1
        self.preconditions["sender_alive"] = sender_alive and after >= before + 1
        self.mark(
            "AC-Q0",
            pass_=q0_ok,
            notes=f"hb before={before} after={after} sender_alive={sender_alive}",
        )

        def timed_range(
            query: str, start: float, end_s: float, step: str, repeats: int
        ) -> tuple[list[float], list[dict[str, Any]], list[int]]:
            times = []
            docs = []
            codes = []
            for _ in range(repeats):
                code, doc, ms = self.client.query_range(query, start, end_s, step)
                times.append(ms)
                docs.append(doc)
                codes.append(code)
            return times, docs, codes

        # AC-Q1
        reps = int(scales["q_tall_short_repeats"])
        t, docs, codes = timed_range(
            "layout_tall", end - 1800, end, "15s", reps
        )
        p95 = pct(t, 95)
        series_n = 0
        if docs and docs[0].get("data", {}).get("result"):
            series_n = len(docs[0]["data"]["result"])
        ok = (
            all(c == 200 for c in codes)
            and all((d.get("status") == "success") for d in docs)
            and p95 is not None
            and p95 <= 1000
            and series_n >= 1
        )
        self.mark(
            "AC-Q1",
            pass_=ok,
            p95_ms=p95,
            softprobe_p95_ms=p95,
            notes=f"repeats={reps} codes={set(codes)} series={series_n}",
        )

        # AC-Q2 / AC-W2
        reps_l = int(scales["q_long_repeats"])
        t, docs, codes = timed_range(
            "layout_tall", end - 30 * 86400, end, "1h", reps_l
        )
        p95 = pct(t, 95)
        series_n = 0
        points = 0
        if docs and docs[0].get("data", {}).get("result"):
            series_n = len(docs[0]["data"]["result"])
            points = sum(len(r.get("values") or []) for r in docs[0]["data"]["result"])
        # EXPLAIN check via SQL if possible
        explain_ok = None
        c_e, d_e, _ = self.client.sql(
            "SELECT 1 WHERE 1=0"
        )  # placeholder — planner explain not exposed; leave None unless we find grain
        del c_e, d_e
        ok = (
            all(c == 200 for c in codes)
            and p95 is not None
            and p95 <= 3000
            and series_n >= 1
            and points >= 600
        )
        self.mark(
            "AC-Q2",
            pass_=ok,
            p95_ms=p95,
            softprobe_p95_ms=p95,
            explain_ok=explain_ok,
            notes=f"series={series_n} points={points}",
        )
        self.mark(
            "AC-W2",
            pass_=all(c == 200 for c in codes)
            and not any(
                "range exceeds" in json.dumps(d).lower() for d in docs
            ),
            notes="30d accept",
        )

        # AC-Q3 — warm posting/scan caches before p95 (G3). Extra warmups so one
        # cold DuckDB stall does not dominate p95 of a short q_long_repeats set.
        for _ in range(3):
            self.client.query_range(
                '{__name__="layout_wide",instance="i-1"}', end - 1800, end, "15s"
            )
        t, docs, codes = timed_range(
            '{__name__="layout_wide",instance="i-1"}',
            end - 1800,
            end,
            "15s",
            reps_l,
        )
        p95 = pct(t, 95)
        sn = 0
        if docs and docs[0].get("data", {}).get("result") is not None:
            sn = len(docs[0]["data"]["result"])
        ok = all(c == 200 for c in codes) and p95 is not None and p95 <= 2000 and sn == 1
        self.mark(
            "AC-Q3",
            pass_=ok,
            p95_ms=p95,
            softprobe_p95_ms=p95,
            notes=f"series={sn}",
        )

        # AC-Q4
        t0 = time.perf_counter()
        code, doc, ms = self.client.query_range(
            '{__name__="layout_wide"}', end - 1800, end, "15s"
        )
        body = json.dumps(doc).lower()
        ok = (
            code >= 400
            and "limit_exceeded" in body
            and "max_series" in body
            and ms < 5000
        ) or (
            doc.get("status") == "error"
            and "limit_exceeded" in body
            and ms < 5000
        )
        self.mark(
            "AC-Q4",
            pass_=ok,
            p95_ms=ms,
            notes=f"http={code} ms={ms:.0f} body_snip={body[:180]}",
        )

        # AC-Q5
        t, docs, codes = timed_range(
            "sum by (job) (rate(layout_http[5m]))",
            end - 30 * 86400,
            end,
            "1h",
            reps_l,
        )
        p95 = pct(t, 95)
        sn = 0
        if docs and docs[0].get("data", {}).get("result") is not None:
            sn = len(docs[0]["data"]["result"])
        ok = (
            all(c == 200 for c in codes)
            and p95 is not None
            and p95 <= 5000
            and sn == j
        )
        self.mark(
            "AC-Q5",
            pass_=ok,
            p95_ms=p95,
            softprobe_p95_ms=p95,
            fixture_scale={"J": j},
            notes=f"series={sn} want_J={j}",
        )

        # AC-Q6 — warm name-values cache before p95 (G4 ratio).
        reps_d = int(scales["q_discover_repeats"])
        for _ in range(3):
            self.client.label_values("__name__")
        times = []
        codes = []
        for _ in range(reps_d):
            code, doc, ms = self.client.label_values("__name__")
            times.append(ms)
            codes.append(code)
        p95 = pct(times, 95)
        ok = all(c == 200 for c in codes) and p95 is not None and p95 <= 500
        self.mark(
            "AC-Q6",
            pass_=ok,
            p95_ms=p95,
            softprobe_p95_ms=p95,
            notes=f"repeats={reps_d}",
        )

        # AC-Q7 measured in unit cargo batch

        # AC-Q8 GOLD
        gold_reps = int(scales["gold_repeats"])
        gold_fail = []
        gold_p95s = []
        for expr in GOLD_EXPRS:
            t, docs, codes = timed_range(expr, end - 1800, end, "60s", gold_reps)
            p95 = pct(t, 95)
            gold_p95s.append(p95 or 999999)
            if not (
                all(c == 200 for c in codes)
                and all(d.get("status") == "success" for d in docs)
                and p95 is not None
                and p95 <= 5000
            ):
                gold_fail.append(f"{expr[:40]} p95={p95} codes={set(codes)}")
        self.mark(
            "AC-Q8",
            pass_=len(gold_fail) == 0,
            p95_ms=max(gold_p95s) if gold_p95s else None,
            notes="; ".join(gold_fail[:5]) or "all 15 exprs ok",
        )

        # AC-Q9: forced downsample/merge while T-Q1 p95 ≤ 5s (ingest heartbeat on)
        q9_times: list[float] = []
        q9_codes: list[int] = []
        maint_notes: list[str] = []

        def _maint_wave() -> list[str]:
            notes = self.force_twcs_merge(["metric_samples", "metric_postings"])
            notes.extend(self.run_downsample_pass())
            return notes

        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(_maint_wave)
            # Overlap interactive Q1 with maintenance.
            for _ in range(max(8, int(scales["q_tall_short_repeats"]) // 2)):
                code, doc, ms = self.client.query_range(
                    "layout_tall", end - 1800, end, "15s"
                )
                q9_times.append(ms)
                q9_codes.append(code)
                if not fut.done():
                    time.sleep(0.05)
            try:
                maint_notes = fut.result(timeout=600)
            except Exception as e:
                maint_notes = [f"maint_error:{e}"]
        p95_q9 = pct(q9_times, 95)
        q9_ok = (
            all(c == 200 for c in q9_codes)
            and p95_q9 is not None
            and p95_q9 <= 5000
            and os.environ.get("LAYOUT_SENDER_ALIVE", "0") == "1"
        )
        self.mark(
            "AC-Q9",
            pass_=q9_ok,
            p95_ms=p95_q9,
            softprobe_p95_ms=p95_q9,
            notes=(
                f"p95={p95_q9} codes={set(q9_codes)} maint={';'.join(maint_notes)[:180]}"
            ),
        )

        # AC-H1 / H2 — Prom `_count` or `_bucket` over F-hist window.
        t, docs, codes = timed_range(
            "layout_latency_count", end - 1800, end, "15s", reps_l
        )
        p95 = pct(t, 95)
        hist_n, samp_n, series_n, hist_diag = self.count_layout_latency_rows()
        prom_series = 0
        prom_points = 0
        prom_metric = "layout_latency_count"
        if docs and isinstance(docs[0].get("data"), dict):
            result = docs[0]["data"].get("result") or []
            prom_series = len(result)
            prom_points = sum(len(r.get("values") or []) for r in result)
        if prom_series < 1 or prom_points < 1:
            t_b, docs_b, codes_b = timed_range(
                "layout_latency_bucket", end - 1800, end, "15s", max(1, reps_l // 2)
            )
            p95_b = pct(t_b, 95)
            if docs_b and isinstance(docs_b[0].get("data"), dict):
                result_b = docs_b[0]["data"].get("result") or []
                ps_b = len(result_b)
                pp_b = sum(len(r.get("values") or []) for r in result_b)
                if ps_b >= 1 and pp_b >= 1 and all(c == 200 for c in codes_b):
                    prom_metric = "layout_latency_bucket"
                    prom_series, prom_points = ps_b, pp_b
                    codes = codes_b
                    if p95_b is not None:
                        p95 = p95_b
        ok_h1 = (
            hist_n > 0
            and samp_n == 0
            and series_n > 0
            and all(c == 200 for c in codes)
            and p95 is not None
            and p95 <= 2000
            and prom_series >= 1
            and prom_points >= 1
        )
        self.mark(
            "AC-H1",
            pass_=ok_h1,
            p95_ms=p95,
            softprobe_p95_ms=p95,
            notes=(
                f"hist_rows={hist_n} sample_rows={samp_n} series={series_n} "
                f"prom={prom_metric} prom_series={prom_series} prom_points={prom_points} "
                f"({hist_diag})"
            ),
        )
        # AC-H2 / AC-C1 / AC-H5 / AC-H6 measured in unit cargo batch

        # AC-H3 / H4 — same F-hist points (last ~30m of EVAL_END) must remain
        # visible through mid/long Prom windows (regression: >2h used empty 1h grain).
        def mark_hist_window(ac_id: str, range_s: int, step: str, p95_budget_ms: float) -> None:
            t_w, docs_w, codes_w = timed_range(
                "layout_latency_count", end - range_s, end, step, max(1, reps_l // 2)
            )
            p95_w = pct(t_w, 95)
            series_w = 0
            points_w = 0
            if docs_w and isinstance(docs_w[0].get("data"), dict):
                result_w = docs_w[0]["data"].get("result") or []
                series_w = len(result_w)
                points_w = sum(len(r.get("values") or []) for r in result_w)
            ok_w = (
                hist_n > 0
                and all(c == 200 for c in codes_w)
                and p95_w is not None
                and p95_w <= p95_budget_ms
                and series_w >= 1
                and points_w >= 1
            )
            self.mark(
                ac_id,
                pass_=ok_w,
                p95_ms=p95_w,
                softprobe_p95_ms=p95_w,
                notes=(
                    f"range_s={range_s} series={series_w} points={points_w} "
                    f"hist_rows={hist_n} p95={p95_w}"
                ),
            )

        mark_hist_window("AC-H3", 3 * 3600, "20s", 3000)
        mark_hist_window("AC-H4", 24 * 3600, "60s", 5000)

        n = int(scales["wide_n"])
        c2, d2, _ = self.client.sql(
            f"SELECT count(*) FROM {qtable('metric_series')} "
            f"WHERE metric_name = 'layout_wide'"
        )
        wide_count = int((d2.get("rows") or [[0]])[0][0] or 0) if c2 == 200 else 0
        self.mark(
            "AC-C2",
            pass_=wide_count == n,
            fixture_scale={"N": n, "count": wide_count},
            notes=f"count={wide_count} want={n}",
        )

        # AC-C3: tall query only tall series — check result metric labels
        code, doc, _ = self.client.query_range(
            "layout_tall", end - 1800, end, "15s"
        )
        bad = False
        if doc.get("data", {}).get("result"):
            for r in doc["data"]["result"]:
                metric = r.get("metric") or {}
                if metric.get("__name__") not in (None, "layout_tall") and "layout_wide" in str(
                    metric
                ):
                    bad = True
        self.mark(
            "AC-C3",
            pass_=code == 200 and not bad and bool(doc.get("data", {}).get("result")),
            notes="tall query_range series isolation",
        )

        # AC-C4 measured in unit cargo batch

        # Long window W1: unit + HTTP 180d/365d must not range-reject
        code, doc, _ = self.client.query_range(
            "layout_tall", end - 180 * 86400, end, "1h"
        )
        body = json.dumps(doc).lower()
        rejected = "range exceeds" in body or "max_query_range" in body
        code2, doc2, _ = self.client.query_range(
            "layout_tall", end - 365 * 86400, end, "1h"
        )
        body2 = json.dumps(doc2).lower()
        rejected2 = "range exceeds" in body2 or "max_query_range" in body2
        unit_ok = self.acs["AC-W1"].pass_
        self.mark(
            "AC-W1",
            pass_=unit_ok
            and not rejected
            and not rejected2
            and code in (200, 400)
            and code2 in (200, 400),
            notes=f"180d http={code} 365d http={code2} unit={unit_ok}",
        )

        # AC-W3
        t, docs, codes = timed_range(
            "sum by (job) (rate(layout_http[5m]))",
            end - 90 * 86400,
            end,
            "1h",
            reps_l,
        )
        p95 = pct(t, 95)
        sn = len((docs[0].get("data") or {}).get("result") or []) if docs else 0
        self.mark(
            "AC-W3",
            pass_=all(c == 200 for c in codes)
            and p95 is not None
            and p95 <= 5000
            and sn == j,
            p95_ms=p95,
            notes=f"series={sn} want={j}",
        )

        # AC-W4 — same EVAL_END anchor as AC-Q4 (F-wide lives on that day only).
        code, doc, ms = self.client.query_range(
            '{__name__="layout_wide"}', end - 31 * 86400, end, "1h"
        )
        body = json.dumps(doc).lower()
        self.mark(
            "AC-W4",
            pass_=(
                ("limit_exceeded" in body or code >= 400 or doc.get("status") == "error")
                and ms < 5000
            ),
            p95_ms=ms,
            notes=f"http={code} ms={ms:.0f} body_snip={body[:120]}",
        )

        # AC-W5 — 90d window on 1h grain; point floor scales with loaded tall days
        # (pr_floor tall_days=30 ⇒ ~720 pts; release_full has ≥90d ⇒ 1800).
        t, docs, codes = timed_range(
            "layout_tall", end - 90 * 86400, end, "1h", reps_l
        )
        p95 = pct(t, 95)
        points = 0
        if docs and docs[0].get("data", {}).get("result"):
            points = sum(len(r.get("values") or []) for r in docs[0]["data"]["result"])
        want_w5 = min(1800, max(600, int(scales["tall_days"]) * 24 - 48))
        self.mark(
            "AC-W5",
            pass_=all(c == 200 for c in codes)
            and p95 is not None
            and p95 <= 3000
            and points >= want_w5,
            p95_ms=p95,
            notes=f"points={points} want>={want_w5}",
        )

        # AC-W6
        t, docs, codes = timed_range(
            "layout_tall", end - 180 * 86400, end, "1h", reps_l
        )
        p95 = pct(t, 95)
        body = json.dumps(docs[0]).lower() if docs else ""
        self.mark(
            "AC-W6",
            pass_=all(c == 200 for c in codes)
            and "range exceeds" not in body
            and p95 is not None
            and p95 <= 3000,
            p95_ms=p95,
            notes=f"tall_days_loaded={scales['tall_days']}",
        )

    def measure_remaining(self) -> None:
        # --- AC-S2 / AC-M2: downsample additive + second pass watermark ---
        before = self._raw_before_downsample
        after_first = self._ladder_after_first.get("metric_samples", -1)
        if after_first < 0:
            after_first = self.sql_scalar(
                f"SELECT count(*) FROM {qtable('metric_samples')}"
            )
        s2_ok = before >= 0 and after_first >= before
        self.mark(
            "AC-S2",
            pass_=s2_ok,
            notes=f"raw_before={before} raw_after_downsample={after_first}",
        )

        # Spec AC-M2: second pass with *no new closed windows* → 0 new grain rows.
        # Pause heartbeat, catch-up any windows closed while ingest was on during Q*,
        # then baseline and run the true second pass (watermark incremental).
        self.pause_heartbeat()
        try:
            _ = self.run_downsample_pass()
            first = self.ladder_counts()
            pass2_notes = self.run_downsample_pass()
            second = self.ladder_counts()
        finally:
            self.resume_heartbeat()
        m2_ok = all(
            second.get(t, -1) == first.get(t, -2)
            for t in ("metric_samples_5m", "metric_samples_1h", "metric_collapse_job_1h")
        ) and all(first.get(t, -1) >= 0 for t in ("metric_samples_5m", "metric_samples_1h"))
        self.mark(
            "AC-M2",
            pass_=m2_ok,
            notes=(
                f"first={first} second={second} pass2={';'.join(pass2_notes)[:120]}"
            ),
        )

        # --- F-files TWCS bars (AC-F1/F2/F4/F5) ---
        closed = list(self._f_files_closed_days)
        today = self._f_files_today or datetime.now(timezone.utc).date().isoformat()
        if not closed:
            self.mark("AC-F1", pass_=False, notes="F-files days missing")
            self.mark("AC-F2", pass_=False, notes="F-files days missing")
            self.mark("AC-F4", pass_=False, notes="F-files days missing")
            self.mark("AC-F5", pass_=False, notes="F-files days missing")
            self.mark("AC-F8", pass_=False, notes="F-files days missing")
        else:
            bytes_before = int(self.preconditions.get("AC-F2_bytes_before_merge") or 0)
            if bytes_before <= 0:
                bytes_before = sum(
                    b for _, _, b in self.live_partition_stats("metric_samples", closed)
                )
                self.preconditions["AC-F2_bytes_before_merge"] = bytes_before

            merge_notes = self.force_twcs_merge()
            print(f"==> F-files merge: {merge_notes}")

            days_retained = max(1, len(closed))
            closed_limit = 2 * days_retained
            sample_stats = self.live_partition_stats("metric_samples", closed)
            closed_files = sum(c for _, c, _ in sample_stats)
            f1_ok = closed_files <= closed_limit and closed_files >= 1
            self.mark(
                "AC-F1",
                pass_=f1_ok,
                fixture_scale={"days_retained": days_retained, "closed_files": closed_files},
                notes=f"closed_files={closed_files} limit={closed_limit} stats={sample_stats}",
            )

            sizes = self.live_file_sizes("metric_samples", closed)
            median_sz = pct([float(s) for s in sizes], 50) if sizes else None
            f2_pre = bytes_before >= 16 * 1024 * 1024
            f2_ok = (
                f2_pre
                and median_sz is not None
                and median_sz >= 8 * 1024 * 1024
            )
            self.mark(
                "AC-F2",
                pass_=f2_ok,
                fixture_scale={
                    "bytes_before": bytes_before,
                    "median_after": median_sz,
                    "n_files": len(sizes),
                },
                notes=(
                    f"bytes_before={bytes_before} median={median_sz} "
                    f"sizes={sizes[:8]} pre={f2_pre}"
                ),
            )

            today_stats = self.live_partition_stats("metric_samples", [today])
            today_files = sum(c for _, c, _ in today_stats)
            f4_ok = today_files <= 20
            self.mark(
                "AC-F4",
                pass_=f4_ok,
                fixture_scale={"today_files": today_files},
                notes=f"today={today} files={today_files} stats={today_stats}",
            )

            f5_pre = bool(self.preconditions.get("AC-F5_precondition_met"))
            f5_notes = []
            f5_ok = f5_pre
            for t in ("metric_postings", "metric_series", "metric_hist_samples"):
                st = self.live_partition_stats(t, closed)
                nfiles = sum(c for _, c, _ in st)
                ok_t = nfiles <= closed_limit
                f5_ok = f5_ok and ok_t
                f5_notes.append(f"{t}:files={nfiles}/{closed_limit}")
            self.mark(
                "AC-F5",
                pass_=f5_ok,
                notes=f"precondition_met={f5_pre}; " + "; ".join(f5_notes),
            )

            f8_notes = []
            f8_ok = True
            for t in (
                "metric_samples",
                "metric_postings",
                "metric_series",
                "metric_hist_samples",
            ):
                st = self.live_partition_stats(t, closed)
                for day, nfiles, nbytes in st:
                    bar = nfiles == 1 or (nfiles == 2 and nbytes > 64 * 1024 * 1024)
                    day_sizes = self.live_file_sizes(t, [day])
                    median = pct([float(s) for s in day_sizes], 50) if day_sizes else None
                    size_ok = nbytes < 8 * 1024 * 1024 or (
                        median is not None and median >= 8 * 1024 * 1024
                    )
                    if not bar or not size_ok:
                        f8_ok = False
                    f8_notes.append(
                        f"{t}@{day}:files={nfiles} bytes={nbytes} median={median} bar={bar}"
                    )
            self.mark(
                "AC-F8",
                pass_=f8_ok and bool(closed),
                notes="; ".join(f8_notes)[:400],
            )

        # AC-N3/N4 already measured during early F-snap in load_fixtures.
        if (self.acs["AC-N3"].notes or "not measured") == "not measured":
            self.run_f_snap(manage_heartbeat=True)

        # AC-N6: after expire, live snapshot count ≤ 50 and none older than A+I.
        a_sec = 60
        i_sec = 60
        exp_code, exp_note = self.force_expire_snapshots(a_sec)
        meta = self.meta()
        snap_count = self.sql_scalar(f"SELECT count(*) FROM {meta}.ducklake_snapshot")
        old_snaps = self.sql_scalar(
            f"SELECT count(*) FROM {meta}.ducklake_snapshot "
            f"WHERE snapshot_time < now() - INTERVAL '{a_sec + i_sec} seconds'"
        )
        self.mark(
            "AC-N6",
            pass_=snap_count >= 0 and snap_count <= 50 and old_snaps == 0,
            fixture_scale={"snapshots": snap_count, "older_than_A_plus_I": old_snaps},
            notes=f"snaps={snap_count} older={old_snaps} expire={exp_code}:{exp_note[:80]}",
        )

        # AC-F7: skinny rows must live in Parquet (not catalog-only inlined).
        unit_f7 = bool(self.acs["AC-F7"].pass_)
        f7_notes = [f"unit_inlining_default_0={unit_f7}"]
        f7_ok = unit_f7
        for t in ("metric_samples", "metric_hist_samples", "metric_postings"):
            rows = self.sql_scalar(f"SELECT count(*) FROM {qtable(t)}")
            files = sum(c for _, c, _ in self.live_partition_stats(t))
            inlined = rows > 0 and files == 0
            if inlined:
                f7_ok = False
            f7_notes.append(f"{t}:rows={rows} parquet_files={files}")
        self.mark("AC-F7", pass_=f7_ok, notes="; ".join(f7_notes)[:400])

    def run_f_snap(self, *, manage_heartbeat: bool = True) -> None:
        """F-snap: ≥120 commits at C=1s, A=60, then expire; assert N3/N4."""
        a_sec = 60
        c_sec = 1  # minimum seconds between OTLP metric commits
        i_sec = 10
        commits = int(os.environ.get("LAYOUT_FSNAP_COMMITS", "120"))
        print(f"==> F-snap commits={commits} A={a_sec} C={c_sec} I={i_sec}")
        meta = self.meta()
        # Pause heartbeat so AC-N4 sample count is stable across expiry.
        if manage_heartbeat:
            self.pause_heartbeat()
        try:
            # Drop any pre-F-snap history so the count bar is commit_rate × A.
            pre_code, pre_note = self.force_expire_snapshots(1)
            print(f"==> F-snap pre-expire http={pre_code} {pre_note}")

            samples_before = self.sql_scalar(
                f"SELECT count(*) FROM {qtable('metric_samples')}"
            )
            t0 = time.time()
            for i in range(commits):
                now_ns = int(time.time() * 1_000_000_000)
                self.post_cmds([{
                    "name": "layout_snap",
                    "kind": "gauge",
                    "labels": {"job": "snap", "instance": "s0"},
                    "points": [[now_ns, float(i)]],
                }])
                elapsed = time.time() - t0
                target = float((i + 1) * c_sec)
                if elapsed < target:
                    time.sleep(target - elapsed)
            # Oldest commits must exceed A so expiry can reclaim them.
            # After ≥A seconds of commits, a short settle is enough.
            settle = max(2.0, float(a_sec) - (time.time() - t0) + 1.0)
            if settle > 0:
                print(f"==> F-snap settle {settle:.1f}s before expire")
                time.sleep(settle)
            samples_pre_expire = self.sql_scalar(
                f"SELECT count(*) FROM {qtable('metric_samples')}"
            )
            snaps_before = self.sql_scalar(
                f"SELECT count(*) FROM {meta}.ducklake_snapshot"
            )
            code, note = self.force_expire_snapshots(a_sec)
            print(f"==> F-snap expire http={code} {note} snaps_before={snaps_before}")
            time.sleep(1.0)
            code2, note2 = self.force_expire_snapshots(a_sec)
            print(f"==> F-snap expire2 http={code2} {note2}")
            # Explicitly expire any remaining ids older than A (timestamptz-correct).
            c_old, d_old, _ = self.client.sql(
                f"SELECT snapshot_id FROM {meta}.ducklake_snapshot "
                f"WHERE snapshot_time < now() - INTERVAL '{a_sec} seconds' "
                f"ORDER BY snapshot_id"
            )
            old_ids = []
            if c_old == 200:
                for row in d_old.get("rows") or []:
                    if row:
                        old_ids.append(int(row[0]))
            if old_ids:
                # DuckLake versions => [id, ...] — batch to keep CALL size sane.
                for i in range(0, len(old_ids), 200):
                    chunk = old_ids[i : i + 200]
                    lit = ", ".join(str(x) for x in chunk)
                    self.sql_exec(
                        f"CALL ducklake_expire_snapshots('{self.ducklake_catalog}', "
                        f"versions => [{lit}])",
                        timeout=120.0,
                    )
                print(f"==> F-snap expired {len(old_ids)} ids via versions=")
            self.sql_exec(
                f"CALL ducklake_cleanup_old_files('{self.ducklake_catalog}', "
                f"older_than => now() - INTERVAL '{a_sec} seconds')",
                timeout=120.0,
            )

            snap_count = self.sql_scalar(
                f"SELECT count(*) FROM {meta}.ducklake_snapshot"
            )
            old_snaps = self.sql_scalar(
                f"SELECT count(*) FROM {meta}.ducklake_snapshot "
                f"WHERE snapshot_time < now() - INTERVAL '{a_sec + i_sec} seconds'"
            )
            # G5 / AC-N3: ceil(A/C)+20 — with C=1 ⇒ 60+20=80.
            ceil_bar = (a_sec + c_sec - 1) // c_sec + 20
            n3_ok = (
                snap_count >= 0
                and snap_count <= ceil_bar
                and old_snaps == 0
            )
            self.mark(
                "AC-N3",
                pass_=n3_ok,
                fixture_scale={
                    "snapshots": snap_count,
                    "snaps_before": snaps_before,
                    "old": old_snaps,
                    "bar": ceil_bar,
                    "expired_ids": len(old_ids),
                },
                notes=(
                    f"snaps={snap_count} before={snaps_before} old_gt_A+I={old_snaps} "
                    f"bar={ceil_bar} expire_http={code}/{code2} commits={commits} "
                    f"expired_ids={len(old_ids)}"
                ),
            )

            samples_after = self.sql_scalar(
                f"SELECT count(*) FROM {qtable('metric_samples')}"
            )
            n4_ok = (
                samples_pre_expire >= 0
                and samples_after == samples_pre_expire
                and samples_after >= samples_before
            )
            self.mark(
                "AC-N4",
                pass_=n4_ok,
                notes=(
                    f"samples_before_snap={samples_before} "
                    f"pre_expire={samples_pre_expire} after={samples_after}"
                ),
            )
            self.fixture_parts.append(f"snap:{commits}c:snaps={snap_count}")
        finally:
            if manage_heartbeat:
                self.resume_heartbeat()

    def _fail_all_g(self, msg: str) -> None:
        self.blocker_notes.append(msg)
        for ac in ("AC-G0", "AC-G1", "AC-G2", "AC-G3", "AC-G4", "AC-G5", "AC-G6"):
            self.mark(ac, pass_=False, notes=msg)
        self.preconditions["greptime_sender_alive"] = False

    def _stop_greptime(self) -> None:
        if self._greptime_hb_proc is not None:
            try:
                self._greptime_hb_proc.terminate()
                self._greptime_hb_proc.wait(timeout=5)
            except Exception:
                try:
                    self._greptime_hb_proc.kill()
                except Exception:
                    pass
            self._greptime_hb_proc = None
        if self._greptime_owned and self._greptime_proc is not None:
            try:
                self._greptime_proc.terminate()
                self._greptime_proc.wait(timeout=15)
            except Exception:
                try:
                    self._greptime_proc.kill()
                except Exception:
                    pass
            self._greptime_proc = None
            self._greptime_owned = False

    def _start_greptime(self, gp_bin: str) -> str:
        """Start standalone Greptime; return HTTP base URL."""
        addrs = greptime_bind_addrs()
        http_base = greptime_http_base()
        state = Path(
            os.environ.get(
                "THELAKE_LAYOUT_STATE_DIR",
                "/tmp/thelake-metrics-layout",
            )
        )
        data_home = Path(
            os.environ.get("GREPTIME_DATA_HOME", str(state / "greptime-data"))
        )
        if data_home.exists():
            import shutil

            shutil.rmtree(data_home, ignore_errors=True)
        data_home.mkdir(parents=True, exist_ok=True)
        log_path = data_home / "greptime.log"
        cmd = [
            gp_bin,
            "standalone",
            "start",
            "--http-addr",
            addrs["http"],
            "--rpc-bind-addr",
            addrs["grpc"],
            "--mysql-addr",
            addrs["mysql"],
            "--postgres-addr",
            addrs["postgres"],
            "--data-home",
            str(data_home),
        ]
        print(f"==> starting Greptime: {' '.join(cmd)}")
        log_f = open(log_path, "w", encoding="utf-8")
        self._greptime_proc = subprocess.Popen(
            cmd,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            cwd=str(data_home),
        )
        self._greptime_owned = True
        client = GreptimeClient(http_base)
        for i in range(120):
            if self._greptime_proc.poll() is not None:
                tail = log_path.read_text(encoding="utf-8", errors="replace")[-1500:]
                raise RuntimeError(
                    f"Greptime exited early code={self._greptime_proc.returncode}: {tail}"
                )
            if client.ready():
                print(f"==> Greptime healthy at {http_base} after {i * 0.5:.1f}s")
                return http_base
            time.sleep(0.5)
        raise RuntimeError(f"Greptime not healthy at {http_base}; see {log_path}")

    def _start_greptime_heartbeat(self, http_base: str) -> None:
        bin_path = fixture_bin()
        if not bin_path.is_file():
            raise RuntimeError(f"missing fixture bin {bin_path}")
        env = os.environ.copy()
        target = Path(
            os.environ.get("CARGO_TARGET_DIR", Path.home() / ".cache" / "thelake" / "target")
        )
        duck_so = next(target.glob("duckdb-download/**/libduckdb.so*"), None)
        if duck_so is not None:
            libdir = str(duck_so.parent)
            prev = env.get("LD_LIBRARY_PATH", "")
            env["LD_LIBRARY_PATH"] = f"{libdir}:{prev}" if prev else libdir
        argv = [
            str(bin_path),
            "--url",
            http_base,
            "--token",
            "",
            "--metrics-path",
            GREPTIME_OTLP_PATH,
            "--header",
            GREPTIME_NO_TRANSLATION_HEADER,
            "--heartbeat-secs",
            "1",
        ]
        print("==> starting Greptime OTLP heartbeat sender")
        self._greptime_hb_proc = subprocess.Popen(
            argv,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )

    def _load_greptime_g9_fixtures(self, http_base: str) -> None:
        """Load same F-tall / F-wide / F-collapse OTLP fixtures into Greptime."""
        prev_parts = list(self.fixture_parts)
        self._otlp_dest = {
            "base": http_base,
            "token": "",
            "metrics_path": GREPTIME_OTLP_PATH,
            "headers": [GREPTIME_NO_TRANSLATION_HEADER],
        }
        self._greptime_ingest_path = GREPTIME_OTLP_PATH
        try:
            g3_scoped = os.environ.get("LAYOUT_G3_SCOPED", "0") == "1"
            if g3_scoped:
                print("==> Greptime load F-wide only (LAYOUT_G3_SCOPED=1)")
                before = len(self.fixture_parts)
                self.load_f_wide()
                self.fixture_parts = prev_parts + [
                    f"greptime:{p}" for p in self.fixture_parts[before:]
                ]
                return
            print("==> Greptime load F-tall / F-wide / F-collapse / F-gold / F-hist (G9)")
            # Temporarily avoid appending Softprobe fixture_parts twice.
            # Gold+hist raise __name__ cardinality so G4 label_values is not an
            # empty/tiny-catalog micro-benchmark vs Softprobe (which also has F-files).
            before = len(self.fixture_parts)
            self.load_f_tall()
            self.load_f_wide()
            if self.fixture_profile == "release_full":
                self.load_f_collapse(int(self.scales["collapse_90d_days"]), "90d-gp")
            else:
                self.load_f_collapse(int(self.scales["collapse_days"]), "30d-gp")
            self.load_f_gold()
            self.load_f_hist(verify=False)
            # Drop greptime-only part tags from Softprobe fixture hash identity.
            self.fixture_parts = prev_parts + [
                f"greptime:{p}" for p in self.fixture_parts[before:]
            ]
        finally:
            self._otlp_dest = None

    def _ratio_pass(
        self,
        ac_id: str,
        soft_p95: float | None,
        gp_p95: float | None,
        *,
        notes: str = "",
    ) -> None:
        if soft_p95 is None or gp_p95 is None or gp_p95 <= 0:
            self.mark(
                ac_id,
                pass_=False,
                softprobe_p95_ms=soft_p95,
                greptime_p95_ms=gp_p95,
                ratio=None,
                notes=notes or "missing p95 for ratio",
            )
            return
        ratio = soft_p95 / gp_p95
        ok = ratio <= G9_RATIO_R
        note = notes or f"OTLP both sides; ingest-on; R={G9_RATIO_R}"
        if not ok:
            msg = (
                f"{ac_id} ratio {ratio:.2f} > R={G9_RATIO_R} "
                f"(soft={soft_p95:.1f}ms greptime={gp_p95:.1f}ms) — escalate per §4.4"
            )
            self.blocker_notes.append(msg)
            note = msg
        self.mark(
            ac_id,
            pass_=ok,
            softprobe_p95_ms=soft_p95,
            greptime_p95_ms=gp_p95,
            ratio=round(ratio, 4),
            p95_ms=soft_p95,
            notes=note,
        )

    def measure_greptime(self) -> None:
        if not self.compare_greptime:
            for ac in ("AC-G0", "AC-G1", "AC-G2", "AC-G3", "AC-G4", "AC-G5", "AC-G6"):
                self.mark(
                    ac,
                    pass_=False,
                    notes="COMPARE_GREPTIME=0 — G9 not run",
                )
            self.preconditions["greptime_sender_alive"] = False
            self.greptime_sha = "skipped"
            return

        gp_bin = os.environ.get("GREPTIME_BIN", "").strip()
        gp_url = os.environ.get("GREPTIME_URL", "").strip()
        sibling = Path(os.environ.get("GREPTIME_SRC", str(ROOT.parent / "greptime")))
        if sibling.is_dir():
            try:
                sha = subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], cwd=str(sibling), text=True
                ).strip()
                self.greptime_sha = sha
            except Exception:
                if self.greptime_sha in ("", "missing"):
                    self.greptime_sha = "unknown"

        # Auto-detect sibling release binary when COMPARE_GREPTIME=1
        if not gp_bin and not gp_url:
            for candidate in (
                sibling / "target" / "release" / "greptime",
                ROOT.parent / "greptime" / "target" / "release" / "greptime",
            ):
                if candidate.is_file() and os.access(candidate, os.X_OK):
                    gp_bin = str(candidate.resolve())
                    os.environ["GREPTIME_BIN"] = gp_bin
                    break

        if not gp_bin and not gp_url:
            msg = (
                "Greptime binary/URL not available in this environment. "
                f"Pinned source SHA={self.greptime_sha}. "
                "Set GREPTIME_BIN or GREPTIME_URL, or build "
                f"{sibling}/target/release/greptime "
                "(cd greptime && make build RELEASE=true)."
            )
            self._fail_all_g(msg)
            return

        try:
            if gp_url:
                http_base = gp_url.rstrip("/")
                os.environ.setdefault("GREPTIME_URL", http_base)
                client = GreptimeClient(http_base)
                if not client.ready():
                    self._fail_all_g(f"GREPTIME_URL={http_base} not healthy")
                    return
                print(f"==> using external Greptime at {http_base}")
            else:
                http_base = self._start_greptime(gp_bin)
                client = GreptimeClient(http_base)

            self._load_greptime_g9_fixtures(http_base)
            self._start_greptime_heartbeat(http_base)
            time.sleep(2.0)

            hb_before = client.heartbeat_sample_count()
            hb_pid_alive = (
                self._greptime_hb_proc is not None
                and self._greptime_hb_proc.poll() is None
            )
            # Allow a few seconds of ingest-on progress for AC-G0.
            time.sleep(2.5)
            hb_after = client.heartbeat_sample_count()
            sender_alive = hb_pid_alive and (
                hb_after > hb_before
                or (hb_before < 0 and hb_after >= 0)  # table created mid-flight
                or hb_after > 0
            )
            # Prefer strict progress when counts are readable.
            if hb_before >= 0 and hb_after >= 0:
                sender_alive = hb_pid_alive and hb_after > hb_before
            self.preconditions["greptime_sender_alive"] = bool(sender_alive)

            versions_ok = (
                bool(self.greptime_sha)
                and self.greptime_sha not in ("missing", "unknown", "skipped", "")
                and self.versions().get("R") == 10
            )
            g0_ok = versions_ok and sender_alive
            self.mark(
                "AC-G0",
                pass_=g0_ok,
                notes=(
                    f"sha={self.greptime_sha} hb_before={hb_before} hb_after={hb_after} "
                    f"hb_alive={hb_pid_alive} ingest={self._greptime_ingest_path}"
                ),
            )

            # AC-G6: OTLP metrics path used (never remote_write-only).
            g6_ok = self._greptime_ingest_path == GREPTIME_OTLP_PATH
            self.mark(
                "AC-G6",
                pass_=g6_ok,
                notes=(
                    f"ingest_path={self._greptime_ingest_path} "
                    "(must be OTLP /v1/otlp/v1/metrics, not remote_write)"
                ),
            )

            scales = self.scales
            end = float(EVAL_END_S)
            reps = int(scales["q_tall_short_repeats"])
            reps_l = int(scales["q_long_repeats"])
            reps_d = int(scales["q_discover_repeats"])

            def timed_range(
                query: str, start: float, end_s: float, step: str, repeats: int
            ) -> tuple[list[float], list[int]]:
                times: list[float] = []
                codes: list[int] = []
                for _ in range(repeats):
                    code, _doc, ms = client.query_range(query, start, end_s, step)
                    times.append(ms)
                    codes.append(code)
                return times, codes

            # Warmup once per gated query class
            for _ in range(3):
                client.query_range("layout_tall", end - 1800, end, "15s")
            for _ in range(3):
                client.label_values("__name__")

            # G1 ← T-Q1
            t, codes = timed_range("layout_tall", end - 1800, end, "15s", reps)
            gp1 = pct(t, 95)
            soft1 = self.acs["AC-Q1"].softprobe_p95_ms or self.acs["AC-Q1"].p95_ms
            self._ratio_pass(
                "AC-G1",
                soft1,
                gp1,
                notes=f"T-Q1 layout_tall 30m; greptime_codes={set(codes)}",
            )

            # G2 ← T-Q2
            t, codes = timed_range(
                "layout_tall", end - 30 * 86400, end, "1h", reps_l
            )
            gp2 = pct(t, 95)
            soft2 = self.acs["AC-Q2"].softprobe_p95_ms or self.acs["AC-Q2"].p95_ms
            self._ratio_pass(
                "AC-G2",
                soft2,
                gp2,
                notes=f"T-Q2 layout_tall 30d; greptime_codes={set(codes)}",
            )

            # G3 ← T-Q3
            for _ in range(3):
                client.query_range(
                    '{__name__="layout_wide",instance="i-1"}',
                    end - 1800,
                    end,
                    "15s",
                )
            t, codes = timed_range(
                '{__name__="layout_wide",instance="i-1"}',
                end - 1800,
                end,
                "15s",
                reps_l,
            )
            gp3 = pct(t, 95)
            soft3 = self.acs["AC-Q3"].softprobe_p95_ms or self.acs["AC-Q3"].p95_ms
            self._ratio_pass(
                "AC-G3",
                soft3,
                gp3,
                notes=f"T-Q3 wide resolve; greptime_codes={set(codes)}",
            )

            # G4 ← T-Q6 discovery (warmup + require non-empty Greptime names)
            for _ in range(3):
                client.label_values("__name__")
            times = []
            codes = []
            name_n = 0
            for _ in range(reps_d):
                code, doc, ms = client.label_values("__name__")
                times.append(ms)
                codes.append(code)
                data = doc.get("data") if isinstance(doc, dict) else None
                if isinstance(data, list):
                    name_n = max(name_n, len(data))
            gp4 = pct(times, 95)
            soft4 = self.acs["AC-Q6"].softprobe_p95_ms or self.acs["AC-Q6"].p95_ms
            if name_n < 3:
                self.mark(
                    "AC-G4",
                    pass_=False,
                    softprobe_p95_ms=soft4,
                    greptime_p95_ms=gp4,
                    notes=f"Greptime __name__ values too few ({name_n}); unfair empty catalog",
                )
            else:
                self._ratio_pass(
                    "AC-G4",
                    soft4,
                    gp4,
                    notes=(
                        f"T-Q6 __name__/values; greptime_names={name_n} "
                        f"greptime_codes={set(codes)}"
                    ),
                )

            # G5 ← T-Q5 collapse
            t, codes = timed_range(
                "sum by (job) (rate(layout_http[5m]))",
                end - 30 * 86400,
                end,
                "1h",
                reps_l,
            )
            gp5 = pct(t, 95)
            soft5 = self.acs["AC-Q5"].softprobe_p95_ms or self.acs["AC-Q5"].p95_ms
            self._ratio_pass(
                "AC-G5",
                soft5,
                gp5,
                notes=f"T-Q5 collapse 30d; greptime_codes={set(codes)}",
            )

            # Re-check sender alive after measure window.
            hb_end = client.heartbeat_sample_count()
            still_alive = (
                self._greptime_hb_proc is not None
                and self._greptime_hb_proc.poll() is None
                and (hb_end > hb_after if hb_after >= 0 and hb_end >= 0 else hb_end > 0)
            )
            if not still_alive:
                self.preconditions["greptime_sender_alive"] = False
                self.mark(
                    "AC-G0",
                    pass_=False,
                    notes=(
                        self.acs["AC-G0"].notes
                        + f"; sender died during measure hb_end={hb_end}"
                    ),
                )
        except Exception as e:
            self._fail_all_g(f"Greptime G9 measure failed: {e}")
        finally:
            self._stop_greptime()

    def fixture_hash(self) -> str:
        blob = json.dumps(
            {
                "seed": LAYOUT_FIXTURE_SEED,
                "profile": self.fixture_profile,
                "scales": self.scales,
                "parts": self.fixture_parts,
                "eval_end": EVAL_END_S,
            },
            sort_keys=True,
        )
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

    def versions(self) -> dict[str, Any]:
        soft = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(ROOT), text=True
        ).strip()
        machine = f"{platform.system().lower()}-{platform.machine()}"
        duckdb_v = "unknown"
        try:
            c, d, _ = self.client.sql("SELECT version()")
            if c == 200 and d.get("rows"):
                duckdb_v = str(d["rows"][0][0])
        except Exception:
            pass
        return {
            "softprobe": soft,
            "greptime": self.greptime_sha,
            "duckdb": duckdb_v,
            "ducklake": os.environ.get("DUCKLAKE_VERSION", "extension"),
            "postgres": os.environ.get("LAYOUT_PG_IMAGE", "postgres:19beta3"),
            "machine_class": machine,
            "R": 10,
        }

    def write_results(self) -> tuple[Path, Path]:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_dir = ROOT / "docs" / "perf" / "results"
        out_dir.mkdir(parents=True, exist_ok=True)
        soft = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=str(ROOT), text=True
        ).strip()
        doc = {
            "schema_version": 1,
            "suite": "metrics-layout",
            "binary_profile": self.binary_profile,
            "fixture_profile": self.fixture_profile,
            "git_sha": soft,
            "fixture_hash": self.fixture_hash(),
            "stamp": stamp,
            "versions": self.versions(),
            "preconditions": self.preconditions,
            "acs": {k: self.acs[k].to_json() for k in REQUIRED_AC_IDS},
            "blocker_notes": self.blocker_notes,
        }
        # Ensure every AC has pass bool (never omit)
        for ac_id in REQUIRED_AC_IDS:
            assert ac_id in doc["acs"]
            assert isinstance(doc["acs"][ac_id]["pass"], bool)

        schema_errs = validate_schema(doc)
        if schema_errs:
            raise SystemExit("internal schema error before write:\n" + "\n".join(schema_errs))

        json_path = out_dir / f"{stamp}-metrics-layout.json"
        md_path = out_dir / f"{stamp}-metrics-layout.md"
        json_path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")

        passed = sum(1 for r in doc["acs"].values() if r["pass"])
        total = len(REQUIRED_AC_IDS)
        failed = total - passed
        lines = [
            f"# metrics-layout {stamp}",
            "",
            f"- binary_profile: `{self.binary_profile}`",
            f"- fixture_profile: `{self.fixture_profile}`",
            f"- COMPARE_GREPTIME: `{int(self.compare_greptime)}`",
            f"- pass/fail: **{passed}/{total}** pass, **{failed}** fail",
            f"- fixture_hash: `{doc['fixture_hash']}`",
            "",
            "| AC | pass | p95_ms | notes |",
            "|----|------|--------|-------|",
        ]
        for ac_id in REQUIRED_AC_IDS:
            r = doc["acs"][ac_id]
            lines.append(
                f"| {ac_id} | {r['pass']} | {r.get('p95_ms')} | {str(r.get('notes',''))[:80]} |"
            )
        if self.blocker_notes:
            lines.append("")
            lines.append("## Blockers")
            for b in self.blocker_notes:
                lines.append(f"- {b}")
        md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"Wrote {json_path}")
        print(f"Wrote {md_path}")
        print(f"PASS_COUNT={passed} FAIL_COUNT={failed}")
        return json_path, md_path

    def run(self) -> int:
        print(f"==> waiting for Softprobe at {self.base}")
        for _ in range(60):
            if self.client.ready():
                break
            time.sleep(0.5)
        else:
            print("Softprobe not ready", file=sys.stderr)
            return 2

        print("==> unit ACs (cargo --exact)")
        self.measure_unit_acs()
        print("==> load fixtures")
        self.load_fixtures()
        print("==> catalog ACs")
        self.measure_catalog_acs()
        print("==> query / cardinality / window ACs")
        self.measure_query_acs()
        print("==> remaining ACs")
        self.measure_remaining()
        print("==> Greptime G9")
        self.measure_greptime()
        json_path, _ = self.write_results()

        # Validate schema always; fail closed on any AC fail or validator reject
        errs = validate_schema(json.loads(json_path.read_text()))
        if errs:
            print("validator schema errors:", file=sys.stderr)
            print("\n".join(errs), file=sys.stderr)
            return 1
        failed = [k for k, v in self.acs.items() if not v.pass_]
        if failed:
            print(f"AC failures ({len(failed)}): {', '.join(failed)}", file=sys.stderr)
            return 1
        return 0


def main() -> int:
    return Harness().run()


if __name__ == "__main__":
    sys.exit(main())
