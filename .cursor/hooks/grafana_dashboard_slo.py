#!/usr/bin/env python3
"""Measure live Grafana dashboard PromQL against Softprobe.

Used by the Cursor stop hook. Every dashboard expr is queried over the demo
windows (5m … 180d). A window fails unless every measured repeat is ≤ SLO_MS.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DASH_DIR = ROOT / "tests" / "compat" / "grafana" / "dashboards"

RANGES: list[tuple[str, int]] = [
    ("5m", 5 * 60),
    ("15m", 15 * 60),
    ("30m", 30 * 60),
    ("1h", 60 * 60),
    ("3h", 3 * 60 * 60),
    ("24h", 24 * 60 * 60),
    ("30d", 30 * 24 * 60 * 60),
    ("180d", 180 * 24 * 60 * 60),
]

LIVE_INGEST_QUERIES = (
    "http_server_request_duration_count",
    "traces_span_metrics_calls",
    "demo_ad_served_total",
    "k6_iterations",
)


def _walk_exprs(node: Any, dashboard: str, panel: str, out: list[dict[str, str]]) -> None:
    if isinstance(node, dict):
        title = str(node.get("title") or panel)
        expr = node.get("expr")
        if isinstance(expr, str) and expr.strip():
            out.append(
                {
                    "dashboard": dashboard,
                    "panel": title,
                    "expr": expr.strip(),
                }
            )
        for child in node.values():
            _walk_exprs(child, dashboard, title, out)
    elif isinstance(node, list):
        for child in node:
            _walk_exprs(child, dashboard, panel, out)


def extract_dashboard_queries(dash_dir: Path = DASH_DIR) -> list[dict[str, str]]:
    queries: list[dict[str, str]] = []
    for path in sorted(dash_dir.rglob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        title = str(doc.get("title") or path.stem)
        _walk_exprs(doc.get("panels") or [], title, title, queries)
    # Keep first occurrence of each (dashboard, panel, expr) triple.
    seen: set[tuple[str, str, str]] = set()
    unique: list[dict[str, str]] = []
    for q in queries:
        key = (q["dashboard"], q["panel"], q["expr"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(q)
    return unique


def grafana_step_seconds(range_secs: int) -> int:
    # Grafana-like: ~1100 max data points, 15s floor.
    return max(15, range_secs // 1100)


class SoftprobeProm:
    def __init__(self, base: str, token: str, timeout_s: float) -> None:
        self.base = base.rstrip("/")
        self.token = token
        self.timeout_s = timeout_s

    def _req(self, method: str, path: str, data: bytes | None = None) -> tuple[int, dict[str, Any], float]:
        url = self.base + path
        headers = {"Authorization": f"Bearer {self.token}"}
        if data is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read()
                code = resp.getcode() or 0
        except urllib.error.HTTPError as exc:
            raw = exc.read() if exc.fp is not None else b""
            code = int(exc.code)
        except Exception as exc:  # noqa: BLE001 — surface any transport failure as a miss
            ms = (time.perf_counter() - t0) * 1000.0
            return 0, {"error": str(exc)}, ms
        ms = (time.perf_counter() - t0) * 1000.0
        try:
            doc = json.loads(raw.decode() or "{}")
        except json.JSONDecodeError:
            doc = {"error": raw.decode(errors="replace")[:500]}
        return code, doc, ms

    def label_names(self) -> tuple[int, dict[str, Any], float]:
        return self._req("GET", "/api/v1/label/__name__/values")

    def query_range(self, expr: str, start: int, end: int, step: int) -> tuple[int, dict[str, Any], float]:
        body = urllib.parse.urlencode(
            {"query": expr, "start": str(start), "end": str(end), "step": str(step)}
        ).encode()
        return self._req("POST", "/api/v1/query_range", data=body)


def check_ingest(client: SoftprobeProm) -> str | None:
    code, doc, _ms = client.label_names()
    names = (doc.get("data") if isinstance(doc.get("data"), list) else []) or []
    if code != 200 or doc.get("status") != "success" or not names:
        return (
            f"Softprobe has no metric names (http={code} status={doc.get('status')!r} "
            f"error={doc.get('error') or doc.get('errorType') or 'empty'})."
        )
    blob = " ".join(str(n) for n in names)
    if not any(
        token in blob
        for token in ("http_", "traces_span", "rpc_", "process_", "otelcol_", "k6_", "demo_")
    ):
        return f"metric names present ({len(names)}) but none look like OTEL demo / spanmetrics."

    end = int(time.time())
    start = end - 300
    best = 0
    used = ""
    for q in LIVE_INGEST_QUERIES:
        _code, body, _ms = client.query_range(q, start, end, 15)
        rows = ((body.get("data") or {}).get("result")) or []
        for series in rows:
            vals = []
            for _ts, v in series.get("values") or []:
                try:
                    vals.append(float(v))
                except (TypeError, ValueError):
                    continue
            changes = sum(1 for a, b in zip(vals, vals[1:]) if a != b)
            if changes > best:
                best = changes
                used = q
        if best >= 2:
            break
    if best < 2:
        return (
            "OTEL demo series are flat (lookback of a single scrape). "
            "Need live ingest: value changes ≥ 2 on http_server / spanmetrics / demo / k6."
        )
    print(f"ingest ok ({used} value changes={best}, names={len(names)})", file=sys.stderr)
    return None


def _measure_one(
    client: SoftprobeProm,
    query: dict[str, str],
    range_name: str,
    range_secs: int,
    repeats: int,
    slo_ms: float,
) -> dict[str, Any]:
    end = int(time.time())
    start = end - range_secs
    step = grafana_step_seconds(range_secs)
    samples: list[float] = []
    last_err = ""
    # One discarded warmup, then `repeats` measured samples.
    for i in range(repeats + 1):
        code, doc, ms = client.query_range(query["expr"], start, end, step)
        ok = code == 200 and doc.get("status") == "success"
        if not ok:
            last_err = (
                f"http={code} status={doc.get('status')!r} "
                f"{doc.get('error') or doc.get('errorType') or ''}"
            ).strip()
            samples.append(max(ms, slo_ms + 1.0))
        else:
            samples.append(ms)
        if i == 0:
            continue
    measured = samples[1:]
    worst = max(measured) if measured else slo_ms + 1.0
    return {
        "dashboard": query["dashboard"],
        "panel": query["panel"],
        "range": range_name,
        "expr": query["expr"],
        "samples_ms": [round(x, 2) for x in measured],
        "worst_ms": round(worst, 2),
        "pass": worst <= slo_ms and not last_err,
        "error": last_err,
    }


def run_slo(
    client: SoftprobeProm,
    queries: list[dict[str, str]],
    repeats: int,
    slo_ms: float,
    workers: int,
) -> list[dict[str, Any]]:
    jobs = [
        (q, range_name, range_secs)
        for q in queries
        for range_name, range_secs in RANGES
    ]
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = [
            pool.submit(_measure_one, client, q, name, secs, repeats, slo_ms)
            for q, name, secs in jobs
        ]
        for fut in as_completed(futs):
            results.append(fut.result())
    results.sort(key=lambda r: (r["dashboard"], r["panel"], r["range"]))
    return results


def percentile(xs: list[float], p: float) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    if len(ys) == 1:
        return ys[0]
    idx = (p / 100.0) * (len(ys) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(ys) - 1)
    frac = idx - lo
    return ys[lo] * (1.0 - frac) + ys[hi] * frac


def select_ranges(spec: str) -> list[tuple[str, int]]:
    by_name = {name: secs for name, secs in RANGES}
    out: list[tuple[str, int]] = []
    for raw in spec.split(","):
        name = raw.strip()
        if not name:
            continue
        if name not in by_name:
            raise SystemExit(f"unknown range {name!r}; want one of {', '.join(by_name)}")
        out.append((name, by_name[name]))
    if not out:
        raise SystemExit("no ranges selected")
    return out


def _one_shot(
    client: SoftprobeProm,
    query: dict[str, str],
    range_name: str,
    range_secs: int,
) -> dict[str, Any]:
    end = int(time.time())
    start = end - range_secs
    step = grafana_step_seconds(range_secs)
    code, doc, ms = client.query_range(query["expr"], start, end, step)
    ok = code == 200 and doc.get("status") == "success"
    err = ""
    series = 0
    if not ok:
        err = (
            f"http={code} status={doc.get('status')!r} "
            f"{doc.get('error') or doc.get('errorType') or ''}"
        ).strip()
    else:
        series = len(((doc.get("data") or {}).get("result")) or [])
    return {
        "dashboard": query["dashboard"],
        "panel": query["panel"],
        "range": range_name,
        "range_secs": range_secs,
        "step_secs": step,
        "expr": query["expr"],
        "ok": ok,
        "http": code,
        "series": series,
        "ms": round(ms, 2),
        "error": err,
    }


def run_bench(
    client: SoftprobeProm,
    queries: list[dict[str, str]],
    ranges: list[tuple[str, int]],
    workers: int,
) -> list[dict[str, Any]]:
    jobs = [(q, name, secs) for q in queries for name, secs in ranges]
    total = len(jobs)
    results: list[dict[str, Any]] = []
    done = 0
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = [
            pool.submit(_one_shot, client, q, name, secs)
            for q, name, secs in jobs
        ]
        for fut in as_completed(futs):
            row = fut.result()
            results.append(row)
            done += 1
            if done == 1 or done % 25 == 0 or done == total:
                elapsed = time.perf_counter() - t0
                print(
                    f"bench {done}/{total} elapsed={elapsed:.0f}s "
                    f"last={row['ms']:.0f}ms ok={row['ok']} "
                    f"{row['dashboard'][:32]} / {row['range']}",
                    file=sys.stderr,
                    flush=True,
                )
    results.sort(key=lambda r: (-r["ms"], r["dashboard"], r["panel"], r["range"]))
    return results


def summarize_bench(results: list[dict[str, Any]]) -> dict[str, Any]:
    ms = [r["ms"] for r in results]
    oks = [r for r in results if r["ok"]]
    by_range: dict[str, list[float]] = {}
    by_dash: dict[str, list[float]] = {}
    for r in results:
        by_range.setdefault(r["range"], []).append(r["ms"])
        by_dash.setdefault(r["dashboard"], []).append(r["ms"])
    return {
        "cells": len(results),
        "ok": len(oks),
        "errors": len(results) - len(oks),
        "empty_ok": sum(1 for r in oks if r["series"] == 0),
        "ms": {
            "min": round(min(ms), 2) if ms else 0.0,
            "p50": round(percentile(ms, 50), 2),
            "p95": round(percentile(ms, 95), 2),
            "p99": round(percentile(ms, 99), 2),
            "max": round(max(ms), 2) if ms else 0.0,
        },
        "buckets": {
            "lt_1s": sum(1 for x in ms if x < 1000),
            "1_to_2s": sum(1 for x in ms if 1000 <= x < 2000),
            "2_to_5s": sum(1 for x in ms if 2000 <= x < 5000),
            "5_to_10s": sum(1 for x in ms if 5000 <= x < 10000),
            "ge_10s": sum(1 for x in ms if x >= 10000),
        },
        "by_range": {
            name: {
                "n": len(xs),
                "p50": round(percentile(xs, 50), 2),
                "p95": round(percentile(xs, 95), 2),
                "max": round(max(xs), 2),
            }
            for name, xs in by_range.items()
        },
        "by_dashboard": {
            name: {
                "n": len(xs),
                "p50": round(percentile(xs, 50), 2),
                "p95": round(percentile(xs, 95), 2),
                "max": round(max(xs), 2),
            }
            for name, xs in sorted(by_dash.items(), key=lambda kv: -max(kv[1]))
        },
    }


def format_bench_markdown(summary: dict[str, Any], results: list[dict[str, Any]], top: int) -> str:
    ms = summary["ms"]
    b = summary["buckets"]
    lines = [
        "# Grafana dashboard PromQL bench",
        "",
        f"Cells **{summary['cells']}** (ok {summary['ok']}, errors {summary['errors']}, "
        f"empty-success {summary['empty_ok']}).",
        "",
        f"| p50 | p95 | p99 | max |",
        f"|-----|-----|-----|-----|",
        f"| {ms['p50']:.0f}ms | {ms['p95']:.0f}ms | {ms['p99']:.0f}ms | {ms['max']:.0f}ms |",
        "",
        "| <1s | 1–2s | 2–5s | 5–10s | ≥10s |",
        "|-----|------|------|-------|------|",
        f"| {b['lt_1s']} | {b['1_to_2s']} | {b['2_to_5s']} | {b['5_to_10s']} | {b['ge_10s']} |",
        "",
        "## By range",
        "",
        "| Range | n | p50 | p95 | max |",
        "|-------|---|-----|-----|-----|",
    ]
    for name, row in summary["by_range"].items():
        lines.append(
            f"| {name} | {row['n']} | {row['p50']:.0f}ms | {row['p95']:.0f}ms | {row['max']:.0f}ms |"
        )
    lines += [
        "",
        "## Worst panels",
        "",
        "| ms | dash | panel | range | series | error | expr |",
        "|----|------|-------|-------|--------|-------|------|",
    ]
    for r in results[:top]:
        expr = r["expr"].replace("|", "\\|").replace("\n", " ")
        if len(expr) > 90:
            expr = expr[:87] + "..."
        err = (r["error"] or "").replace("|", " ")[:80]
        lines.append(
            f"| {r['ms']:.0f} | {r['dashboard']} | {r['panel']} | {r['range']} | "
            f"{r['series']} | {err} | `{expr}` |"
        )
    lines.append("")
    return "\n".join(lines)


def format_failures(results: list[dict[str, Any]], slo_ms: float, limit: int = 20) -> str:
    fails = [r for r in results if not r["pass"]]
    if not fails:
        return ""
    fails.sort(key=lambda r: r["worst_ms"], reverse=True)
    lines = [
        f"Grafana SLO {slo_ms:.0f}ms failed: {len(fails)}/{len(results)} query×range cells."
    ]
    for r in fails[:limit]:
        expr = r["expr"].replace("\n", " ")
        if len(expr) > 120:
            expr = expr[:117] + "..."
        extra = f" error={r['error']}" if r["error"] else ""
        lines.append(
            f"- {r['dashboard']} / {r['panel']} / {r['range']}: "
            f"worst={r['worst_ms']}ms samples={r['samples_ms']}{extra} expr={expr}"
        )
    if len(fails) > limit:
        lines.append(f"- … {len(fails) - limit} more")
    return "\n".join(lines)


def self_test() -> int:
    queries = extract_dashboard_queries()
    assert queries, "no dashboard exprs found"
    assert len(queries) >= 100, f"expected ≥100 exprs, got {len(queries)}"
    dashboards = {q["dashboard"] for q in queries}
    assert len(dashboards) >= 15, f"expected many dashboards, got {sorted(dashboards)}"
    assert grafana_step_seconds(5 * 60) == 15
    assert grafana_step_seconds(180 * 24 * 60 * 60) >= 1000
    names = {n for n, _ in RANGES}
    assert names == {"5m", "15m", "30m", "1h", "3h", "24h", "30d", "180d"}
    assert abs(percentile([1, 2, 3, 4], 50) - 2.5) < 1e-9
    print(f"self-test ok ({len(queries)} exprs, {len(dashboards)} dashboards)", file=sys.stderr)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--extract-only", action="store_true")
    parser.add_argument("--check-ingest", action="store_true")
    parser.add_argument("--skip-ingest", action="store_true", help="run queries even if ingest liveness check fails")
    parser.add_argument("--bench", action="store_true", help="full ranked latency report for every dashboard expr")
    parser.add_argument(
        "--ranges",
        default="5m,15m,30m,1h,3h,24h",
        help="comma-separated windows from 5m,15m,30m,1h,3h,24h,30d,180d",
    )
    parser.add_argument("--json-out", default="")
    parser.add_argument("--md-out", default="")
    parser.add_argument("--top", type=int, default=40)
    parser.add_argument("--timeout-s", type=float, default=20.0)
    parser.add_argument("--slo-ms", type=float, default=float(os.environ.get("THELAKE_GRAFANA_SLO_MS", "100")))
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--workers", type=int, default=0, help="0 = 1 for --bench, 4 for SLO")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("SOFTPROBE_LISTEN", "http://127.0.0.1:8090"),
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("SOFTPROBE_API_KEY", "local-dev-key"),
    )
    args = parser.parse_args()

    if args.self_test:
        return self_test()

    queries = extract_dashboard_queries()
    if args.extract_only:
        json.dump(
            {"dashboards": sorted({q["dashboard"] for q in queries}), "count": len(queries)},
            sys.stdout,
        )
        sys.stdout.write("\n")
        return 0 if queries else 1

    timeout_s = args.timeout_s if args.bench else max(args.timeout_s, 5.0)
    client = SoftprobeProm(args.base_url, args.token, timeout_s=timeout_s)

    if args.check_ingest:
        err = check_ingest(client)
        if err:
            print(err, file=sys.stderr)
            return 1
        return 0

    ingest_err = None if args.skip_ingest else check_ingest(client)
    if ingest_err:
        print(ingest_err, file=sys.stderr)
        return 1

    if args.bench:
        ranges = select_ranges(args.ranges)
        workers = args.workers if args.workers > 0 else 1
        print(
            f"bench {len(queries)} exprs × {len(ranges)} ranges, "
            f"workers={workers} timeout={timeout_s:.0f}s",
            file=sys.stderr,
        )
        results = run_bench(client, queries, ranges, workers)
        summary = summarize_bench(results)
        payload = {"summary": summary, "results": results}
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        json_out = Path(args.json_out) if args.json_out else ROOT / "docs" / "perf" / "results" / f"{stamp}-grafana-dashboards.json"
        md_out = Path(args.md_out) if args.md_out else json_out.with_suffix(".md")
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        md_out.write_text(format_bench_markdown(summary, results, args.top), encoding="utf-8")
        print(json.dumps(summary, indent=2), file=sys.stderr)
        print(f"wrote {json_out}", file=sys.stderr)
        print(f"wrote {md_out}", file=sys.stderr)
        return 0

    workers = args.workers if args.workers > 0 else 4
    results = run_slo(client, queries, args.repeats, args.slo_ms, workers)
    fails = [r for r in results if not r["pass"]]
    summary = {
        "total": len(results),
        "pass": len(results) - len(fails),
        "fail": len(fails),
        "slo_ms": args.slo_ms,
        "worst_ms": max((r["worst_ms"] for r in results), default=0.0),
    }
    print(json.dumps(summary), file=sys.stderr)
    if fails:
        print(format_failures(results, args.slo_ms), file=sys.stderr)
        return 1
    print(
        f"grafana slo ok: {summary['pass']}/{summary['total']} ≤ {args.slo_ms:.0f}ms "
        f"(worst {summary['worst_ms']}ms)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
