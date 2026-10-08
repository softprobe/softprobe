#!/usr/bin/env python3
"""Validate metrics-layout result JSON against docs/metrics-timeseries-layout.md §10.3.1.

Usage:
  scripts/validate-metrics-layout-results.py path/to/*-metrics-layout.json
  scripts/validate-metrics-layout-results.py --ready path.json   # ready gate
  python3 -m unittest scripts.test_validate_metrics_layout_results -v
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
SUITE = "metrics-layout"

REQUIRED_AC_IDS: tuple[str, ...] = (
    *(f"AC-D{i}" for i in range(1, 5)),
    *(f"AC-Q{i}" for i in range(0, 10)),
    *(f"AC-H{i}" for i in range(1, 7)),
    *(f"AC-C{i}" for i in range(1, 5)),
    *(f"AC-W{i}" for i in range(1, 7)),
    *(f"AC-N{i}" for i in range(1, 7)),
    *(f"AC-F{i}" for i in range(1, 9)),
    *(f"AC-S{i}" for i in range(1, 4)),
    "AC-M1",
    "AC-M2",
    *(f"AC-G{i}" for i in range(0, 7)),
)

assert len(REQUIRED_AC_IDS) == 56, len(REQUIRED_AC_IDS)

G_RATIO_IDS = ("AC-G1", "AC-G2", "AC-G3", "AC-G4", "AC-G5")

TOP_LEVEL_REQUIRED = (
    "schema_version",
    "suite",
    "binary_profile",
    "fixture_profile",
    "git_sha",
    "fixture_hash",
    "stamp",
    "versions",
    "preconditions",
    "acs",
)

VERSION_KEYS = (
    "softprobe",
    "greptime",
    "duckdb",
    "ducklake",
    "postgres",
    "machine_class",
    "R",
)


class ValidationError(Exception):
    """One or more schema / ready-gate failures."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("\n".join(errors))


def _require_dict(obj: Any, path: str, errors: list[str]) -> dict[str, Any] | None:
    if not isinstance(obj, dict):
        errors.append(f"{path}: expected object, got {type(obj).__name__}")
        return None
    return obj


def _ac_row_ok(ac_id: str, row: Any, errors: list[str]) -> None:
    if not isinstance(row, dict):
        errors.append(f"acs.{ac_id}: expected object")
        return
    if "pass" not in row:
        errors.append(f"acs.{ac_id}: missing 'pass'")
        return
    if not isinstance(row["pass"], bool):
        errors.append(f"acs.{ac_id}.pass: must be bool")


def validate_schema(doc: Any) -> list[str]:
    """Return schema errors (empty = structurally valid §10.3.1 skeleton)."""
    errors: list[str] = []
    root = _require_dict(doc, "$", errors)
    if root is None:
        return errors

    for key in TOP_LEVEL_REQUIRED:
        if key not in root:
            errors.append(f"missing top-level field: {key}")

    if root.get("schema_version") != SCHEMA_VERSION:
        errors.append(
            f"schema_version: expected {SCHEMA_VERSION}, got {root.get('schema_version')!r}"
        )
    if root.get("suite") != SUITE:
        errors.append(f"suite: expected {SUITE!r}, got {root.get('suite')!r}")

    if root.get("binary_profile") not in ("release", "dev", "debug"):
        errors.append(
            f"binary_profile: expected release|dev|debug, got {root.get('binary_profile')!r}"
        )
    if root.get("fixture_profile") not in ("pr_floor", "release_full"):
        errors.append(
            f"fixture_profile: expected pr_floor|release_full, got {root.get('fixture_profile')!r}"
        )

    versions = _require_dict(root.get("versions"), "versions", errors)
    if versions is not None:
        for k in VERSION_KEYS:
            if k not in versions:
                errors.append(f"versions missing: {k}")
        if "R" in versions and versions["R"] != 10:
            errors.append(f"versions.R: expected 10, got {versions['R']!r}")

    pre = _require_dict(root.get("preconditions"), "preconditions", errors)
    if pre is not None:
        for k in (
            "AC-F2_bytes_before_merge",
            "AC-F5_precondition_met",
            "sender_alive",
            "greptime_sender_alive",
        ):
            if k not in pre:
                errors.append(f"preconditions missing: {k}")

    acs = _require_dict(root.get("acs"), "acs", errors)
    if acs is not None:
        missing = [i for i in REQUIRED_AC_IDS if i not in acs]
        extra_ok = True  # extras allowed
        del extra_ok
        for m in missing:
            errors.append(f"acs missing required id: {m}")
        for ac_id in REQUIRED_AC_IDS:
            if ac_id in acs:
                _ac_row_ok(ac_id, acs[ac_id], errors)

    return errors


def validate_ready(doc: Any, *, require_greptime: bool = True) -> list[str]:
    """Ready gate: release + release_full + all pass + G* populated when G9 claimed."""
    errors = validate_schema(doc)
    if errors:
        return errors

    assert isinstance(doc, dict)
    if doc.get("binary_profile") != "release":
        errors.append(
            f"ready: binary_profile must be 'release', got {doc.get('binary_profile')!r}"
        )
    if doc.get("fixture_profile") != "release_full":
        errors.append(
            f"ready: fixture_profile must be 'release_full', got {doc.get('fixture_profile')!r}"
        )

    acs: dict[str, Any] = doc["acs"]
    for ac_id in REQUIRED_AC_IDS:
        row = acs.get(ac_id) or {}
        if row.get("pass") is not True:
            errors.append(f"ready: {ac_id} pass!=true ({row.get('pass')!r})")

    pre = doc.get("preconditions") or {}
    if pre.get("sender_alive") is not True:
        errors.append("ready: preconditions.sender_alive must be true")

    if require_greptime:
        if pre.get("greptime_sender_alive") is not True:
            errors.append("ready: preconditions.greptime_sender_alive must be true")
        versions = doc.get("versions") or {}
        gp = versions.get("greptime")
        if not gp or gp in ("", "missing", "skipped", None):
            errors.append(f"ready: versions.greptime must be pinned SHA, got {gp!r}")
        for ac_id in G_RATIO_IDS:
            row = acs.get(ac_id) or {}
            if row.get("softprobe_p95_ms") is None:
                errors.append(f"ready: {ac_id}.softprobe_p95_ms required")
            if row.get("greptime_p95_ms") is None:
                errors.append(f"ready: {ac_id}.greptime_p95_ms required")
            if row.get("ratio") is None:
                errors.append(f"ready: {ac_id}.ratio required")
        g0 = acs.get("AC-G0") or {}
        if g0.get("pass") is not True:
            errors.append("ready: AC-G0 must pass (pins + greptime_sender_alive)")
        g6 = acs.get("AC-G6") or {}
        if g6.get("pass") is not True:
            errors.append("ready: AC-G6 must pass (OTLP ingest on Greptime)")

    return errors


def validate_file(
    path: Path, *, ready: bool = False, require_greptime: bool = True
) -> list[str]:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        return [f"cannot read/parse {path}: {e}"]
    if ready:
        return validate_ready(doc, require_greptime=require_greptime)
    return validate_schema(doc)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "paths",
        nargs="+",
        type=Path,
        help="metrics-layout result JSON path(s)",
    )
    p.add_argument(
        "--ready",
        action="store_true",
        help="enforce ready gate (release + release_full + all pass + G9)",
    )
    p.add_argument(
        "--no-greptime",
        action="store_true",
        help="with --ready, skip Greptime field requirements (Softprobe-absolute only)",
    )
    args = p.parse_args(argv)

    all_errors: list[str] = []
    for path in args.paths:
        errs = validate_file(
            path, ready=args.ready, require_greptime=not args.no_greptime
        )
        if errs:
            all_errors.append(f"== {path} ==")
            all_errors.extend(f"  {e}" for e in errs)
        else:
            mode = "ready" if args.ready else "schema"
            print(f"OK ({mode}): {path}")

    if all_errors:
        print("\n".join(all_errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
