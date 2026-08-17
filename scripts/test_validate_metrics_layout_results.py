#!/usr/bin/env python3
"""Unit tests for validate-metrics-layout-results.py (§10.3.1)."""

from __future__ import annotations

import copy
import importlib.util
import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
MODULE_PATH = SCRIPTS / "validate-metrics-layout-results.py"


def _load():
    spec = importlib.util.spec_from_file_location(
        "validate_metrics_layout_results", MODULE_PATH
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


V = _load()


def _minimal_doc(*, all_pass: bool = True, ready_profiles: bool = True) -> dict:
    acs = {}
    for ac_id in V.REQUIRED_AC_IDS:
        row = {
            "pass": all_pass,
            "p95_ms": None,
            "softprobe_p95_ms": None,
            "greptime_p95_ms": None,
            "ratio": None,
            "fixture_scale": {},
            "explain_ok": None,
            "notes": "",
        }
        if ac_id.startswith("AC-G") and ac_id != "AC-G0" and ac_id != "AC-G6":
            row["softprobe_p95_ms"] = 100
            row["greptime_p95_ms"] = 20
            row["ratio"] = 5.0
        acs[ac_id] = row
    return {
        "schema_version": 1,
        "suite": "metrics-layout",
        "binary_profile": "release" if ready_profiles else "dev",
        "fixture_profile": "release_full" if ready_profiles else "pr_floor",
        "git_sha": "abc1234",
        "fixture_hash": "deadbeef",
        "stamp": "20260815T000000Z",
        "versions": {
            "softprobe": "abc1234",
            "greptime": "a8924bb95c43b562e4312af309d2a5e80c103185",
            "duckdb": "1.4.3",
            "ducklake": "0.x",
            "postgres": "19",
            "machine_class": "linux-amd64",
            "R": 10,
        },
        "preconditions": {
            "AC-F2_bytes_before_merge": 16 * 1024 * 1024,
            "AC-F5_precondition_met": True,
            "sender_alive": True,
            "greptime_sender_alive": True,
        },
        "acs": acs,
    }


class TestRequiredIds(unittest.TestCase):
    def test_exactly_fifty_three(self):
        self.assertEqual(len(V.REQUIRED_AC_IDS), 53)
        self.assertEqual(len(set(V.REQUIRED_AC_IDS)), 53)
        for hid in ("AC-H1", "AC-H2", "AC-H3", "AC-H4", "AC-H5", "AC-H6"):
            self.assertIn(hid, V.REQUIRED_AC_IDS)

    def test_includes_g_and_q_bounds(self):
        self.assertIn("AC-Q0", V.REQUIRED_AC_IDS)
        self.assertIn("AC-Q9", V.REQUIRED_AC_IDS)
        self.assertIn("AC-G0", V.REQUIRED_AC_IDS)
        self.assertIn("AC-G6", V.REQUIRED_AC_IDS)
        self.assertIn("AC-D1", V.REQUIRED_AC_IDS)
        self.assertIn("AC-M2", V.REQUIRED_AC_IDS)


class TestSchema(unittest.TestCase):
    def test_minimal_ok(self):
        self.assertEqual(V.validate_schema(_minimal_doc()), [])

    def test_missing_ac_rejected(self):
        doc = _minimal_doc()
        del doc["acs"]["AC-Q1"]
        errs = V.validate_schema(doc)
        self.assertTrue(any("AC-Q1" in e for e in errs))

    def test_wrong_suite(self):
        doc = _minimal_doc()
        doc["suite"] = "other"
        errs = V.validate_schema(doc)
        self.assertTrue(any("suite" in e for e in errs))

    def test_pr_floor_schema_ok(self):
        doc = _minimal_doc(ready_profiles=False)
        self.assertEqual(V.validate_schema(doc), [])

    def test_pass_must_be_bool(self):
        doc = _minimal_doc()
        doc["acs"]["AC-D1"]["pass"] = "yes"
        errs = V.validate_schema(doc)
        self.assertTrue(any("pass" in e for e in errs))


class TestReadyGate(unittest.TestCase):
    def test_all_pass_ready(self):
        self.assertEqual(V.validate_ready(_minimal_doc()), [])

    def test_pr_floor_rejected(self):
        doc = _minimal_doc(ready_profiles=False)
        errs = V.validate_ready(doc)
        self.assertTrue(any("fixture_profile" in e for e in errs))
        self.assertTrue(any("binary_profile" in e for e in errs))

    def test_one_fail_rejected(self):
        doc = _minimal_doc()
        doc["acs"]["AC-Q1"]["pass"] = False
        errs = V.validate_ready(doc)
        self.assertTrue(any("AC-Q1" in e for e in errs))

    def test_missing_greptime_ratio_rejected(self):
        doc = _minimal_doc()
        doc["acs"]["AC-G1"]["greptime_p95_ms"] = None
        doc["acs"]["AC-G1"]["ratio"] = None
        errs = V.validate_ready(doc)
        self.assertTrue(any("AC-G1" in e and "greptime" in e for e in errs))

    def test_ready_no_greptime_skips_g_fields(self):
        doc = _minimal_doc()
        doc["versions"]["greptime"] = "skipped"
        doc["preconditions"]["greptime_sender_alive"] = False
        for ac_id in V.G_RATIO_IDS:
            doc["acs"][ac_id]["greptime_p95_ms"] = None
            doc["acs"][ac_id]["ratio"] = None
        # Softprobe-absolute ready still requires all pass including G*
        doc["acs"]["AC-G0"]["pass"] = False
        errs = V.validate_ready(doc, require_greptime=False)
        self.assertTrue(any("AC-G0" in e for e in errs))
        # With G* all pass but greptime skipped, no-greptime ready accepts G pins skip
        doc2 = copy.deepcopy(doc)
        doc2["acs"]["AC-G0"]["pass"] = True
        for ac_id in V.G_RATIO_IDS:
            doc2["acs"][ac_id]["pass"] = True
            doc2["acs"][ac_id]["softprobe_p95_ms"] = 1
        errs2 = V.validate_ready(doc2, require_greptime=False)
        self.assertEqual(errs2, [])


if __name__ == "__main__":
    unittest.main()
