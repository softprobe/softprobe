"""Emit panel targets for Grafana system-smoke dashboard checks.

Reads a Grafana dashboard API response on argv[1] and prints one
TAB-separated `panel_id<TAB>panel_type<TAB>target_json` record per target.

- Merges the panel-level datasource into targets (schema >= v37 keeps the
  datasource at panel level; /api/ds/query requires an identifier per query).
- Resolves dashboard template variables ("$name") into target strings, since
  /api/ds/query does not perform variable interpolation.
"""

import json
import pathlib
import sys

obj = json.loads(pathlib.Path(sys.argv[1]).read_text()).get("dashboard", {})
DOLLAR = chr(36)

variables = {}
for variable in obj.get("templating", {}).get("list", []):
    current = variable.get("current", {})
    if isinstance(current, dict) and current.get("value") is not None:
        variables[variable.get("name", "")] = str(current["value"])


def resolve(value):
    if isinstance(value, str):
        # Only the bare DOLLAR-name form appears in checked-in fixtures.
        for name, selected in variables.items():
            value = value.replace(DOLLAR + name, selected)
    return value


def emit(panel):
    window = {"from": obj.get("time", {}).get("from", "now-15m"), "to": obj.get("time", {}).get("to", "now")}
    range_field = window["from"] + "|" + window["to"]
    panel_datasource = panel.get("datasource")
    for target in panel.get("targets", []):
        if isinstance(target, dict):
            if not target.get("datasource") and panel_datasource:
                merged = dict(target)
                merged["datasource"] = panel_datasource
                target = merged
            target = {key: resolve(val) for key, val in target.items()}
        print("\t".join((str(panel.get("id", "unknown")), panel.get("type", "unknown"), json.dumps(target, sort_keys=True), range_field)))
    for child in panel.get("panels", []):
        emit(child)


for panel in obj.get("panels", []):
    emit(panel)
