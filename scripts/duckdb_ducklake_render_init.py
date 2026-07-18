#!/usr/bin/env python3
"""Emit DuckDB init SQL for ad-hoc shells from the same YAML as softprobe-runtime (CONFIG_FILE).

Parses the `ducklake` and `object_store` sections (simple two-space YAML subset).
Credentials are never read from YAML; they come from the environment:
  - `s3://` → `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` [/ `AWS_SESSION_TOKEN`]
  - `gs://` → `GCS_HMAC_ACCESS_KEY_ID` / `GCS_HMAC_SECRET` (or `GCP_HMAC_*`)

Writes shell exports to --meta for catalog/schema-qualified names in duckdb_ducklake_combo.sh.
"""
from __future__ import annotations

import argparse
import os
import re
import shlex
import sys
from pathlib import Path


def extract_section(lines: list[str], name: str) -> dict[str, str] | None:
    needle = f"{name}:"
    for i, line in enumerate(lines):
        if line.strip() == needle:
            section: dict[str, str] = {}
            j = i + 1
            while j < len(lines):
                l = lines[j]
                if l.strip() == "" or l.strip().startswith("#"):
                    j += 1
                    continue
                if not l.startswith("  ") or l.startswith("    "):
                    break
                m = re.match(r"^  ([A-Za-z0-9_]+):\s*(.*)$", l)
                if not m:
                    j += 1
                    continue
                k, v = m.group(1), m.group(2).split("#", 1)[0].strip()
                if not v:
                    section[k] = ""
                elif (v.startswith('"') and v.endswith('"')) or (
                    v.startswith("'") and v.endswith("'")
                ):
                    section[k] = v[1:-1]
                else:
                    section[k] = v
                j += 1
            return section
    return None


def load_config(path: Path) -> tuple[dict[str, str] | None, dict[str, str] | None]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    return (
        extract_section(lines, "ducklake"),
        extract_section(lines, "object_store"),
    )


def escape_sql_literal(s: str) -> str:
    return s.replace("'", "''")


def attach_target(dl: dict[str, str]) -> str:
    ct = dl.get("catalog_type", "duckdb")
    mp = dl.get("metadata_path", "")
    if not mp:
        sys.stderr.write("ducklake.metadata_path is empty\n")
        sys.exit(1)
    if ct == "postgres":
        return mp if mp.startswith("postgres:") else f"postgres:{mp}"
    if ct == "sqlite":
        return mp if mp.startswith("sqlite:") else f"sqlite:{mp}"
    return mp


def env_first(*names: str) -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def render_object_store_sql(data_path: str, object_store: dict[str, str] | None) -> list[str]:
    """Match runtime `configure_object_store` credential / endpoint setup."""
    object_store = object_store or {}
    parts: list[str] = []

    if data_path.startswith("gs://"):
        key_id = env_first("GCS_HMAC_ACCESS_KEY_ID", "GCP_HMAC_ACCESS_KEY_ID")
        secret = env_first("GCS_HMAC_SECRET", "GCP_HMAC_SECRET")
        if key_id and secret:
            parts += [
                "",
                (
                    "CREATE OR REPLACE SECRET gcs_hmac ("
                    f"TYPE GCS, KEY_ID '{escape_sql_literal(key_id)}', "
                    f"SECRET '{escape_sql_literal(secret)}'"
                    ");"
                ),
            ]
        else:
            sys.stderr.write(
                "warning: gs:// data_path but GCS_HMAC_ACCESS_KEY_ID/GCS_HMAC_SECRET "
                "(or GCP_HMAC_*) are unset; object-store I/O may fail\n"
            )
        return parts

    endpoint = (object_store.get("endpoint") or "").strip()
    region = (object_store.get("region") or "us-east-1").strip()
    if endpoint:
        host = endpoint.removeprefix("http://").removeprefix("https://")
        use_ssl = "false" if endpoint.startswith("http://") else "true"
        parts += [
            "",
            f"SET s3_endpoint = '{escape_sql_literal(host)}';",
            "SET s3_url_style = 'path';",
            f"SET s3_use_ssl = {use_ssl};",
        ]

    access_key = env_first("AWS_ACCESS_KEY_ID")
    secret_key = env_first("AWS_SECRET_ACCESS_KEY")
    session_token = env_first("AWS_SESSION_TOKEN")
    if access_key:
        parts.append(f"SET s3_access_key_id = '{escape_sql_literal(access_key)}';")
    if secret_key:
        parts.append(f"SET s3_secret_access_key = '{escape_sql_literal(secret_key)}';")
    if session_token:
        parts.append(f"SET s3_session_token = '{escape_sql_literal(session_token)}';")
    if data_path.startswith("s3://") or endpoint:
        parts.append(f"SET s3_region = '{escape_sql_literal(region)}';")
        if data_path.startswith("s3://") and not (access_key and secret_key):
            sys.stderr.write(
                "warning: s3:// data_path but AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                "are unset; object-store I/O may fail\n"
            )
    return parts


def render_attach_sql(
    dl: dict[str, str],
    object_store: dict[str, str] | None,
) -> str:
    ct = dl.get("catalog_type", "duckdb")
    alias = dl.get("catalog_alias", "softprobe")
    schema = dl.get("metadata_schema", "main")
    data_path = dl.get("data_path", "")
    if not data_path:
        sys.stderr.write("ducklake.data_path is empty\n")
        sys.exit(1)

    lim = dl.get("data_inlining_row_limit", "").strip()
    parts: list[str] = [
        "-- Generated by duckdb_ducklake_render_init.py (same ATTACH shape as runtime query worker)",
        "",
        "INSTALL httpfs;",
        "LOAD httpfs;",
        "INSTALL ducklake;",
        "LOAD ducklake;",
    ]
    if ct == "postgres":
        parts += ["INSTALL postgres;", "LOAD postgres;"]
    elif ct == "sqlite":
        parts += ["INSTALL sqlite;", "LOAD sqlite;"]

    parts += render_object_store_sql(data_path, object_store)

    opts = [f"DATA_PATH '{escape_sql_literal(data_path)}'"]
    if lim not in ("", "null", "None"):
        opts.append(f"DATA_INLINING_ROW_LIMIT {lim}")
    if ct == "postgres" and schema != "main":
        esc = escape_sql_literal(schema)
        opts.append(f"METADATA_SCHEMA '{esc}'")
        opts.append(f"META_SCHEMA '{esc}'")

    at = escape_sql_literal(attach_target(dl))
    parts += [
        "",
        f"ATTACH 'ducklake:{at}' AS {alias} ({', '.join(opts)});",
        "",
    ]
    return "\n".join(parts)


def qualified_base(dl: dict[str, str]) -> str:
    alias = dl.get("catalog_alias", "softprobe")
    schema = dl.get("metadata_schema", "main")
    if schema == "main":
        return f"{alias}"
    return f"{alias}.{schema}"


def write_meta(path: Path, dl: dict[str, str]) -> None:
    alias = dl.get("catalog_alias", "softprobe")
    schema = dl.get("metadata_schema", "main")
    qb = qualified_base(dl)
    lines = [
        f"SOFTPROBE_DL_CATALOG_ALIAS={shlex.quote(alias)}",
        f"SOFTPROBE_DL_METADATA_SCHEMA={shlex.quote(schema)}",
        f"SOFTPROBE_DL_QUALIFIED_PREFIX={shlex.quote(qb)}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def default_config_path(root: Path) -> Path | None:
    env = os.environ.get("CONFIG_FILE", "").strip()
    if env:
        p = Path(env)
        return p if p.is_file() else None
    # Host `duckdb-shell`: localhost Postgres/MinIO. Docker configs use service hostnames.
    for cand in (
        "tests/config/duckdb-shell-host.yaml",
        "config.yaml",
        "tests/config/test-docker.yaml",
    ):
        p = root / cand
        if p.is_file():
            return p
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--config", type=Path, help="Runtime YAML (default: CONFIG_FILE or test-docker/config.yaml)"
    )
    ap.add_argument("--meta", type=Path, required=True, help="Write shell sourceable exports for combo.sh")
    ap.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parent.parent, help="Repo root for defaults"
    )
    args = ap.parse_args()
    cfg_path = args.config or default_config_path(args.root)
    if cfg_path is None:
        sys.stderr.write(
            "No config found. Set CONFIG_FILE to your runtime YAML (with ducklake:), "
            "or create tests/config/test-docker.yaml / config.yaml.\n"
        )
        sys.exit(1)
    if not cfg_path.is_file():
        sys.stderr.write(f"Config not found: {cfg_path}\n")
        sys.exit(1)

    dl, object_store = load_config(cfg_path)
    if not dl:
        sys.stderr.write(f"No ducklake: section in {cfg_path}\n")
        sys.exit(1)

    write_meta(args.meta, dl)
    sys.stdout.write(
        f"-- CONFIG_FILE={cfg_path}\n"
        f"-- DuckLake scope: catalog_alias={dl.get('catalog_alias')} "
        f"metadata_schema={dl.get('metadata_schema')} data_path={dl.get('data_path')}\n"
    )
    sys.stdout.write(render_attach_sql(dl, object_store))


if __name__ == "__main__":
    main()
