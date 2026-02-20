#!/usr/bin/env python3
"""Export dependency sources registry from DuckDB or CSV seed.

Deterministic export (stable ordering, no timestamps).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SERVER_AUDIT_ROOT_DEFAULT = "/Users/server/audit/eudr_dmi"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def ensure_repo_relative(path: Path, *, label: str) -> None:
    if path.is_absolute():
        raise ValueError(f"{label} must be repo-relative (no absolute paths): {path}")


def resolve_under_repo(rel: Path) -> Path:
    root = repo_root().resolve()
    resolved = (root / rel).resolve()
    if root not in resolved.parents and resolved != root:
        raise ValueError(f"Path escapes repo root: {rel} -> {resolved}")
    return resolved


def _load_from_duckdb(db_path: Path) -> list[dict[str, Any]] | None:
    try:
        import duckdb  # type: ignore
    except Exception:
        return None

    if not db_path.exists():
        return None

    con = duckdb.connect(str(db_path))
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if "dependency_sources" not in tables:
            return None
        rows = con.execute("SELECT * FROM dependency_sources").fetchall()
        cols = [row[1] for row in con.execute("PRAGMA table_info('dependency_sources')").fetchall()]
    finally:
        con.close()

    records = []
    for row in rows:
        records.append({cols[i]: row[i] for i in range(len(cols))})
    return records


def _load_from_csv(csv_path: Path) -> list[dict[str, Any]]:
    import csv

    with csv_path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader]


def _normalize_sources(records: list[dict[str, Any]]) -> list[dict[str, str]]:
    sources: list[dict[str, str]] = []
    for row in records:
        dep_id = (row.get("dependency_id") or "").strip()
        url = (row.get("url") or "").strip()
        if not dep_id or not url:
            continue
        sources.append(
            {
                "id": dep_id,
                "url": url,
                "expected_content_type": (row.get("expected_content_type") or "").strip(),
                "server_audit_path": (row.get("server_audit_path") or "").strip(),
            }
        )
    return sorted(sources, key=lambda item: item["id"])


def write_sources_json(path: Path, *, sources: list[dict[str, str]], server_audit_root: str) -> None:
    payload = {
        "schema_version": "1.0",
        "server_audit_root": server_audit_root,
        "sources": sources,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_sources_md(path: Path, *, sources: list[dict[str, str]], server_audit_root: str) -> None:
    rows = [
        "| id | url | expected content type | server audit path |",
        "|---|---|---|---|",
    ]
    for src in sources:
        dep_id = src.get("id", "")
        url = src.get("url", "")
        content_type = src.get("expected_content_type", "")
        server_audit_path = src.get("server_audit_path", "")
        rows.append(
            "| {id} | {url} | {ctype} | {path} |".format(
                id=f"`{dep_id}`" if dep_id else "",
                url=url,
                ctype=f"`{content_type}`" if content_type else "",
                path=f"`{server_audit_path}`" if server_audit_path else "",
            )
        )

    md = """# Dependency Sources Registry

_This file is generated from data_db/dependency_sources.csv; do not edit by hand._

## Role in the ecosystem

This repository is the authoritative implementation for dependency provenance. The Digital Twin repository is the public, non-authoritative portal for inspection and governance.

Purpose: record the upstream dependency identifiers, URLs, expected content types, and server audit paths referenced by the Digital Twin Dependencies page.

Implementation references (authoritative):
- [docs/dependencies/README.md](README.md)
- [docs/architecture/dependency_register.md](../architecture/dependency_register.md)

Server audit root (convention): `{server_audit_root}`

{table}

Notes:
- Do not paste upstream content into this repository.
- Store any mirrored/verified artifacts under the corresponding server audit path.

Changes to dependencies or evidence sources may originate from stakeholder DAO proposals reviewed via the DTE.

## See also

- [README.md](../../README.md)
- [docs/governance/roles_and_workflow.md](../governance/roles_and_workflow.md)
- Digital Twin DTE Instructions (Canonical): https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/dte_instructions.md
- Digital Twin Inspection Index: https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/INSPECTION_INDEX.md
- https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin
""".format(server_audit_root=server_audit_root, table="\n".join(rows))

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(md, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export dependency sources to docs/dependencies/sources.*")
    parser.add_argument(
        "--data-dir",
        default="data_db",
        help="Repo-relative directory holding seed CSVs (default: data_db)",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Repo-relative path to DuckDB catalogue (default: <data-dir>/geodata_catalogue.duckdb)",
    )
    args = parser.parse_args(argv)

    try:
        data_dir_rel = Path(args.data_dir)
        ensure_repo_relative(data_dir_rel, label="--data-dir")
        db_rel = Path(args.db_path) if args.db_path is not None else data_dir_rel / "geodata_catalogue.duckdb"
        ensure_repo_relative(db_rel, label="--db-path")
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    db_path = resolve_under_repo(db_rel)
    csv_path = resolve_under_repo(data_dir_rel / "dependency_sources.csv")

    records = _load_from_duckdb(db_path)
    if records is None:
        if not csv_path.exists():
            print(f"ERROR: missing dependency_sources.csv at {csv_path}", file=sys.stderr)
            return 2
        records = _load_from_csv(csv_path)

    sources = _normalize_sources(records)

    server_audit_root = SERVER_AUDIT_ROOT_DEFAULT

    json_path = resolve_under_repo(Path("docs/dependencies/sources.json"))
    md_path = resolve_under_repo(Path("docs/dependencies/sources.md"))

    write_sources_json(json_path, sources=sources, server_audit_root=server_audit_root)
    write_sources_md(md_path, sources=sources, server_audit_root=server_audit_root)

    print(f"OK: exported dependency sources to {json_path} and {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
