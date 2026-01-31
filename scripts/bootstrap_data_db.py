#!/usr/bin/env python3
"""Bootstrap the repo-local DuckDB geodata catalogue.

Creates `data_db/geodata_catalogue.duckdb` (by default) and initializes a minimal
set of tables (empty is fine) so downstream tools can open the catalogue.

This script intentionally keeps all paths repo-relative.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_db_path() -> Path:
    return Path("data_db") / "geodata_catalogue.duckdb"


def create_schema(con) -> None:
    # Minimal catalogue tables. Kept deliberately generic.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS datasets (
            dataset_id TEXT PRIMARY KEY,
            name TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT current_timestamp
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS layers (
            layer_id TEXT PRIMARY KEY,
            dataset_id TEXT,
            name TEXT,
            crs_epsg INTEGER,
            geom_type TEXT,
            storage_uri TEXT,
            checksum_sha256 TEXT,
            created_at TIMESTAMP DEFAULT current_timestamp
        );
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sources (
            source_id TEXT PRIMARY KEY,
            url TEXT,
            expected_content_type TEXT,
            audit_path TEXT,
            sha256 TEXT,
            last_checked_at TIMESTAMP
        );
        """
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bootstrap data_db/geodata_catalogue.duckdb")
    parser.add_argument(
        "--path",
        default=str(default_db_path()),
        help="Repo-relative path to the DuckDB file (default: data_db/geodata_catalogue.duckdb)",
    )
    args = parser.parse_args(argv)

    db_rel = Path(args.path)
    if db_rel.is_absolute():
        print("ERROR: --path must be repo-relative (no absolute paths)", file=sys.stderr)
        return 2

    root = repo_root()
    db_path = (root / db_rel).resolve()

    (root / db_rel.parent).mkdir(parents=True, exist_ok=True)

    try:
        import duckdb  # type: ignore
    except Exception as e:
        print(
            "ERROR: duckdb Python package is required. Install with `pip install duckdb`.\n"
            f"Details: {e}",
            file=sys.stderr,
        )
        return 3

    con = duckdb.connect(str(db_path))
    try:
        create_schema(con)
    finally:
        con.close()

    print(f"OK: bootstrapped catalogue at {db_rel.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
