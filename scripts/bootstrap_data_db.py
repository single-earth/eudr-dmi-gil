#!/usr/bin/env python3
"""Bootstrap the repo-local DuckDB geodata catalogue.

CSV-seed-first contract:

Inputs (tracked in git, under `data_db/` by default):
- `dataset_catalogue_auto.csv`
- `dataset_families_summary.csv`

Outputs (generated locally; ignored by git):
- `geodata_catalogue.duckdb`
- Optional: `dataset_catalogue_with_families.csv`

This script intentionally keeps all paths repo-relative.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


DEFAULT_DATA_DIR = Path("data_db")
DEFAULT_DB_NAME = "geodata_catalogue.duckdb"


def ensure_repo_relative(path: Path, *, label: str) -> None:
    if path.is_absolute():
        raise ValueError(f"{label} must be repo-relative (no absolute paths): {path}")


def resolve_under_repo(rel: Path) -> Path:
    root = repo_root().resolve()
    resolved = (root / rel).resolve()
    if root not in resolved.parents and resolved != root:
        raise ValueError(f"Path escapes repo root: {rel} -> {resolved}")
    return resolved


def default_db_path(data_dir: Path) -> Path:
    return data_dir / DEFAULT_DB_NAME


def seed_paths(data_dir: Path) -> tuple[Path, Path]:
    return (
        data_dir / "dataset_catalogue_auto.csv",
        data_dir / "dataset_families_summary.csv",
    )


def import_seed_csv(con, *, table: str, csv_path: Path) -> None:
    # Deterministic import: treat all columns as VARCHAR and keep file order.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM read_csv(?, header=true, all_varchar=true)
        """,
        [str(csv_path)],
    )


def table_columns_lower(con, table: str) -> set[str]:
    cols = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return {row[1].lower() for row in cols}


def create_joined_table_if_possible(con, *, export_csv_path: Path | None) -> bool:
    left_cols = table_columns_lower(con, "dataset_catalogue_auto")
    right_cols = table_columns_lower(con, "dataset_families_summary")

    if "dataset_id" not in left_cols or "dataset_id" not in right_cols:
        return False

    con.execute(
        """
        CREATE OR REPLACE TABLE dataset_catalogue_with_families AS
        SELECT a.*, f.* EXCLUDE (dataset_id)
        FROM dataset_catalogue_auto a
        LEFT JOIN dataset_families_summary f
        USING (dataset_id)
        """
    )

    if export_csv_path is not None:
        con.execute(
            "COPY dataset_catalogue_with_families TO ? (HEADER, DELIMITER ',')",
            [str(export_csv_path)],
        )

    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Bootstrap data_db/geodata_catalogue.duckdb from git-tracked CSV seeds "
            "(default mode: --from-csv)."
        )
    )
    parser.add_argument(
        "--from-csv",
        action="store_true",
        default=True,
        help="Generate the DuckDB catalogue from CSV seeds (default).",
    )
    parser.add_argument(
        "--data-dir",
        default=str(DEFAULT_DATA_DIR),
        help="Repo-relative directory holding seed CSVs (default: data_db).",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help=(
            "Repo-relative path to the DuckDB file (default: <data-dir>/geodata_catalogue.duckdb)"
        ),
    )
    parser.add_argument(
        "--path",
        dest="db_path",
        default=None,
        help="Deprecated alias for --db-path.",
    )
    parser.add_argument(
        "--export-with-families-csv",
        action="store_true",
        help=(
            "Also export data_db/dataset_catalogue_with_families.csv (repo-relative; ignored by git)."
        ),
    )
    args = parser.parse_args(argv)

    try:
        data_dir_rel = Path(args.data_dir)
        ensure_repo_relative(data_dir_rel, label="--data-dir")

        db_rel = (
            Path(args.db_path)
            if args.db_path is not None
            else default_db_path(data_dir_rel)
        )
        ensure_repo_relative(db_rel, label="--db-path")
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    root = repo_root()
    db_path_abs = resolve_under_repo(db_rel)

    (root / data_dir_rel).mkdir(parents=True, exist_ok=True)
    db_path_abs.parent.mkdir(parents=True, exist_ok=True)

    catalogue_csv_rel, families_csv_rel = seed_paths(data_dir_rel)
    catalogue_csv = resolve_under_repo(catalogue_csv_rel)
    families_csv = resolve_under_repo(families_csv_rel)

    if not catalogue_csv.exists() or not families_csv.exists():
        missing = []
        if not catalogue_csv.exists():
            missing.append(catalogue_csv_rel.as_posix())
        if not families_csv.exists():
            missing.append(families_csv_rel.as_posix())
        print(
            "ERROR: missing seed CSV(s): " + ", ".join(missing) + "\n"
            "Expected git-tracked inputs under data_dir.",
            file=sys.stderr,
        )
        return 2

    try:
        import duckdb  # type: ignore
    except Exception as e:
        print(
            "ERROR: duckdb Python package is required. Install with `pip install -r requirements-methods.txt`.\n"
            f"Details: {e}",
            file=sys.stderr,
        )
        return 3

    con = duckdb.connect(str(db_path_abs))
    try:
        import_seed_csv(con, table="dataset_catalogue_auto", csv_path=catalogue_csv)
        import_seed_csv(con, table="dataset_families_summary", csv_path=families_csv)

        export_csv = None
        if args.export_with_families_csv:
            export_csv = resolve_under_repo(data_dir_rel / "dataset_catalogue_with_families.csv")

        created = create_joined_table_if_possible(con, export_csv_path=export_csv)
    finally:
        con.close()

    print(f"OK: bootstrapped catalogue at {db_rel.as_posix()}")
    if created:
        print("OK: created dataset_catalogue_with_families")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
