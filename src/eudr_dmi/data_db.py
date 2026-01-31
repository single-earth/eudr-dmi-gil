"""Repo-local DuckDB geodata catalogue path helpers.

This repository treats the DuckDB geodata catalogue as a first-class component
under `data_db/`.

All defaults are repo-relative (no hard-coded absolute paths).
"""

from __future__ import annotations

import os
from pathlib import Path


ENV_GEODATA_CATALOGUE_PATH = "EUDR_GEODATA_CATALOGUE_PATH"
DEFAULT_CATALOGUE_REL = Path("data_db") / "geodata_catalogue.duckdb"


def find_repo_root(start: Path | None = None) -> Path:
    """Find repo root by walking upward to `pyproject.toml`.

    Falls back to the expected `src/..` layout if not found.
    """

    anchor = (start or Path(__file__)).resolve()
    for candidate in [anchor, *anchor.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    # Fallback for `src/eudr_dmi/...` layout
    return anchor.parents[3]


def get_catalogue_path() -> Path:
    """Return the resolved path to the geodata catalogue.

    If `EUDR_GEODATA_CATALOGUE_PATH` is set, it must be a repo-relative path.
    """

    root = find_repo_root()
    configured = os.getenv(ENV_GEODATA_CATALOGUE_PATH)

    rel = DEFAULT_CATALOGUE_REL if not configured else Path(configured)

    if rel.is_absolute():
        raise ValueError(
            f"{ENV_GEODATA_CATALOGUE_PATH} must be repo-relative, got absolute path: {rel}"
        )

    resolved = (root / rel).resolve()

    # Guardrail: keep inside repo root.
    if root not in resolved.parents and resolved != root:
        raise ValueError(
            f"Catalogue path escapes repo root: {rel} -> {resolved}. Use a repo-relative path under the repo."
        )

    return resolved
