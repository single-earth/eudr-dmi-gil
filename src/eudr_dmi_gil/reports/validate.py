from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import jsonschema
from jsonschema import Draft202012Validator


def _find_repo_root(start: Path) -> Path:
    current = start
    for _ in range(10):
        if (current / "pyproject.toml").exists() and (current / "schemas").exists():
            return current
        if current.parent == current:
            break
        current = current.parent
    raise RuntimeError("Could not locate repo root (pyproject.toml + schemas/) from: " + str(start))


def _default_schema_path() -> Path:
    repo_root = _find_repo_root(Path(__file__).resolve())
    return repo_root / "schemas" / "reports" / "aoi_report_v1.schema.json"


def load_schema(schema_path: str | Path | None = None) -> dict[str, Any]:
    path = Path(schema_path) if schema_path is not None else _default_schema_path()
    return json.loads(path.read_text(encoding="utf-8"))


def validate_aoi_report_v1(
    report: Mapping[str, Any],
    *,
    schema_path: str | Path | None = None,
) -> None:
    """Validate an AOI report JSON object against the AOI report v1 schema.

    Raises:
      jsonschema.exceptions.ValidationError if invalid.
    """

    schema = load_schema(schema_path)
    validator = Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
    validator.validate(dict(report))


def validate_aoi_report_v1_file(
    json_path: str | Path,
    *,
    schema_path: str | Path | None = None,
) -> dict[str, Any]:
    """Load and validate a report JSON file; returns the parsed JSON."""

    obj = json.loads(Path(json_path).read_text(encoding="utf-8"))
    validate_aoi_report_v1(obj, schema_path=schema_path)
    return obj
