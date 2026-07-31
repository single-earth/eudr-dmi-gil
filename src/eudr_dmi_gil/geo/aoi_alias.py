"""Geometry-equivalence check for commodity-neutral AOI aliases.

Used when the same AOI geometry is reused under a different, commodity-neutral file (for
example, assessing a different commodity than the one implied by a legacy source AOI's
filename/properties) to verify the alias's geometry is byte-for-byte identical to the source
polygon: no buffering, simplification, reprojection, dissolve, or coordinate edits.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _extract_geometries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if payload.get("type") == "FeatureCollection":
        return [f["geometry"] for f in payload.get("features", []) if f.get("geometry")]
    if payload.get("type") == "Feature":
        return [payload["geometry"]] if payload.get("geometry") else []
    if payload.get("type") in {
        "Polygon",
        "MultiPolygon",
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "GeometryCollection",
    }:
        return [payload]
    return []


def geometries_are_identical(path_a: Path, path_b: Path) -> bool:
    """True iff every geometry in `path_a` exactly matches the corresponding one in `path_b`.

    Exact means identical coordinate values in identical order/structure (a strict equality
    check on the parsed GeoJSON geometry objects), not merely topologically equivalent -
    catching any buffering, simplification, reprojection, dissolve, or coordinate edit.
    """
    payload_a = json.loads(Path(path_a).read_text(encoding="utf-8"))
    payload_b = json.loads(Path(path_b).read_text(encoding="utf-8"))
    geoms_a = _extract_geometries(payload_a)
    geoms_b = _extract_geometries(payload_b)
    if len(geoms_a) != len(geoms_b) or not geoms_a:
        return False
    return geoms_a == geoms_b
