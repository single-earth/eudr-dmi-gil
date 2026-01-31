"""Maa-amet forest-layer crosscheck (compatibility shim).

This module exists to preserve a stable public path referenced by the Digital
Twin Dependencies page (“Used by”).

The authoritative Maa-amet forest reference is a WFS endpoint; this shim does
not perform network calls by default.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


MAA_AMET_FOREST_SOURCE: dict[str, str] = {
    "id": "maa-amet/forest/v1",
    "url": "https://gsavalik.envir.ee/geoserver/wfs",
    "expected_content_type": "application/xml",
    "server_audit_path": "/Users/server/audit/eudr_dmi/dependencies/maa_amet_forest_v1",
}


@dataclass(frozen=True)
class BBox:
    """Simple bbox container.

    Order matches common WFS usage: min_lon, min_lat, max_lon, max_lat.
    """

    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float


def get_dependency_source_record() -> Mapping[str, str]:
    """Return the dependency registry record for the Maa-amet forest layer."""

    return dict(MAA_AMET_FOREST_SOURCE)


def crosscheck_forest_area(
    *,
    bbox: BBox,
    observed_forest_area_m2: float | None,
    tolerance_ratio: float = 0.05,
) -> dict[str, Any]:
    """Return a deterministic scaffold result for Maa-amet crosscheck.

    This function intentionally does not call the Maa-amet WFS endpoint. It only
    returns a stable, machine-readable structure suitable for wiring into report
    scaffolds.
    """

    # Deterministic scaffold: we can only compare if an expected area is known.
    expected_forest_area_m2 = None
    delta_m2 = None
    discrepancy_pct = None

    status = None
    deterministic_rule = (
        "PASS if discrepancy_pct <= tolerance_ratio*100 else UNDETERMINED"
    )

    return {
        "status": status,
        "dependency": get_dependency_source_record(),
        "params": {
            "bbox": {
                "min_lon": bbox.min_lon,
                "min_lat": bbox.min_lat,
                "max_lon": bbox.max_lon,
                "max_lat": bbox.max_lat,
            },
            "tolerance_ratio": tolerance_ratio,
        },
        "comparison": {
            "deterministic_rule": deterministic_rule,
            "observed_forest_area_m2": observed_forest_area_m2,
            "observed_forest_area_ha": None
            if observed_forest_area_m2 is None
            else observed_forest_area_m2 / 10_000.0,
            "expected_forest_area_m2": expected_forest_area_m2,
            "expected_forest_area_ha": None,
            "delta_m2": delta_m2,
            "discrepancy_pct": discrepancy_pct,
        },
        "note": (
            "Compatibility shim: Maa-amet WFS acquisition and geometry/area "
            "integration not implemented in this repo snapshot."
        ),
    }
