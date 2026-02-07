from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from pyproj import Geod
from shapely.geometry import shape

from eudr_dmi_gil.reports.determinism import write_json


@dataclass(frozen=True)
class ParcelRecord:
    parcel_id: str
    forest_area_ha: float | None
    reference_source: str
    reference_method: str


@dataclass(frozen=True)
class MaaAmetCrosscheckResult:
    outcome: str
    tolerance_percent: float
    reason: str | None
    fields_used: list[str]
    reference_source: str
    reference_method: str
    reference_value_ha: float | None
    computed_forest_area_ha: float | None
    diff_pct: float | None
    csv_path: Path
    summary_path: Path


class MaaAmetProvider:
    def fetch_parcels(self, *, aoi_geojson_path: Path) -> list[ParcelRecord]:
        raise NotImplementedError


class LocalFileMaaAmetProvider(MaaAmetProvider):
    def __init__(self, path: Path) -> None:
        self._path = path

    def fetch_parcels(self, *, aoi_geojson_path: Path) -> list[ParcelRecord]:
        if not self._path.exists():
            return []
        if self._path.suffix.lower() == ".json":
            data = json.loads(self._path.read_text(encoding="utf-8"))
            return _parcels_from_json(data)
        if self._path.suffix.lower() == ".csv":
            rows = []
            with self._path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    forest_area_value = row.get("forest_area_ha")
                    forest_area_ha = (
                        float(forest_area_value)
                        if forest_area_value not in (None, "")
                        else None
                    )
                    rows.append(
                        ParcelRecord(
                            parcel_id=str(row.get("parcel_id")),
                            forest_area_ha=forest_area_ha,
                            reference_source="attribute:forest_area_ha"
                            if forest_area_ha is not None
                            else "missing",
                            reference_method="reported" if forest_area_ha is not None else "missing",
                        )
                    )
            return rows
        return []


def _geodesic_area_ha(geom: dict[str, Any]) -> float:
    geod = Geod(ellps="WGS84")
    area_m2, _ = geod.geometry_area_perimeter(shape(geom))
    return abs(float(area_m2)) / 10_000.0


def _parcels_from_json(data: Any) -> list[ParcelRecord]:
    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        parcels: list[ParcelRecord] = []
        for idx, feat in enumerate(data.get("features", []), start=1):
            props = feat.get("properties") or {}
            parcel_id = str(props.get("parcel_id") or f"parcel-{idx}")
            forest_area_value = props.get("forest_area_ha")
            if forest_area_value not in (None, ""):
                forest_area_ha = float(forest_area_value)
                reference_source = "attribute:forest_area_ha"
                reference_method = "reported"
            else:
                geom = feat.get("geometry")
                if geom:
                    forest_area_ha = _geodesic_area_ha(geom)
                    reference_source = "geometry"
                    reference_method = "geodesic_wgs84_pyproj"
                else:
                    forest_area_ha = None
                    reference_source = "missing"
                    reference_method = "missing"
            parcels.append(
                ParcelRecord(
                    parcel_id=parcel_id,
                    forest_area_ha=None if forest_area_ha is None else round(forest_area_ha, 6),
                    reference_source=reference_source,
                    reference_method=reference_method,
                )
            )
        return parcels

    if isinstance(data, list):
        parcels = []
        for idx, item in enumerate(data, start=1):
            if not isinstance(item, dict):
                continue
            parcel_id = str(item.get("parcel_id") or f"parcel-{idx}")
            forest_area_value = item.get("forest_area_ha")
            if forest_area_value not in (None, ""):
                forest_area_ha = float(forest_area_value)
                reference_source = "attribute:forest_area_ha"
                reference_method = "reported"
            else:
                geom = item.get("geometry")
                if geom:
                    forest_area_ha = _geodesic_area_ha(geom)
                    reference_source = "geometry"
                    reference_method = "geodesic_wgs84_pyproj"
                else:
                    forest_area_ha = None
                    reference_source = "missing"
                    reference_method = "missing"
            parcels.append(
                ParcelRecord(
                    parcel_id=parcel_id,
                    forest_area_ha=None if forest_area_ha is None else round(forest_area_ha, 6),
                    reference_source=reference_source,
                    reference_method=reference_method,
                )
            )
        return parcels

    return []


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows_list = list(rows)
    headers = [
        "parcel_id",
        "reference_area_ha",
        "reference_source",
        "reference_method",
        "computed_forest_area_ha",
        "diff_pct",
        "tolerance_percent",
        "outcome",
        "reason",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows_list:
            writer.writerow({h: row.get(h) for h in headers})


def run_maaamet_crosscheck(
    *,
    aoi_geojson_path: Path,
    output_dir: Path,
    computed_forest_area_ha: float | None,
    tolerance_percent: float = 5.0,
    provider: MaaAmetProvider | None = None,
) -> MaaAmetCrosscheckResult:
    output_dir.mkdir(parents=True, exist_ok=True)

    if provider is None:
        env_path = os.environ.get("EUDR_DMI_MAAAMET_LOCAL_PATH")
        provider = LocalFileMaaAmetProvider(Path(env_path)) if env_path else None

    parcels: list[ParcelRecord] = []
    if provider is not None:
        parcels = provider.fetch_parcels(aoi_geojson_path=aoi_geojson_path)

    rows = []
    for parcel in sorted(parcels, key=lambda p: p.parcel_id):
        if computed_forest_area_ha is None or not parcel.forest_area_ha or parcel.forest_area_ha <= 0:
            diff_pct = None
        else:
            diff_pct = (
                (computed_forest_area_ha - parcel.forest_area_ha)
                / parcel.forest_area_ha
                * 100.0
            )
        rows.append(
            {
                "parcel_id": parcel.parcel_id,
                "reference_area_ha": parcel.forest_area_ha,
                "reference_source": parcel.reference_source,
                "reference_method": parcel.reference_method,
                "computed_forest_area_ha": None
                if computed_forest_area_ha is None
                else round(computed_forest_area_ha, 6),
                "diff_pct": None if diff_pct is None else round(diff_pct, 6),
                "tolerance_percent": tolerance_percent,
                "outcome": "not_comparable" if diff_pct is None else "comparable",
                "reason": "missing_reference_forest_area"
                if parcel.forest_area_ha is None
                else ("missing_computed_forest_area" if computed_forest_area_ha is None else None),
            }
        )

    csv_path = output_dir / "maaamet_forest_area_crosscheck.csv"
    _write_csv(csv_path, rows)

    total_maaamet = sum(p.forest_area_ha for p in parcels if p.forest_area_ha)
    if computed_forest_area_ha is None or total_maaamet <= 0:
        outcome = "not_comparable"
        diff_pct_total = None
        if computed_forest_area_ha is None:
            reason = "missing_computed_forest_area"
        elif total_maaamet <= 0:
            reason = "missing_reference_forest_area"
        else:
            reason = "not_comparable"
    else:
        diff_pct_total = (
            (computed_forest_area_ha - total_maaamet) / total_maaamet * 100.0
        )
        outcome = "pass" if abs(diff_pct_total) <= tolerance_percent else "fail"
        reason = None

    reference_sources = sorted({p.reference_source for p in parcels if p.reference_source})
    reference_methods = sorted({p.reference_method for p in parcels if p.reference_method})
    reference_source = (
        reference_sources[0]
        if len(reference_sources) == 1
        else "mixed" if reference_sources else "missing"
    )
    reference_method = (
        reference_methods[0]
        if len(reference_methods) == 1
        else "mixed" if reference_methods else "missing"
    )

    summary = {
        "outcome": outcome,
        "reason": reason,
        "reference": {
            "source": reference_source,
            "method": reference_method,
            "value_ha": None if total_maaamet <= 0 else round(total_maaamet, 6),
        },
        "computed": {
            "forest_area_ha": None
            if computed_forest_area_ha is None
            else round(computed_forest_area_ha, 6)
        },
        "fields_used": ["forest_area_ha"],
        "comparison": {
            "tolerance_percent": tolerance_percent,
            "diff_pct": None if diff_pct_total is None else round(diff_pct_total, 6),
            "status": "not_comparable" if outcome == "not_comparable" else "comparable",
        },
        "csv_relpath": csv_path.name,
    }

    summary_path = output_dir / "maaamet_forest_area_crosscheck.json"
    write_json(summary_path, summary)

    return MaaAmetCrosscheckResult(
        outcome=outcome,
        tolerance_percent=tolerance_percent,
        reason=reason,
        fields_used=["forest_area_ha"],
        reference_source=reference_source,
        reference_method=reference_method,
        reference_value_ha=None if total_maaamet <= 0 else round(total_maaamet, 6),
        computed_forest_area_ha=None
        if computed_forest_area_ha is None
        else round(computed_forest_area_ha, 6),
        diff_pct=None if diff_pct_total is None else round(diff_pct_total, 6),
        csv_path=csv_path,
        summary_path=summary_path,
    )
