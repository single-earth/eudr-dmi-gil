from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from eudr_dmi_gil.reports.determinism import write_json


@dataclass(frozen=True)
class ParcelRecord:
    parcel_id: str
    forest_area_ha: float


@dataclass(frozen=True)
class MaaAmetCrosscheckResult:
    outcome: str
    tolerance_percent: float
    reason: str | None
    fields_used: list[str]
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
            return [
                ParcelRecord(
                    parcel_id=str(item.get("parcel_id")),
                    forest_area_ha=float(item.get("forest_area_ha")),
                )
                for item in data
            ]
        if self._path.suffix.lower() == ".csv":
            rows = []
            with self._path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(
                        ParcelRecord(
                            parcel_id=str(row.get("parcel_id")),
                            forest_area_ha=float(row.get("forest_area_ha")),
                        )
                    )
            return rows
        return []


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows_list = list(rows)
    headers = [
        "parcel_id",
        "maaamet_forest_area_ha",
        "computed_forest_area_ha",
        "diff_pct",
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
        if computed_forest_area_ha is None or parcel.forest_area_ha <= 0:
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
                "maaamet_forest_area_ha": round(parcel.forest_area_ha, 6),
                "computed_forest_area_ha": None
                if computed_forest_area_ha is None
                else round(computed_forest_area_ha, 6),
                "diff_pct": None if diff_pct is None else round(diff_pct, 6),
            }
        )

    csv_path = output_dir / "maaamet_forest_area_crosscheck.csv"
    _write_csv(csv_path, rows)

    total_maaamet = sum(p.forest_area_ha for p in parcels)
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
        outcome = (
            "consistent"
            if abs(diff_pct_total) <= tolerance_percent
            else "divergent"
        )
        reason = None

    summary = {
        "outcome": outcome,
        "reason": reason,
        "fields_used": ["forest_area_ha"],
        "comparison": {
            "tolerance_percent": tolerance_percent,
            "computed_forest_area_ha": computed_forest_area_ha,
            "maaamet_forest_area_ha": total_maaamet,
            "diff_pct": diff_pct_total,
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
        csv_path=csv_path,
        summary_path=summary_path,
    )
