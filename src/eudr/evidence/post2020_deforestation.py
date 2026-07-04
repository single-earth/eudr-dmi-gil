from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from eudr.evidence.change_layers import radd, sentinel_confirmation, tmf
from eudr.evidence.change_layers.hansen import compute_hansen_loss_inside_gfc2020
from eudr.evidence.dataset_registry import (
    DATASET_REGISTRY,
    get_dataset,
    resolve_end_year,
    selected_change_datasets,
)
from eudr.evidence.decision_rules import build_evidence_state
from eudr.evidence.gfc2020_baseline import (
    compute_gfc2020_baseline,
    load_aoi_geometry,
    parse_forest_values,
)


LIMITATIONS = [
    "GFC2020 is a baseline evidence layer, not a legal determination.",
    "Tree-cover loss is not automatically EUDR deforestation.",
    "Agricultural conversion evidence is required before inferring EUDR-relevant deforestation.",
    "Dataset disagreement must be preserved as an evidence conflict, not hidden.",
    "The resolved end year reflects dataset availability, not necessarily the current calendar year.",
]


def generated_at_utc() -> str:
    override = os.environ.get("EUDR_EVIDENCE_GENERATED_AT_UTC", "").strip()
    if override:
        dt = datetime.fromisoformat(override.replace("Z", "+00:00"))
    else:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        raise ValueError("Generated timestamp must include timezone information")
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def infer_aoi_id(aoi_geojson_path: Path) -> str:
    try:
        data = json.loads(aoi_geojson_path.read_text(encoding="utf-8"))
    except Exception:
        return aoi_geojson_path.stem
    if data.get("type") == "FeatureCollection":
        for feat in data.get("features", []):
            props = feat.get("properties") or {}
            for key in ("aoi_id", "plot_id", "id", "name"):
                value = props.get(key)
                if value:
                    return str(value)
    if data.get("type") == "Feature":
        props = data.get("properties") or {}
        for key in ("aoi_id", "plot_id", "id", "name"):
            value = props.get(key)
            if value:
                return str(value)
    return aoi_geojson_path.stem


def _used_through_year(dataset_id: str, resolved_end_year: int) -> int | None:
    dataset = get_dataset(dataset_id)
    if dataset.latest_available_year is None:
        return None
    return min(dataset.latest_available_year, resolved_end_year)


def _dataset_versions(selected_optional: list[str], resolved_end_year: int) -> dict[str, Any]:
    versions: dict[str, Any] = {
        "gfc2020": DATASET_REGISTRY["gfc2020"].output_version(used_through_year=2020),
        "hansen_gfc": DATASET_REGISTRY["hansen_gfc"].output_version(
            used_through_year=resolved_end_year
        ),
    }
    for dataset_id in sorted(selected_optional):
        versions[dataset_id] = get_dataset(dataset_id).output_version(
            used_through_year=_used_through_year(dataset_id, resolved_end_year)
        )
    return versions


def build_post2020_evidence(
    *,
    aoi_geojson_path: Path,
    gfc2020_raster_path: Path,
    hansen_lossyear_raster_path: Path,
    out_path: Path | None,
    start_year: int,
    requested_end_year: str,
    include_tmf: bool,
    include_radd: bool,
    include_sentinel_confirmation: bool,
    min_report_area_ha: float,
    forest_values: tuple[int, ...] = (1,),
) -> dict[str, Any]:
    optional = []
    if include_tmf:
        optional.append("jrc_tmf")
    if include_radd:
        optional.append("radd")
    if include_sentinel_confirmation:
        optional.append("sentinel_confirmation")

    selected = selected_change_datasets(optional)
    resolved_year, resolution_mode, warnings = resolve_end_year(
        requested_end_year=requested_end_year,
        selected_datasets=selected,
    )

    baseline = compute_gfc2020_baseline(
        aoi_geojson_path=aoi_geojson_path,
        gfc2020_raster_path=gfc2020_raster_path,
        forest_values=forest_values,
    )
    hansen = compute_hansen_loss_inside_gfc2020(
        aoi_geojson_path=aoi_geojson_path,
        gfc2020_raster_path=gfc2020_raster_path,
        hansen_lossyear_raster_path=hansen_lossyear_raster_path,
        start_year=start_year,
        end_year=resolved_year,
        forest_values=forest_values,
    )

    union_disturbance_ha = hansen.loss_inside_gfc2020_ha
    metrics: dict[str, Any] = {
        "aoi_area_ha": baseline.aoi_area_ha,
        "gfc2020_forest_area_ha": baseline.gfc2020_forest_area_ha,
        "gfc2020_forest_share": baseline.gfc2020_forest_share,
        "hansen_loss_2021_resolved_end_year_inside_gfc2020_ha": (
            hansen.loss_inside_gfc2020_ha
        ),
        "union_post2020_disturbance_candidate_ha": union_disturbance_ha,
    }
    metrics.update(tmf.unavailable_metrics())
    metrics.update(radd.unavailable_metrics())
    if include_sentinel_confirmation:
        metrics.update(sentinel_confirmation.unavailable_metrics())

    if include_tmf or include_radd or include_sentinel_confirmation:
        warnings.append(
            {
                "code": "optional_layers_not_computed",
                "message": (
                    "TMF, RADD, and Sentinel confirmation are represented in the registry "
                    "and output schema but are not computed by the local raster provider yet."
                ),
                "datasets": optional,
            }
        )

    payload = {
        "aoi_id": infer_aoi_id(aoi_geojson_path),
        "generated_at_utc": generated_at_utc(),
        "inputs": {
            "aoi_file": aoi_geojson_path.as_posix(),
            "baseline_layer": DATASET_REGISTRY["gfc2020"].asset_id,
            "post2020_layers": [dataset.display_name for dataset in selected],
            "start_year": start_year,
            "requested_end_year": requested_end_year,
            "resolved_end_year": resolved_year,
            "end_year_resolution_mode": resolution_mode,
            "gfc2020_raster_file": gfc2020_raster_path.as_posix(),
            "hansen_lossyear_raster_file": hansen_lossyear_raster_path.as_posix(),
            "gfc2020_forest_values": list(forest_values),
        },
        "dataset_versions": _dataset_versions(optional, resolved_year),
        "metrics": metrics,
        "metrics_by_year": {
            "hansen_loss_inside_gfc2020_ha": hansen.loss_inside_gfc2020_ha_by_year
        },
        "evidence_state": build_evidence_state(
            union_post2020_disturbance_candidate_ha=union_disturbance_ha,
            min_report_area_ha=min_report_area_ha,
        ),
        "warnings": sorted(warnings, key=lambda item: json.dumps(item, sort_keys=True)),
        "limitations": LIMITATIONS,
    }

    # Validate AOI early enough to make malformed inputs fail before writing output.
    load_aoi_geometry(aoi_geojson_path)

    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Estimate post-2020 disturbance evidence inside the GFC2020 baseline."
    )
    parser.add_argument("--aoi", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--gfc2020-raster", required=True, type=Path)
    parser.add_argument("--hansen-lossyear-raster", required=True, type=Path)
    parser.add_argument("--start-year", type=int, default=2021)
    parser.add_argument("--end-year", default="auto")
    parser.add_argument("--include-hansen", action="store_true", default=True)
    parser.add_argument("--include-tmf", action="store_true")
    parser.add_argument("--include-radd", action="store_true")
    parser.add_argument("--include-sentinel-confirmation", action="store_true")
    parser.add_argument("--min-report-area-ha", type=float, default=0.01)
    parser.add_argument("--gfc2020-forest-values", default="1")
    args = parser.parse_args(argv)

    build_post2020_evidence(
        aoi_geojson_path=args.aoi,
        gfc2020_raster_path=args.gfc2020_raster,
        hansen_lossyear_raster_path=args.hansen_lossyear_raster,
        out_path=args.out,
        start_year=args.start_year,
        requested_end_year=str(args.end_year),
        include_tmf=args.include_tmf,
        include_radd=args.include_radd,
        include_sentinel_confirmation=args.include_sentinel_confirmation,
        min_report_area_ha=args.min_report_area_ha,
        forest_values=parse_forest_values(args.gfc2020_forest_values),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
