#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import urlopen

from pyproj import Geod
from shapely.geometry import shape
from shapely.ops import transform, unary_union

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from eudr.evidence.source_roles import evidence_gaps, load_source_layers, p0_layers  # noqa: E402
from eudr_dmi_gil.reports.bundle import compute_sha256  # noqa: E402


TASK_BUNDLE_ID = "eudr-wood-liberia-fmc-area-k"
AOI_ID = "liberia_fmc_area_k_contract_boundary"


def _generated_at() -> str:
    override = os.environ.get("EUDR_DMI_GENERATED_AT_UTC", "").strip()
    if override:
        dt = datetime.fromisoformat(override.replace("Z", "+00:00"))
    else:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        raise ValueError("generated timestamp must include timezone")
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_commit() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True).strip()


def _git_dirty() -> bool:
    out = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=REPO_ROOT, text=True
    )
    return bool(out.strip())


def _load_geometry(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("type") == "FeatureCollection":
        geoms = [shape(feature["geometry"]) for feature in payload["features"] if feature.get("geometry")]
        return unary_union(geoms), payload
    if payload.get("type") == "Feature":
        return shape(payload["geometry"]), payload
    return shape(payload), payload


def _geodesic_area_ha(geom) -> float:
    geod = Geod(ellps="WGS84")
    area_m2, _ = geod.geometry_area_perimeter(geom)
    return abs(float(area_m2)) / 10000.0


def _centroid_lon_lat(geom) -> tuple[float, float]:
    c = geom.centroid
    return (round(float(c.x), 7), round(float(c.y), 7))


def _fetch_forest_atlas_area_k(out_path: Path) -> bool:
    where = quote("con_name='\"K\"'")
    url = (
        "https://gis.forest-atlas.org/server/rest/services/lbr/open_data/MapServer/36/query"
        f"?where={where}&outFields=*&returnGeometry=true&outSR=4326&f=geojson"
    )
    try:
        with urlopen(url, timeout=30) as response:
            data = response.read()
    except Exception:
        return False
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(data)
    return True


def _boundary_comparison(aoi_path: Path, atlas_path: Path | None) -> dict[str, Any]:
    aoi_geom, aoi_payload = _load_geometry(aoi_path)
    props = {}
    if aoi_payload.get("type") == "FeatureCollection" and aoi_payload.get("features"):
        props = aoi_payload["features"][0].get("properties") or {}
    elif aoi_payload.get("type") == "Feature":
        props = aoi_payload.get("properties") or {}

    result: dict[str, Any] = {
        "contract_boundary": {
            "source_path": aoi_path.as_posix(),
            "sha256": compute_sha256(aoi_path),
            "computed_geodesic_area_ha": round(_geodesic_area_ha(aoi_geom), 3),
            "metadata_computed_geodesic_area_ha": props.get(
                "computed_geodesic_area_ha_straight_chord_geometry"
            ),
            "contract_stated_area_ha": props.get("stated_contract_area_ha"),
            "difference_from_stated_area_ha": props.get("difference_from_stated_area_ha"),
            "difference_from_stated_area_percent": props.get("difference_from_stated_area_percent"),
            "centroid_lon_lat": _centroid_lon_lat(aoi_geom),
            "bbox_wgs84": [round(float(v), 7) for v in aoi_geom.bounds],
            "natural_boundary_caveat": props.get("natural_boundary_caveat"),
        },
        "forest_atlas_boundary": None,
        "comparison": {
            "status": "not_available",
            "note": "Forest Atlas Area K polygon was not supplied or fetched.",
        },
    }
    if atlas_path is None or not atlas_path.is_file():
        return result

    atlas_geom, atlas_payload = _load_geometry(atlas_path)
    atlas_props = {}
    if atlas_payload.get("type") == "FeatureCollection" and atlas_payload.get("features"):
        atlas_props = atlas_payload["features"][0].get("properties") or {}
    elif atlas_payload.get("type") == "Feature":
        atlas_props = atlas_payload.get("properties") or {}

    intersection = aoi_geom.intersection(atlas_geom)
    symdiff = aoi_geom.symmetric_difference(atlas_geom)
    result["forest_atlas_boundary"] = {
        "source_path": atlas_path.as_posix(),
        "sha256": compute_sha256(atlas_path),
        "computed_geodesic_area_ha": round(_geodesic_area_ha(atlas_geom), 3),
        "attributes": atlas_props,
        "centroid_lon_lat": _centroid_lon_lat(atlas_geom),
        "bbox_wgs84": [round(float(v), 7) for v in atlas_geom.bounds],
    }
    result["comparison"] = {
        "status": "computed",
        "overlap_area_ha": round(_geodesic_area_ha(intersection), 3),
        "symmetric_difference_area_ha": round(_geodesic_area_ha(symdiff), 3),
        "contract_minus_forest_atlas_area_ha": round(
            _geodesic_area_ha(aoi_geom) - _geodesic_area_ha(atlas_geom), 3
        ),
        "centroid_offset_degrees": round(aoi_geom.centroid.distance(atlas_geom.centroid), 7),
        "boundary_distance_degrees_hausdorff": round(aoi_geom.hausdorff_distance(atlas_geom), 7),
        "note": (
            "Distances are angular degrees in EPSG:4326; area metrics are geodesic. "
            "The Forest Atlas polygon is an observer for comparison, not a replacement."
        ),
    }
    return result


def _write_inventory(path: Path, registry_path: Path) -> list[dict[str, Any]]:
    layers = load_source_layers(registry_path)
    records = [layer.to_inventory_record() for layer in layers]
    path.write_text(json.dumps({"sources": records}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return records


def _write_agreement_matrix(path: Path, inventory: list[dict[str, Any]]) -> None:
    observers = [
        item["dataset_id"]
        for item in inventory
        if item["implementation_priority"] == "P0"
        and any(role in item["roles"] for role in ["deforestation_change", "degradation_change", "alert"])
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "observer_a",
                "observer_b",
                "comparison_state",
                "overlap_area_ha",
                "symmetric_difference_area_ha",
                "note",
            ],
        )
        writer.writeheader()
        for i, left in enumerate(observers):
            for right in observers[i + 1 :]:
                writer.writerow(
                    {
                        "observer_a": left,
                        "observer_b": right,
                        "comparison_state": "not_computed_unfrozen_source_artifacts",
                        "overlap_area_ha": "",
                        "symmetric_difference_area_ha": "",
                        "note": (
                            "Pairwise comparison requires pinned AOI-clipped rasters/alerts. "
                            "The source roles are registered, but no local frozen change layers "
                            "were supplied in this deterministic run."
                        ),
                    }
                )


def _write_status(path: Path) -> dict[str, Any]:
    status = {
        "geometry_role": "forest_concession",
        "production_geometry_role": "concession",
        "production_plot_status": "unresolved",
        "harvesting_block_geometry": "missing",
        "annual_operational_plan": "missing",
        "annual_harvesting_certificate": "missing",
        "chain_of_custody_evidence": "missing",
        "shipment_specific_source_linkage": "missing",
        "interpretation": (
            "Detected disturbances anywhere in the concession cannot be attributed to a shipment "
            "or harvesting block without source/harvest linkage evidence."
        ),
    }
    path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return status


def _write_interactive_map(path: Path, aoi_path: Path, atlas_path: Path | None) -> None:
    aoi_payload = json.loads(aoi_path.read_text(encoding="utf-8"))
    atlas_payload = None
    if atlas_path and atlas_path.is_file():
        atlas_payload = json.loads(atlas_path.read_text(encoding="utf-8"))
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Liberia FMC Area K Screening Map</title>
  <style>
    html, body, #map {{ height: 100%; margin: 0; }}
    body {{ font-family: system-ui, sans-serif; }}
    .panel {{ position: absolute; z-index: 500; right: 12px; top: 12px; background: white; padding: 10px; border: 1px solid #bbb; max-width: 320px; }}
    label {{ display: block; margin: 6px 0; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="panel">
    <strong>FMC Area K</strong>
    <label><input type="checkbox" id="aoi" checked> Contract boundary</label>
    <label><input type="checkbox" id="atlas" checked> Forest Atlas boundary</label>
  </div>
  <script>
    const aoi = {json.dumps(aoi_payload, sort_keys=True)};
    const atlas = {json.dumps(atlas_payload, sort_keys=True)};
    const mapEl = document.getElementById('map');
    mapEl.innerHTML = '<pre style="white-space:pre-wrap;padding:16px">Interactive fallback map data embedded. Use a GeoJSON viewer for layer toggles if JavaScript map libraries are unavailable.\\n\\nContract boundary features: ' + (aoi.features || []).length + '\\nForest Atlas boundary features: ' + (atlas && atlas.features ? atlas.features.length : 0) + '</pre>';
  </script>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def _write_handoff(
    *,
    path: Path,
    out_dir: Path,
    generation_command: str,
    production_status: dict[str, Any],
) -> dict[str, Any]:
    artifacts = []
    for artifact in sorted(out_dir.rglob("*")):
        if artifact.is_file() and artifact != path and artifact.name != "manifest.json":
            artifacts.append(
                {
                    "path": artifact.relative_to(out_dir).as_posix(),
                    "sha256": compute_sha256(artifact),
                    "size_bytes": artifact.stat().st_size,
                    "role": _role_for_artifact(artifact.name),
                    "required": artifact.name
                    in {
                        "source_layer_inventory.json",
                        "source_agreement_matrix.csv",
                        "evidence_gaps.json",
                        "production_geometry_status.json",
                    },
                }
            )
    payload = {
        "schema_version": "okf-gsp-evidence-handoff-v1",
        "task_bundle_id": TASK_BUNDLE_ID,
        "aoi_id": AOI_ID,
        "commodity": "wood",
        "country": "Liberia",
        "country_code": "LR",
        "region": "Nimba; Grand Gedeh; River Cess",
        "purpose": "eudr",
        "generation_command": generation_command,
        "counterpart_repository": "GeorgeMadlis/eudr-dmi-gil",
        "counterpart_commit": _git_commit(),
        "counterpart_dirty": _git_dirty(),
        "evidence_bundle_id": out_dir.name,
        "evidence_bundle_path": str(out_dir.resolve()),
        "bundle_manifest_relpath": "manifest.json",
        "bundle_manifest_sha256": "",
        "report_pdf_relpath": "",
        "report_pdf_sha256": "",
        "report_page_count": 0,
        "report_page_titles": [],
        "resolved_end_year": {"hansen_gfc": 2025, "jrc_tmf": 2025, "radd": "unresolved"},
        "production_geometry_status": production_status,
        "artifacts": artifacts,
    }
    manifest = {
        "schema_version": "liberia-wood-screening-manifest-v1",
        "generated_at_utc": _generated_at(),
        "artifacts": artifacts,
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    payload["bundle_manifest_sha256"] = compute_sha256(manifest_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _role_for_artifact(name: str) -> str:
    return {
        "source_layer_inventory.json": "source_layer_inventory",
        "source_agreement_matrix.csv": "source_agreement_matrix",
        "evidence_gaps.json": "evidence_gaps",
        "production_geometry_status.json": "production_geometry_status",
        "boundary_comparison.json": "boundary_comparison",
        "area_k_layer_map.html": "interactive_layer_map_html",
        "manifest.json": "bundle_manifest",
    }.get(name, "supporting_artifact")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run deterministic Liberia FMC Area K wood screening.")
    parser.add_argument("--aoi", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, default=REPO_ROOT / "out/liberia_wood_fmc_area_k")
    parser.add_argument("--handoff", type=Path, default=REPO_ROOT / "out/okf_handoffs/eudr-wood-liberia-fmc-area-k.json")
    parser.add_argument("--forest-atlas-fmc-geojson", type=Path)
    parser.add_argument("--fetch-forest-atlas-area-k", action="store_true")
    args = parser.parse_args(argv)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    atlas_path = args.forest_atlas_fmc_geojson
    if args.fetch_forest_atlas_area_k:
        atlas_path = args.out_dir / "inputs/forest_atlas_fmc_area_k.geojson"
        _fetch_forest_atlas_area_k(atlas_path)

    inventory = _write_inventory(args.out_dir / "source_layer_inventory.json", args.registry)
    _write_agreement_matrix(args.out_dir / "source_agreement_matrix.csv", inventory)
    gaps = evidence_gaps(load_source_layers(args.registry))
    (args.out_dir / "evidence_gaps.json").write_text(
        json.dumps({"evidence_gaps": gaps}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    status = _write_status(args.out_dir / "production_geometry_status.json")
    comparison = _boundary_comparison(args.aoi, atlas_path)
    (args.out_dir / "boundary_comparison.json").write_text(
        json.dumps(comparison, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_interactive_map(args.out_dir / "area_k_layer_map.html", args.aoi, atlas_path)

    command = " ".join([Path(sys.argv[0]).name, *sys.argv[1:]])
    _write_handoff(
        path=args.handoff,
        out_dir=args.out_dir,
        generation_command=command,
        production_status=status,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
