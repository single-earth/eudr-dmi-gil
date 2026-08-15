#!/usr/bin/env python3
"""Run the Liberia FMC Area K wood-source GEE screening workflow with geemap.

This is the Python/geemap counterpart of the user-provided Earth Engine Code Editor script. It
keeps the same analytical meaning, but moves source IDs, run variables and AOI geometry into
repo-local data files so the workflow is inspectable and repeatable.

Outputs are screening/inspection artifacts, not automatic EUDR compliance conclusions.
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import os
import sys
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
MPL_CACHE_DIR = REPO_ROOT / ".tmp" / "matplotlib"
MPL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CACHE_DIR))

import ee  # noqa: E402
import geemap  # noqa: E402,F401
from PIL import Image, ImageDraw, ImageFont  # noqa: E402

from eudr_dmi_gil.reports.determinism import sha256_file  # noqa: E402


DEFAULT_CONFIG_PATH = REPO_ROOT / "data_db" / "liberia_wood_geemap_pipeline_config.json"

RGB_VIS = {"bands": ["B4", "B3", "B2"], "min": 0.02, "max": 0.30, "gamma": 1.15}


@dataclass(frozen=True)
class PipelineConfig:
    config_path: Path
    asset_catalogue_path: Path
    aoi_geojson_paths: tuple[Path, ...]
    baseline_year: int
    change_start_year: int
    change_end_year: int
    fdp_year: int
    fdp_threshold: float
    forest_scale: int
    change_scale: int
    s2_season_start_mmdd: str
    s2_season_end_mmdd: str
    s2_max_scene_cloud: float
    asset_ids: dict[str, str]
    esri_world_imagery: str
    esri_attribution: str
    openstreetmap_mapnik: str
    openstreetmap_attribution: str


@dataclass(frozen=True)
class AoiConfig:
    aoi_id: str
    title: str
    description: str
    geometry_type: str
    coordinates: Any
    source_geojson: Path
    center_lat: float
    center_lon: float
    zoom: int
    properties: dict[str, Any]


@dataclass(frozen=True)
class LayerSpec:
    label: str
    filename: str
    layer_key: str
    opacity: float
    default_on: bool
    vis: dict[str, Any] | None = None
    dimensions: int = 1500


RUN_CONFIG: PipelineConfig | None = None


def require_run_config() -> PipelineConfig:
    if RUN_CONFIG is None:
        raise RuntimeError("Pipeline config has not been loaded")
    return RUN_CONFIG


def resolve_repo_path(raw_path: str | Path, *, label: str) -> Path:
    path = Path(raw_path)
    resolved = path if path.is_absolute() else REPO_ROOT / path
    resolved = resolved.resolve()
    repo = REPO_ROOT.resolve()
    if resolved != repo and repo not in resolved.parents:
        raise ValueError(f"{label} escapes repo root: {raw_path}")
    return resolved


def parse_gee_asset_id(raw: str) -> str:
    value = raw.strip()
    prefix = "GEE asset:"
    if value.startswith(prefix):
        value = value[len(prefix) :].strip()
    return value


def load_asset_ids(asset_catalogue_path: Path) -> dict[str, str]:
    with asset_catalogue_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assets: dict[str, str] = {}
    for row in rows:
        dataset_id = str(row.get("dataset_id") or "").strip()
        asset_id = parse_gee_asset_id(str(row.get("asset_id") or row.get("api_endpoint") or ""))
        if dataset_id and asset_id:
            assets[dataset_id] = asset_id
    return assets


def load_pipeline_config(path: Path) -> PipelineConfig:
    config_path = resolve_repo_path(path, label="pipeline config")
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    variables = raw["variables"]
    asset_catalogue_path = resolve_repo_path(raw["asset_catalogue"], label="asset catalogue")
    asset_ids_by_dataset = load_asset_ids(asset_catalogue_path)
    asset_ids: dict[str, str] = {}
    for logical_name, dataset_id in raw["asset_keys"].items():
        try:
            asset_ids[logical_name] = asset_ids_by_dataset[dataset_id]
        except KeyError as exc:
            raise KeyError(
                f"Missing dataset_id {dataset_id!r} for {logical_name!r} in "
                f"{asset_catalogue_path.relative_to(REPO_ROOT)}"
            ) from exc
    basemap = raw["basemap"]
    return PipelineConfig(
        config_path=config_path,
        asset_catalogue_path=asset_catalogue_path,
        aoi_geojson_paths=tuple(
            resolve_repo_path(p, label="AOI GeoJSON") for p in raw["aoi_geojsons"]
        ),
        baseline_year=int(variables["baseline_year"]),
        change_start_year=int(variables["change_start_year"]),
        change_end_year=int(variables["change_end_year"]),
        fdp_year=int(variables["fdp_year"]),
        fdp_threshold=float(variables["fdp_threshold"]),
        forest_scale=int(variables["forest_scale"]),
        change_scale=int(variables["change_scale"]),
        s2_season_start_mmdd=str(variables["s2_season_start_mmdd"]),
        s2_season_end_mmdd=str(variables["s2_season_end_mmdd"]),
        s2_max_scene_cloud=float(variables["s2_max_scene_cloud"]),
        asset_ids=asset_ids,
        esri_world_imagery=str(basemap["esri_world_imagery"]),
        esri_attribution=str(basemap["esri_attribution"]),
        openstreetmap_mapnik=str(basemap["openstreetmap_mapnik"]),
        openstreetmap_attribution=str(basemap["openstreetmap_attribution"]),
    )


def load_aoi_config(path: Path) -> AoiConfig:
    payload = json.loads(path.read_text(encoding="utf-8"))
    features = payload.get("features") or []
    if len(features) != 1:
        raise ValueError(f"{path.relative_to(REPO_ROOT)} must contain exactly one feature")
    feature = features[0]
    properties = dict(feature.get("properties") or {})
    geometry = feature.get("geometry") or {}
    geometry_type = str(geometry.get("type") or "")
    if geometry_type != "Polygon":
        raise ValueError(f"{path.relative_to(REPO_ROOT)} must provide a Polygon geometry")
    aoi_id = str(properties.get("aoi_id") or properties.get("id") or "").strip()
    title = str(properties.get("title") or properties.get("name") or aoi_id).strip()
    description = str(properties.get("description") or properties.get("eudr_use") or "").strip()
    if not aoi_id or not title:
        raise ValueError(f"{path.relative_to(REPO_ROOT)} must provide aoi_id/id and title/name")
    return AoiConfig(
        aoi_id=aoi_id,
        title=title,
        description=description,
        geometry_type=geometry_type,
        coordinates=close_polygon_coordinates(geometry["coordinates"]),
        source_geojson=path,
        center_lat=float(properties.get("center_lat") or properties.get("centroid_lat")),
        center_lon=float(properties.get("center_lon") or properties.get("centroid_lon")),
        zoom=int(properties.get("zoom") or 10),
        properties=properties,
    )


def close_polygon_coordinates(coordinates: Any) -> Any:
    """Return Polygon coordinates with each linear ring explicitly closed."""
    closed = []
    for ring in coordinates:
        if not ring:
            closed.append(ring)
            continue
        first = ring[0]
        last = ring[-1]
        closed.append(ring if first == last else [*ring, first])
    return closed


def load_aoi_configs(run_config: PipelineConfig) -> tuple[AoiConfig, ...]:
    return tuple(load_aoi_config(path) for path in run_config.aoi_geojson_paths)


def ee_geometry(config: AoiConfig) -> ee.Geometry:
    return ee.Geometry.Polygon(config.coordinates, None, False)


def aoi_bounds(config: AoiConfig) -> tuple[float, float, float, float]:
    ring = config.coordinates[0]
    lons = [float(point[0]) for point in ring]
    lats = [float(point[1]) for point in ring]
    return min(lons), min(lats), max(lons), max(lats)


def aoi_geojson(config: AoiConfig) -> dict[str, Any]:
    properties = dict(config.properties)
    properties.update(
        {
            "aoi_id": config.aoi_id,
            "name": config.title,
            "description": config.description,
            "source_geojson": str(config.source_geojson),
            "center_lat": config.center_lat,
            "center_lon": config.center_lon,
            "zoom": config.zoom,
        }
    )
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": properties,
                "geometry": {"type": "Polygon", "coordinates": config.coordinates},
            }
        ],
    }


def mask_s2_sr(image: ee.Image) -> ee.Image:
    scl = image.select("SCL")
    keep = (
        scl.neq(3)
        .And(scl.neq(8))
        .And(scl.neq(9))
        .And(scl.neq(10))
        .And(scl.neq(11))
    )
    return ee.Image(
        image.updateMask(keep)
        .divide(10000)
        .copyProperties(image, ["system:time_start", "CLOUDY_PIXEL_PERCENTAGE"])
    )


def s2_season_collection(aoi: ee.Geometry, year: int) -> ee.ImageCollection:
    run_config = require_run_config()
    start_year = year
    end_year = year
    start = run_config.s2_season_start_mmdd
    end = run_config.s2_season_end_mmdd
    if end < start:
        end_year = year + 1
    return (
        ee.ImageCollection(run_config.asset_ids["sentinel2"])
        .filterBounds(aoi)
        .filterDate(f"{start_year}-{start}", f"{end_year}-{end}")
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", run_config.s2_max_scene_cloud))
    )


def s2_context(aoi: ee.Geometry, year: int) -> dict[str, Any]:
    col = s2_season_collection(aoi, year)
    size = col.size()
    masked = col.map(mask_s2_sr)
    median = masked.median().clip(aoi)
    valid_obs = masked.select("B4").count().rename("valid_obs").clip(aoi)
    sorted_first = col.sort("CLOUDY_PIXEL_PERCENTAGE").first()
    best_scene = ee.Image(mask_s2_sr(ee.Image(sorted_first))).clip(aoi)
    best = ee.Image(ee.Algorithms.If(size.gt(0), best_scene, median))
    return {
        "year": year,
        "scene_count": size,
        "median": median,
        "best": best,
        "best_date": ee.Algorithms.If(
            size.gt(0), ee.Image(sorted_first).date().format("YYYY-MM-dd"), "none"
        ),
        "best_cloud_pct": ee.Algorithms.If(
            size.gt(0), ee.Image(sorted_first).get("CLOUDY_PIXEL_PERCENTAGE"), None
        ),
        "mean_valid_obs": valid_obs.reduceRegion(
            reducer=ee.Reducer.mean(), geometry=aoi, scale=20, maxPixels=1e13, tileScale=4
        ).get("valid_obs"),
        "min_valid_obs": valid_obs.reduceRegion(
            reducer=ee.Reducer.min(), geometry=aoi, scale=20, maxPixels=1e13, tileScale=4
        ).get("valid_obs"),
    }


def fdp_mask(aoi: ee.Geometry, commodity: str) -> dict[str, Any]:
    run_config = require_run_config()
    key = f"fdp_{commodity}"
    collection = ee.ImageCollection(run_config.asset_ids[key])
    subset = collection.filterBounds(aoi).filterDate(
        ee.Date.fromYMD(run_config.fdp_year, 1, 1),
        ee.Date.fromYMD(run_config.fdp_year + 1, 1, 1),
    )
    empty = ee.Image.constant(0).rename("probability").updateMask(ee.Image.constant(0))
    probability = ee.Image(ee.Algorithms.If(subset.size().gt(0), subset.mosaic(), empty))
    probability = probability.select("probability").clip(aoi)
    mask = probability.gte(run_config.fdp_threshold).selfMask().rename(f"fdp_{commodity}")
    return {"commodity": commodity, "count": subset.size(), "probability": probability, "mask": mask}


def area_ha(mask: ee.Image, aoi: ee.Geometry, scale: int) -> ee.Number:
    value = (
        ee.Image.pixelArea()
        .updateMask(mask.unmask(0))
        .rename("area")
        .reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=aoi,
            scale=scale,
            maxPixels=1e13,
            tileScale=4,
        )
        .get("area")
    )
    return ee.Number(ee.Algorithms.If(value, value, 0)).divide(10000)


def build_layers(config: AoiConfig) -> dict[str, Any]:
    run_config = require_run_config()
    aoi = ee_geometry(config)

    jrc_forest_2020 = (
        ee.Image(run_config.asset_ids["jrc_gfc2020"])
        .select("Map")
        .eq(1)
        .selfMask()
        .clip(aoi)
        .rename("jrc_forest_2020")
    )
    forest_types_2020 = (
        ee.Image(run_config.asset_ids["jrc_forest_types"])
        .select("Map")
        .clip(aoi)
        .rename("jrc_forest_type_2020")
    )
    lossyear = ee.Image(run_config.asset_ids["hansen_gfc"]).select("lossyear")
    hansen_loss = (
        lossyear.gte(run_config.change_start_year - 2000)
        .And(lossyear.lte(run_config.change_end_year - 2000))
        .selfMask()
        .clip(aoi)
        .rename(f"hansen_loss_{run_config.change_start_year}_{run_config.change_end_year}")
    )
    loss_on_jrc_forest = hansen_loss.And(jrc_forest_2020).selfMask().rename(
        f"jrc_forest_lost_{run_config.change_start_year}_{run_config.change_end_year}"
    )
    forest_remaining = (
        jrc_forest_2020.unmask(0)
        .And(loss_on_jrc_forest.unmask(0).Not())
        .selfMask()
        .clip(aoi)
        .rename(f"jrc_forest_remaining_end_{run_config.change_end_year}")
    )

    tmf_def_year = (
        ee.ImageCollection(run_config.asset_ids["tmf_deforestation_year"])
        .mosaic()
        .rename("tmf_deforestation_year")
        .clip(aoi)
    )
    tmf_deg_year = (
        ee.ImageCollection(run_config.asset_ids["tmf_degradation_year"])
        .mosaic()
        .rename("tmf_degradation_year")
        .clip(aoi)
    )
    tmf_deforestation = (
        tmf_def_year.gte(run_config.change_start_year)
        .And(tmf_def_year.lte(run_config.change_end_year))
        .selfMask()
        .rename(f"tmf_deforestation_{run_config.change_start_year}_{run_config.change_end_year}")
    )
    tmf_degradation = (
        tmf_deg_year.gte(run_config.change_start_year)
        .And(tmf_deg_year.lte(run_config.change_end_year))
        .selfMask()
        .rename(f"tmf_degradation_{run_config.change_start_year}_{run_config.change_end_year}")
    )

    wood_concession_context = (
        ee.Image.constant(1).clip(aoi).selfMask().rename("wood_concession_source_context")
    )
    fdp = {commodity: fdp_mask(aoi, commodity) for commodity in ("cocoa", "coffee", "palm", "rubber")}
    fdp_any_tree_crop = (
        ee.ImageCollection(
            [
                fdp["cocoa"]["mask"].unmask(0).rename("mask"),
                fdp["coffee"]["mask"].unmask(0).rename("mask"),
                fdp["palm"]["mask"].unmask(0).rename("mask"),
                fdp["rubber"]["mask"].unmask(0).rename("mask"),
            ]
        )
        .max()
        .selfMask()
        .rename(f"fdp_any_tree_crop_{run_config.fdp_year}")
    )
    fused_context = (
        wood_concession_context.unmask(0)
        .where(fdp_any_tree_crop.unmask(0).eq(1), 2)
        .selfMask()
        .rename("fused_commodity_source_context")
    )
    loss_inside_concession = (
        loss_on_jrc_forest.And(wood_concession_context)
        .selfMask()
        .rename("loss_inside_wood_concession_context")
    )
    loss_on_fdp_tree_crop = (
        loss_on_jrc_forest.And(fdp_any_tree_crop)
        .selfMask()
        .rename("loss_on_fdp_tree_crop_candidate")
    )

    s2_baseline = s2_context(aoi, run_config.baseline_year)
    s2_recent = s2_context(aoi, run_config.change_end_year)

    return {
        "aoi": aoi,
        "jrc_forest_2020": jrc_forest_2020,
        "forest_types_2020": forest_types_2020,
        "loss_on_jrc_forest": loss_on_jrc_forest,
        "forest_remaining": forest_remaining,
        "tmf_deforestation": tmf_deforestation,
        "tmf_degradation": tmf_degradation,
        "wood_concession_context": wood_concession_context,
        "fdp": fdp,
        "fdp_any_tree_crop": fdp_any_tree_crop,
        "fused_context": fused_context,
        "loss_inside_concession": loss_inside_concession,
        "loss_on_fdp_tree_crop": loss_on_fdp_tree_crop,
        "s2_baseline": s2_baseline,
        "s2_recent": s2_recent,
    }


def number(value: Any, digits: int = 4) -> float | int | str | None:
    if value is None:
        return None
    if isinstance(value, (int, str)):
        return value
    return round(float(value), digits)


def collect_metrics(config: AoiConfig, layers: dict[str, Any]) -> dict[str, Any]:
    run_config = require_run_config()
    aoi = layers["aoi"]
    aoi_buffer = aoi.buffer(50000)
    lossyear = ee.Image(run_config.asset_ids["hansen_gfc"]).select("lossyear")
    s2_baseline = layers["s2_baseline"]
    s2_recent = layers["s2_recent"]
    change_key = f"{run_config.change_start_year}_{run_config.change_end_year}"

    metrics = ee.Dictionary(
        {
            "aoi_geodesic_area_ha": aoi.area(maxError=1).divide(10000),
            f"jrc_forest_{run_config.baseline_year}_ha": area_ha(
                layers["jrc_forest_2020"], aoi, run_config.forest_scale
            ),
            f"jrc_forest_remaining_end_{run_config.change_end_year}_ha": area_ha(
                layers["forest_remaining"], aoi, run_config.forest_scale
            ),
            f"hansen_loss_{change_key}_on_jrc_forest_ha": area_ha(
                layers["loss_on_jrc_forest"], aoi, run_config.change_scale
            ),
            f"tmf_deforestation_{change_key}_ha": area_ha(
                layers["tmf_deforestation"], aoi, run_config.change_scale
            ),
            f"tmf_degradation_{change_key}_ha": area_ha(
                layers["tmf_degradation"], aoi, run_config.change_scale
            ),
            "wood_concession_context_area_ha": area_ha(
                layers["wood_concession_context"], aoi, run_config.change_scale
            ),
            f"fdp_any_tree_crop_{run_config.fdp_year}_ha": area_ha(
                layers["fdp_any_tree_crop"], aoi, run_config.forest_scale
            ),
            "hansen_loss_inside_wood_concession_context_ha": area_ha(
                layers["loss_inside_concession"], aoi, run_config.change_scale
            ),
            "hansen_loss_on_fdp_tree_crop_candidate_ha": area_ha(
                layers["loss_on_fdp_tree_crop"], aoi, run_config.change_scale
            ),
            "fdp_cocoa_image_count": layers["fdp"]["cocoa"]["count"],
            "fdp_coffee_image_count": layers["fdp"]["coffee"]["count"],
            "fdp_palm_image_count": layers["fdp"]["palm"]["count"],
            "fdp_rubber_image_count": layers["fdp"]["rubber"]["count"],
            "hansen_lossyear_max_in_aoi": lossyear.reduceRegion(
                reducer=ee.Reducer.max(), geometry=aoi, scale=30, maxPixels=1e13, tileScale=4
            ).get("lossyear"),
            "hansen_lossyear_max_in_50km_buffer": lossyear.reduceRegion(
                reducer=ee.Reducer.max(), geometry=aoi_buffer, scale=100, maxPixels=1e13, tileScale=4
            ).get("lossyear"),
            f"s2_{run_config.baseline_year}_scene_count": s2_baseline["scene_count"],
            f"s2_{run_config.baseline_year}_least_cloudy_scene_date": s2_baseline["best_date"],
            f"s2_{run_config.baseline_year}_least_cloudy_scene_cloud_pct": s2_baseline[
                "best_cloud_pct"
            ],
            f"s2_{run_config.baseline_year}_mean_valid_obs_per_pixel": s2_baseline[
                "mean_valid_obs"
            ],
            f"s2_{run_config.baseline_year}_min_valid_obs_per_pixel": s2_baseline["min_valid_obs"],
            f"s2_{run_config.change_end_year}_scene_count": s2_recent["scene_count"],
            f"s2_{run_config.change_end_year}_least_cloudy_scene_date": s2_recent["best_date"],
            f"s2_{run_config.change_end_year}_least_cloudy_scene_cloud_pct": s2_recent[
                "best_cloud_pct"
            ],
            f"s2_{run_config.change_end_year}_mean_valid_obs_per_pixel": s2_recent[
                "mean_valid_obs"
            ],
            f"s2_{run_config.change_end_year}_min_valid_obs_per_pixel": s2_recent["min_valid_obs"],
        }
    ).getInfo()
    out = {key: number(value) for key, value in metrics.items()}
    out["aoi_id"] = config.aoi_id
    out["commodity"] = "wood"
    out["production_geometry_role"] = "concession"
    out["production_plot_status"] = "unresolved"
    out["screening_interpretation"] = {
        "wood_concession_context": (
            "The AOI is source/concession context only. It is not harvesting-block, tree-origin, "
            "log-origin or shipment-specific proof."
        ),
        "hansen_loss_inside_wood_concession_context_ha": (
            "Post-2020 Hansen stand-replacement disturbance on JRC 2020 forest inside the concession."
        ),
        "tmf_degradation_ha": (
            "JRC TMF degradation is a separate wood-path observer and is not collapsed into "
            "deforestation hectares."
        ),
        "fdp_tree_crop_candidate": (
            "Optional non-wood tree-crop probability context from FDP. It is not timber-source "
            "evidence and does not resolve wood legality or source linkage."
        ),
    }
    return out


def layer_specs() -> list[LayerSpec]:
    run_config = require_run_config()
    change = f"{run_config.change_start_year}-{run_config.change_end_year}"
    return [
        LayerSpec(
            f"Sentinel-2 {run_config.baseline_year} visual context",
            "s2_2020_context.png",
            "s2_baseline_median",
            1.0,
            False,
        ),
        LayerSpec(
            f"Sentinel-2 {run_config.change_end_year} visual context",
            "s2_2025_context.png",
            "s2_recent_median",
            1.0,
            True,
        ),
        LayerSpec(
            f"JRC forest baseline {run_config.baseline_year}",
            "jrc_forest_2020.png",
            "jrc_forest_2020",
            0.55,
            True,
            {"palette": ["1b9e77"]},
        ),
        LayerSpec(
            f"JRC forest remaining by end-{run_config.change_end_year}",
            "jrc_forest_remaining_2025.png",
            "forest_remaining",
            0.65,
            False,
            {"palette": ["66a61e"]},
        ),
        LayerSpec(
            "JRC forest types 2020",
            "jrc_forest_types_2020.png",
            "forest_types_2020",
            0.75,
            False,
            {
                "min": 1,
                "max": 20,
                "palette": [
                    "78c679",
                    "000000",
                    "000000",
                    "000000",
                    "000000",
                    "000000",
                    "000000",
                    "000000",
                    "000000",
                    "006837",
                    "000000",
                    "000000",
                    "000000",
                    "000000",
                    "000000",
                    "000000",
                    "000000",
                    "000000",
                    "000000",
                    "cc6600",
                ],
            },
        ),
        LayerSpec(
            f"Hansen loss {change} on JRC forest",
            "hansen_loss_2021_2025_on_jrc_forest.png",
            "loss_on_jrc_forest",
            0.9,
            True,
            {"palette": ["d73027"]},
        ),
        LayerSpec(
            f"TMF deforestation {change}",
            "tmf_deforestation_2021_2025.png",
            "tmf_deforestation",
            0.75,
            False,
            {"palette": ["ff0000"]},
        ),
        LayerSpec(
            f"TMF degradation {change}",
            "tmf_degradation_2021_2025.png",
            "tmf_degradation",
            0.75,
            False,
            {"palette": ["ff7f00"]},
        ),
        LayerSpec(
            "Wood concession/source context",
            "wood_concession_source_context.png",
            "wood_concession_context",
            0.35,
            False,
            {"palette": ["bdbdbd"]},
        ),
        LayerSpec(
            f"FDP any cocoa/coffee/palm/rubber {run_config.fdp_year}",
            "fdp_any_tree_crop_2024.png",
            "fdp_any_tree_crop",
            0.8,
            False,
            {"palette": ["ffd92f"]},
        ),
        LayerSpec(
            "Fused commodity/source context",
            "fused_commodity_source_context.png",
            "fused_context",
            0.45,
            False,
            {"min": 1, "max": 2, "palette": ["bdbdbd", "ffd92f"]},
        ),
        LayerSpec(
            "Loss inside wood concession/source context",
            "loss_inside_wood_concession_context.png",
            "loss_inside_concession",
            1.0,
            True,
            {"palette": ["ff00ff"]},
        ),
        LayerSpec(
            "Loss on FDP tree-crop candidate",
            "loss_on_fdp_tree_crop_candidate.png",
            "loss_on_fdp_tree_crop",
            1.0,
            False,
            {"palette": ["00ffff"]},
        ),
    ]


def image_for_spec(spec: LayerSpec, layers: dict[str, Any]) -> ee.Image:
    if spec.layer_key == "s2_baseline_median":
        return layers["s2_baseline"]["median"].visualize(**RGB_VIS)
    if spec.layer_key == "s2_recent_median":
        return layers["s2_recent"]["median"].visualize(**RGB_VIS)
    if spec.vis is None:
        raise ValueError(f"Layer {spec.layer_key} has no visualization")
    return layers[spec.layer_key].visualize(**spec.vis)


def thumbnail_url(image: ee.Image, aoi: ee.Geometry, dimensions: int = 1500) -> str:
    return image.getThumbURL({"region": aoi, "dimensions": dimensions, "format": "png"})


def download_png(image: ee.Image, aoi: ee.Geometry, output_path: Path, dimensions: int = 1500) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    url = thumbnail_url(image, aoi, dimensions=dimensions)
    with urllib.request.urlopen(url, timeout=180) as response:  # noqa: S310
        output_path.write_bytes(response.read())


def write_local_layer_pngs(config: AoiConfig, layers: dict[str, Any], out_dir: Path) -> dict[str, str]:
    layer_dir = out_dir / "layers"
    layer_dir.mkdir(parents=True, exist_ok=True)
    aoi = layers["aoi"]
    relpaths: dict[str, str] = {}
    expected = {spec.filename for spec in layer_specs()}
    for stale_png in layer_dir.glob("*.png"):
        if stale_png.name not in expected:
            stale_png.unlink()
    for spec in layer_specs():
        path = layer_dir / spec.filename
        download_png(image_for_spec(spec, layers), aoi, path, dimensions=spec.dimensions)
        relpaths[spec.filename] = f"layers/{spec.filename}"
    manifest = {
        "aoi_id": config.aoi_id,
        "bounds_wgs84": {
            "min_lon": aoi_bounds(config)[0],
            "min_lat": aoi_bounds(config)[1],
            "max_lon": aoi_bounds(config)[2],
            "max_lat": aoi_bounds(config)[3],
        },
        "layers": relpaths,
    }
    (layer_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return relpaths


def evidence_composite(layers: dict[str, Any]) -> ee.Image:
    base = layers["s2_recent"]["median"].visualize(**RGB_VIS)
    outline = ee.Image().byte().paint(layers["aoi"], 1, 3).visualize(palette=["ffff00"])
    return (
        base.blend(layers["jrc_forest_2020"].visualize(palette=["1b9e77"]))
        .blend(layers["forest_remaining"].visualize(palette=["66a61e"]))
        .blend(layers["loss_on_jrc_forest"].visualize(palette=["d73027"]))
        .blend(layers["tmf_deforestation"].visualize(palette=["ff0000"]))
        .blend(layers["tmf_degradation"].visualize(palette=["ff7f00"]))
        .blend(layers["fdp_any_tree_crop"].visualize(palette=["ffd92f"]))
        .blend(layers["loss_inside_concession"].visualize(palette=["ff00ff"]))
        .blend(layers["loss_on_fdp_tree_crop"].visualize(palette=["00ffff"]))
        .blend(outline)
    )


def make_before_after(out_dir: Path, layer_relpaths: dict[str, str], out_path: Path) -> None:
    run_config = require_run_config()
    before = Image.open(out_dir / layer_relpaths["s2_2020_context.png"]).convert("RGB")
    after = Image.open(out_dir / layer_relpaths["s2_2025_context.png"]).convert("RGB")
    width = before.width + after.width + 8
    height = max(before.height, after.height) + 36
    canvas = Image.new("RGB", (width, height), "white")
    canvas.paste(before, (0, 36))
    canvas.paste(after, (before.width + 8, 36))
    draw = ImageDraw.Draw(canvas)
    font = ImageFont.load_default()
    draw.text((12, 12), f"Sentinel-2 context, {run_config.baseline_year}", fill=(0, 0, 0), font=font)
    draw.text(
        (before.width + 20, 12),
        f"Sentinel-2 context, {run_config.change_end_year}",
        fill=(0, 0, 0),
        font=font,
    )
    canvas.save(out_path)


def write_geemap_html(config: AoiConfig, out_path: Path, layer_relpaths: dict[str, str]) -> None:
    run_config = require_run_config()
    west, south, east, north = aoi_bounds(config)
    bounds = [[south, west], [north, east]]
    center = [config.center_lat, config.center_lon]
    overlay_js = []
    control_entries = []
    for index, spec in enumerate(layer_specs()):
        var_name = f"layer{index}"
        overlay_js.append(
            f"const {var_name} = L.imageOverlay({json.dumps(layer_relpaths[spec.filename])}, "
            f"bounds, {{opacity: {spec.opacity}, interactive: false}});"
        )
        if spec.default_on:
            overlay_js.append(f"{var_name}.addTo(map);")
        control_entries.append(f"{json.dumps(spec.label)}: {var_name}")
    doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(config.aoi_id)} Liberia wood geemap evidence map</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    html, body, #map {{ height: 100%; margin: 0; background: #101416; }}
    .title {{
      position: absolute; z-index: 1000; left: 58px; top: 12px; max-width: 70%;
      color: #f7fbfb; font: 700 16px Arial, sans-serif;
      background: rgba(7, 10, 12, 0.82); padding: 8px 12px; border-radius: 8px;
    }}
    .leaflet-control-layers {{ font: 13px Arial, sans-serif; }}
  </style>
</head>
<body>
<div id="map"></div>
<div class="title">{html.escape(config.title)} - local PNG evidence overlays</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const bounds = {json.dumps(bounds)};
const map = L.map('map', {{center: {json.dumps(center)}, zoom: {config.zoom}}});
const esriBase = L.tileLayer({json.dumps(run_config.esri_world_imagery)}, {{
  maxZoom: 19,
  attribution: {json.dumps(run_config.esri_attribution)}
}}).addTo(map);
const osmBase = L.tileLayer({json.dumps(run_config.openstreetmap_mapnik)}, {{
  maxZoom: 19,
  attribution: {json.dumps(run_config.openstreetmap_attribution)}
}});
{chr(10).join(overlay_js)}
const aoiLayer = L.geoJSON({json.dumps(aoi_geojson(config))}, {{
  style: {{color: '#ffff00', weight: 3, dashArray: '6 5', fillColor: '#ffff00', fillOpacity: 0.08}}
}}).addTo(map);
const baseLayers = {{"Esri.WorldImagery": esriBase, "OpenStreetMap.Mapnik": osmBase}};
const overlays = {{{", ".join(control_entries)}, "AOI boundary": aoiLayer}};
L.control.layers(baseLayers, overlays, {{collapsed: false}}).addTo(map);
map.fitBounds(bounds, {{padding: [20, 20]}});
</script>
</body>
</html>
"""
    out_path.write_text(doc, encoding="utf-8")


def glossary_html() -> str:
    terms = [
        ("AOI", "Area of Interest: the polygon being screened."),
        ("FMC", "Forest Management Contract: Liberia's concession contract area category used here as the source/context boundary."),
        ("EUDR", "European Union Deforestation Regulation, Regulation (EU) 2023/1115."),
        ("GEE", "Google Earth Engine: cloud geospatial analysis platform that hosts the source rasters used here."),
        ("geemap", "Python package that wraps Earth Engine and mapping workflows; this script uses ee/geemap-style Python rather than Code Editor JavaScript."),
        ("JRC", "Joint Research Centre of the European Commission, provider of GFC2020, GFC2020_subtypes and TMF products."),
        ("GFC2020", "JRC Global Forest Cover 2020; Map == 1 is treated as forest baseline evidence at the EUDR cutoff."),
        ("GFT / forest types", "JRC Global Forest Types 2020; class 1 = naturally regenerating, 10 = primary, 20 = planted/plantation forest."),
        ("Hansen GFC", "UMD/Hansen Global Forest Change; lossyear 21..25 means first detected gross forest-cover loss in 2021..2025."),
        ("TMF", "JRC Tropical Moist Forest product family; this script uses DeforestationYear and DegradationYear observers."),
        ("Deforestation", "In this script, a mapped forest-loss/deforestation observer. It is screening evidence, not a legal conclusion by itself."),
        ("Degradation", "A separate wood-path disturbance stream, often canopy disturbance without full stand replacement. It is not merged into deforestation hectares."),
        ("FDP", "Forest Data Partnership; optional 10 m probability models for cocoa, coffee, oil palm and rubber."),
        ("Probability threshold", "The minimum FDP probability for admitting a pixel as a candidate tree-crop pixel; configured here as 0.25."),
        ("Fused commodity/source context", "Categorical display layer: concession context plus optional FDP tree-crop candidate context. It is not shipment/source proof."),
        ("Production geometry role", "The meaning of the supplied geometry. Here it is only a concession, not a harvesting block, tree/log origin, or shipment plot."),
        ("Production plot status", "Whether plot/source geometry is resolved. Here it remains unresolved because harvesting-block/log-origin geometry is missing."),
        ("Source linkage", "Evidence tying a shipment or product to a specific harvesting block, tree, log origin, permit or chain-of-custody record."),
        ("Sentinel-2 SR", "Copernicus Sentinel-2 Level-2A surface reflectance imagery used for optional visual context."),
        ("ha", "Hectare, 10,000 square meters."),
        ("lossyear", "Hansen band encoding 0 for no loss, or 1..25 for first detected loss in 2001..2025."),
    ]
    rows = "\n".join(
        f"<tr><th>{html.escape(term)}</th><td>{html.escape(text)}</td></tr>" for term, text in terms
    )
    return f"<table>{rows}</table>"


def verdict_text(metrics: dict[str, Any]) -> str:
    run_config = require_run_config()
    change_key = f"{run_config.change_start_year}_{run_config.change_end_year}"
    loss = float(metrics.get(f"hansen_loss_{change_key}_on_jrc_forest_ha") or 0)
    degradation = float(metrics.get(f"tmf_degradation_{change_key}_ha") or 0)
    if loss > 0 or degradation > 0:
        return (
            "This run detected post-2020 forest disturbance signals inside the concession-level "
            "source context. Because the supplied geometry is a concession boundary, not a harvesting "
            "block or log-origin geometry, the result is a human-review screening signal rather than "
            "shipment-specific source or legality evidence."
        )
    return (
        "This run did not measure post-2020 forest disturbance above zero in the configured Hansen/JRC "
        "and TMF observer masks. The concession/source-linkage gap still remains unresolved."
    )


def render_report(
    config: AoiConfig,
    out_dir: Path,
    metrics: dict[str, Any],
    generated_utc: str,
) -> None:
    run_config = require_run_config()
    script_path = REPO_ROOT / "scripts" / "run_liberia_wood_geemap_pipeline.py"
    rows = "\n".join(
        f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"
        for key, value in metrics.items()
        if key not in {"screening_interpretation"}
    )
    interpretation_rows = "\n".join(
        f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"
        for key, value in metrics["screening_interpretation"].items()
    )
    summary = verdict_text(metrics)
    doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(config.aoi_id)} Liberia wood geemap report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 28px; color: #1f2933; line-height: 1.45; }}
    main {{ max-width: 1120px; margin: 0 auto; }}
    h1, h2 {{ color: #111827; }}
    .muted {{ color: #596579; }}
    .callout {{ border-left: 5px solid #ff00ff; background: #f8fafc; padding: 12px 16px; }}
    img {{ max-width: 100%; border: 1px solid #d7dde8; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 14px; margin: 12px 0; }}
    th, td {{ border: 1px solid #d7dde8; padding: 7px 8px; text-align: left; vertical-align: top; }}
    th {{ width: 36%; background: #f3f6fa; }}
    code {{ background: #f1f5f9; padding: 2px 4px; }}
  </style>
</head>
<body>
<main>
  <h1>{html.escape(config.title)}</h1>
  <p class="muted">AOI: <code>{html.escape(config.aoi_id)}</code> | generated {generated_utc}</p>
  <p>{html.escape(config.description)}</p>
  <div class="callout"><strong>Screening interpretation:</strong> {html.escape(summary)}</div>

  <h2>Interactive Evidence Map</h2>
  <p><a href="map.html">Open map.html</a>. The map uses local PNG overlays generated from Earth Engine through this Python/geemap pipeline. Basemap tiles are live Leaflet layers; analytical overlays are saved under <code>layers/</code>.</p>

  <h2>Static Evidence Composite</h2>
  <p>The composite overlays forest baseline, remaining forest, Hansen loss, TMF deforestation, TMF degradation, optional FDP tree-crop probability context, and the concession-level source context on Sentinel-2 visual context.</p>
  <img src="evidence_composite_2025.png" alt="Liberia FMC Area K evidence composite">

  <h2>Before / After Visual Context</h2>
  <p>Sentinel-2 panels are context only. They help human review, but the numerical screening metrics come from the named raster observers.</p>
  <img src="before_after_sentinel2.png" alt="Sentinel-2 before-after comparison">

  <h2>Evidence Logic</h2>
  <p><strong>Forest baseline</strong> is JRC GFC2020 forest at the EUDR cutoff. <strong>Forest coverage by end-{run_config.change_end_year}</strong> is baseline forest minus Hansen first-loss pixels dated {run_config.change_start_year}-{run_config.change_end_year}. <strong>Wood source context</strong> is the concession polygon. It is deliberately marked unresolved for plot-level sourcing because no Annual Operational Plan block, harvesting block, tree-origin, log-origin, chain-of-custody or shipment-specific geometry is supplied.</p>
  <p><strong>Fused commodity/source context</strong> combines two different context types for display: concession-level wood context and optional FDP tree-crop candidates. This fusion is a map product, not a claim that timber came from a tree-crop pixel, nor a compliance finding.</p>

  <h2>Screening Interpretation Fields</h2>
  <table>{interpretation_rows}</table>

  <h2>Numerical Results</h2>
  <table>{rows}</table>

  <h2>Glossary Of Terms And Abbreviations</h2>
  {glossary_html()}

  <h2>Source And Reproducibility</h2>
  <p>Pipeline script: <code>{html.escape(str(script_path))}</code> (sha256 {sha256_file(script_path)})</p>
  <p>Pipeline config: <code>{html.escape(str(run_config.config_path))}</code> (sha256 {sha256_file(run_config.config_path)})</p>
  <p>Dataset asset catalogue: <code>{html.escape(str(run_config.asset_catalogue_path))}</code> (sha256 {sha256_file(run_config.asset_catalogue_path)})</p>
  <p>AOI GeoJSON: <code>{html.escape(str(config.source_geojson))}</code> (sha256 {sha256_file(config.source_geojson)})</p>
  <p>Machine-readable outputs: <a href="metrics.json">metrics.json</a>, <a href="metrics.csv">metrics.csv</a>.</p>
</main>
</body>
</html>
"""
    (out_dir / "report.html").write_text(doc, encoding="utf-8")

    glossary_md = "\n".join(
        f"- **{term}**: {text}"
        for term, text in [
            ("AOI", "Area of Interest: the polygon being screened."),
            ("FMC", "Forest Management Contract: concession-level source context."),
            ("EUDR", "European Union Deforestation Regulation."),
            ("GEE", "Google Earth Engine."),
            ("JRC", "Joint Research Centre."),
            ("GFC2020", "JRC Global Forest Cover 2020 baseline."),
            ("Hansen GFC", "UMD/Hansen Global Forest Change lossyear product."),
            ("TMF", "JRC Tropical Moist Forest product family."),
            ("FDP", "Forest Data Partnership probability models."),
            ("ha", "Hectare, 10,000 square meters."),
        ]
    )
    md = f"""# {config.title}

AOI: `{config.aoi_id}`

Generated: {generated_utc}

{config.description}

## Screening Interpretation

{summary}

## Inspection Links

- [Interactive map](map.html)
- [Static evidence composite](evidence_composite_2025.png)
- [Before/after Sentinel-2 comparison](before_after_sentinel2.png)
- [Metrics JSON](metrics.json)
- [Metrics CSV](metrics.csv)

## Key Evidence Logic

Forest coverage by end-{run_config.change_end_year} is JRC GFC2020 forest baseline minus Hansen loss pixels dated {run_config.change_start_year}-{run_config.change_end_year}. TMF deforestation and TMF degradation are reported separately. The supplied AOI is a concession/source-context geometry, not plot-level or shipment-level proof.

## Glossary

{glossary_md}
"""
    (out_dir / "report.md").write_text(md, encoding="utf-8")


def write_aoi_geojson(config: AoiConfig, out_dir: Path, root: Path) -> None:
    payload = aoi_geojson(config)
    out_dir.joinpath("aoi.geojson").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    geojson_dir = root / "geojson"
    geojson_dir.mkdir(parents=True, exist_ok=True)
    geojson_dir.joinpath(f"{config.aoi_id}.geojson").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def write_metrics(out_dir: Path, metrics: dict[str, Any]) -> None:
    (out_dir / "metrics.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with (out_dir / "metrics.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["variable", "value"])
        for key in sorted(metrics):
            writer.writerow([key, metrics[key]])


def run(config: AoiConfig, root: Path, generated_utc: str) -> tuple[AoiConfig, Path, dict[str, Any]]:
    run_config = require_run_config()
    out_dir = root / config.aoi_id
    out_dir.mkdir(parents=True, exist_ok=True)
    write_aoi_geojson(config, out_dir, root)
    layers = build_layers(config)
    metrics = collect_metrics(config, layers)
    metrics["generated_utc"] = generated_utc
    metrics["pipeline_script"] = str(REPO_ROOT / "scripts" / "run_liberia_wood_geemap_pipeline.py")
    metrics["pipeline_script_sha256"] = sha256_file(
        REPO_ROOT / "scripts" / "run_liberia_wood_geemap_pipeline.py"
    )
    metrics["pipeline_config"] = str(run_config.config_path)
    metrics["pipeline_config_sha256"] = sha256_file(run_config.config_path)
    metrics["asset_catalogue"] = str(run_config.asset_catalogue_path)
    metrics["asset_catalogue_sha256"] = sha256_file(run_config.asset_catalogue_path)
    metrics["source_geojson"] = str(config.source_geojson)
    metrics["source_geojson_sha256"] = sha256_file(config.source_geojson)
    for logical_name, asset_id in sorted(run_config.asset_ids.items()):
        metrics[f"asset_id_{logical_name}"] = asset_id

    layer_relpaths = write_local_layer_pngs(config, layers, out_dir)
    write_geemap_html(config, out_dir / "map.html", layer_relpaths)
    download_png(evidence_composite(layers), layers["aoi"], out_dir / "evidence_composite_2025.png")
    make_before_after(out_dir, layer_relpaths, out_dir / "before_after_sentinel2.png")
    write_metrics(out_dir, metrics)
    render_report(config, out_dir, metrics, generated_utc)
    return config, out_dir, metrics


def write_index(root: Path, reports: list[tuple[AoiConfig, Path, dict[str, Any]]], generated_utc: str) -> None:
    run_config = require_run_config()
    change_key = f"{run_config.change_start_year}_{run_config.change_end_year}"
    rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(config.aoi_id)}</td>"
        f"<td><a href=\"{html.escape(path.name)}/report.html\">report</a></td>"
        f"<td><a href=\"{html.escape(path.name)}/map.html\">map</a></td>"
        f"<td>{metrics.get(f'hansen_loss_{change_key}_on_jrc_forest_ha')}</td>"
        f"<td>{metrics.get(f'tmf_deforestation_{change_key}_ha')}</td>"
        f"<td>{metrics.get(f'tmf_degradation_{change_key}_ha')}</td>"
        "</tr>"
        for config, path, metrics in reports
    )
    doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Liberia wood geemap pipeline outputs</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 28px; color: #1f2933; }}
    main {{ max-width: 1000px; margin: 0 auto; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #d7dde8; padding: 8px; text-align: left; }}
    th {{ background: #f3f6fa; }}
  </style>
</head>
<body>
<main>
  <h1>Liberia wood geemap pipeline outputs</h1>
  <p>Generated {generated_utc}. Outputs are inspection artifacts produced from AOI GeoJSON and data_db source configuration.</p>
  <table>
    <tr><th>AOI</th><th>Report</th><th>Map</th><th>Hansen/JRC loss ha</th><th>TMF deforestation ha</th><th>TMF degradation ha</th></tr>
    {rows}
  </table>
</main>
</body>
</html>
"""
    (root / "index.html").write_text(doc, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="myproject-gq-74696", help="Earth Engine project id")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "out" / "liberia_wood_geemap_pipeline")
    parser.add_argument("--aoi", action="append", help="Run only this AOI. May be repeated.")
    return parser.parse_args()


def main() -> None:
    global RUN_CONFIG
    args = parse_args()
    RUN_CONFIG = load_pipeline_config(args.config)
    aois = load_aoi_configs(RUN_CONFIG)
    requested = set(args.aoi or [])
    unknown = requested.difference(a.aoi_id for a in aois)
    if unknown:
        raise SystemExit(f"Unknown AOI(s): {', '.join(sorted(unknown))}")
    ee.Initialize(project=args.project)
    generated_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )
    selected = [a for a in aois if not requested or a.aoi_id in requested]
    args.out.mkdir(parents=True, exist_ok=True)
    reports = [run(config, args.out, generated_utc) for config in selected]
    write_index(args.out, reports, generated_utc)
    print(f"Wrote {len(reports)} AOI reports to {args.out}")


if __name__ == "__main__":
    main()
