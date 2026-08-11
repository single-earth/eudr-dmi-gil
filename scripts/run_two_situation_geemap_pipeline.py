#!/usr/bin/env python3
"""Run the JS "two situations" EUDR coffee screening workflow with geemap.

The three source JS files in ``tmp/`` differ only by AOI geometry/metadata. This script keeps that
shape: one AOI config table, one Earth Engine layer/statistics builder, and one report renderer.
Outputs are inspection artifacts, not canonical bundle source.
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


RGB_VIS = {"bands": ["B4", "B3", "B2"], "min": 0.02, "max": 0.30, "gamma": 1.15}
DEFAULT_CONFIG_PATH = REPO_ROOT / "data_db" / "two_situation_geemap_pipeline_config.json"


@dataclass(frozen=True)
class PipelineConfig:
    config_path: Path
    asset_catalogue_path: Path
    aoi_geojson_paths: tuple[Path, ...]
    baseline_year: int
    change_start_year: int
    change_end_year: int
    latest_coffee_year: int
    coffee_class: int
    fdp_threshold: float
    intersection_scale: int
    season_start_mmdd: str
    season_end_mmdd: str
    s2_max_scene_cloud: float
    mapbiomas_collection_id: float
    mapbiomas_version: str
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
    source_js: Path
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
    palette: str
    opacity: float
    default_on: bool
    dimensions: int = 1400


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
    variables = raw.get("variables") or {}
    asset_catalogue_path = resolve_repo_path(raw["asset_catalogue"], label="asset catalogue")
    asset_ids_by_dataset = load_asset_ids(asset_catalogue_path)
    asset_keys = raw["asset_keys"]
    asset_ids = {}
    for logical_name, dataset_id in asset_keys.items():
        try:
            asset_ids[logical_name] = asset_ids_by_dataset[dataset_id]
        except KeyError as exc:
            raise KeyError(
                f"Missing dataset_id {dataset_id!r} for {logical_name!r} in "
                f"{asset_catalogue_path.relative_to(REPO_ROOT)}"
            ) from exc
    basemap = raw.get("basemap") or {}
    return PipelineConfig(
        config_path=config_path,
        asset_catalogue_path=asset_catalogue_path,
        aoi_geojson_paths=tuple(
            resolve_repo_path(path, label="AOI GeoJSON") for path in raw["aoi_geojsons"]
        ),
        baseline_year=int(variables["baseline_year"]),
        change_start_year=int(variables["change_start_year"]),
        change_end_year=int(variables["change_end_year"]),
        latest_coffee_year=int(variables["latest_coffee_year"]),
        coffee_class=int(variables["coffee_class"]),
        fdp_threshold=float(variables["fdp_threshold"]),
        intersection_scale=int(variables["intersection_scale"]),
        season_start_mmdd=str(variables["season_start_mmdd"]),
        season_end_mmdd=str(variables["season_end_mmdd"]),
        s2_max_scene_cloud=float(variables["s2_max_scene_cloud"]),
        mapbiomas_collection_id=float(variables["mapbiomas_collection_id"]),
        mapbiomas_version=str(variables["mapbiomas_version"]),
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
    geometry_type = str(properties.get("geometry_type") or geometry.get("type") or "")
    if geometry_type != "Polygon":
        raise ValueError(f"{path.relative_to(REPO_ROOT)} must provide a Polygon geometry")
    required = ["aoi_id", "source_js", "center_lat", "center_lon", "zoom"]
    missing = [key for key in required if properties.get(key) in (None, "")]
    if missing:
        raise ValueError(f"{path.relative_to(REPO_ROOT)} missing AOI metadata: {', '.join(missing)}")
    title = str(properties.get("title") or properties.get("name") or properties.get("aoi_name") or "")
    description = str(properties.get("description") or properties.get("note") or "")
    if not title or not description:
        raise ValueError(f"{path.relative_to(REPO_ROOT)} must provide title/name and description/note")
    return AoiConfig(
        aoi_id=str(properties["aoi_id"]),
        title=title,
        description=description,
        geometry_type=geometry_type,
        coordinates=geometry["coordinates"],
        source_js=resolve_repo_path(str(properties["source_js"]), label="source_js"),
        source_geojson=path,
        center_lat=float(properties["center_lat"]),
        center_lon=float(properties["center_lon"]),
        zoom=int(properties["zoom"]),
        properties=properties,
    )


def load_aoi_configs(run_config: PipelineConfig) -> tuple[AoiConfig, ...]:
    return tuple(load_aoi_config(path) for path in run_config.aoi_geojson_paths)


def ee_geometry(config: AoiConfig) -> ee.Geometry:
    if config.geometry_type == "Rectangle":
        return ee.Geometry.Rectangle(config.coordinates)
    return ee.Geometry.Polygon(config.coordinates, None, False)


def aoi_geojson(config: AoiConfig) -> dict[str, Any]:
    if config.geometry_type == "Rectangle":
        west, south, east, north = config.coordinates
        coordinates = [
            [
                [west, south],
                [east, south],
                [east, north],
                [west, north],
                [west, south],
            ]
        ]
    else:
        coordinates = config.coordinates
    properties = dict(config.properties)
    properties.update(
        {
            "aoi_id": config.aoi_id,
            "name": config.title,
            "description": config.description,
            "source_js": str(config.source_js),
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
                "geometry": {"type": "Polygon", "coordinates": coordinates},
            }
        ],
    }


def aoi_ring(config: AoiConfig) -> list[list[float]]:
    if config.geometry_type == "Rectangle":
        west, south, east, north = config.coordinates
        return [[west, south], [east, south], [east, north], [west, north], [west, south]]
    return list(config.coordinates[0])


def aoi_bounds(config: AoiConfig) -> tuple[float, float, float, float]:
    ring = aoi_ring(config)
    lons = [float(point[0]) for point in ring]
    lats = [float(point[1]) for point in ring]
    return min(lons), min(lats), max(lons), max(lats)


def map_layer_specs(config: AoiConfig) -> list[LayerSpec]:
    run_config = require_run_config()
    attribution_end = attribution_end_year(run_config)
    raw_specs = [
        (
            f"S2 {run_config.baseline_year} median (before)",
            "s2_2020_median.png",
            "s2_before_median",
            None,
            1.0,
            True,
        ),
        (
            f"S2 {run_config.change_end_year} median (after)",
            "s2_2025_median.png",
            "s2_after_median",
            None,
            1.0,
            True,
        ),
        (
            f"JRC forest {run_config.baseline_year}",
            "jrc_forest_2020.png",
            "jrc_forest_2020",
            "377eb8",
            0.55,
            False,
        ),
        (
            f"JRC forest {run_config.baseline_year} lost "
            f"{run_config.change_start_year}-{run_config.change_end_year}",
            "jrc_forest_loss_2021_2025.png",
            "jrc_loss_change",
            "e41a1c",
            0.9,
            True,
        ),
        (
            f"FDP new-coffee signal since {run_config.baseline_year} "
            "(baseline-to-latest transition)",
            "fdp_new_coffee_since_2020.png",
            "fdp_new",
            "00cfd5",
            0.82,
            False,
        ),
        (
            f"MapBiomas new-coffee signal since {run_config.baseline_year} "
            f"(any year through {attribution_end})",
            "mapbiomas_new_coffee_since_2020.png",
            "mb_new",
            "00ff80",
            0.82,
            False,
        ),
        (
            f"FDP coffee in {run_config.baseline_year}",
            "fdp_coffee_2020.png",
            "fdp_2020",
            "d4a017",
            0.82,
            False,
        ),
        (
            f"MapBiomas in {run_config.baseline_year}",
            "mapbiomas_coffee_2020.png",
            "mb_2020",
            "f4a261",
            0.82,
            False,
        ),
        (
            f"CANDIDATE: loss + FDP new-coffee signal ({run_config.change_start_year}-{attribution_end})",
            "conversion_fdp_new_coffee.png",
            "loss_and_fdp_new",
            "ff00ff",
            1.0,
            True,
        ),
        (
            "CANDIDATE: loss + MapBiomas new-coffee signal "
            f"({run_config.change_start_year}-{attribution_end})",
            "conversion_mapbiomas_new_coffee.png",
            "loss_and_mb_new",
            "8e24aa",
            1.0,
            True,
        ),
        (
            f"SCREENING: loss + ANY new-coffee signal ({run_config.change_start_year}-{attribution_end})",
            "screening_loss_any_new_coffee.png",
            "loss_and_any_new",
            "ff8c00",
            1.0,
            True,
        ),
        (
            "CROSS-SOURCE AGREEMENT: loss + BOTH new-coffee signals "
            f"({run_config.change_start_year}-{attribution_end})",
            "conversion_both_new_coffee.png",
            "loss_and_agreement_new",
            "ffffff",
            1.0,
            True,
        ),
        (
            "SCREENING: loss + ANY latest-coffee signal "
            f"({run_config.change_start_year}-{run_config.change_end_year}, spatial association only)",
            "screening_loss_any_latest_coffee.png",
            "loss_and_any_latest",
            "1e88e5",
            0.9,
            False,
        ),
    ]
    return [
        LayerSpec(
            label=label,
            filename=filename,
            layer_key=layer_key,
            palette=palette or "",
            opacity=opacity,
            default_on=default_on,
        )
        for label, filename, layer_key, palette, opacity, default_on in raw_specs
    ]


def write_aoi_geojson(config: AoiConfig, out_dir: Path, root: Path) -> None:
    payload = aoi_geojson(config)
    out_dir.joinpath("aoi.geojson").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    geojson_dir = root / "geojson"
    geojson_dir.mkdir(parents=True, exist_ok=True)
    geojson_dir.joinpath(f"{config.aoi_id}.geojson").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


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
    return (
        ee.ImageCollection(run_config.asset_ids["sentinel2"])
        .filterBounds(aoi)
        .filterDate(
            f"{year}-{run_config.season_start_mmdd}",
            f"{year}-{run_config.season_end_mmdd}",
        )
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", run_config.s2_max_scene_cloud))
    )


def s2_min_cloud(aoi: ee.Geometry, year: int) -> dict[str, Any]:
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


def fdp_year(aoi: ee.Geometry, year: int) -> dict[str, ee.Image | ee.Number]:
    run_config = require_run_config()
    collection = ee.ImageCollection(run_config.asset_ids["fdp_coffee"])
    subset = collection.filterBounds(aoi).filterDate(
        ee.Date.fromYMD(year, 1, 1), ee.Date.fromYMD(year + 1, 1, 1)
    )
    count = subset.size()
    empty = ee.Image.constant(0).rename("probability").updateMask(ee.Image.constant(0))
    probability = ee.Image(ee.Algorithms.If(count.gt(0), subset.mosaic(), empty)).select(
        "probability"
    )
    probability = probability.clip(aoi)
    coffee = probability.gte(run_config.fdp_threshold).selfMask().rename(f"fdp_coffee_{year}")
    return {"count": count, "probability": probability, "coffee": coffee}


def mapbiomas_year(aoi: ee.Geometry, year: int) -> dict[str, ee.Image | ee.Number]:
    run_config = require_run_config()
    collection = (
        ee.ImageCollection(run_config.asset_ids["mapbiomas_lulc"])
        .filterBounds(aoi)
        .filter(ee.Filter.eq("collection_id", run_config.mapbiomas_collection_id))
        .filter(ee.Filter.eq("version", run_config.mapbiomas_version))
    )
    subset = collection.filter(ee.Filter.eq("year", year))
    count = subset.size()
    empty = ee.Image.constant(0).rename("classification").updateMask(ee.Image.constant(0))
    classification = ee.Image(ee.Algorithms.If(count.gt(0), subset.mosaic(), empty)).select(
        "classification"
    )
    classification = classification.clip(aoi)
    coffee = classification.eq(run_config.coffee_class).selfMask().rename(f"mapbiomas_coffee_{year}")
    return {"count": count, "classification": classification, "coffee": coffee}


def mapbiomas_new_after_baseline(
    aoi: ee.Geometry,
    start_year: int,
    end_year: int,
    baseline_classification: ee.Image,
) -> dict[str, Any]:
    run_config = require_run_config()
    collection = (
        ee.ImageCollection(run_config.asset_ids["mapbiomas_lulc"])
        .filterBounds(aoi)
        .filter(ee.Filter.eq("collection_id", run_config.mapbiomas_collection_id))
        .filter(ee.Filter.eq("version", run_config.mapbiomas_version))
        .filter(ee.Filter.rangeContains("year", start_year, end_year))
    )
    coffee = (
        collection.map(lambda image: ee.Image(image).eq(run_config.coffee_class))
        .max()
        .updateMask(baseline_classification.eq(run_config.coffee_class).Not())
        .selfMask()
        .clip(aoi)
        .rename(f"mapbiomas_new_coffee_after_{run_config.baseline_year}")
    )
    return {"count": collection.size(), "coffee": coffee}


def attribution_end_year(run_config: PipelineConfig) -> int:
    """Latest year through which forest loss can be attributed to a new-coffee signal.

    A commodity observation from ``latest_coffee_year`` cannot provide evidence that forest loss
    first detected after it was followed by coffee establishment, so new-coffee conversion
    screening must stop no later than the newest coffee evidence actually available. General
    forest-disturbance metrics are not affected and continue through ``change_end_year``.
    """
    return min(run_config.change_end_year, run_config.latest_coffee_year)


def fdp_new_coffee_with_validity(
    previous_probability: ee.Image,
    previous_coffee: ee.Image,
    current_probability: ee.Image,
    current_coffee: ee.Image,
    name: str,
) -> ee.Image:
    """FDP baseline-to-latest new-coffee signal, restricted to pixels observed at both epochs.

    ``unmask(0)`` alone would treat a no-observation baseline pixel as "known non-coffee", turning
    missing FDP coverage into a spurious new-coffee transition. Require a real observation in both
    the baseline and latest probability rasters before counting a pixel as a new-coffee signal.
    """
    valid_both = previous_probability.mask().And(current_probability.mask())
    return (
        current_coffee.unmask(0)
        .And(previous_coffee.unmask(0).Not())
        .updateMask(valid_both)
        .selfMask()
        .rename(name)
    )


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
    jrc_forest_2020 = ee.Image(run_config.asset_ids["jrc_gfc2020"]).select("Map").eq(1).selfMask().clip(aoi)
    lossyear = ee.Image(run_config.asset_ids["hansen_gfc"]).select("lossyear")
    hansen_loss_change = (
        lossyear.gte(run_config.change_start_year - 2000)
        .And(lossyear.lte(run_config.change_end_year - 2000))
        .selfMask()
        .clip(aoi)
        .rename(f"hansen_loss_{run_config.change_start_year}_{run_config.change_end_year}")
    )
    jrc_loss_change = hansen_loss_change.And(jrc_forest_2020).selfMask().rename(
        f"jrc_forest_loss_{run_config.change_start_year}_{run_config.change_end_year}"
    )

    # Temporally eligible loss for commodity-attribution metrics: loss through the latest year
    # for which coffee evidence actually exists. See attribution_end_year() docstring.
    attribution_end = attribution_end_year(run_config)
    hansen_loss_attribution = (
        lossyear.gte(run_config.change_start_year - 2000)
        .And(lossyear.lte(attribution_end - 2000))
        .selfMask()
        .clip(aoi)
        .rename(f"hansen_loss_{run_config.change_start_year}_{attribution_end}")
    )
    jrc_loss_attribution = hansen_loss_attribution.And(jrc_forest_2020).selfMask().rename(
        f"jrc_forest_loss_{run_config.change_start_year}_{attribution_end}"
    )

    fdp_baseline = fdp_year(aoi, run_config.baseline_year)
    fdp_latest = fdp_year(aoi, run_config.latest_coffee_year)
    mb_baseline = mapbiomas_year(aoi, run_config.baseline_year)
    mb_latest = mapbiomas_year(aoi, run_config.latest_coffee_year)
    mb_new = mapbiomas_new_after_baseline(
        aoi,
        run_config.change_start_year,
        attribution_end,
        mb_baseline["classification"],
    )
    mb_new_coffee = mb_new["coffee"]

    # FDP baseline-to-latest new-coffee signal (requires valid observations at both epochs).
    fdp_new = fdp_new_coffee_with_validity(
        fdp_baseline["probability"],
        fdp_baseline["coffee"],
        fdp_latest["probability"],
        fdp_latest["coffee"],
        f"fdp_new_coffee_since_{run_config.baseline_year}",
    )
    fdp_temporal_valid = fdp_baseline["probability"].mask().And(fdp_latest["probability"].mask())

    coffee_agreement_2020 = (
        fdp_baseline["coffee"]
        .And(mb_baseline["coffee"])
        .selfMask()
        .rename(f"coffee_agreement_{run_config.baseline_year}")
    )
    coffee_agreement_latest = (
        fdp_latest["coffee"]
        .And(mb_latest["coffee"])
        .selfMask()
        .rename(f"coffee_agreement_{run_config.latest_coffee_year}")
    )

    # ANY-source new-coffee screening signal: at least one commodity source reports new coffee.
    # This is a screening-sensitivity signal, not a cross-source agreement signal.
    any_new_coffee = (
        fdp_new.unmask(0)
        .Or(mb_new_coffee.unmask(0))
        .selfMask()
        .rename(f"any_new_coffee_since_{run_config.baseline_year}")
    )
    # BOTH-source new-coffee agreement: the conservative cross-source signal, kept from before.
    new_coffee_agreement = (
        fdp_new.And(mb_new_coffee).selfMask().rename(f"new_coffee_agreement_since_{run_config.baseline_year}")
    )

    # ANY-source latest-coffee spatial-association signal. Union first, then mask, so the area is
    # never double-counted between the two source-specific latest-coffee intersections.
    any_latest_coffee = (
        fdp_latest["coffee"]
        .unmask(0)
        .Or(mb_latest["coffee"].unmask(0))
        .selfMask()
        .rename(f"any_latest_coffee_{run_config.latest_coffee_year}")
    )

    # Conversion-screening intersections use the temporally eligible loss mask: loss first
    # detected after attribution_end_year predates the coffee evidence available to explain it.
    loss_and_fdp_new = jrc_loss_attribution.And(fdp_new).selfMask().rename(
        f"loss_{run_config.change_start_year}_{attribution_end}_and_fdp_new_coffee"
    )
    loss_and_mb_new = jrc_loss_attribution.And(mb_new_coffee).selfMask().rename(
        f"loss_{run_config.change_start_year}_{attribution_end}_and_mb_new_coffee"
    )
    loss_and_any_new = jrc_loss_attribution.And(any_new_coffee).selfMask().rename(
        f"loss_{run_config.change_start_year}_{attribution_end}_and_any_new_coffee"
    )
    loss_and_agreement_new = jrc_loss_attribution.And(new_coffee_agreement).selfMask().rename(
        f"loss_{run_config.change_start_year}_{attribution_end}_and_agreement_new_coffee"
    )

    # Latest-coffee association intersections use the full loss period: spatial overlap only,
    # with no claim about which came first, the loss or the coffee.
    loss_and_fdp_latest = jrc_loss_change.And(fdp_latest["coffee"]).selfMask().rename(
        f"loss_{run_config.change_start_year}_{run_config.change_end_year}_and_fdp_coffee_latest"
    )
    loss_and_mb_latest = jrc_loss_change.And(mb_latest["coffee"]).selfMask().rename(
        f"loss_{run_config.change_start_year}_{run_config.change_end_year}_and_mapbiomas_coffee_latest"
    )
    loss_and_any_latest = jrc_loss_change.And(any_latest_coffee).selfMask().rename(
        f"loss_{run_config.change_start_year}_{run_config.change_end_year}_and_any_latest_coffee"
    )

    s2_before = s2_min_cloud(aoi, run_config.baseline_year)
    s2_after = s2_min_cloud(aoi, run_config.change_end_year)

    return {
        "aoi": aoi,
        "attribution_end_year": attribution_end,
        "jrc_forest_2020": jrc_forest_2020,
        "jrc_loss_change": jrc_loss_change,
        "jrc_loss_attribution": jrc_loss_attribution,
        "fdp_baseline": fdp_baseline,
        "fdp_latest": fdp_latest,
        "fdp_temporal_valid": fdp_temporal_valid,
        "mb_baseline": mb_baseline,
        "mb_latest": mb_latest,
        "mb_new": mb_new_coffee,
        "mb_new_count": mb_new["count"],
        "fdp_2020": fdp_baseline["coffee"],
        "mb_2020": mb_baseline["coffee"],
        "fdp_new": fdp_new,
        "coffee_agreement_2020": coffee_agreement_2020,
        "coffee_agreement_latest": coffee_agreement_latest,
        "new_coffee_agreement": new_coffee_agreement,
        "any_new_coffee": any_new_coffee,
        "any_latest_coffee": any_latest_coffee,
        "loss_and_fdp_new": loss_and_fdp_new,
        "loss_and_mb_new": loss_and_mb_new,
        "loss_and_any_new": loss_and_any_new,
        "loss_and_agreement_new": loss_and_agreement_new,
        "loss_and_fdp_latest": loss_and_fdp_latest,
        "loss_and_mb_latest": loss_and_mb_latest,
        "loss_and_any_latest": loss_and_any_latest,
        "s2_before": s2_before,
        "s2_after": s2_after,
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
    s2_before = layers["s2_before"]
    s2_after = layers["s2_after"]
    baseline = run_config.baseline_year
    change_start = run_config.change_start_year
    change_end = run_config.change_end_year
    latest_coffee = run_config.latest_coffee_year
    change_key = f"{change_start}_{change_end}"
    attribution_end = layers["attribution_end_year"]
    attribution_key = f"{change_start}_{attribution_end}"

    metrics = ee.Dictionary(
        {
            "aoi_geodesic_area_ha": aoi.area(maxError=1).divide(10000),
            "attribution_end_year": attribution_end,
            f"jrc_forest_{baseline}_ha": area_ha(layers["jrc_forest_2020"], aoi, 10),
            f"fdp_coffee_{baseline}_ha": area_ha(layers["fdp_baseline"]["coffee"], aoi, 10),
            f"mapbiomas_coffee_{baseline}_ha": area_ha(layers["mb_baseline"]["coffee"], aoi, 30),
            f"coffee_agreement_{baseline}_ha": area_ha(layers["coffee_agreement_2020"], aoi, 30),
            f"jrc_forest_loss_{change_key}_ha": area_ha(layers["jrc_loss_change"], aoi, 30),
            f"jrc_forest_loss_{attribution_key}_ha": area_ha(layers["jrc_loss_attribution"], aoi, 30),
            f"fdp_new_coffee_since_{baseline}_ha": area_ha(layers["fdp_new"], aoi, 10),
            f"fdp_{baseline}_{latest_coffee}_temporal_comparison_valid_ha": area_ha(
                layers["fdp_temporal_valid"], aoi, 10
            ),
            f"mapbiomas_new_coffee_since_{baseline}_ha": area_ha(layers["mb_new"], aoi, 30),
            f"any_new_coffee_since_{baseline}_ha": area_ha(layers["any_new_coffee"], aoi, 10),
            f"agreement_new_coffee_since_{baseline}_ha": area_ha(
                layers["new_coffee_agreement"], aoi, 30
            ),
            f"loss_{attribution_key}_and_fdp_new_coffee_ha": area_ha(
                layers["loss_and_fdp_new"], aoi, run_config.intersection_scale
            ),
            f"loss_{attribution_key}_and_mapbiomas_new_coffee_ha": area_ha(
                layers["loss_and_mb_new"], aoi, run_config.intersection_scale
            ),
            f"loss_{attribution_key}_and_any_new_coffee_ha": area_ha(
                layers["loss_and_any_new"], aoi, run_config.intersection_scale
            ),
            f"loss_{attribution_key}_and_agreement_new_coffee_ha": area_ha(
                layers["loss_and_agreement_new"], aoi, run_config.intersection_scale
            ),
            f"loss_{change_key}_and_fdp_coffee_latest_ha": area_ha(
                layers["loss_and_fdp_latest"], aoi, run_config.intersection_scale
            ),
            f"loss_{change_key}_and_mapbiomas_coffee_latest_ha": area_ha(
                layers["loss_and_mb_latest"], aoi, run_config.intersection_scale
            ),
            f"loss_{change_key}_and_any_latest_coffee_ha": area_ha(
                layers["loss_and_any_latest"], aoi, run_config.intersection_scale
            ),
            f"fdp_{baseline}_image_count": layers["fdp_baseline"]["count"],
            f"fdp_{latest_coffee}_image_count": layers["fdp_latest"]["count"],
            f"mapbiomas_c10_{baseline}_image_count": layers["mb_baseline"]["count"],
            f"mapbiomas_c10_{latest_coffee}_image_count": layers["mb_latest"]["count"],
            f"mapbiomas_c10_{change_start}_{latest_coffee}_image_count": layers["mb_new_count"],
            "hansen_lossyear_max_in_aoi": lossyear.reduceRegion(
                reducer=ee.Reducer.max(), geometry=aoi, scale=30, maxPixels=1e13, tileScale=4
            ).get("lossyear"),
            "hansen_lossyear_max_in_50km_buffer": lossyear.reduceRegion(
                reducer=ee.Reducer.max(), geometry=aoi_buffer, scale=100, maxPixels=1e13, tileScale=4
            ).get("lossyear"),
            f"s2_{baseline}_scene_count": s2_before["scene_count"],
            f"s2_{baseline}_least_cloudy_scene_date": s2_before["best_date"],
            f"s2_{baseline}_least_cloudy_scene_cloud_pct": s2_before["best_cloud_pct"],
            f"s2_{baseline}_mean_valid_obs_per_pixel": s2_before["mean_valid_obs"],
            f"s2_{baseline}_min_valid_obs_per_pixel": s2_before["min_valid_obs"],
            f"s2_{change_end}_scene_count": s2_after["scene_count"],
            f"s2_{change_end}_least_cloudy_scene_date": s2_after["best_date"],
            f"s2_{change_end}_least_cloudy_scene_cloud_pct": s2_after["best_cloud_pct"],
            f"s2_{change_end}_mean_valid_obs_per_pixel": s2_after["mean_valid_obs"],
            f"s2_{change_end}_min_valid_obs_per_pixel": s2_after["min_valid_obs"],
        }
    ).getInfo()
    out = {key: number(value) for key, value in metrics.items()}
    out["aoi_id"] = config.aoi_id
    out["screening_interpretation"] = {
        f"loss_{attribution_key}_and_any_new_coffee_ha": "candidate_conversion_requires_review",
        f"loss_{attribution_key}_and_agreement_new_coffee_ha": (
            "cross_source_candidate_conversion_requires_review"
        ),
        f"loss_{change_key}_and_any_latest_coffee_ha": "spatial_association_only",
    }
    return out


def thumbnail_url(image: ee.Image, aoi: ee.Geometry, dimensions: int = 1400) -> str:
    return image.getThumbURL(
        {
            "region": aoi,
            "dimensions": dimensions,
            "format": "png",
        }
    )


def download_png(image: ee.Image, aoi: ee.Geometry, output_path: Path, dimensions: int = 1400) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    url = thumbnail_url(image, aoi, dimensions=dimensions)
    with urllib.request.urlopen(url, timeout=180) as response:  # noqa: S310
        output_path.write_bytes(response.read())


def write_local_layer_pngs(config: AoiConfig, layers: dict[str, Any], out_dir: Path) -> dict[str, str]:
    layer_dir = out_dir / "layers"
    layer_dir.mkdir(parents=True, exist_ok=True)
    aoi = layers["aoi"]
    specs: list[tuple[str, ee.Image, int]] = []
    for spec in map_layer_specs(config):
        if spec.layer_key == "s2_before_median":
            image = layers["s2_before"]["median"].visualize(**RGB_VIS)
        elif spec.layer_key == "s2_after_median":
            image = layers["s2_after"]["median"].visualize(**RGB_VIS)
        else:
            image = layers[spec.layer_key].visualize(palette=[spec.palette])
        specs.append((spec.filename, image, spec.dimensions))
    relpaths: dict[str, str] = {}
    expected_filenames = {filename for filename, _, _ in specs}
    for stale_png in layer_dir.glob("*.png"):
        if stale_png.name not in expected_filenames:
            stale_png.unlink()
    for filename, image, dimensions in specs:
        path = layer_dir / filename
        download_png(image, aoi, path, dimensions=dimensions)
        relpaths[filename] = f"layers/{filename}"
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
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return relpaths


def evidence_composite(config: AoiConfig, layers: dict[str, Any]) -> ee.Image:
    colors = {spec.filename: spec.palette for spec in map_layer_specs(config)}
    base = layers["s2_after"]["median"].visualize(**RGB_VIS)
    aoi_outline = ee.Image().byte().paint(layers["aoi"], 1, 3).visualize(palette=["ffcc00"])
    return (
        base.blend(layers["jrc_forest_2020"].visualize(palette=[colors["jrc_forest_2020.png"]]))
        .blend(layers["jrc_loss_change"].visualize(palette=[colors["jrc_forest_loss_2021_2025.png"]]))
        .blend(layers["fdp_new"].visualize(palette=[colors["fdp_new_coffee_since_2020.png"]]))
        .blend(layers["mb_new"].visualize(palette=[colors["mapbiomas_new_coffee_since_2020.png"]]))
        .blend(layers["fdp_2020"].visualize(palette=[colors["fdp_coffee_2020.png"]]))
        .blend(layers["mb_2020"].visualize(palette=[colors["mapbiomas_coffee_2020.png"]]))
        .blend(layers["loss_and_fdp_new"].visualize(palette=[colors["conversion_fdp_new_coffee.png"]]))
        .blend(layers["loss_and_mb_new"].visualize(palette=[colors["conversion_mapbiomas_new_coffee.png"]]))
        .blend(layers["loss_and_any_new"].visualize(palette=[colors["screening_loss_any_new_coffee.png"]]))
        .blend(layers["loss_and_agreement_new"].visualize(palette=[colors["conversion_both_new_coffee.png"]]))
        .blend(aoi_outline)
    )


def make_before_after(out_dir: Path, layer_relpaths: dict[str, str], out_path: Path) -> None:
    run_config = require_run_config()
    before = Image.open(out_dir / layer_relpaths["s2_2020_median.png"]).convert("RGB")
    after = Image.open(out_dir / layer_relpaths["s2_2025_median.png"]).convert("RGB")
    width = before.width + after.width + 8
    height = max(before.height, after.height) + 36
    canvas = Image.new("RGB", (width, height), "white")
    canvas.paste(before, (0, 36))
    canvas.paste(after, (before.width + 8, 36))
    draw = ImageDraw.Draw(canvas)
    font = ImageFont.load_default()
    draw.text(
        (12, 12),
        f"Sentinel-2 matched season, {run_config.baseline_year} baseline",
        fill=(0, 0, 0),
        font=font,
    )
    draw.text(
        (before.width + 20, 12),
        f"Sentinel-2 matched season, {run_config.change_end_year} recent",
        fill=(0, 0, 0),
        font=font,
    )
    canvas.save(out_path)


def write_geemap_html(
    config: AoiConfig,
    out_path: Path,
    layer_relpaths: dict[str, str],
) -> None:
    """Write a durable local Leaflet map instead of geemap's live EE-widget export.

    ``geemap.Map.to_html`` serializes Earth Engine map IDs as live tile URLs. Those URLs are useful
    during an authenticated session but brittle as report artifacts. This file keeps the geemap/GEE
    pipeline for generating layers, then points Leaflet at local PNG overlays saved beside it.
    """
    west, south, east, north = aoi_bounds(config)
    run_config = require_run_config()
    bounds = [[south, west], [north, east]]
    center = [config.center_lat, config.center_lon]
    aoi_feature = aoi_geojson(config)
    overlay_js = []
    control_entries = []
    for index, spec in enumerate(map_layer_specs(config)):
        label, filename, opacity = spec.label, spec.filename, spec.opacity
        relpath = layer_relpaths[filename]
        var_name = f"layer{index}"
        overlay_js.append(
            f"const {var_name} = L.imageOverlay({json.dumps(relpath)}, bounds, "
            f"{{opacity: {opacity}, interactive: false}});"
        )
        if spec.default_on:
            overlay_js.append(f"{var_name}.addTo(map);")
        control_entries.append(f"{json.dumps(label)}: {var_name}")
    doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(config.aoi_id)} local evidence map</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    html, body, #map {{ height: 100%; margin: 0; background: #101416; }}
    .title {{
      position: absolute; z-index: 1000; left: 58px; top: 12px; max-width: 68%;
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
const aoiLayer = L.geoJSON({json.dumps(aoi_feature)}, {{
  style: {{color: '#ffcc00', weight: 3, dashArray: '6 5', fillColor: '#ffcc00', fillOpacity: 0.08}}
}}).addTo(map);
const baseLayers = {{"Esri.WorldImagery": esriBase, "OpenStreetMap.Mapnik": osmBase}};
const overlays = {{{", ".join(control_entries)}, "AOI boundary": aoiLayer}};
L.control.layers(baseLayers, overlays, {{collapsed: false}}).addTo(map);
map.fitBounds(bounds, {{padding: [20, 20]}});
</script>
</body>
</html>
"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(doc, encoding="utf-8")


def verdict_text(metrics: dict[str, Any]) -> str:
    """Evidence-screening interpretation. Never a compliance/non-compliance determination.

    Evidence hierarchy, strongest to weakest:
    cross-source agreement (BOTH) > single-source screening (ANY) > latest-coffee spatial
    association only (no temporal-order claim).
    """
    run_config = require_run_config()
    tolerance = 0.005
    change_key = f"{run_config.change_start_year}_{run_config.change_end_year}"
    attribution_end = int(metrics["attribution_end_year"])
    attribution_key = f"{run_config.change_start_year}_{attribution_end}"

    loss = float(metrics[f"jrc_forest_loss_{change_key}_ha"] or 0)
    agreement = float(metrics[f"loss_{attribution_key}_and_agreement_new_coffee_ha"] or 0)
    any_new = float(metrics[f"loss_{attribution_key}_and_any_new_coffee_ha"] or 0)
    any_latest = float(metrics[f"loss_{change_key}_and_any_latest_coffee_ha"] or 0)

    if loss <= tolerance:
        return (
            f"No JRC-{run_config.baseline_year}/Hansen "
            f"{run_config.change_start_year}-{run_config.change_end_year} forest-loss pixels "
            "were measured inside the AOI, so this run finds no forest-loss evidence to attribute "
            "to coffee."
        )
    if max(agreement, any_new, any_latest) <= tolerance:
        return (
            "Forest-loss evidence is present, but this run finds no spatial association with the "
            "configured new-coffee or latest-coffee layers. This is a "
            "disturbance-without-coffee-attribution screening result."
        )
    if agreement > tolerance:
        return (
            "Temporally eligible forest loss "
            f"({run_config.change_start_year}-{attribution_end}) is co-located with post-"
            f"{run_config.baseline_year} new-coffee signals from both commodity sources. This is "
            "stronger cross-source candidate-conversion evidence and requires human review."
        )
    if any_new > tolerance:
        return (
            "At least one commodity source provides a post-"
            f"{run_config.baseline_year} new-coffee signal co-located with temporally eligible "
            f"forest loss ({run_config.change_start_year}-{attribution_end}), while cross-source "
            "agreement is absent. Additional screening is required."
        )
    return (
        "Forest loss is spatially associated with land classified as coffee by at least one "
        "latest commodity source, but this run does not establish a post-"
        f"{run_config.baseline_year} coffee transition at those loss pixels. Additional "
        "temporal/visual screening may be warranted."
    )


def render_report(
    config: AoiConfig,
    out_dir: Path,
    metrics: dict[str, Any],
    generated_utc: str,
) -> None:
    run_config = require_run_config()

    def rel(path: Path) -> str:
        return html.escape(path.name)

    script_path = REPO_ROOT / "scripts" / "run_two_situation_geemap_pipeline.py"
    script_sha = sha256_file(script_path)
    config_sha = sha256_file(run_config.config_path)
    asset_catalogue_sha = sha256_file(run_config.asset_catalogue_path)
    aoi_sha = sha256_file(config.source_geojson)
    source_sha = sha256_file(config.source_js) if config.source_js.is_file() else None
    rows = "\n".join(
        f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"
        for key, value in metrics.items()
        if key not in ("aoi_id", "screening_interpretation")
    )
    interpretation_rows = "\n".join(
        f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"
        for key, value in metrics.get("screening_interpretation", {}).items()
    )
    attribution_end = int(metrics["attribution_end_year"])
    summary = verdict_text(metrics)
    report_html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(config.aoi_id)} two-situation geemap report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 28px; color: #1f2933; line-height: 1.45; }}
    main {{ max-width: 1100px; margin: 0 auto; }}
    h1, h2 {{ color: #111827; }}
    .muted {{ color: #596579; }}
    .callout {{ border-left: 5px solid #8e24aa; background: #f8fafc; padding: 12px 16px; }}
    img {{ max-width: 100%; border: 1px solid #d7dde8; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
    th, td {{ border: 1px solid #d7dde8; padding: 7px 8px; text-align: left; }}
    th {{ width: 42%; background: #f3f6fa; }}
    code {{ background: #f1f5f9; padding: 2px 4px; }}
  </style>
</head>
<body>
<main>
  <h1>{html.escape(config.title)}</h1>
  <p class="muted">AOI: <code>{html.escape(config.aoi_id)}</code> | generated {generated_utc}</p>
  <p>{html.escape(config.description)}</p>
  <div class="callout"><strong>Analysis:</strong> {html.escape(summary)}</div>

  <h2>Interactive Evidence Map</h2>
  <p><a href="{rel(out_dir / 'map.html')}">Open map.html</a>. Layers include Sentinel-2 {run_config.baseline_year}/{run_config.change_end_year}, JRC forest baseline, Hansen/JRC loss, FDP new coffee after {run_config.baseline_year}, FDP coffee in {run_config.baseline_year}, MapBiomas new coffee after {run_config.baseline_year}, MapBiomas in {run_config.baseline_year}, source-specific conversion intersections, the ANY-source and BOTH-source new-coffee screening intersections, and the ANY-source latest-coffee spatial-association intersection. Analytical layers are local PNG overlays under <code>layers/</code>; Esri.WorldImagery and OpenStreetMap.Mapnik basemaps are available as toggles and the Leaflet library is loaded remotely.</p>

  <h2>Static Evidence Composite</h2>
  <p>Satellite background is Sentinel-2 {run_config.change_end_year} matched-season median. Overlays: JRC forest {run_config.baseline_year} (blue), forest loss {run_config.change_start_year}-{run_config.change_end_year} (red), FDP new coffee after {run_config.baseline_year} (cyan), MapBiomas new coffee after {run_config.baseline_year} (green), FDP coffee in {run_config.baseline_year} (gold), MapBiomas in {run_config.baseline_year} (orange), loss+FDP new-coffee signal (magenta), loss+MapBiomas new-coffee signal (purple), loss+ANY new-coffee signal screening (orange), loss+BOTH new-coffee signals agreement (white), AOI boundary (yellow).</p>
  <img src="{rel(out_dir / 'evidence_composite_2025.png')}" alt="Evidence composite">

  <h2>Before / After Satellite Comparison</h2>
  <p>Both panels use the same season window ({run_config.season_start_mmdd} to {run_config.season_end_mmdd}) and the same Sentinel-2 SR cloud-mask rules. Use this image for visual context, not as a standalone verdict.</p>
  <img src="{rel(out_dir / 'before_after_sentinel2.png')}" alt="Sentinel-2 before-after comparison">

  <h2>Evidentiary Categories</h2>
  <p>This run distinguishes four evidence categories. Confusing them would misstate the strength of the screening result:</p>
  <ul>
    <li><strong>Forest disturbance</strong>: JRC {run_config.baseline_year} baseline &cap; Hansen loss {run_config.change_start_year}-{run_config.change_end_year}. General disturbance, no commodity claim.</li>
    <li><strong>Candidate new-coffee conversion screening (ANY)</strong>: temporally eligible loss ({run_config.change_start_year}-{attribution_end}) &cap; (FDP-new OR MapBiomas-new). At least one source flags new coffee; requires additional screening.</li>
    <li><strong>Cross-source new-coffee agreement (BOTH)</strong>: temporally eligible loss &cap; FDP-new &cap; MapBiomas-new. Both sources agree; stronger candidate evidence, still requires human review.</li>
    <li><strong>Latest-coffee spatial association</strong>: full-period loss ({run_config.change_start_year}-{run_config.change_end_year}) &cap; (FDP-latest OR MapBiomas-latest). Overlap only &mdash; it does <em>not</em> establish that the loss preceded the coffee.</li>
  </ul>
  <p>The attribution window is capped at {attribution_end} (<code>attribution_end_year</code>): a {run_config.latest_coffee_year} coffee observation cannot provide evidence that loss first detected after it was followed by coffee establishment, so new-coffee screening never attributes loss beyond the newest available coffee evidence. General forest-disturbance metrics are unaffected and continue through {run_config.change_end_year}.</p>
  <p>FDP and MapBiomas "new coffee" are related but not temporally identical: the FDP signal is a baseline-to-latest transition restricted to pixels with a valid observation at both epochs, while the MapBiomas signal is coffee observed in any year from {run_config.change_start_year} through {attribution_end} that was not coffee at baseline.</p>

  <h2>Screening Interpretation</h2>
  <table>{interpretation_rows}</table>

  <h2>Numerical Results</h2>
  <table>{rows}</table>

  <h2>Source And Reproducibility</h2>
  <p>Pipeline script: <code>{html.escape(str(script_path))}</code> (sha256 {script_sha})</p>
  <p>Pipeline config: <code>{html.escape(str(run_config.config_path))}</code> (sha256 {config_sha})</p>
  <p>Dataset asset catalogue: <code>{html.escape(str(run_config.asset_catalogue_path))}</code> (sha256 {asset_catalogue_sha})</p>
  <p>AOI config: <code>{html.escape(str(config.source_geojson))}</code> (sha256 {aoi_sha})</p>
  <p>Legacy JS lineage: <code>{html.escape(str(config.source_js))}</code>{' (sha256 ' + source_sha + ')' if source_sha else ''}</p>
  <p>Machine-readable outputs: <a href="metrics.json">metrics.json</a>, <a href="metrics.csv">metrics.csv</a>.</p>
</main>
</body>
</html>
"""
    (out_dir / "report.html").write_text(report_html, encoding="utf-8")
    report_md = f"""# {config.title}

AOI: `{config.aoi_id}`

Generated: {generated_utc}

{config.description}

## Analysis

{summary}

## Evidentiary Categories

- **Forest disturbance**: JRC {run_config.baseline_year} baseline + Hansen loss {run_config.change_start_year}-{run_config.change_end_year}. No commodity claim.
- **Candidate new-coffee conversion screening (ANY)**: temporally eligible loss ({run_config.change_start_year}-{attribution_end}) + (FDP-new OR MapBiomas-new). Requires additional screening.
- **Cross-source new-coffee agreement (BOTH)**: temporally eligible loss + FDP-new + MapBiomas-new. Stronger candidate evidence, still requires human review.
- **Latest-coffee spatial association**: full-period loss ({run_config.change_start_year}-{run_config.change_end_year}) + (FDP-latest OR MapBiomas-latest). Spatial overlap only, no temporal-order claim.

Attribution window capped at {attribution_end} (`attribution_end_year`): coffee evidence from {run_config.latest_coffee_year} cannot attribute loss detected after it.

## Inspection Links

- [Interactive map](map.html)
- [Static evidence composite](evidence_composite_2025.png)
- [Before/after Sentinel-2 comparison](before_after_sentinel2.png)
- [Metrics JSON](metrics.json)
- [Metrics CSV](metrics.csv)

## Source

Pipeline script: `{script_path}` (sha256 `{script_sha}`)

Pipeline config: `{run_config.config_path}` (sha256 `{config_sha}`)

Dataset asset catalogue: `{run_config.asset_catalogue_path}` (sha256 `{asset_catalogue_sha}`)

AOI config: `{config.source_geojson}` (sha256 `{aoi_sha}`)

Legacy JS lineage: `{config.source_js}`{f" (sha256 `{source_sha}`)" if source_sha else ""}
"""
    (out_dir / "report.md").write_text(report_md, encoding="utf-8")


def write_metrics(out_dir: Path, metrics: dict[str, Any]) -> None:
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2, sort_keys=True) + "\n")
    with (out_dir / "metrics.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["variable", "value"])
        for key in sorted(metrics):
            writer.writerow([key, metrics[key]])


def write_index(root: Path, reports: list[tuple[AoiConfig, Path, dict[str, Any]]], generated_utc: str) -> None:
    run_config = require_run_config()
    change_key = f"{run_config.change_start_year}_{run_config.change_end_year}"
    attribution_end = attribution_end_year(run_config)
    attribution_key = f"{run_config.change_start_year}_{attribution_end}"
    rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(config.aoi_id)}</td>"
        f"<td><a href=\"{html.escape(path.name)}/report.html\">report</a></td>"
        f"<td><a href=\"{html.escape(path.name)}/map.html\">map</a></td>"
        f"<td>{metrics.get(f'jrc_forest_loss_{change_key}_ha')}</td>"
        f"<td>{metrics.get(f'loss_{attribution_key}_and_any_new_coffee_ha')}</td>"
        f"<td>{metrics.get(f'loss_{attribution_key}_and_agreement_new_coffee_ha')}</td>"
        "</tr>"
        for config, path, metrics in reports
    )
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Two-situation geemap pipeline outputs</title>
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
  <h1>Two-situation geemap pipeline outputs</h1>
  <p>Generated {generated_utc}. These are inspection artifacts generated by <code>scripts/run_two_situation_geemap_pipeline.py</code> from AOI GeoJSONs and data_db pipeline configuration.</p>
  <p>"Loss + ANY new-coffee signal" is the primary screening column: at least one commodity source flags new coffee co-located with temporally eligible loss. "Loss + BOTH new-coffee signals" is the stronger cross-source-agreement column. Neither is a compliance determination; see each AOI's report for the full evidentiary breakdown, including the latest-coffee spatial-association metric.</p>
  <table>
    <tr>
      <th>AOI</th><th>Report</th><th>Map</th>
      <th>JRC/Hansen loss {run_config.change_start_year}-{run_config.change_end_year} ha</th>
      <th>Loss + ANY new-coffee signal {run_config.change_start_year}-{attribution_end} ha</th>
      <th>Loss + BOTH new-coffee signals {run_config.change_start_year}-{attribution_end} ha</th>
    </tr>
    {rows}
  </table>
  <p><a href="report_generation_suggestions.html">Report-generation suggestions</a></p>
</main>
</body>
</html>
"""
    (root / "index.html").write_text(html_doc, encoding="utf-8")


def write_suggestions(root: Path) -> None:
    md = """# Suggestions for the existing bundle report generator

These suggestions come from comparing the geemap two-situation outputs with the current bundle
artifacts, especially `report.html`, `evidence/01c_aoi_satellite_map.html`, and the contact-sheet
PDF in the 2026-08-06 Brazil snapshot.

1. Add a first-class "two situations" section to `report.html`: baseline 2020 and change 2021-2025
   should appear as paired metric blocks, not only as scattered layer rows. The key fields are
   JRC forest 2020, coffee 2020 by source, JRC/Hansen forest loss 2021-2025, new coffee after
   2020 by source, and loss+new-coffee intersections.
2. Promote "new coffee after 2020" into the canonical commodity analysis when both baseline and
   latest commodity years are available. The current report generator already shows commodity and
   loss/commodity overlap; the JS workflow adds a stricter conversion signal: latest coffee minus
   2020 coffee, intersected with post-cutoff forest loss.
3. Keep source-specific and agreement layers separate. FDP-only, MapBiomas-only, and both-source
   agreement should be displayed as different evidentiary strengths, rather than collapsed into a
   single commodity mask.
4. Extend `evidence/01c_aoi_satellite_map.html` with the same basemap and overlay toggle set used
   here: Esri.WorldImagery, OpenStreetMap.Mapnik, baseline forest, loss, new FDP coffee, new
   MapBiomas coffee, source-specific conversion, and both-source agreement conversion. Omit
   unavailable/empty layers, but record the omission in metrics.
5. Add the Sentinel-2 matched-season scene/depth diagnostics to the report table: scene count,
   least-cloudy date/cloud percent, mean valid observations, and minimum valid observations for
   2020 and 2025. This makes visual before/after comparisons auditable instead of decorative.
6. Preserve the existing visual-acceptance requirements from the OKF bundles: bounded AOI buffer,
   no forced aspect-ratio letterboxing, legend rows only for actually rendered layers, and
   high-contrast overlay colors validated against the real basemaps.
7. Store the JS/source-equivalent AOI configuration hash beside report outputs. This prevents the
   "same method, different AOI" scripts from becoming unverifiable copy-paste state.

Implementation target in the counterpart repo: `src/eudr_dmi_gil/reports/report_model.py` already
materializes `01c_aoi_satellite_map.html` and static PNG evidence; `src/eudr_dmi_gil/commodities/analysis.py`
is the natural place to add baseline-vs-latest commodity-year differencing when the provider can
serve both years.
"""
    (root / "report_generation_suggestions.md").write_text(md, encoding="utf-8")
    body = html.escape(md).replace("\n", "<br>\n")
    (root / "report_generation_suggestions.html").write_text(
        f"<!doctype html><meta charset=\"utf-8\"><title>Report-generation suggestions</title><body style=\"font-family:Arial,sans-serif;max-width:900px;margin:28px auto;line-height:1.45\"><pre style=\"white-space:pre-wrap\">{body}</pre></body>\n",
        encoding="utf-8",
    )


def run(config: AoiConfig, root: Path, generated_utc: str) -> tuple[AoiConfig, Path, dict[str, Any]]:
    run_config = require_run_config()
    out_dir = root / config.aoi_id
    out_dir.mkdir(parents=True, exist_ok=True)
    write_aoi_geojson(config, out_dir, root)
    layers = build_layers(config)
    metrics = collect_metrics(config, layers)
    metrics["generated_utc"] = generated_utc
    metrics["pipeline_script"] = str(REPO_ROOT / "scripts" / "run_two_situation_geemap_pipeline.py")
    metrics["pipeline_script_sha256"] = sha256_file(REPO_ROOT / "scripts" / "run_two_situation_geemap_pipeline.py")
    metrics["pipeline_config"] = str(run_config.config_path)
    metrics["pipeline_config_sha256"] = sha256_file(run_config.config_path)
    metrics["asset_catalogue"] = str(run_config.asset_catalogue_path)
    metrics["asset_catalogue_sha256"] = sha256_file(run_config.asset_catalogue_path)
    metrics["source_geojson"] = str(config.source_geojson)
    metrics["source_geojson_sha256"] = sha256_file(config.source_geojson)
    for logical_name, asset_id in sorted(run_config.asset_ids.items()):
        metrics[f"asset_id_{logical_name}"] = asset_id
    metrics["source_js"] = str(config.source_js)
    metrics["source_js_sha256"] = sha256_file(config.source_js) if config.source_js.is_file() else None

    layer_relpaths = write_local_layer_pngs(config, layers, out_dir)
    write_geemap_html(config, out_dir / "map.html", layer_relpaths)
    download_png(evidence_composite(config, layers), layers["aoi"], out_dir / "evidence_composite_2025.png")
    make_before_after(out_dir, layer_relpaths, out_dir / "before_after_sentinel2.png")
    write_metrics(out_dir, metrics)
    render_report(config, out_dir, metrics, generated_utc)
    return config, out_dir, metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="myproject-gq-74696", help="Earth Engine project id")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Repo-local pipeline config JSON",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "out" / "geemap_two_situation_pipeline",
        help="Output directory",
    )
    parser.add_argument(
        "--aoi",
        action="append",
        help="Run only this AOI. May be repeated. Defaults to all AOIs.",
    )
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
    write_suggestions(args.out)
    write_index(args.out, reports, generated_utc)
    print(f"Wrote {len(reports)} AOI reports to {args.out}")


if __name__ == "__main__":
    main()
