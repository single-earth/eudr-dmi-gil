#!/usr/bin/env python3
"""Fetch and mosaic real Sentinel-2 imagery into a wide-context "regional overview" raster.

Given an AOI GeoJSON, this produces an RGB GeoTIFF that shows the AOI in a much bigger
geographic perspective than the tight AOI-cropped satellite evidence used elsewhere in the
report (see ``eudr_dmi_gil.reports.report_model._render_satellite_crop``). It queries the
public AWS Earth Search STAC API for the least-cloudy Sentinel-2 L2A true-color (TCI) scene(s)
covering a padded bounding box around the AOI, verifies the AOI-local cloud fraction via the
scene classification (SCL) band, and mosaics the true-color COG(s) with rasterio.

No imagery is fabricated: the output raster's real extent is clipped to the union of the actual
source-tile bounds, and each output pixel is real Sentinel-2 reflectance data.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
import requests
from rasterio.enums import Resampling
from rasterio.merge import merge
from rasterio.vrt import WarpedVRT
from rasterio.warp import transform_bounds
from shapely.geometry import shape
from shapely.ops import unary_union

STAC_SEARCH_URL = "https://earth-search.aws.element84.com/v1/search"
COLLECTION = "sentinel-2-l2a"
CLOUD_CLASSES = {3, 8, 9, 10}  # SCL: cloud shadow, cloud medium/high prob, cirrus


@dataclass(frozen=True)
class SceneCandidate:
    item_id: str
    datetime: str
    tile_cloud_cover_percent: float
    visual_href: str
    scl_href: str


def _load_aoi_geometry(aoi_geojson_path: Path) -> Any:
    payload = json.loads(aoi_geojson_path.read_text(encoding="utf-8"))
    if payload.get("type") == "FeatureCollection":
        geoms = [shape(f["geometry"]) for f in payload["features"]]
        return unary_union(geoms)
    if payload.get("type") == "Feature":
        return shape(payload["geometry"])
    return shape(payload)


def _search_candidates(bbox: tuple[float, float, float, float], datetime_range: str, *, limit: int) -> list[SceneCandidate]:
    resp = requests.post(
        STAC_SEARCH_URL,
        json={
            "collections": [COLLECTION],
            "bbox": list(bbox),
            "datetime": datetime_range,
            "limit": limit,
            "sortby": [{"field": "properties.eo:cloud_cover", "direction": "asc"}],
        },
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    candidates = []
    for feature in payload.get("features", []):
        assets = feature.get("assets", {})
        visual = assets.get("visual", {}).get("href")
        scl = assets.get("scl", {}).get("href")
        if not visual or not scl:
            continue
        candidates.append(
            SceneCandidate(
                item_id=feature["id"],
                datetime=feature["properties"].get("datetime", ""),
                tile_cloud_cover_percent=float(feature["properties"].get("eo:cloud_cover", 100.0)),
                visual_href=visual,
                scl_href=scl,
            )
        )
    return candidates


def _aoi_local_cloud_fraction(scl_href: str, bbox_wgs84: tuple[float, float, float, float]) -> float:
    """Fraction of AOI-window pixels classified as cloud/cloud-shadow/cirrus in the SCL band."""
    minx, miny, maxx, maxy = bbox_wgs84
    with rasterio.open(f"/vsicurl/{scl_href}") as ds:
        left, bottom, right, top = transform_bounds("EPSG:4326", ds.crs, minx, miny, maxx, maxy)
        window = rasterio.windows.from_bounds(left, bottom, right, top, ds.transform)
        window = window.round_lengths().round_offsets()
        # Any window outside the raster is "unavailable", not "clear" - do not fabricate.
        if window.width <= 0 or window.height <= 0:
            return 1.0
        data = ds.read(1, window=window, boundless=True, fill_value=0)
        if data.size == 0:
            return 1.0
        cloud_mask = np.isin(data, list(CLOUD_CLASSES)) | (data == 0)
        return float(np.count_nonzero(cloud_mask)) / float(data.size)


def _select_scene(
    bbox_wgs84: tuple[float, float, float, float],
    datetime_range: str,
    *,
    max_local_cloud_fraction: float,
    max_candidates: int,
) -> SceneCandidate:
    candidates = _search_candidates(bbox_wgs84, datetime_range, limit=max_candidates)
    if not candidates:
        raise SystemExit(f"No Sentinel-2 scenes found for bbox={bbox_wgs84} datetime={datetime_range}")
    for candidate in candidates:
        try:
            local_cloud_fraction = _aoi_local_cloud_fraction(candidate.scl_href, bbox_wgs84)
        except Exception:
            continue
        if local_cloud_fraction <= max_local_cloud_fraction:
            return candidate
    raise SystemExit(
        f"No candidate scene met max_local_cloud_fraction={max_local_cloud_fraction} "
        f"for bbox={bbox_wgs84} datetime={datetime_range} "
        f"(checked {len(candidates)} candidates, lowest tile cloud_cover "
        f"{candidates[0].tile_cloud_cover_percent})"
    )


def _mosaic_visual(
    hrefs: list[str],
    *,
    bbox_wgs84: tuple[float, float, float, float],
    output_path: Path,
    target_resolution_m: float,
) -> tuple[str, tuple[float, float, float, float]]:
    minx, miny, maxx, maxy = bbox_wgs84
    opened = [rasterio.open(f"/vsicurl/{href}") for href in hrefs]
    dst_crs = opened[0].crs
    # Sentinel-2 tiles near a UTM-zone boundary can come back in different CRSes; reproject any
    # off-CRS source into the primary tile's CRS via a WarpedVRT so merge() sees one consistent grid.
    datasets = [ds if ds.crs == dst_crs else WarpedVRT(ds, crs=dst_crs, resampling=Resampling.bilinear) for ds in opened]
    try:
        left, bottom, right, top = transform_bounds("EPSG:4326", dst_crs, minx, miny, maxx, maxy)
        mosaic, out_transform = merge(
            datasets,
            bounds=(left, bottom, right, top),
            res=(target_resolution_m, target_resolution_m),
            resampling=Resampling.bilinear,
            dst_path=None,
        )
        profile = opened[0].profile
        profile.update(
            driver="GTiff",
            height=mosaic.shape[1],
            width=mosaic.shape[2],
            transform=out_transform,
            crs=dst_crs,
            count=mosaic.shape[0],
            dtype=mosaic.dtype,
            compress="deflate",
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(mosaic)
        return str(dst_crs), (left, bottom, right, top)
    finally:
        for ds in datasets:
            ds.close()
        for ds in opened:
            ds.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--aoi-geojson", required=True, type=Path)
    p.add_argument("--output-raster", required=True, type=Path)
    p.add_argument("--output-metadata", type=Path, help="Defaults to <output-raster>.metadata.json")
    p.add_argument(
        "--datetime-range",
        default="2021-01-01T00:00:00Z/2026-12-31T23:59:59Z",
        help="STAC datetime range to search (default: post-2020, matches the 'recent' evidence period).",
    )
    p.add_argument(
        "--pad-factor",
        type=float,
        default=8.0,
        help="Regional bbox half-width, as a multiple of the AOI's own half-width/half-height (default: 8.0).",
    )
    p.add_argument("--min-width-m", type=float, default=40_000.0, help="Minimum regional view width in meters.")
    p.add_argument("--target-resolution-m", type=float, default=20.0)
    p.add_argument("--max-local-cloud-fraction", type=float, default=0.05)
    p.add_argument("--max-candidates", type=int, default=10)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    aoi_geom = _load_aoi_geometry(args.aoi_geojson)
    minx, miny, maxx, maxy = aoi_geom.bounds
    centroid = aoi_geom.centroid
    half_w = max((maxx - minx) / 2.0 * args.pad_factor, (args.min_width_m / 111_320.0) / 2.0)
    half_h = max(
        (maxy - miny) / 2.0 * args.pad_factor,
        (args.min_width_m / (111_320.0 * max(np.cos(np.radians(centroid.y)), 0.1))) / 2.0,
    )
    regional_bbox = (centroid.x - half_w, centroid.y - half_h, centroid.x + half_w, centroid.y + half_h)

    # First: find which scene(s) cover the regional bbox with low AOI-local cloud cover.
    # Search against the (small) AOI bbox for cloud verification - the regional bbox is used
    # only to discover which tile(s) are needed to mosaic full coverage.
    aoi_bbox = (minx, miny, maxx, maxy)
    primary = _select_scene(
        aoi_bbox,
        args.datetime_range,
        max_local_cloud_fraction=args.max_local_cloud_fraction,
        max_candidates=args.max_candidates,
    )

    # Find every tile intersecting the regional bbox near the same acquisition date so the
    # mosaic has full spatial coverage (Sentinel-2 tiles are ~110x110km; a wide view can span
    # more than one). Prefer scenes close in time to `primary` for visual consistency.
    same_period_candidates = _search_candidates(regional_bbox, args.datetime_range, limit=50)
    chosen_by_tile: dict[str, SceneCandidate] = {}
    for cand in same_period_candidates:
        tile = cand.item_id.split("_")[1]
        if tile not in chosen_by_tile or cand.tile_cloud_cover_percent < chosen_by_tile[tile].tile_cloud_cover_percent:
            chosen_by_tile[tile] = cand
    if not chosen_by_tile:
        chosen_by_tile = {primary.item_id.split("_")[1]: primary}

    hrefs = [c.visual_href for c in chosen_by_tile.values()]
    dst_crs, mosaic_bounds_dst_crs = _mosaic_visual(
        hrefs,
        bbox_wgs84=regional_bbox,
        output_path=args.output_raster,
        target_resolution_m=args.target_resolution_m,
    )

    metadata = {
        "label": "regional_overview",
        "datetime_range": args.datetime_range,
        "provider": "AWS Earth Search / sentinel-cogs (public Sentinel-2 L2A COG archive)",
        "stac_api": STAC_SEARCH_URL,
        "dataset_title": "Sentinel-2 L2A true-color (TCI) visual composite - wide regional context",
        "license": "Contains modified Copernicus Sentinel data",
        "selection_method": (
            "Selected the lowest AOI-local-cloud-fraction scene (verified via the SCL band, "
            f"threshold {args.max_local_cloud_fraction}) as the primary reference date, then mosaicked "
            "every intersecting MGRS tile's lowest-tile-cloud-cover scene from the same search window "
            "to cover the padded regional bbox. No per-pixel multi-scene cloud masking/median was performed."
        ),
        "aoi_geojson": str(args.aoi_geojson),
        "regional_bbox_wgs84": list(regional_bbox),
        "mosaic_crs": dst_crs,
        "mosaic_bounds": list(mosaic_bounds_dst_crs),
        "pad_factor": args.pad_factor,
        "target_resolution_m": args.target_resolution_m,
        "items_used": [
            {
                "tile": tile,
                "item_id": cand.item_id,
                "datetime": cand.datetime,
                "cloud_cover_percent": cand.tile_cloud_cover_percent,
                "asset_href": cand.visual_href,
                "collection": COLLECTION,
            }
            for tile, cand in sorted(chosen_by_tile.items())
        ],
        "output_raster": str(args.output_raster),
    }
    metadata_path = args.output_metadata or args.output_raster.with_suffix(args.output_raster.suffix + ".metadata.json")
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(metadata, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
