"""Acquire local raster inputs for the REAL Liberia FMC Area K contract-boundary AOI
(`aoi_json_examples/liberia_fmc_area_k_contract_boundary.geojson`, ~266,910 ha stated /
~266,342-267,500 ha geodesic-measured concession), for use with the canonical report CLI
(`python -m eudr_dmi_gil.reports.cli --evidence-only-assessment ...`).

Modeled on `tmp/acquire_fazenda_sucuri_inputs.py` (acquisition/validation conventions) and the
committed `eudr_dmi_gil.deps.tmf_acquire.EarthEngineTmfAdapter` /
`eudr_dmi_gil.deps.radd_acquire.EarthEngineRaddAdapter` (asset ids, RADD alert-tile filtering,
TMF layer/band conventions). This script does NOT import those adapters directly: their
`export_layer_geotiff`/`export_alert_geotiff` each make one single `Image.getDownloadURL` call,
which is fine for the ~493 ha smoke-test AOI they were validated against
(`aoi_json_examples/liberia_fmc_area_k_wood_evidence_smoke_aoi.geojson`) but not for this AOI:
Area K's bbox is ~85.3 km x 70.5 km (non-convex/river-bounded polygon, bbox area is ~2.26x the
polygon's own geodesic area), so a single-shot 10 m export (RADD Alert/Date, Sentinel-2) would
be tens of millions of pixels per band -- well past what `getDownloadURL` can return in one
call. This script instead tiles every export into sub-regions sized to stay safely under a
conservative per-request pixel budget, downloads each tile, and mosaics them locally with
`rasterio.merge` -- same asset ids/bands/filtering rules as the committed adapters, just chunked.

Extent/buffer choices (stated explicitly per the round-24 over-fetch lesson -- a prior bug where
satellite context covered ~150x the AOI):

- AOI-clipped analysis layers (JRC GFC2020, Hansen lossyear/treecover2000, JRC TMF x4, RADD
  Alert+Date, Sentinel-2 baseline/recent visual context): exported over the AOI's bounding
  rectangle buffered by AOI_BUFFER_M = 2000 m (2 km) on every side -- proportionate to a
  266,910 ha concession, matching the 1500 m buffer `tmp/acquire_fazenda_sucuri_inputs.py` uses
  for a much smaller farm AOI. Note the export rectangle is inherently larger than the polygon's
  own area (~2.26x) purely because Area K's boundary is non-convex (it follows the Jobo River,
  Cestos River, and Gwen Creek) -- this is a rectangular-raster-grid necessity, not an
  over-fetch; pixels outside the true polygon are still real, correctly-georeferenced data, just
  not summed into any AOI-area metric (the canonical CLI's own JRC/Hansen/TMF/RADD area
  computations clip to the true AOI polygon, not this bounding rectangle).
- Regional-overview Sentinel-2 composite: `eudr_dmi_gil.reports.report_model._write_regional_overview_png`
  crops whatever `--satellite-regional-raster` is given to the AOI bbox padded by
  `pad_factor=3.0` on every side (i.e. bbox width/height each become ~7x), and when the supplied
  raster's real coverage falls short of that padded frame (and `allow_variable_width` is not
  set, which it is not on this code path), the uncovered destination pixels are left
  zero-initialized -- a solid black nodata band, the exact defect Fazenda Sucuri rounds 1-7 hit
  repeatedly on this same page. So the regional raster acquired here DOES cover the full
  pad_factor=3.0 frame (~597 km x 492 km) -- but at REGIONAL_SCALE_M = 200 m/px, not 10 m/px:
  the rendered artifact is a fixed 900x620 px PNG (`_reproject_raster_to_grid(width=900,
  height=620, ...)`), so 10 m source resolution across a 597 km-wide frame would be discarded
  detail bought at an enormous (and, per the round-24 lesson, unjustifiable) download cost. 200
  m/px is ~3x finer than the final render actually needs (597 km / 900 px = ~663 m/px) and keeps
  the regional export in the tens-of-megabytes range. This raster is visual context ONLY -- it
  is never an input to any area/hectare metric.
- Admin boundaries (Nimba, Grand Gedeh, River Cess): fetched via
  `scripts/fetch_admin_boundaries.py --country liberia --pad-factor 3.0`, matching the same
  pad_factor so the boundary polygons line up with what the regional composite actually shows
  (this run adds a `liberia` entry to that script's `COUNTRY_SOURCES`, sourced from geoBoundaries
  gbOpen LBR/ADM1, build 9469f09 -- verified live 2026-08-15, 15 real Liberia counties returned,
  including Nimba, Grand Gedeh, and Rivercess/River Cess).

Comparison grid: JRC/Hansen/TMF are reprojected internally by the canonical CLI's analysis
modules to an equal-area CRS (EPSG:6933, "World Cylindrical Equal Area") at 30 m resolution
using nearest-neighbor resampling (`Resampling.nearest`, preserves discrete class/year values --
see `eudr_dmi_gil.analysis.jrc_post2020_loss._reproject_categorical`, reused by
`analysis.tmf_change` and `analysis.radd_alerts`). RADD is reprojected to the SAME EPSG:6933 CRS
but kept at its OWN native 10 m resolution (its own `target_resolution_m` default), not
resampled up to 30 m -- so area totals across sources are computed on a shared equal-area datum
but each source's own native pixel grid, never forced into Hansen's 30 m grid. This script's job
is only to acquire pinned local rasters at each source's native scale (10 m JRC baseline / RADD /
Sentinel-2, 30 m Hansen / TMF); the CLI's analysis modules perform the actual reprojection at
report-generation time.

If GEE access to a layer fails (most likely RADD, being newest/most fragile), this script prints
and records the failure explicitly in the manifest rather than fabricating that layer's output.
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.merge import merge as rasterio_merge

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))

import ee  # noqa: E402

from eudr_dmi_gil.analysis.radd_alerts import RADD_COLLECTION_ID  # noqa: E402
from eudr_dmi_gil.analysis.tmf_change import (  # noqa: E402
    TMF_DATASET_VERSION_DEFAULT,
    TMF_DEFORESTATION_ASSET_ID,
    TMF_DEGRADATION_ASSET_ID,
    TMF_DURATION_ASSET_ID,
    TMF_INTENSITY_ASSET_ID,
)
from eudr_dmi_gil.reports.determinism import sha256_file, write_json  # noqa: E402

PROJECT = "myproject-gq-74696"
AOI_ID = "liberia_fmc_area_k_contract_boundary"
AOI_PATH = REPO_ROOT / "aoi_json_examples" / f"{AOI_ID}.geojson"
OUT_DIR = REPO_ROOT / "out" / f"{AOI_ID}_inputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
TILE_TMP_DIR = OUT_DIR / "_tiles_tmp"
TILE_TMP_DIR.mkdir(parents=True, exist_ok=True)
FORCE_REFRESH_LABELS = {v.strip() for v in os.environ.get("FORCE_REFRESH_LABELS", "").split(",") if v.strip()}

AOI_BUFFER_M = 2000.0
REGIONAL_PAD_FACTOR = 3.0
REGIONAL_SCALE_M = 200.0
TARGET_TILE_BYTES = int(
    os.environ.get("AREA_K_TARGET_TILE_BYTES", "16000000")
)  # conservative per-request byte budget (well under GEE's cap)
DTYPE_BYTES = {"uint8": 1, "int16": 2, "uint16": 2, "int32": 4, "float32": 4}

RADD_GEOGRAPHY = "africa"
ALERT_BAND_SET = frozenset({"Alert", "Date"})

TMF_LAYERS: dict[str, tuple[str, str]] = {
    "deforestation": (TMF_DEFORESTATION_ASSET_ID, "constant"),
    "degradation": (TMF_DEGRADATION_ASSET_ID, "constant"),
    "duration": (TMF_DURATION_ASSET_ID, "constant"),
    "intensity": (TMF_INTENSITY_ASSET_ID, "sum"),
}

ASSETS = {
    "jrc_gfc2020": "JRC/GFC2020/V3",
    "hansen_gfc": "UMD/hansen/global_forest_change_2025_v1_13",
    "sentinel2": "COPERNICUS/S2_SR_HARMONIZED",
    "modis_mcd43a4": "MODIS/061/MCD43A4",
}

access_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

print(f"Initializing Earth Engine (project={PROJECT}) ...")
ee.Initialize(project=PROJECT)

aoi_geojson = json.loads(AOI_PATH.read_text(encoding="utf-8"))
aoi_feature_geom = aoi_geojson["features"][0]["geometry"]
aoi_geometry = ee.Geometry(aoi_feature_geom)


# ---------------------------------------------------------------------------
# Geometry / tiling helpers
# ---------------------------------------------------------------------------


def _bbox_of_geometry(geom: ee.Geometry) -> tuple[float, float, float, float]:
    coords = geom.bounds().getInfo()["coordinates"][0]
    lons = [pt[0] for pt in coords]
    lats = [pt[1] for pt in coords]
    return min(lons), min(lats), max(lons), max(lats)


def _meters_per_degree(lat_deg: float) -> tuple[float, float]:
    m_per_deg_lat = 110_540.0
    m_per_deg_lon = 111_320.0 * math.cos(math.radians(lat_deg))
    return m_per_deg_lon, m_per_deg_lat


aoi_bbox = _bbox_of_geometry(aoi_geometry)
aoi_center_lat = (aoi_bbox[1] + aoi_bbox[3]) / 2.0
m_per_deg_lon, m_per_deg_lat = _meters_per_degree(aoi_center_lat)

buffer_deg_lon = AOI_BUFFER_M / m_per_deg_lon
buffer_deg_lat = AOI_BUFFER_M / m_per_deg_lat
aoi_buffered_bbox = (
    aoi_bbox[0] - buffer_deg_lon,
    aoi_bbox[1] - buffer_deg_lat,
    aoi_bbox[2] + buffer_deg_lon,
    aoi_bbox[3] + buffer_deg_lat,
)
aoi_buffered_width_m = (aoi_buffered_bbox[2] - aoi_buffered_bbox[0]) * m_per_deg_lon
aoi_buffered_height_m = (aoi_buffered_bbox[3] - aoi_buffered_bbox[1]) * m_per_deg_lat

regional_pad_lon = (aoi_bbox[2] - aoi_bbox[0]) * REGIONAL_PAD_FACTOR
regional_pad_lat = (aoi_bbox[3] - aoi_bbox[1]) * REGIONAL_PAD_FACTOR
regional_bbox = (
    aoi_bbox[0] - regional_pad_lon,
    aoi_bbox[1] - regional_pad_lat,
    aoi_bbox[2] + regional_pad_lon,
    aoi_bbox[3] + regional_pad_lat,
)
regional_width_m = (regional_bbox[2] - regional_bbox[0]) * m_per_deg_lon
regional_height_m = (regional_bbox[3] - regional_bbox[1]) * m_per_deg_lat

print(
    f"AOI bbox (WGS84): {aoi_bbox}\n"
    f"AOI+{AOI_BUFFER_M:.0f}m-buffer bbox: {aoi_buffered_bbox} "
    f"(~{aoi_buffered_width_m / 1000:.1f} km x {aoi_buffered_height_m / 1000:.1f} km)\n"
    f"Regional (pad_factor={REGIONAL_PAD_FACTOR}) bbox: {regional_bbox} "
    f"(~{regional_width_m / 1000:.1f} km x {regional_height_m / 1000:.1f} km)"
)


def _tile_grid(
    bbox: tuple[float, float, float, float],
    *,
    scale_m: float,
    band_count: int = 1,
    bytes_per_pixel: int = 4,
) -> list[tuple[float, float, float, float]]:
    minx, miny, maxx, maxy = bbox
    width_m = (maxx - minx) * m_per_deg_lon
    height_m = (maxy - miny) * m_per_deg_lat
    width_px = max(1, math.ceil(width_m / scale_m))
    height_px = max(1, math.ceil(height_m / scale_m))
    total_px = width_px * height_px
    bytes_per_total_pixel = max(1, band_count * bytes_per_pixel)
    budget = max(1, TARGET_TILE_BYTES // bytes_per_total_pixel)
    n_tiles_needed = math.ceil(total_px / budget)
    # square-ish grid
    nx = max(1, math.ceil(math.sqrt(n_tiles_needed * width_px / height_px)))
    ny = max(1, math.ceil(n_tiles_needed / nx))
    dx = (maxx - minx) / nx
    dy = (maxy - miny) / ny
    tiles = []
    for i in range(nx):
        for j in range(ny):
            tiles.append(
                (minx + i * dx, miny + j * dy, minx + (i + 1) * dx, miny + (j + 1) * dy)
            )
    return tiles


def _download_url(url: str, out_path: Path, *, retries: int = 4) -> None:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=180) as response:  # noqa: S310
                out_path.write_bytes(response.read())
            return
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            last_err = exc
            wait = min(30, 2**attempt)
            print(f"  download attempt {attempt}/{retries} failed ({exc}); retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Download failed after {retries} attempts: {url}") from last_err


@dataclass
class TileDownloadResult:
    output_path: Path
    tile_count: int
    output_dimensions: list[int]
    output_sha256: str
    output_size_bytes: int


def download_image_tiled(
    *,
    image: ee.Image,
    bands: list[str],
    bbox: tuple[float, float, float, float],
    scale_m: float,
    out_path: Path,
    dtype: str,
    nodata: float | int | None,
    label: str,
) -> TileDownloadResult:
    """Tile `bbox`, download `image` (already `.select(bands)`) per tile, mosaic locally.

    Resumable: if `out_path` already exists on disk (e.g. from a prior run of this script that
    was interrupted after this layer completed but before the whole script finished -- see the
    2026-08-15 real run, killed partway through stage 5c after JRC/Hansen/TMF/RADD/baseline/
    recent had already downloaded successfully), this re-uses the existing file instead of
    re-downloading real, already-correct data.
    """
    if out_path.is_file() and label not in FORCE_REFRESH_LABELS:
        with rasterio.open(out_path) as ds:
            dims = [ds.width, ds.height]
        print(f"[{label}] SKIP (already on disk): {out_path} dims={dims}")
        return TileDownloadResult(
            output_path=out_path,
            tile_count=-1,
            output_dimensions=dims,
            output_sha256=sha256_file(out_path),
            output_size_bytes=out_path.stat().st_size,
        )
    if out_path.is_file() and label in FORCE_REFRESH_LABELS:
        print(f"[{label}] FORCE refresh: replacing existing {out_path}")
        out_path.unlink()
    cast = {"uint8": image.toByte, "int32": image.toInt32}.get(dtype)
    if cast is not None:
        image = cast()
    bytes_per_pixel = DTYPE_BYTES.get(dtype, 4)
    tiles = _tile_grid(bbox, scale_m=scale_m, band_count=len(bands), bytes_per_pixel=bytes_per_pixel)
    print(f"[{label}] downloading {len(tiles)} tile(s) at {scale_m}m ...")
    tile_paths: list[Path] = []
    for idx, (tminx, tminy, tmaxx, tmaxy) in enumerate(tiles):
        tile_rect = ee.Geometry.Rectangle([tminx, tminy, tmaxx, tmaxy], proj="EPSG:4326", geodesic=False)
        clipped = image.clip(tile_rect)
        url = clipped.getDownloadURL(
            {"region": tile_rect, "scale": scale_m, "crs": "EPSG:4326", "format": "GEO_TIFF"}
        )
        tile_path = TILE_TMP_DIR / f"{out_path.stem}_tile{idx:03d}.tif"
        _download_url(url, tile_path)
        tile_paths.append(tile_path)
        print(f"    tile {idx + 1}/{len(tiles)} -> {tile_path.name} ({tile_path.stat().st_size} bytes)")

    if len(tile_paths) == 1:
        tile_paths[0].replace(out_path)
    else:
        srcs = [rasterio.open(p) for p in tile_paths]
        try:
            mosaic, out_transform = rasterio_merge(srcs, nodata=nodata)
            profile = srcs[0].profile
            profile.update(
                height=mosaic.shape[1],
                width=mosaic.shape[2],
                transform=out_transform,
                count=mosaic.shape[0],
                nodata=nodata,
            )
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with rasterio.open(out_path, "w", **profile) as dst:
                dst.write(mosaic)
        finally:
            for s in srcs:
                s.close()
        for p in tile_paths:
            p.unlink(missing_ok=True)

    with rasterio.open(out_path) as ds:
        dims = [ds.width, ds.height]

    return TileDownloadResult(
        output_path=out_path,
        tile_count=len(tiles),
        output_dimensions=dims,
        output_sha256=sha256_file(out_path),
        output_size_bytes=out_path.stat().st_size,
    )


def validate_visual_raster_coverage(path: Path, *, min_valid_percent: float = 98.0) -> dict[str, Any]:
    """Reject visual-context rasters with large nodata/black export gaps (Fazenda Sucuri round 2)."""
    with rasterio.open(path) as ds:
        mask = ds.dataset_mask()
        data = ds.read()
        valid_pixels = int(np.count_nonzero(mask))
        total_pixels = int(mask.size)
        all_zero_rgb = np.all(data == 0, axis=0) if data.shape[0] >= 3 else np.zeros(mask.shape, dtype=bool)
        valid_nonzero_mask = (mask > 0) & (~all_zero_rgb)
        valid_nonzero_pixels = int(np.count_nonzero(valid_nonzero_mask))
        valid_percent = (valid_nonzero_pixels / total_pixels * 100.0) if total_pixels else 0.0
        all_zero_rgb_pixels = int(np.count_nonzero(all_zero_rgb))
    result = {
        "valid_pixels": valid_nonzero_pixels,
        "gdal_mask_valid_pixels": valid_pixels,
        "all_zero_rgb_pixels": all_zero_rgb_pixels,
        "all_zero_rgb_percent": round((all_zero_rgb_pixels / total_pixels * 100.0) if total_pixels else 0.0, 6),
        "total_pixels": total_pixels,
        "valid_percent": round(valid_percent, 6),
        "coverage_gate_min_valid_percent": min_valid_percent,
        "passed": valid_percent >= min_valid_percent,
    }
    if not result["passed"]:
        print(f"  WARNING: {path.name} coverage {valid_percent:.2f}% below gate {min_valid_percent}%")
    return result


# ---------------------------------------------------------------------------
# Sentinel-2 helpers
# ---------------------------------------------------------------------------


def mask_s2_sr(image: ee.Image) -> ee.Image:
    scl = image.select("SCL")
    keep = (
        scl.neq(0)
        .And(scl.neq(1))
        .And(scl.neq(3))
        .And(scl.neq(8))
        .And(scl.neq(9))
        .And(scl.neq(10))
        .And(scl.neq(11))
    )
    return image.updateMask(keep).divide(10000).copyProperties(
        image, ["system:time_start", "CLOUDY_PIXEL_PERCENTAGE"]
    )


def mask_s2_zero_rgb(image: ee.Image) -> ee.Image:
    """Mask Sentinel-2 tile padding where RGB bands are all zero.

    Some wide regional requests include pixels inside a granule rectangle but outside real image
    content. Those pixels are not always masked by SCL, and if left unmasked they win in a mosaic
    as hard black rectangles.
    """
    rgb_sum = image.select(["B4", "B3", "B2"]).reduce(ee.Reducer.sum())
    return image.updateMask(rgb_sum.gt(0)).copyProperties(
        image, ["system:time_start", "CLOUDY_PIXEL_PERCENTAGE"]
    )


def s2_collection(geom: ee.Geometry, start: str, end: str, max_cloud: float) -> ee.ImageCollection:
    return (
        ee.ImageCollection(ASSETS["sentinel2"])
        .filterBounds(geom)
        .filterDate(start, end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", max_cloud))
    )


def find_s2_window(
    geom: ee.Geometry, *, candidate_windows: list[tuple[str, str]], max_cloud: float
) -> tuple[ee.ImageCollection, tuple[str, str], int]:
    """Try candidate (start, end) windows in order; return the first with >=1 qualifying scene."""
    for start, end in candidate_windows:
        col = s2_collection(geom, start, end, max_cloud)
        n = col.size().getInfo()
        if n > 0:
            return col, (start, end), n
    raise RuntimeError(f"No Sentinel-2 scenes found under cloud<{max_cloud} in any candidate window")


def s2_composite_and_diagnostics(
    col: ee.ImageCollection,
    geom: ee.Geometry,
    *,
    include_valid_obs_diagnostics: bool = True,
    valid_obs_scale: float = 20.0,
    composite_method: str = "median",
) -> tuple[ee.Image, dict[str, Any]]:
    """Build the cloud-masked median composite + scene diagnostics.

    BUG FOUND DURING THE REAL RUN (2026-08-15): the original version of this function always
    ran a `reduceRegion(scale=20)` mean/min valid-obs-per-pixel diagnostic over `geom`. That is
    cheap for the tight AOI+buffer geometry (~89km x 74km) but the regional-overview call passes
    a ~597km x 492km rectangle -- reducing that at 20m/px is tens of millions of pixels of
    server-side work and is what actually stalled the live run partway through stage 5c (it had
    already successfully finished JRC/Hansen/TMF/RADD/baseline/recent by then; only the regional
    composite's diagnostics call hung). Fixed by making the valid-obs diagnostic opt-out
    (`include_valid_obs_diagnostics=False` for the regional call, which only ever needs
    scene_count/least-cloudy-date -- cheap metadata, not a full reduceRegion) and by scaling
    `valid_obs_scale` to the caller's own raster scale when it is kept on.
    """
    masked = col.map(mask_s2_sr).map(mask_s2_zero_rgb)
    sorted_first = col.sort("CLOUDY_PIXEL_PERCENTAGE").first()
    if composite_method == "median":
        composite = masked.median()
    elif composite_method == "mosaic_least_cloudy":
        # Per-pixel selection from the least-cloudy available scene, not a full per-pixel
        # median reduction across every scene -- much cheaper for a collection with a very
        # large scene count (the regional composite's real run pulled 1026 scenes over the
        # ~597x492km frame; a synchronous getPixels() median composite across that many images
        # returned HTTP 400 Bad Request from Earth Engine -- too complex a computation graph
        # for a synchronous request. `.mosaic()` on a cloud-percentage-sorted collection is the
        # standard cheaper alternative and is what this AOI's own baseline/recent composites
        # would have needed too had their scene counts been this large.)
        masked_mosaic = masked.sort("CLOUDY_PIXEL_PERCENTAGE", False).mosaic()
        raw_fallback = col.map(mask_s2_zero_rgb).sort("CLOUDY_PIXEL_PERCENTAGE", False).mosaic().divide(10000)
        composite = masked_mosaic.unmask(raw_fallback)
    else:
        raise ValueError(f"Unknown composite_method: {composite_method}")
    diag_dict: dict[str, Any] = {
        "scene_count": col.size(),
        "least_cloudy_scene_date": ee.Image(sorted_first).date().format("YYYY-MM-dd"),
        "least_cloudy_scene_cloud_pct": ee.Image(sorted_first).get("CLOUDY_PIXEL_PERCENTAGE"),
    }
    if include_valid_obs_diagnostics:
        valid_obs = masked.select("B4").count().rename("valid_obs").clip(geom)
        diag_dict["mean_valid_obs_per_pixel"] = valid_obs.reduceRegion(
            reducer=ee.Reducer.mean(), geometry=geom, scale=valid_obs_scale, maxPixels=1e13, tileScale=4
        ).get("valid_obs")
        diag_dict["min_valid_obs_per_pixel"] = valid_obs.reduceRegion(
            reducer=ee.Reducer.min(), geometry=geom, scale=valid_obs_scale, maxPixels=1e13, tileScale=4
        ).get("valid_obs")
    diagnostics = ee.Dictionary(diag_dict).getInfo()
    visual = composite.visualize(bands=["B4", "B3", "B2"], min=0.0, max=0.3)
    return visual, diagnostics


def modis_regional_composite_and_diagnostics(
    geom: ee.Geometry, *, start: str, end: str
) -> tuple[ee.Image, dict[str, Any]]:
    col = ee.ImageCollection(ASSETS["modis_mcd43a4"]).filterBounds(geom).filterDate(start, end)
    n = col.size().getInfo()
    if n == 0:
        raise RuntimeError(f"No MODIS MCD43A4 scenes found in {start}..{end}")
    composite = col.median()
    visual = composite.visualize(
        bands=["Nadir_Reflectance_Band1", "Nadir_Reflectance_Band4", "Nadir_Reflectance_Band3"],
        min=0,
        max=4000,
        gamma=1.2,
    )
    diagnostics = {
        "scene_count": n,
        "window_start": start,
        "window_end": end,
        "regional_fallback_reason": "Sentinel-2 regional mosaic retained tile-shaped RGB-zero nodata in the wide coastal frame.",
    }
    return visual, diagnostics


# ===========================================================================
# 1. JRC GFC2020 baseline (10m native)
# ===========================================================================

manifest: dict[str, Any] = {
    "access_timestamp_utc": access_ts,
    "aoi_id": AOI_ID,
    "aoi_geojson_path": str(AOI_PATH.relative_to(REPO_ROOT)),
    "aoi_geojson_sha256": sha256_file(AOI_PATH),
    "aoi_bbox_wgs84": list(aoi_bbox),
    "aoi_buffer_m": AOI_BUFFER_M,
    "aoi_buffered_bbox_wgs84": list(aoi_buffered_bbox),
    "aoi_buffered_extent_km": [
        round(aoi_buffered_width_m / 1000, 3),
        round(aoi_buffered_height_m / 1000, 3),
    ],
    "regional_pad_factor": REGIONAL_PAD_FACTOR,
    "regional_bbox_wgs84": list(regional_bbox),
    "regional_extent_km": [round(regional_width_m / 1000, 3), round(regional_height_m / 1000, 3)],
    "regional_scale_m": REGIONAL_SCALE_M,
    "layers": {},
    "failures": {},
}

print("\n=== 1. JRC GFC2020/V3 baseline ===")
try:
    jrc_image = ee.Image(ASSETS["jrc_gfc2020"]).select(["Map"])
    jrc_result = download_image_tiled(
        image=jrc_image,
        bands=["Map"],
        bbox=aoi_buffered_bbox,
        scale_m=10.0,
        out_path=OUT_DIR / "jrc_gfc2020_v3.tif",
        dtype="uint8",
        nodata=None,
        label="jrc_gfc2020",
    )
    manifest["layers"]["jrc_gfc2020"] = {
        "asset_id": ASSETS["jrc_gfc2020"],
        "dataset_version": "V3",
        "band": "Map",
        "scale_m": 10.0,
        "role": "forest_baseline",
        "tile_count": jrc_result.tile_count,
        "output_dimensions": jrc_result.output_dimensions,
        "output_path": str(jrc_result.output_path.relative_to(REPO_ROOT)),
        "output_sha256": jrc_result.output_sha256,
        "output_size_bytes": jrc_result.output_size_bytes,
    }
    print(f"  OK: {jrc_result.output_path} dims={jrc_result.output_dimensions}")
except Exception as exc:  # noqa: BLE001
    print(f"  FAILED: {exc}")
    manifest["failures"]["jrc_gfc2020"] = str(exc)

# ===========================================================================
# 2. Hansen lossyear + treecover2000 (30m native)
# ===========================================================================

print("\n=== 2. Hansen GFC 2025 v1.13 lossyear + treecover2000 ===")
for band, out_name, role in [
    ("lossyear", "hansen_lossyear_2025_v1_13.tif", "forest_loss"),
    ("treecover2000", "hansen_treecover2000_2025_v1_13.tif", "hansen_canopy_baseline"),
]:
    key = f"hansen_{band}"
    try:
        img = ee.Image(ASSETS["hansen_gfc"]).select([band])
        result = download_image_tiled(
            image=img,
            bands=[band],
            bbox=aoi_buffered_bbox,
            scale_m=30.0,
            out_path=OUT_DIR / out_name,
            dtype="uint8" if band == "treecover2000" else "uint8",
            nodata=None,
            label=key,
        )
        manifest["layers"][key] = {
            "asset_id": ASSETS["hansen_gfc"],
            "dataset_version": "2025-v1.13",
            "band": band,
            "scale_m": 30.0,
            "role": role,
            "tile_count": result.tile_count,
            "output_dimensions": result.output_dimensions,
            "output_path": str(result.output_path.relative_to(REPO_ROOT)),
            "output_sha256": result.output_sha256,
            "output_size_bytes": result.output_size_bytes,
        }
        print(f"  OK: {result.output_path} dims={result.output_dimensions}")
    except Exception as exc:  # noqa: BLE001
        print(f"  FAILED ({key}): {exc}")
        manifest["failures"][key] = str(exc)

# ===========================================================================
# 3. JRC TMF DeforestationYear / DegradationYear / Duration / Intensity (30m native)
# ===========================================================================

print("\n=== 3. JRC TMF v1_2025 layers ===")
for layer_name, (asset_id, band) in TMF_LAYERS.items():
    key = f"tmf_{layer_name}"
    try:
        collection = ee.ImageCollection(asset_id).filterBounds(aoi_geometry)
        info = collection.getInfo()
        tile_ids = [f["id"] for f in info.get("features", [])]
        if not tile_ids:
            raise RuntimeError(f"No {asset_id} tiles intersect the AOI")
        images = [ee.Image(tid).select([band]) for tid in tile_ids]
        mosaic = images[0] if len(images) == 1 else ee.ImageCollection(images).mosaic()
        result = download_image_tiled(
            image=mosaic,
            bands=[band],
            bbox=aoi_buffered_bbox,
            scale_m=30.0,
            out_path=OUT_DIR / f"tmf_{layer_name}.tif",
            dtype="int32",
            nodata=None,
            label=key,
        )
        manifest["layers"][key] = {
            "asset_id": asset_id,
            "dataset_version": TMF_DATASET_VERSION_DEFAULT,
            "band": band,
            "scale_m": 30.0,
            "source_tile_ids": tile_ids,
            "tile_count": result.tile_count,
            "output_dimensions": result.output_dimensions,
            "output_path": str(result.output_path.relative_to(REPO_ROOT)),
            "output_sha256": result.output_sha256,
            "output_size_bytes": result.output_size_bytes,
        }
        print(f"  OK ({layer_name}): {result.output_path} dims={result.output_dimensions} source_tiles={tile_ids}")
    except Exception as exc:  # noqa: BLE001
        print(f"  FAILED ({key}): {exc}")
        manifest["failures"][key] = str(exc)

# ===========================================================================
# 4. RADD alert + date (10m native)
# ===========================================================================

print("\n=== 4. RADD alert (Sentinel-1 SAR) ===")
try:
    radd_collection = (
        ee.ImageCollection(RADD_COLLECTION_ID)
        .filterBounds(aoi_geometry)
        .filter(ee.Filter.eq("geography", RADD_GEOGRAPHY))
    )
    radd_info = radd_collection.getInfo()
    all_tiles = []
    for f in radd_info.get("features", []):
        props = f.get("properties", {}) or {}
        bands = tuple(b.get("id", "") for b in f.get("bands", []) or [])
        all_tiles.append({"id": f["id"], "system_index": props.get("system:index", f["id"]), "bands": bands})
    alert_tiles = sorted(
        [t for t in all_tiles if set(t["bands"]) == ALERT_BAND_SET], key=lambda t: t["system_index"]
    )
    dropped_non_alert = [t["id"] for t in all_tiles if t not in alert_tiles]
    print(f"  found {len(all_tiles)} total tiles, {len(alert_tiles)} dated alert tiles, "
          f"{len(dropped_non_alert)} dropped (non-Alert+Date band set)")
    if not alert_tiles:
        raise RuntimeError(
            f"No RADD alert tiles (Alert+Date bands) intersect this AOI for geography={RADD_GEOGRAPHY!r}"
        )
    tile_ids = [t["id"] for t in alert_tiles]
    images = [ee.Image(tid) for tid in tile_ids]
    radd_mosaic = images[0] if len(images) == 1 else ee.ImageCollection(images).mosaic()

    radd_layer_meta: dict[str, Any] = {}
    for band, out_name in [("Alert", "radd_alert.tif"), ("Date", "radd_date.tif")]:
        band_img = radd_mosaic.select([band])
        result = download_image_tiled(
            image=band_img,
            bands=[band],
            bbox=aoi_buffered_bbox,
            scale_m=10.0,
            out_path=OUT_DIR / "radd" / out_name,
            dtype="int32",
            nodata=None,
            label=f"radd_{band.lower()}",
        )
        radd_layer_meta[band] = {
            "output_dimensions": result.output_dimensions,
            "tile_count": result.tile_count,
            "output_path": str(result.output_path.relative_to(REPO_ROOT)),
            "output_sha256": result.output_sha256,
            "output_size_bytes": result.output_size_bytes,
        }
        print(f"  OK ({band}): {result.output_path} dims={result.output_dimensions}")

    manifest["layers"]["radd"] = {
        "collection_id": RADD_COLLECTION_ID,
        "geography": RADD_GEOGRAPHY,
        "scale_m": 10.0,
        "status": "exported",
        "tile_ids": tile_ids,
        "dropped_non_alert_tile_ids": dropped_non_alert,
        "confidence_semantics": {
            "alert_2": "unconfirmed / low-confidence alert",
            "alert_3": "confirmed / high-confidence alert",
        },
        "date_band_encoding": "YYDOY (2-digit year + 3-digit day-of-year, e.g. 24001 = 2024-01-01)",
        "sensor": "Sentinel-1 SAR (genuinely different sensor from Hansen/TMF's Landsat optical "
                  "detection); NOTE: RADD's own primary-forest baseline/domain mask incorporates "
                  "historical Hansen loss, so RADD is not fully statistically independent of "
                  "Hansen even though its per-alert detection sensor is independent.",
        "bands": radd_layer_meta,
    }
except Exception as exc:  # noqa: BLE001
    print(f"  FAILED (radd): {exc}")
    manifest["failures"]["radd"] = str(exc)

# ===========================================================================
# 5. Sentinel-2 baseline (2020), recent (latest available), regional (wide context)
# ===========================================================================

print("\n=== 5. Sentinel-2 baseline (2020) ===")
try:
    col, window, n = find_s2_window(
        aoi_geometry,
        candidate_windows=[("2020-01-01", "2020-04-01"), ("2020-01-01", "2020-06-01"), ("2020-01-01", "2021-01-01")],
        max_cloud=40,
    )
    print(f"  window {window} -> {n} scenes")
    visual, diagnostics = s2_composite_and_diagnostics(col, aoi_geometry)
    result = download_image_tiled(
        image=visual,
        bands=["vis-red", "vis-green", "vis-blue"],
        bbox=aoi_buffered_bbox,
        scale_m=10.0,
        out_path=OUT_DIR / "sentinel2_baseline_2020.tif",
        dtype="uint8",
        nodata=None,
        label="sentinel2_baseline_2020",
    )
    coverage = validate_visual_raster_coverage(result.output_path, min_valid_percent=90.0)
    manifest["layers"]["sentinel2_baseline_2020"] = {
        "asset_id": ASSETS["sentinel2"],
        "dataset_version": "sentinel-2-l2a",
        "role": "sentinel2_visual_context_2020",
        "window": list(window),
        "scene_count": n,
        "scale_m": 10.0,
        "tile_count": result.tile_count,
        "output_dimensions": result.output_dimensions,
        "output_path": str(result.output_path.relative_to(REPO_ROOT)),
        "output_sha256": result.output_sha256,
        "output_size_bytes": result.output_size_bytes,
        "diagnostics": diagnostics,
        "coverage_check": coverage,
    }
    print(f"  OK: {result.output_path} dims={result.output_dimensions} coverage={coverage['valid_percent']:.2f}%")
except Exception as exc:  # noqa: BLE001
    print(f"  FAILED (sentinel2_baseline_2020): {exc}")
    manifest["failures"]["sentinel2_baseline_2020"] = str(exc)

print("\n=== 5b. Sentinel-2 recent (latest available, lowest cloud) ===")
try:
    now = datetime.now(timezone.utc)
    recent_windows = [
        ("2026-01-01", "2026-08-15"),
        ("2025-06-01", "2026-08-15"),
        ("2024-06-01", "2026-08-15"),
    ]
    col, window, n = find_s2_window(aoi_geometry, candidate_windows=recent_windows, max_cloud=40)
    print(f"  window {window} -> {n} scenes")
    visual, diagnostics = s2_composite_and_diagnostics(col, aoi_geometry)
    result = download_image_tiled(
        image=visual,
        bands=["vis-red", "vis-green", "vis-blue"],
        bbox=aoi_buffered_bbox,
        scale_m=10.0,
        out_path=OUT_DIR / "sentinel2_recent.tif",
        dtype="uint8",
        nodata=None,
        label="sentinel2_recent",
    )
    coverage = validate_visual_raster_coverage(result.output_path, min_valid_percent=90.0)
    manifest["layers"]["sentinel2_recent"] = {
        "asset_id": ASSETS["sentinel2"],
        "dataset_version": "sentinel-2-l2a",
        "role": "sentinel2_visual_context_recent",
        "window": list(window),
        "scene_count": n,
        "scale_m": 10.0,
        "tile_count": result.tile_count,
        "output_dimensions": result.output_dimensions,
        "output_path": str(result.output_path.relative_to(REPO_ROOT)),
        "output_sha256": result.output_sha256,
        "output_size_bytes": result.output_size_bytes,
        "diagnostics": diagnostics,
        "coverage_check": coverage,
    }
    print(f"  OK: {result.output_path} dims={result.output_dimensions} coverage={coverage['valid_percent']:.2f}%")
except Exception as exc:  # noqa: BLE001
    print(f"  FAILED (sentinel2_recent): {exc}")
    manifest["failures"]["sentinel2_recent"] = str(exc)

print("\n=== 5c. Sentinel-2 regional-context composite (wide, coarse) ===")
try:
    regional_rect = ee.Geometry.Rectangle(list(regional_bbox), proj="EPSG:4326", geodesic=False)
    regional_source = os.environ.get("AREA_K_REGIONAL_VISUAL_SOURCE", "sentinel2").strip().lower()
    if regional_source == "modis_mcd43a4":
        window = ("2026-01-01", "2026-04-01")
        visual, diagnostics = modis_regional_composite_and_diagnostics(
            regional_rect, start=window[0], end=window[1]
        )
        n = int(diagnostics["scene_count"])
        asset_id = ASSETS["modis_mcd43a4"]
        dataset_version = "061"
        source_note = (
            "Coarse (200m/px request, MODIS native 500m reflectance) wide-context composite for "
            "the Regional Overview page only; selected as a real global-reflectance fallback after "
            "Sentinel-2 retained tile-shaped RGB-zero nodata in the wide coastal regional frame."
        )
    else:
        # Narrower dry-season-first windows than the original attempt: the first real run's
        # ('2025-11-01','2026-08-15') window returned 1026 scenes over this huge frame, and a
        # synchronous median composite across that many images returned HTTP 400 (computation graph
        # too complex for one getPixels() request). A single dry-season window is both cheaper and a
        # more representative single-season regional snapshot than a 9-month multi-season pull.
        col, window, n = find_s2_window(
            regional_rect,
            candidate_windows=[
                ("2026-01-01", "2026-04-01"),
                ("2025-11-01", "2026-04-01"),
                ("2025-01-01", "2025-04-01"),
            ],
            max_cloud=30,
        )
        print(f"  window {window} -> {n} scenes")
        visual, diagnostics = s2_composite_and_diagnostics(
            col,
            regional_rect,
            include_valid_obs_diagnostics=False,
            composite_method="mosaic_least_cloudy",
        )
        asset_id = ASSETS["sentinel2"]
        dataset_version = "sentinel-2-l2a"
        source_note = "Coarse (200m/px) wide-context composite for the Regional Overview page only (rendered at a fixed 900x620px); never used for area/hectare metrics."
    print(f"  source {regional_source}, window {window} -> {n} scenes")
    result = download_image_tiled(
        image=visual,
        bands=["vis-red", "vis-green", "vis-blue"],
        bbox=regional_bbox,
        scale_m=REGIONAL_SCALE_M,
        out_path=OUT_DIR / "sentinel2_regional_overview.tif",
        dtype="uint8",
        nodata=None,
        label="sentinel2_regional",
    )
    coverage = validate_visual_raster_coverage(result.output_path, min_valid_percent=85.0)
    manifest["layers"]["sentinel2_regional_overview"] = {
        "asset_id": asset_id,
        "dataset_version": dataset_version,
        "role": "sentinel2_regional_overview",
        "regional_visual_source": regional_source,
        "window": list(window),
        "scene_count": n,
        "scale_m": REGIONAL_SCALE_M,
        "note": source_note,
        "tile_count": result.tile_count,
        "output_dimensions": result.output_dimensions,
        "output_path": str(result.output_path.relative_to(REPO_ROOT)),
        "output_sha256": result.output_sha256,
        "output_size_bytes": result.output_size_bytes,
        "diagnostics": diagnostics,
        "coverage_check": coverage,
    }
    print(f"  OK: {result.output_path} dims={result.output_dimensions} coverage={coverage['valid_percent']:.2f}%")
except Exception as exc:  # noqa: BLE001
    print(f"  FAILED (sentinel2_regional_overview): {exc}")
    manifest["failures"]["sentinel2_regional_overview"] = str(exc)

# ===========================================================================
# 6. Admin boundaries (Nimba, Grand Gedeh, River Cess) via fetch_admin_boundaries.py
# ===========================================================================

print("\n=== 6. Liberia admin boundaries ===")
try:
    import subprocess

    admin_out = OUT_DIR / "regional_admin_boundaries.geojson"
    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "fetch_admin_boundaries.py"),
        "--aoi-geojson",
        str(AOI_PATH),
        "--country",
        "liberia",
        "--output-geojson",
        str(admin_out),
        "--pad-factor",
        str(REGIONAL_PAD_FACTOR),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    print(f"  {proc.stdout.strip()}")
    admin_data = json.loads(admin_out.read_text(encoding="utf-8"))
    manifest["layers"]["regional_admin_boundaries"] = {
        "output_path": str(admin_out.relative_to(REPO_ROOT)),
        "output_sha256": sha256_file(admin_out),
        "feature_count": len(admin_data.get("features", [])),
        "feature_names": [f["properties"].get("name") for f in admin_data.get("features", [])],
        "source_dataset_title": admin_data.get("metadata", {}).get("dataset_title"),
        "source_url": admin_data.get("metadata", {}).get("source_url"),
        "pad_factor": REGIONAL_PAD_FACTOR,
    }
    print(f"  OK: {admin_out} features={manifest['layers']['regional_admin_boundaries']['feature_count']}")
except Exception as exc:  # noqa: BLE001
    print(f"  FAILED (regional_admin_boundaries): {exc}")
    manifest["failures"]["regional_admin_boundaries"] = str(exc)

# ===========================================================================
# Write manifest
# ===========================================================================

try:
    TILE_TMP_DIR.rmdir()
except OSError:
    pass  # left non-empty only if a failed run left partial tiles; inspect manually

write_json(OUT_DIR / "acquisition_manifest.json", manifest)
print(f"\nWrote manifest: {OUT_DIR / 'acquisition_manifest.json'}")
print(json.dumps({"layers": list(manifest["layers"].keys()), "failures": manifest["failures"]}, indent=2))
