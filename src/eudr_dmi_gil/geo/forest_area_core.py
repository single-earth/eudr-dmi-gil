from __future__ import annotations

from typing import Any

import numpy as np
from pyproj import Geod
from rasterio.crs import CRS
from rasterio.features import rasterize


def pixel_area_m2_raster(transform: Any, height: int, width: int, crs: Any) -> np.ndarray:
    """Return per-pixel area in square meters for a raster.

    Units:
      - Output is square meters ($m^2$) for each pixel.

    Determinism:
      - Iterates rows/cols in stable order and uses np.float64 for repeatable results.

    Notes:
      - If `crs` is EPSG:4326, compute geodesic WGS84 pixel area using
        pyproj.Geod polygon areas for each pixel footprint.
      - Otherwise, treat pixels as projected and use constant area from affine scale.
    """

    if height <= 0 or width <= 0:
        return np.zeros((max(height, 0), max(width, 0)), dtype=np.float64)

    if crs is None:
        raise ValueError("CRS is required to compute pixel areas")

    crs_obj = CRS.from_user_input(crs)
    epsg = crs_obj.to_epsg()

    if epsg == 4326:
        geod = Geod(ellps="WGS84")
        area_m2 = np.zeros((height, width), dtype=np.float64)
        for row in range(height):
            for col in range(width):
                x0, y0 = transform * (col, row)
                x1, y1 = transform * (col + 1, row + 1)
                lons = [x0, x1, x1, x0]
                lats = [y0, y0, y1, y1]
                pixel_area, _ = geod.polygon_area_perimeter(lons, lats)
                area_m2[row, col] = abs(pixel_area)
        return area_m2

    pixel_area = abs(float(transform.a) * float(transform.e))
    return np.full((height, width), pixel_area, dtype=np.float64)


def forest_mask_end_year(
    treecover2000: np.ndarray,
    lossyear: np.ndarray,
    canopy_threshold: int,
    end_year: int,
) -> np.ndarray:
    """Return mask for forest remaining as of `end_year`.

    Units:
      - Input rasters are percent tree cover and lossyear codes (not area units).

    Loss-year mapping:
      - `lossyear` code = calendar_year - 2000 (1..24 => 2001..2024).

    Determinism:
      - Uses vectorized numpy operations with stable boolean logic.
    """

    forest2000 = treecover2000 >= canopy_threshold
    loss_after_end_year = lossyear > (end_year - 2000)
    remaining_forest = forest2000 & ((lossyear == 0) | loss_after_end_year)
    return remaining_forest


def rfm_mask(
    treecover2000: np.ndarray,
    canopy_threshold: int,
) -> np.ndarray:
    """Return reference forest mask (RFM) based on 2000 tree cover.

    Definition:
      - RFM is treecover2000 >= canopy_threshold.
    """

    return treecover2000 >= canopy_threshold


def loss_total_mask(
    treecover2000: np.ndarray,
    lossyear: np.ndarray,
    canopy_threshold: int,
) -> np.ndarray:
    """Return total loss mask for any loss year > 0 within RFM."""

    return rfm_mask(treecover2000, canopy_threshold) & (lossyear > 0)


def loss_2021_2024_mask(
    treecover2000: np.ndarray,
    lossyear: np.ndarray,
    canopy_threshold: int,
) -> np.ndarray:
    """Return loss mask for 2021-2024 (inclusive) within RFM."""

    return loss_mask_range(treecover2000, lossyear, canopy_threshold, 2021, 2024)


def forest_2024_mask(
    treecover2000: np.ndarray,
    lossyear: np.ndarray,
    canopy_threshold: int,
) -> np.ndarray:
    """Return forest mask for 2024: RFM and lossyear == 0."""

    return rfm_mask(treecover2000, canopy_threshold) & (lossyear == 0)


def loss_mask_range(
    treecover2000: np.ndarray,
    lossyear: np.ndarray,
    canopy_threshold: int,
    start_year: int,
    end_year: int,
) -> np.ndarray:
    """Return mask for forest loss between `start_year` and `end_year` (inclusive).

    Units:
      - Input rasters are percent tree cover and lossyear codes (not area units).

    Loss-year mapping:
      - `lossyear` code = calendar_year - 2000 (1..24 => 2001..2024).

    Determinism:
      - Uses vectorized numpy operations with stable boolean logic.
    """

    forest2000 = treecover2000 >= canopy_threshold
    sy = start_year - 2000
    ey = end_year - 2000
    return forest2000 & (lossyear >= sy) & (lossyear <= ey)


def zonal_area_ha(
    mask_bool: np.ndarray,
    pixel_area_m2: np.ndarray,
    zone_mask_bool: np.ndarray,
) -> float:
    """Compute zonal area in hectares for a boolean mask.

    Units:
      - `pixel_area_m2` is in $m^2$ per pixel.
      - Return value is in hectares (ha).

    Determinism:
      - Uses np.float64 reduction for stable results.
    """

    if mask_bool.shape != pixel_area_m2.shape or mask_bool.shape != zone_mask_bool.shape:
        raise ValueError("mask_bool, pixel_area_m2, and zone_mask_bool must share shape")

    combined = mask_bool & zone_mask_bool
    area_m2 = np.sum(pixel_area_m2[combined], dtype=np.float64)
    return float(area_m2) / 10_000.0


def rasterize_zone_mask(
    geom: Any,
    out_shape: tuple[int, int],
    transform: Any,
    all_touched: bool = True,
) -> np.ndarray:
    """Rasterize a polygon geometry into a boolean mask.

    Units:
      - Output is a boolean raster (True where geometry covers the pixel).

    Determinism:
      - Rasterization with fixed `all_touched` yields stable results.
    """

    if geom is None:
        raise ValueError("Geometry is required for rasterization")

    if hasattr(geom, "__geo_interface__"):
        geom = geom.__geo_interface__

    burned = rasterize(
        [(geom, 1)],
        out_shape=out_shape,
        transform=transform,
        fill=0,
        dtype=np.uint8,
        all_touched=all_touched,
    )
    return burned.astype(bool)
