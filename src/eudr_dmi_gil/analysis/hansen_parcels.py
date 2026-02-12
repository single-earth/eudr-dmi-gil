from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import numpy as np
import rasterio
from rasterio.warp import transform_geom

try:  # Optional speed-up
    from numba import njit, prange  # type: ignore[import-not-found]

    _NUMBA_AVAILABLE = True
except Exception:
    _NUMBA_AVAILABLE = False

from eudr_dmi_gil.geo.forest_area_core import (
    forest_mask_end_year,
    pixel_area_m2_raster,
    rasterize_zone_mask,
)
from eudr_dmi_gil.tasks.forest_loss_post_2020_clean import LocalTileSource, _pair_tiles


@dataclass(frozen=True)
class HansenParcelStats:
    parcel_id: str
    hansen_land_area_ha: float
    hansen_forest_area_ha: float


def _extract_land_use_designation(parcel: object) -> str | None:
    for attr in ("land_use_designation", "land_use_code", "siht1", "land_use"):
        value = getattr(parcel, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()

    props = getattr(parcel, "properties", None)
    if isinstance(props, Mapping):
        for key in ("siht1", "land_use_designation", "land_use", "sihtotstarve"):
            value = props.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    meta = getattr(parcel, "metadata", None)
    if isinstance(meta, Mapping):
        for key in ("siht1", "land_use_designation", "land_use", "sihtotstarve"):
            value = meta.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    return None


def land_use_designation_counts(parcels: Iterable[object]) -> dict[str, int]:
    """Return counts of unique land-use designations across parcels.

    Mirrors the eudr_dmi Maa-amet demo approach (typically using Maa-amet WFS
    `siht1` / land-use designation fields), but supports multiple parcel object
    shapes via `_extract_land_use_designation`.
    """

    counts: Counter[str] = Counter()
    for parcel in parcels:
        designation = _extract_land_use_designation(parcel)
        if designation is None:
            continue
        key = designation.strip()
        if not key:
            continue
        counts[key] += 1
    return dict(counts)


if _NUMBA_AVAILABLE:

    @njit(parallel=True)
    def _sum_area_m2_numba(mask: np.ndarray, pixel_area_m2: np.ndarray) -> float:
        total = 0.0
        rows, cols = mask.shape
        for r in prange(rows):
            row_total = 0.0
            for c in range(cols):
                if mask[r, c]:
                    row_total += float(pixel_area_m2[r, c])
            total += row_total
        return total


def _sum_area_m2(mask: np.ndarray, pixel_area_m2: np.ndarray) -> float:
    if _NUMBA_AVAILABLE:
        return float(_sum_area_m2_numba(mask, pixel_area_m2))
    return float(np.sum(pixel_area_m2[mask], dtype=np.float64))


def compute_hansen_parcel_stats(
    *,
    parcels: Iterable[object],
    tile_dir: Path,
    canopy_threshold_percent: int,
    end_year: int,
    parcel_crs: str | int = "EPSG:4326",
    all_touched: bool = False,
    include_only_land_use_designation: str | None = None,
) -> dict[str, HansenParcelStats]:
    """Compute Hansen-based land/forest area for parcel geometries.

    Expects each parcel to expose `parcel_id` and `geometry` attributes.

        Notes:
        - `parcel_crs` must match the CRS of `parcel.geometry`.
        - `all_touched=False` tends to reduce systematic boundary over-counting,
            and typically compares better to vector/cadastral areas.
    """

    parcel_list = [p for p in parcels if getattr(p, "geometry", None)]
    if include_only_land_use_designation is not None:
        want = include_only_land_use_designation.strip()
        parcel_list = [
            p
            for p in parcel_list
            if (_extract_land_use_designation(p) or "").strip() == want
        ]
    if not parcel_list:
        return {}

    stats: dict[str, HansenParcelStats] = {
        p.parcel_id: HansenParcelStats(
            parcel_id=p.parcel_id,
            hansen_land_area_ha=0.0,
            hansen_forest_area_ha=0.0,
        )
        for p in parcel_list
    }

    tile_source = LocalTileSource(tile_dir)
    treecover_tiles = tile_source.list_layer_files("treecover2000")
    lossyear_tiles = tile_source.list_layer_files("lossyear")
    pairs = _pair_tiles(treecover_tiles, lossyear_tiles)

    for tree_path, loss_path in pairs:
        with rasterio.open(tree_path) as tree_ds, rasterio.open(loss_path) as loss_ds:
            tree_band = tree_ds.read(1, masked=True)
            loss_band = loss_ds.read(1, masked=True)

            if tree_band.shape != loss_band.shape:
                raise RuntimeError("Mismatched raster shapes for treecover2000 and lossyear")

            valid = (~tree_band.mask) & (~loss_band.mask)
            tree_values = np.ma.filled(tree_band, 0)
            loss_values = np.ma.filled(loss_band, 0)

            forest_end_mask = forest_mask_end_year(
                tree_values,
                loss_values,
                canopy_threshold_percent,
                end_year,
            ) & valid

            pixel_area_m2 = pixel_area_m2_raster(
                tree_ds.transform,
                height=tree_band.shape[0],
                width=tree_band.shape[1],
                crs=tree_ds.crs,
            )

            for parcel in parcel_list:
                parcel_geom = transform_geom(parcel_crs, tree_ds.crs, parcel.geometry)
                zone_mask = rasterize_zone_mask(
                    parcel_geom,
                    out_shape=tree_band.shape,
                    transform=tree_ds.transform,
                    all_touched=all_touched,
                )

                zone_valid = zone_mask & valid
                if not np.any(zone_valid):
                    continue

                land_area_ha = _sum_area_m2(zone_valid, pixel_area_m2) / 10_000.0
                forest_area_ha = (
                    _sum_area_m2(forest_end_mask & zone_mask, pixel_area_m2) / 10_000.0
                )

                current = stats[parcel.parcel_id]
                stats[parcel.parcel_id] = HansenParcelStats(
                    parcel_id=parcel.parcel_id,
                    hansen_land_area_ha=current.hansen_land_area_ha + land_area_ha,
                    hansen_forest_area_ha=current.hansen_forest_area_ha + forest_area_ha,
                )

    return stats
