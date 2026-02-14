from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Iterable, Mapping

import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.warp import calculate_default_transform, reproject, transform_geom
from rasterio.enums import Resampling
from rasterio.transform import array_bounds
from rasterio.windows import Window, from_bounds, transform as window_transform
from shapely.geometry import shape

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
from eudr_dmi_gil.deps.hansen_tiles import hansen_tile_ids_for_bbox
from eudr_dmi_gil.tasks.forest_loss_post_2020_clean import LocalTileSource, _pair_tiles


@dataclass(frozen=True)
class HansenParcelStats:
    parcel_id: str
    hansen_land_area_ha: float
    hansen_forest_area_ha: float
    hansen_forest_loss_ha: float


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

    @njit(parallel=True, fastmath=True, cache=True)
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


def _crs_cache_key(crs: CRS | None) -> str:
    if crs is None:
        return "none"
    try:
        return crs.to_string()
    except Exception:
        return str(crs)


def _bounds_intersect(
    left_bounds: tuple[float, float, float, float],
    right_bounds: tuple[float, float, float, float],
) -> bool:
    left_minx, left_miny, left_maxx, left_maxy = left_bounds
    right_minx, right_miny, right_maxx, right_maxy = right_bounds
    return not (
        left_maxx < right_minx
        or right_maxx < left_minx
        or left_maxy < right_miny
        or right_maxy < left_miny
    )


_HANSEN_TILE_ID_RE = re.compile(r"([NS]\d{2}_[EW]\d{3})", flags=re.IGNORECASE)


def _tile_id_from_path(path: Path) -> str | None:
    parent_name = path.parent.name.upper()
    if _HANSEN_TILE_ID_RE.fullmatch(parent_name):
        return parent_name

    stem = path.stem.upper()
    match = _HANSEN_TILE_ID_RE.search(stem)
    if match is not None:
        return match.group(1)

    return None


def _filter_tiles_by_bbox(
    paths: list[Path],
    *,
    bbox_wgs84: tuple[float, float, float, float],
) -> list[Path]:
    required_ids = {tile_id.upper() for tile_id in hansen_tile_ids_for_bbox(bbox_wgs84)}
    if not required_ids:
        return paths

    detected = [(path, _tile_id_from_path(path)) for path in paths]
    if not any(tile_id is not None for _, tile_id in detected):
        return paths

    filtered = [path for path, tile_id in detected if tile_id is not None and tile_id in required_ids]
    return filtered or paths


def _reproject_to_projected(
    *,
    tree_values: np.ndarray,
    loss_values: np.ndarray,
    valid_mask: np.ndarray,
    source_transform: rasterio.Affine,
    source_crs: CRS | None,
    target_crs: str | int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, rasterio.Affine, object]:
    if source_crs is None:
        return tree_values, loss_values, valid_mask, source_transform, source_crs

    def _attempt_reproject(target: CRS) -> tuple[np.ndarray, np.ndarray, np.ndarray, rasterio.Affine, CRS] | None:
        if target == source_crs:
            return None

        west, south, east, north = array_bounds(
            tree_values.shape[0],
            tree_values.shape[1],
            source_transform,
        )

        dst_transform, dst_width, dst_height = calculate_default_transform(
            source_crs,
            target,
            tree_values.shape[1],
            tree_values.shape[0],
            west,
            south,
            east,
            north,
        )

        if dst_width <= 0 or dst_height <= 0:
            return None

        dst_tree = np.zeros((dst_height, dst_width), dtype=tree_values.dtype)
        dst_loss = np.zeros((dst_height, dst_width), dtype=loss_values.dtype)
        dst_valid = np.zeros((dst_height, dst_width), dtype=np.uint8)

        reproject(
            source=tree_values,
            destination=dst_tree,
            src_transform=source_transform,
            src_crs=source_crs,
            dst_transform=dst_transform,
            dst_crs=target,
            resampling=Resampling.nearest,
        )
        reproject(
            source=loss_values,
            destination=dst_loss,
            src_transform=source_transform,
            src_crs=source_crs,
            dst_transform=dst_transform,
            dst_crs=target,
            resampling=Resampling.nearest,
        )
        reproject(
            source=valid_mask.astype(np.uint8),
            destination=dst_valid,
            src_transform=source_transform,
            src_crs=source_crs,
            dst_transform=dst_transform,
            dst_crs=target,
            resampling=Resampling.nearest,
        )

        return dst_tree, dst_loss, dst_valid.astype(bool), dst_transform, target

    target = CRS.from_user_input(target_crs)
    try:
        result = _attempt_reproject(target)
    except Exception:
        result = None

    if result is None:
        fallback = CRS.from_epsg(3857)
        try:
            result = _attempt_reproject(fallback)
        except Exception:
            result = None

    if result is None:
        return tree_values, loss_values, valid_mask, source_transform, source_crs

    return result


def compute_hansen_parcel_stats(
    *,
    parcels: Iterable[object],
    tile_dir: Path,
    canopy_threshold_percent: int,
    end_year: int,
    cutoff_year: int = 2020,
    parcel_crs: str | int = "EPSG:4326",
    all_touched: bool = False,
    include_only_land_use_designation: str | None = None,
    reproject_to_projected: bool = True,
    projected_crs: str | int = "EPSG:6933",
) -> dict[str, HansenParcelStats]:
    """Compute Hansen-based land/forest area for parcel geometries.

    Expects each parcel to expose `parcel_id` and `geometry` attributes.

        Notes:
        - `parcel_crs` must match the CRS of `parcel.geometry`.
        - `all_touched=False` tends to reduce systematic boundary over-counting,
            and typically compares better to vector/cadastral areas.
        - When `reproject_to_projected=True` and the tile CRS is geographic,
            tiles are reprojected once to `projected_crs` for constant pixel area.
            This is an approximation intended for AOIs under ~50,000 ha.
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
            hansen_forest_loss_ha=0.0,
        )
        for p in parcel_list
    }

    parcel_geometry_cache: dict[
        str,
        list[tuple[object, dict[str, Any], tuple[float, float, float, float]]],
    ] = {}

    def _parcel_entries_for_crs(
        target_crs: CRS | None,
    ) -> list[tuple[object, dict[str, Any], tuple[float, float, float, float]]]:
        cache_key = _crs_cache_key(target_crs)
        cached = parcel_geometry_cache.get(cache_key)
        if cached is not None:
            return cached

        entries: list[tuple[object, dict[str, Any], tuple[float, float, float, float]]] = []
        for parcel in parcel_list:
            source_geom = getattr(parcel, "geometry", None)
            if source_geom is None:
                continue
            if target_crs is None:
                transformed_geom = source_geom
            else:
                transformed_geom = transform_geom(parcel_crs, target_crs, source_geom)
            entries.append((parcel, transformed_geom, shape(transformed_geom).bounds))
        parcel_geometry_cache[cache_key] = entries
        return entries

    tile_source = LocalTileSource(tile_dir)
    parcel_bounds_wgs84 = [shape(getattr(p, "geometry")).bounds for p in parcel_list]
    bbox_wgs84 = (
        min(bounds[0] for bounds in parcel_bounds_wgs84),
        min(bounds[1] for bounds in parcel_bounds_wgs84),
        max(bounds[2] for bounds in parcel_bounds_wgs84),
        max(bounds[3] for bounds in parcel_bounds_wgs84),
    )
    treecover_tiles = tile_source.list_layer_files("treecover2000")
    lossyear_tiles = tile_source.list_layer_files("lossyear")
    treecover_tiles = _filter_tiles_by_bbox(treecover_tiles, bbox_wgs84=bbox_wgs84)
    lossyear_tiles = _filter_tiles_by_bbox(lossyear_tiles, bbox_wgs84=bbox_wgs84)
    pairs = _pair_tiles(treecover_tiles, lossyear_tiles)

    for tree_path, loss_path in pairs:
        with rasterio.open(tree_path) as tree_ds, rasterio.open(loss_path) as loss_ds:
            tree_bounds = (
                float(tree_ds.bounds.left),
                float(tree_ds.bounds.bottom),
                float(tree_ds.bounds.right),
                float(tree_ds.bounds.top),
            )
            parcel_entries = [
                entry
                for entry in _parcel_entries_for_crs(tree_ds.crs)
                if _bounds_intersect(entry[2], tree_bounds)
            ]
            if not parcel_entries:
                continue

            parcel_minx = min(entry[2][0] for entry in parcel_entries)
            parcel_miny = min(entry[2][1] for entry in parcel_entries)
            parcel_maxx = max(entry[2][2] for entry in parcel_entries)
            parcel_maxy = max(entry[2][3] for entry in parcel_entries)

            crop_bounds = (
                max(tree_bounds[0], parcel_minx),
                max(tree_bounds[1], parcel_miny),
                min(tree_bounds[2], parcel_maxx),
                min(tree_bounds[3], parcel_maxy),
            )
            if crop_bounds[0] >= crop_bounds[2] or crop_bounds[1] >= crop_bounds[3]:
                continue

            read_window = from_bounds(*crop_bounds, transform=tree_ds.transform)
            full_window = Window(col_off=0, row_off=0, width=tree_ds.width, height=tree_ds.height)
            read_window = read_window.intersection(full_window).round_offsets().round_lengths()
            if read_window.width <= 0 or read_window.height <= 0:
                continue

            tree_band = tree_ds.read(1, window=read_window, masked=True)
            loss_band = loss_ds.read(1, window=read_window, masked=True)

            if tree_band.shape != loss_band.shape:
                raise RuntimeError("Mismatched raster shapes for treecover2000 and lossyear")

            valid = (~tree_band.mask) & (~loss_band.mask)
            tree_values = np.ma.filled(tree_band, 0)
            loss_values = np.ma.filled(loss_band, 0)

            active_crs = tree_ds.crs
            active_transform = window_transform(read_window, tree_ds.transform)
            if reproject_to_projected and active_crs is not None and active_crs.is_geographic:
                tree_values, loss_values, valid, active_transform, active_crs = _reproject_to_projected(
                    tree_values=tree_values,
                    loss_values=loss_values,
                    valid_mask=valid,
                    source_transform=active_transform,
                    source_crs=active_crs,
                    target_crs=projected_crs,
                )

            west, south, east, north = array_bounds(
                tree_values.shape[0],
                tree_values.shape[1],
                active_transform,
            )
            active_bounds = (float(west), float(south), float(east), float(north))
            parcel_entries = [
                entry
                for entry in _parcel_entries_for_crs(active_crs)
                if _bounds_intersect(entry[2], active_bounds)
            ]
            if not parcel_entries:
                continue

            forest_end_mask = forest_mask_end_year(
                tree_values,
                loss_values,
                canopy_threshold_percent,
                end_year,
            ) & valid
            cutoff_code = max(cutoff_year - 2000, 0)
            forest_loss_mask = (
                (tree_values >= canopy_threshold_percent)
                & (loss_values > cutoff_code)
                & valid
            )

            pixel_area_m2 = pixel_area_m2_raster(
                active_transform,
                height=tree_values.shape[0],
                width=tree_values.shape[1],
                crs=active_crs,
            )

            for parcel, parcel_geom, _ in parcel_entries:
                zone_mask = rasterize_zone_mask(
                    parcel_geom,
                    out_shape=tree_values.shape,
                    transform=active_transform,
                    all_touched=all_touched,
                )

                zone_valid = zone_mask & valid
                if not np.any(zone_valid):
                    continue

                land_area_ha = _sum_area_m2(zone_valid, pixel_area_m2) / 10_000.0
                forest_area_ha = (
                    _sum_area_m2(forest_end_mask & zone_mask, pixel_area_m2) / 10_000.0
                )
                forest_loss_ha = (
                    _sum_area_m2(forest_loss_mask & zone_mask, pixel_area_m2) / 10_000.0
                )

                current = stats[parcel.parcel_id]
                stats[parcel.parcel_id] = HansenParcelStats(
                    parcel_id=parcel.parcel_id,
                    hansen_land_area_ha=current.hansen_land_area_ha + land_area_ha,
                    hansen_forest_area_ha=current.hansen_forest_area_ha + forest_area_ha,
                    hansen_forest_loss_ha=current.hansen_forest_loss_ha + forest_loss_ha,
                )

    return stats
