from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.crs import CRS
from pyproj import Geod
from rasterio.enums import Resampling
from rasterio.errors import RasterioIOError
from rasterio.mask import mask as rio_mask
from rasterio.transform import array_bounds
from rasterio.warp import calculate_default_transform, reproject, transform_geom
from shapely.geometry import mapping, shape
from shapely.ops import unary_union

from eudr_dmi_gil.deps.hansen_acquire import (
    DATASET_VERSION_DEFAULT,
    HansenLayerEntry,
    ensure_hansen_layers_present,
    hansen_default_base_dir,
    infer_hansen_latest_year,
    resolve_hansen_url_template,
)
from eudr_dmi_gil.deps.hansen_bootstrap import ensure_hansen_for_aoi
from eudr_dmi_gil.deps.hansen_tiles import hansen_tile_ids_for_bbox, load_aoi_bbox
from eudr_dmi_gil.geo.forest_area_core import (
    forest_2024_mask,
    forest_mask_end_year,
    loss_2021_2024_mask,
    loss_mask_range,
    loss_total_mask,
    pixel_area_m2_raster,
    rasterize_zone_mask,
    rfm_mask,
    zonal_area_ha,
)
from eudr_dmi_gil.reports.determinism import sha256_file, write_json


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class HansenConfig:
    tile_dir: Path
    canopy_threshold_percent: int = 10
    cutoff_year: int = 2020
    write_masks: bool = False
    dataset_version: str = "unknown"
    tile_source: str = "local"
    tile_entries: list[HansenLayerEntry] | None = None
    tile_ids: list[str] | None = None
    url_template: str = ""
    minio_cache_enabled: bool = False
    reproject_to_projected: bool = True
    projected_crs: str | int = "EPSG:6933"


@dataclass(frozen=True)
class TileProvenance:
    layer: str
    relpath: str
    sha256: str


@dataclass(frozen=True)
class ForestLossResult:
    summary_path: Path
    tile_provenance: list[TileProvenance]
    forest_loss_post_2020_ha: float
    initial_tree_cover_ha: float
    current_tree_cover_ha: float
    mask_forest_loss_post_2020_path: Path
    mask_forest_current_path: Path
    mask_forest_2000_path: Path
    mask_forest_end_year_path: Path
    forest_mask_debug_path: Path
    forest_metrics: "ForestMetrics"
    forest_metrics_params: "ForestMetricsParams"
    forest_metrics_debug: "ForestMetricsDebug"


@dataclass(frozen=True)
class ForestMetrics:
    canopy_threshold_pct: int
    reference_forest_mask_year: int
    loss_year_code_basis: int
    end_year: int
    rfm_area_ha: float
    forest_end_year_area_ha: float
    loss_total_2001_2024_ha: float
    loss_2021_2024_ha: float
    loss_2021_2024_pct_of_rfm: float
    loss_total_ha: float
    forest_2024_ha: float
    forest_end_year_ha: float


@dataclass(frozen=True)
class ForestMetricsParams:
    canopy_threshold_pct: int
    start_year: int
    end_year: int
    crs: str
    method_area: str
    method_zonal: str
    method_notes: str
    loss_year_code_basis: int


@dataclass(frozen=True)
class ForestMetricsDebug:
    raster_shapes: list[tuple[int, int]]
    pixel_area_m2_min: float
    pixel_area_m2_max: float
    pixel_area_m2_mean: float
    rfm_true_pixels: int
    loss_21_24_true_pixels: int
    forest_end_year_true_pixels: int
    rfm_area_ha: float
    loss_total_2001_2024_ha: float
    loss_2021_2024_ha: float
    forest_end_year_area_ha: float
    loss_total_ha: float
    forest_2024_ha: float
    forest_end_year_ha: float


class TileSource:
    def list_layer_files(self, layer: str) -> list[Path]:
        raise NotImplementedError

    def tile_relpath(self, path: Path) -> str:
        raise NotImplementedError


class LocalTileSource(TileSource):
    def __init__(self, tile_dir: Path) -> None:
        self._tile_dir = tile_dir

    def list_layer_files(self, layer: str) -> list[Path]:
        if not self._tile_dir.exists():
            return []

        layer_dir = self._tile_dir / layer
        candidates: list[Path] = []
        if layer_dir.is_dir():
            candidates.extend(sorted(layer_dir.glob("*.tif")))
        else:
            direct = self._tile_dir / f"{layer}.tif"
            if direct.is_file():
                candidates.append(direct)
            candidates.extend(sorted(self._tile_dir.glob(f"{layer}_*.tif")))
            candidates.extend(sorted(self._tile_dir.glob(f"{layer}-*.tif")))
            candidates.extend(sorted(self._tile_dir.glob(f"**/{layer}.tif")))
        return sorted(set(candidates))

    def tile_relpath(self, path: Path) -> str:
        try:
            return path.relative_to(self._tile_dir).as_posix()
        except ValueError:
            return path.as_posix()


def _load_aoi_geometry(aoi_geojson_path: Path) -> dict[str, Any]:
    data = json.loads(aoi_geojson_path.read_text(encoding="utf-8"))
    if data.get("type") == "FeatureCollection":
        geometries = [shape(feat["geometry"]) for feat in data.get("features", [])]
        if not geometries:
            raise ValueError("AOI GeoJSON FeatureCollection has no features")
        geom = unary_union(geometries)
        return mapping(geom)
    if data.get("type") == "Feature":
        return data["geometry"]
    if "type" in data:
        return data
    raise ValueError("Unsupported AOI GeoJSON")


def _pair_tiles(treecover_tiles: list[Path], lossyear_tiles: list[Path]) -> list[tuple[Path, Path]]:
    if not treecover_tiles or not lossyear_tiles:
        raise RuntimeError("Missing required Hansen tiles (treecover2000/lossyear)")

    if len(treecover_tiles) == 1 and len(lossyear_tiles) == 1:
        return [(treecover_tiles[0], lossyear_tiles[0])]

    tree_names = {p.name for p in treecover_tiles}
    loss_names = {p.name for p in lossyear_tiles}

    if len(tree_names) == len(treecover_tiles) and len(loss_names) == len(lossyear_tiles):
        lossyear_by_name = {p.name: p for p in lossyear_tiles}
        pairs: list[tuple[Path, Path]] = []
        for tree_path in treecover_tiles:
            match = lossyear_by_name.get(tree_path.name)
            if match is None:
                raise RuntimeError(
                    f"No matching lossyear tile for treecover2000 tile: {tree_path.name}"
                )
            pairs.append((tree_path, match))
        return pairs

    lossyear_by_parent = {p.parent.name: p for p in lossyear_tiles}
    pairs = []
    for tree_path in treecover_tiles:
        match = lossyear_by_parent.get(tree_path.parent.name)
        if match is None:
            raise RuntimeError(
                f"No matching lossyear tile for treecover2000 tile in {tree_path.parent.name}"
            )
        pairs.append((tree_path, match))
    return pairs


def _mask_raster(
    dataset: rasterio.io.DatasetReader,
    geom: dict[str, Any],
) -> tuple[np.ma.MaskedArray, Any]:
    geom_crs = dataset.crs
    if geom_crs is None:
        raise RuntimeError("Raster dataset has no CRS")

    geom_in_crs = transform_geom("EPSG:4326", geom_crs, geom)
    try:
        data, transform = rio_mask(dataset, [geom_in_crs], crop=True, filled=False)
    except ValueError:
        return np.ma.masked_all((1, 1, 1)), dataset.transform
    return data, transform


def _extract_loss_band(dataset: rasterio.io.DatasetReader) -> np.ma.MaskedArray | None:
    if dataset.count <= 1:
        return None
    descriptions = list(dataset.descriptions or [])
    for idx, desc in enumerate(descriptions, start=1):
        if desc and "loss" in str(desc).lower():
            return dataset.read(idx, masked=True)
    return None


def _warn_loss_consistency(lossyear_values: np.ndarray, loss_values: np.ndarray, valid: np.ndarray) -> None:
    if loss_values.size == 0:
        return
    lossyear_positive = lossyear_values > 0
    loss_positive = loss_values > 0
    mismatch_lossyear_positive_loss_zero = np.count_nonzero(
        lossyear_positive & ~loss_positive & valid
    )
    mismatch_lossyear_zero_loss_one = np.count_nonzero(
        (~lossyear_positive) & loss_positive & valid
    )
    if mismatch_lossyear_positive_loss_zero or mismatch_lossyear_zero_loss_one:
        LOGGER.warning(
            "Loss/losyear mismatch: lossyear>0 & loss==0: %s; lossyear==0 & loss==1: %s",
            int(mismatch_lossyear_positive_loss_zero),
            int(mismatch_lossyear_zero_loss_one),
        )


def _pixel_area_ha_projected(transform: Any) -> float:
    return abs(transform.a * transform.e) / 10000.0


def _pixel_area_ha_geographic(transform: Any, mask: np.ndarray) -> float:
    geod = Geod(ellps="WGS84")
    rows, cols = np.where(mask)
    if rows.size == 0:
        return 0.0
    total_area_m2 = 0.0
    for row, col in zip(rows.tolist(), cols.tolist()):
        x0, y0 = transform * (col, row)
        x1, y1 = transform * (col + 1, row + 1)
        lons = [x0, x1, x1, x0]
        lats = [y0, y0, y1, y1]
        area, _ = geod.polygon_area_perimeter(lons, lats)
        total_area_m2 += abs(area)
    return total_area_m2 / 10000.0


def _compute_area_ha(crs: Any, transform: Any, mask: np.ndarray) -> float:
    if crs is not None and getattr(crs, "is_projected", False):
        return float(mask.sum()) * _pixel_area_ha_projected(transform)
    return _pixel_area_ha_geographic(transform, mask)


def _entries_from_manifest(manifest_path: Path) -> tuple[list[str] | None, list[HansenLayerEntry]]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries_raw = payload.get("entries", [])
    entries: list[HansenLayerEntry] = []
    for entry in entries_raw:
        entries.append(
            HansenLayerEntry(
                tile_id=str(entry.get("tile_id", "")),
                layer=str(entry.get("layer", "")),
                local_path=str(entry.get("local_path", "")),
                sha256=str(entry.get("sha256", "")),
                size_bytes=int(entry.get("size_bytes", 0) or 0),
                source_url=str(entry.get("source_url", "")),
                status=str(entry.get("status", "")),
            )
        )
    tile_ids = payload.get("tile_ids")
    if isinstance(tile_ids, list):
        tile_ids = [str(tile_id) for tile_id in tile_ids]
    else:
        tile_ids = None
    return tile_ids, entries


def _reproject_to_projected(
    *,
    tree_ds: rasterio.io.DatasetReader,
    loss_ds: rasterio.io.DatasetReader,
    tree_values: np.ndarray,
    loss_values: np.ndarray,
    tree_mask: np.ndarray,
    loss_mask: np.ndarray,
    src_transform: rasterio.Affine,
    src_crs: CRS,
    target_crs: str | int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, rasterio.Affine, object]:
    if src_crs is None:
        return tree_values, loss_values, tree_mask, loss_mask, src_transform, src_crs

    def _attempt_reproject(
        target: CRS,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, rasterio.Affine, CRS] | None:
        if target == src_crs:
            return None

        src_height = int(tree_values.shape[0])
        src_width = int(tree_values.shape[1])
        src_bounds = array_bounds(src_height, src_width, src_transform)

        dst_transform, dst_width, dst_height = calculate_default_transform(
            src_crs,
            target,
            src_width,
            src_height,
            *src_bounds,
        )
        if dst_width <= 0 or dst_height <= 0:
            return None

        dst_tree = np.zeros((dst_height, dst_width), dtype=tree_values.dtype)
        dst_loss = np.zeros((dst_height, dst_width), dtype=loss_values.dtype)
        dst_tree_mask = np.zeros((dst_height, dst_width), dtype=np.uint8)
        dst_loss_mask = np.zeros((dst_height, dst_width), dtype=np.uint8)

        reproject(
            source=tree_values,
            destination=dst_tree,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=dst_transform,
            dst_crs=target,
            resampling=Resampling.nearest,
        )
        reproject(
            source=loss_values,
            destination=dst_loss,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=dst_transform,
            dst_crs=target,
            resampling=Resampling.nearest,
        )
        reproject(
            source=tree_mask.astype(np.uint8),
            destination=dst_tree_mask,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=dst_transform,
            dst_crs=target,
            resampling=Resampling.nearest,
        )
        reproject(
            source=loss_mask.astype(np.uint8),
            destination=dst_loss_mask,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=dst_transform,
            dst_crs=target,
            resampling=Resampling.nearest,
        )

        return (
            dst_tree,
            dst_loss,
            dst_tree_mask.astype(bool),
            dst_loss_mask.astype(bool),
            dst_transform,
            target,
        )

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
        return tree_values, loss_values, tree_mask, loss_mask, src_transform, src_crs

    return result


def _mask_features(mask: np.ndarray, transform: Any, crs: Any) -> list[dict[str, Any]]:
    from rasterio.features import shapes

    shapes_iter = shapes(mask.astype(np.uint8), mask=mask, transform=transform)
    features: list[dict[str, Any]] = []
    for geom, value in shapes_iter:
        if value != 1:
            continue
        geom_wgs84 = transform_geom(crs, "EPSG:4326", geom)
        features.append({"type": "Feature", "properties": {}, "geometry": geom_wgs84})
    return features


def _write_mask_geojson(path: Path, features: list[dict[str, Any]]) -> None:
    ordered = sorted(
        features, key=lambda f: json.dumps(f.get("geometry"), sort_keys=True, ensure_ascii=False)
    )
    write_json(path, {"type": "FeatureCollection", "features": ordered})


def compute_forest_loss_post_2020(
    *,
    aoi_geojson_path: Path,
    output_dir: Path,
    config: HansenConfig,
    zone_geom_wgs84: dict[str, Any] | None = None,
    parcel_ids: list[str] | None = None,
) -> ForestLossResult:
    tile_source = LocalTileSource(config.tile_dir)

    treecover_tiles = tile_source.list_layer_files("treecover2000")
    lossyear_tiles = tile_source.list_layer_files("lossyear")
    pairs = _pair_tiles(treecover_tiles, lossyear_tiles)

    geom = _load_aoi_geometry(aoi_geojson_path)
    aoi_shape = shape(geom)
    if zone_geom_wgs84 is None:
        zone_shape = aoi_shape
    else:
        zone_shape = shape(zone_geom_wgs84)
        zone_shape = zone_shape.intersection(aoi_shape)

    forest_loss_ha = 0.0
    initial_cover_ha = 0.0
    current_cover_ha = 0.0
    current_forest_true_pixels = 0
    loss_post_2020_true_pixels = 0
    tree_nodata_pixels = 0
    lossyear_nodata_pixels = 0

    end_year = infer_hansen_latest_year(
        dataset_version=config.dataset_version,
        tile_dir=config.tile_dir,
    )
    rfm_area_ha = np.float64(0.0)
    loss_total_2001_2024_ha = np.float64(0.0)
    loss_2021_2024_ha = np.float64(0.0)
    forest_end_year_area_ha = np.float64(0.0)
    forest_2024_area_ha = np.float64(0.0)

    raster_shapes: list[tuple[int, int]] = []
    pixel_area_sum = np.float64(0.0)
    pixel_area_count = 0
    pixel_area_min: float | None = None
    pixel_area_max: float | None = None
    rfm_true_pixels = 0
    loss_21_24_true_pixels = 0
    forest_end_year_true_pixels = 0
    crs_values: list[str] = []

    provenance: list[TileProvenance] = []
    loss_features: list[dict[str, Any]] = []
    current_features: list[dict[str, Any]] = []
    baseline_features: list[dict[str, Any]] = []
    end_year_features: list[dict[str, Any]] = []

    cutoff_threshold = max(config.cutoff_year - 2000, 0)
    used_projected = False

    for tree_path, loss_path in pairs:
        try:
            with rasterio.open(tree_path) as tree_ds, rasterio.open(loss_path) as loss_ds:
                tree_data, tree_transform = _mask_raster(tree_ds, geom)
                loss_data, _ = _mask_raster(loss_ds, geom)

                if tree_data.shape != loss_data.shape:
                    raise RuntimeError("Mismatched raster shapes for treecover2000 and lossyear")

                tree_band = tree_data[0]
                loss_band = loss_data[0]
                tree_mask = np.ma.getmaskarray(tree_band)
                loss_mask = np.ma.getmaskarray(loss_band)
                valid_original = (~tree_mask) & (~loss_mask)

                loss_band_optional = _extract_loss_band(loss_ds)
                if loss_band_optional is not None:
                    loss_optional_values = np.ma.filled(loss_band_optional, 0)
                    _warn_loss_consistency(
                        np.ma.filled(loss_band, 0),
                        loss_optional_values,
                        valid_original & (~loss_band_optional.mask),
                    )

                tree_values = np.ma.filled(tree_band, 0)
                loss_values = np.ma.filled(loss_band, 0)
                active_transform = tree_transform
                active_crs = tree_ds.crs
                if (
                    config.reproject_to_projected
                    and active_crs is not None
                    and active_crs.is_geographic
                ):
                    (
                        tree_values,
                        loss_values,
                        tree_mask,
                        loss_mask,
                        active_transform,
                        active_crs,
                    ) = _reproject_to_projected(
                        tree_ds=tree_ds,
                        loss_ds=loss_ds,
                        tree_values=tree_values,
                        loss_values=loss_values,
                        tree_mask=tree_mask,
                        loss_mask=loss_mask,
                        src_transform=active_transform,
                        src_crs=active_crs,
                        target_crs=config.projected_crs,
                    )
                    used_projected = True

                valid = (~tree_mask) & (~loss_mask)
                raster_shapes.append((int(tree_values.shape[0]), int(tree_values.shape[1])))
                if active_crs:
                    crs_values.append(active_crs.to_string())

                rfm = rfm_mask(tree_values, config.canopy_threshold_percent)
                baseline = valid & rfm
                loss_post_2020 = baseline & (loss_values > cutoff_threshold)
                current_cover = baseline & (loss_values == 0)

                if zone_shape is None or zone_shape.is_empty:
                    zone_mask = np.zeros(tree_values.shape, dtype=bool)
                else:
                    zone_geom = mapping(zone_shape)
                    zone_in_crs = transform_geom("EPSG:4326", active_crs, zone_geom)
                    zone_mask = rasterize_zone_mask(
                        zone_in_crs,
                        out_shape=tree_values.shape,
                        transform=active_transform,
                        all_touched=True,
                    )

                pixel_area_m2 = pixel_area_m2_raster(
                    active_transform,
                    height=tree_values.shape[0],
                    width=tree_values.shape[1],
                    crs=active_crs,
                )
                rfm_zone_mask = rfm & valid
                loss_total_mask_bool = loss_total_mask(
                    tree_values,
                    loss_values,
                    config.canopy_threshold_percent,
                ) & valid
                loss_recent_mask = loss_mask_range(
                    tree_values,
                    loss_values,
                    config.canopy_threshold_percent,
                    2021,
                    end_year,
                ) & valid
                forest_2024_mask_bool = forest_2024_mask(
                    tree_values,
                    loss_values,
                    config.canopy_threshold_percent,
                ) & valid
                forest_end_mask = forest_mask_end_year(
                    tree_values,
                    loss_values,
                    config.canopy_threshold_percent,
                    end_year,
                ) & valid

                loss_post_2020_zone = loss_post_2020 & zone_mask
                current_cover_zone = current_cover & zone_mask
                baseline_zone = baseline & zone_mask

                zone_valid = zone_mask & valid
                if np.any(zone_valid):
                    pixel_vals = pixel_area_m2[zone_valid]
                    if pixel_vals.size:
                        vmin = float(np.min(pixel_vals))
                        vmax = float(np.max(pixel_vals))
                        vsum = np.sum(pixel_vals, dtype=np.float64)
                        pixel_area_sum += np.float64(vsum)
                        pixel_area_count += int(pixel_vals.size)
                        pixel_area_min = vmin if pixel_area_min is None else min(pixel_area_min, vmin)
                        pixel_area_max = vmax if pixel_area_max is None else max(pixel_area_max, vmax)

                rfm_true_pixels += int(np.count_nonzero(rfm_zone_mask & zone_mask))
                loss_21_24_true_pixels += int(np.count_nonzero(loss_recent_mask & zone_mask))
                forest_end_year_true_pixels += int(
                    np.count_nonzero(forest_end_mask & zone_mask)
                )
                current_forest_true_pixels += int(
                    np.count_nonzero(current_cover_zone & zone_mask)
                )
                loss_post_2020_true_pixels += int(
                    np.count_nonzero(loss_post_2020_zone & zone_mask)
                )
                tree_nodata_pixels += int(np.count_nonzero(tree_mask & zone_mask))
                lossyear_nodata_pixels += int(np.count_nonzero(loss_mask & zone_mask))

                rfm_area_ha += np.float64(zonal_area_ha(rfm_zone_mask, pixel_area_m2, zone_mask))
                loss_total_2001_2024_ha += np.float64(
                    zonal_area_ha(loss_total_mask_bool, pixel_area_m2, zone_mask)
                )
                loss_2021_2024_ha += np.float64(
                    zonal_area_ha(loss_recent_mask, pixel_area_m2, zone_mask)
                )
                forest_end_year_area_ha += np.float64(
                    zonal_area_ha(forest_end_mask, pixel_area_m2, zone_mask)
                )
                forest_2024_area_ha += np.float64(
                    zonal_area_ha(forest_2024_mask_bool, pixel_area_m2, zone_mask)
                )

                area_loss = _compute_area_ha(active_crs, active_transform, loss_post_2020_zone)
                area_initial = _compute_area_ha(active_crs, active_transform, baseline_zone)
                area_current = _compute_area_ha(active_crs, active_transform, current_cover_zone)

                forest_loss_ha += area_loss
                initial_cover_ha += area_initial
                current_cover_ha += area_current

                if config.write_masks:
                    loss_features.extend(
                        _mask_features(loss_post_2020_zone, active_transform, active_crs)
                    )
                    current_features.extend(
                        _mask_features(current_cover_zone, active_transform, active_crs)
                    )
                    baseline_features.extend(
                        _mask_features(baseline_zone, active_transform, active_crs)
                    )
                    end_year_features.extend(
                        _mask_features(forest_end_mask & zone_mask, active_transform, active_crs)
                    )
        except RasterioIOError as exc:
            raise RuntimeError(f"Failed to read Hansen tile: {exc}") from exc

        provenance.append(
            TileProvenance(
                layer="treecover2000",
                relpath=tile_source.tile_relpath(tree_path),
                sha256=sha256_file(tree_path),
            )
        )
        provenance.append(
            TileProvenance(
                layer="lossyear",
                relpath=tile_source.tile_relpath(loss_path),
                sha256=sha256_file(loss_path),
            )
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    loss_mask_path = output_dir / "forest_loss_post_2020_mask.geojson"
    current_mask_path = output_dir / "forest_current_tree_cover_mask.geojson"
    forest_2000_mask_path = output_dir / "forest_2000_tree_cover_mask.geojson"
    forest_end_year_mask_path = output_dir / "forest_end_year_tree_cover_mask.geojson"
    if config.write_masks:
        _write_mask_geojson(loss_mask_path, loss_features)
        _write_mask_geojson(current_mask_path, current_features)
        _write_mask_geojson(forest_2000_mask_path, baseline_features)
        _write_mask_geojson(forest_end_year_mask_path, end_year_features)

    debug_path = output_dir / "forest_mask_debug.json"
    write_json(
        debug_path,
        {
            "canopy_threshold_percent": config.canopy_threshold_percent,
            "rfm_true_pixels": rfm_true_pixels,
            "current_forest_true_pixels": current_forest_true_pixels,
            "loss_post_2020_true_pixels": loss_post_2020_true_pixels,
            "nodata_pixels": {
                "treecover2000": tree_nodata_pixels,
                "lossyear": lossyear_nodata_pixels,
            },
            "parcel_count": len(parcel_ids or []),
            "parcel_ids": sorted(parcel_ids or []),
        },
    )

    summary = {
        "cutoff_year": config.cutoff_year,
        "canopy_threshold_percent": config.canopy_threshold_percent,
        "pixel_forest_loss_post_2020_ha": round(forest_loss_ha, 6),
        "pixel_initial_tree_cover_ha": round(initial_cover_ha, 6),
        "pixel_current_tree_cover_ha": round(current_cover_ha, 6),
        "mask_forest_loss_post_2020": loss_mask_path.name,
        "mask_forest_current_year": current_mask_path.name,
        "mask_forest_2000": forest_2000_mask_path.name,
        "mask_forest_end_year": forest_end_year_mask_path.name,
        "tiles": [
            {"layer": p.layer, "path": p.relpath, "sha256": p.sha256}
            for p in sorted(provenance, key=lambda p: (p.layer, p.relpath))
        ],
    }

    loss_2021_2024_pct_of_rfm = (
        float(loss_2021_2024_ha) / float(rfm_area_ha) * 100.0
        if float(rfm_area_ha) > 0.0
        else 0.0
    )
    forest_metrics = ForestMetrics(
        canopy_threshold_pct=config.canopy_threshold_percent,
        reference_forest_mask_year=2000,
        loss_year_code_basis=2000,
        end_year=end_year,
        rfm_area_ha=float(rfm_area_ha),
        forest_end_year_area_ha=float(forest_end_year_area_ha),
        loss_total_2001_2024_ha=float(loss_total_2001_2024_ha),
        loss_2021_2024_ha=float(loss_2021_2024_ha),
        loss_2021_2024_pct_of_rfm=float(loss_2021_2024_pct_of_rfm),
        loss_total_ha=float(loss_total_2001_2024_ha),
        forest_2024_ha=float(forest_2024_area_ha),
        forest_end_year_ha=float(forest_end_year_area_ha),
    )

    crs_used = sorted(set([c for c in crs_values if c]))
    method_area = "projected_constant_pixel_area" if used_projected else "geodesic_pixel_area_wgs84"
    method_notes = (
        "area_ha = sum(mask) * pixel_area_ha (projected; approx for AOI < 50k ha)"
        if used_projected
        else "area_ha = sum(pixel_area_m2 * mask * zone_mask)/10000"
    )
    forest_metrics_params = ForestMetricsParams(
        canopy_threshold_pct=config.canopy_threshold_percent,
        start_year=2001,
        end_year=end_year,
        crs=crs_used[0] if crs_used else "",
        method_area=method_area,
        method_zonal="rasterize_polygon_all_touched",
        method_notes=method_notes,
        loss_year_code_basis=2000,
    )
    pixel_area_mean = float(pixel_area_sum) / float(pixel_area_count) if pixel_area_count else 0.0
    forest_metrics_debug = ForestMetricsDebug(
        raster_shapes=raster_shapes,
        pixel_area_m2_min=float(pixel_area_min) if pixel_area_min is not None else 0.0,
        pixel_area_m2_max=float(pixel_area_max) if pixel_area_max is not None else 0.0,
        pixel_area_m2_mean=pixel_area_mean,
        rfm_true_pixels=rfm_true_pixels,
        loss_21_24_true_pixels=loss_21_24_true_pixels,
        forest_end_year_true_pixels=forest_end_year_true_pixels,
        rfm_area_ha=float(rfm_area_ha),
        loss_total_2001_2024_ha=float(loss_total_2001_2024_ha),
        loss_2021_2024_ha=float(loss_2021_2024_ha),
        forest_end_year_area_ha=float(forest_end_year_area_ha),
        loss_total_ha=float(loss_total_2001_2024_ha),
        forest_2024_ha=float(forest_2024_area_ha),
        forest_end_year_ha=float(forest_end_year_area_ha),
    )

    summary_path = output_dir / "forest_loss_post_2020_summary.json"
    write_json(summary_path, summary)

    return ForestLossResult(
        summary_path=summary_path,
        tile_provenance=provenance,
        forest_loss_post_2020_ha=summary["pixel_forest_loss_post_2020_ha"],
        initial_tree_cover_ha=summary["pixel_initial_tree_cover_ha"],
        current_tree_cover_ha=summary["pixel_current_tree_cover_ha"],
        mask_forest_loss_post_2020_path=loss_mask_path,
        mask_forest_current_path=current_mask_path,
        mask_forest_2000_path=forest_2000_mask_path,
        mask_forest_end_year_path=forest_end_year_mask_path,
        forest_mask_debug_path=debug_path,
        forest_metrics=forest_metrics,
        forest_metrics_params=forest_metrics_params,
        forest_metrics_debug=forest_metrics_debug,
    )


def load_hansen_config(
    *,
    tile_dir: Path | None,
    canopy_threshold_percent: int,
    cutoff_year: int,
    write_masks: bool = False,
    aoi_geojson_path: Path | None = None,
    download: bool = True,
    minio_cache_enabled: bool = False,
    minio_offline: bool = False,
    reproject_to_projected: bool = True,
    projected_crs: str | int = "EPSG:6933",
) -> HansenConfig:
    tile_entries: list[HansenLayerEntry] | None = None
    tile_ids: list[str] | None = None
    tile_source = os.environ.get("EUDR_DMI_HANSEN_TILE_SOURCE", "").strip()

    if tile_dir is None:
        if aoi_geojson_path is None:
            raise RuntimeError("AOI GeoJSON is required to derive Hansen tile IDs")
        tile_dir = hansen_default_base_dir() / "tiles"
        if minio_cache_enabled:
            manifest_path = ensure_hansen_for_aoi(
                aoi_id=aoi_geojson_path.stem,
                aoi_geojson_path=aoi_geojson_path,
                layers=["treecover2000", "lossyear"],
                download=download,
                minio_cache_enabled=True,
                offline=minio_offline,
            )
            tile_ids, tile_entries = _entries_from_manifest(manifest_path)
        else:
            bbox = load_aoi_bbox(aoi_geojson_path)
            tile_ids = hansen_tile_ids_for_bbox(bbox)
            tile_entries = []
            for tile_id in tile_ids:
                tile_entries.extend(
                    ensure_hansen_layers_present(
                        tile_id,
                        ["treecover2000", "lossyear"],
                        download=download,
                    )
                )
    if not tile_source:
        tile_source = "minio-cache" if minio_cache_enabled else "local"

    dataset_version = os.environ.get(
        "EUDR_DMI_HANSEN_DATASET_VERSION", DATASET_VERSION_DEFAULT
    )
    url_template = resolve_hansen_url_template()
    return HansenConfig(
        tile_dir=tile_dir,
        canopy_threshold_percent=canopy_threshold_percent,
        cutoff_year=cutoff_year,
        write_masks=write_masks,
        dataset_version=dataset_version,
        tile_source=tile_source,
        tile_entries=tile_entries,
        tile_ids=tile_ids,
        url_template=url_template,
        minio_cache_enabled=minio_cache_enabled,
        reproject_to_projected=reproject_to_projected,
        projected_crs=projected_crs,
    )
