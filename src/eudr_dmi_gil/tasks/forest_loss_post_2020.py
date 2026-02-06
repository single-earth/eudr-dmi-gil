from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import rasterio
from pyproj import Geod
from rasterio.errors import RasterioIOError
from rasterio.mask import mask as rio_mask
from rasterio.warp import transform_geom
from shapely.geometry import shape, mapping
from shapely.ops import unary_union

from eudr_dmi_gil.reports.determinism import sha256_file, write_json


@dataclass(frozen=True)
class HansenConfig:
    tile_dir: Path
    canopy_threshold_percent: int = 30
    cutoff_year: int = 2020
    write_masks: bool = False
    dataset_version: str = "unknown"
    tile_source: str = "local"


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


def _mask_raster(dataset: rasterio.io.DatasetReader, geom: dict[str, Any]) -> tuple[np.ma.MaskedArray, Any]:
    geom_crs = dataset.crs
    if geom_crs is None:
        raise RuntimeError("Raster dataset has no CRS")

    geom_in_crs = transform_geom("EPSG:4326", geom_crs, geom)
    try:
        data, transform = rio_mask(dataset, [geom_in_crs], crop=True, filled=False)
    except ValueError:
        # No overlap between AOI and raster
        # Match rasterio.mask.mask shape: (bands, rows, cols)
        return np.ma.masked_all((1, 1, 1)), dataset.transform
    return data, transform


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


def _compute_area_ha(dataset: rasterio.io.DatasetReader, transform: Any, mask: np.ndarray) -> float:
    if dataset.crs and dataset.crs.is_projected:
        return float(mask.sum()) * _pixel_area_ha_projected(transform)
    return _pixel_area_ha_geographic(transform, mask)


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
) -> ForestLossResult:
    tile_source = LocalTileSource(config.tile_dir)

    treecover_tiles = tile_source.list_layer_files("treecover2000")
    lossyear_tiles = tile_source.list_layer_files("lossyear")
    pairs = _pair_tiles(treecover_tiles, lossyear_tiles)

    geom = _load_aoi_geometry(aoi_geojson_path)

    forest_loss_ha = 0.0
    initial_cover_ha = 0.0
    current_cover_ha = 0.0

    provenance: list[TileProvenance] = []
    loss_features: list[dict[str, Any]] = []
    current_features: list[dict[str, Any]] = []

    cutoff_threshold = max(config.cutoff_year - 2000, 0)

    for tree_path, loss_path in pairs:
        try:
            with rasterio.open(tree_path) as tree_ds, rasterio.open(loss_path) as loss_ds:
                tree_data, tree_transform = _mask_raster(tree_ds, geom)
                loss_data, loss_transform = _mask_raster(loss_ds, geom)

                if tree_data.shape != loss_data.shape:
                    raise RuntimeError("Mismatched raster shapes for treecover2000 and lossyear")

                tree_band = tree_data[0]
                loss_band = loss_data[0]
                valid = (~tree_band.mask) & (~loss_band.mask)

                tree_values = np.ma.filled(tree_band, 0)
                loss_values = np.ma.filled(loss_band, 0)

                baseline = valid & (tree_values >= config.canopy_threshold_percent)
                loss_post_2020 = baseline & (loss_values > cutoff_threshold)
                current_cover = baseline & (loss_values == 0)

                area_loss = _compute_area_ha(tree_ds, tree_transform, loss_post_2020)
                area_initial = _compute_area_ha(tree_ds, tree_transform, baseline)
                area_current = _compute_area_ha(tree_ds, tree_transform, current_cover)

                forest_loss_ha += area_loss
                initial_cover_ha += area_initial
                current_cover_ha += area_current

                if config.write_masks:
                    loss_features.extend(
                        _mask_features(loss_post_2020, tree_transform, tree_ds.crs)
                    )
                    current_features.extend(
                        _mask_features(current_cover, tree_transform, tree_ds.crs)
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

    output_dir.mkdir(parents=True, exist_ok=True)

    loss_mask_path = output_dir / "forest_loss_post_2020_mask.geojson"
    current_mask_path = output_dir / "forest_current_tree_cover_mask.geojson"
    if config.write_masks:
        _write_mask_geojson(loss_mask_path, loss_features)
        _write_mask_geojson(current_mask_path, current_features)

    summary = {
        "cutoff_year": config.cutoff_year,
        "canopy_threshold_percent": config.canopy_threshold_percent,
        "pixel_forest_loss_post_2020_ha": round(forest_loss_ha, 6),
        "pixel_initial_tree_cover_ha": round(initial_cover_ha, 6),
        "pixel_current_tree_cover_ha": round(current_cover_ha, 6),
        "mask_forest_loss_post_2020": loss_mask_path.name,
        "mask_forest_current_year": current_mask_path.name,
        "tiles": [
            {"layer": p.layer, "path": p.relpath, "sha256": p.sha256}
            for p in sorted(provenance, key=lambda p: (p.layer, p.relpath))
        ],
    }

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
    )


def load_hansen_config(
    *,
    tile_dir: Path | None,
    canopy_threshold_percent: int,
    cutoff_year: int,
    write_masks: bool = False,
) -> HansenConfig:
    if tile_dir is None:
        env = os.environ.get("EUDR_DMI_HANSEN_TILE_DIR")
        if not env:
            raise RuntimeError("EUDR_DMI_HANSEN_TILE_DIR must be set for Hansen processing")
        tile_dir = Path(env)
    dataset_version = os.environ.get("EUDR_DMI_HANSEN_DATASET_VERSION", "unknown")
    tile_source = os.environ.get("EUDR_DMI_HANSEN_TILE_SOURCE", "local")
    return HansenConfig(
        tile_dir=tile_dir,
        canopy_threshold_percent=canopy_threshold_percent,
        cutoff_year=cutoff_year,
        write_masks=write_masks,
        dataset_version=dataset_version,
        tile_source=tile_source,
    )
