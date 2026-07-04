from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.mask import mask as rio_mask
from rasterio.warp import transform_geom
from shapely.geometry import mapping, shape
from shapely.ops import unary_union

from eudr_dmi_gil.geo.aoi_area import compute_aoi_geodesic_area_ha
from eudr_dmi_gil.geo.forest_area_core import pixel_area_m2_raster


@dataclass(frozen=True)
class Gfc2020BaselineMetrics:
    aoi_area_ha: float
    gfc2020_forest_area_ha: float
    gfc2020_forest_share: float
    area_method: str


def load_aoi_geometry(aoi_geojson_path: Path) -> dict[str, Any]:
    data = json.loads(aoi_geojson_path.read_text(encoding="utf-8"))
    if data.get("type") == "FeatureCollection":
        geoms = [shape(feat["geometry"]) for feat in data.get("features", []) if feat.get("geometry")]
        if not geoms:
            raise ValueError("AOI GeoJSON FeatureCollection has no features")
        return mapping(unary_union(geoms))
    if data.get("type") == "Feature":
        return data["geometry"]
    if data.get("type"):
        return data
    raise ValueError("Unsupported AOI GeoJSON")


def parse_forest_values(raw: str) -> tuple[int, ...]:
    values = []
    for item in raw.split(","):
        item = item.strip()
        if item:
            values.append(int(item))
    if not values:
        raise ValueError("At least one GFC2020 forest value is required")
    return tuple(sorted(set(values)))


def mask_raster_to_aoi(
    dataset: rasterio.io.DatasetReader,
    aoi_geom_wgs84: dict[str, Any],
) -> tuple[np.ma.MaskedArray, Any]:
    if dataset.crs is None:
        raise RuntimeError("Raster dataset has no CRS")
    geom = transform_geom("EPSG:4326", dataset.crs, aoi_geom_wgs84)
    try:
        data, transform = rio_mask(dataset, [geom], crop=True, filled=False, all_touched=True)
    except ValueError:
        return np.ma.masked_all((1, 1, 1)), dataset.transform
    return data, transform


def compute_gfc2020_baseline(
    *,
    aoi_geojson_path: Path,
    gfc2020_raster_path: Path,
    forest_values: tuple[int, ...] = (1,),
) -> Gfc2020BaselineMetrics:
    aoi_area_ha, aoi_method = compute_aoi_geodesic_area_ha(aoi_geojson_path)
    aoi_geom = load_aoi_geometry(aoi_geojson_path)

    with rasterio.open(gfc2020_raster_path) as ds:
        data, transform = mask_raster_to_aoi(ds, aoi_geom)
        band = data[0]
        values = np.ma.filled(band, 0)
        valid = ~np.ma.getmaskarray(band)
        forest = np.isin(values, forest_values) & valid
        pixel_area_m2 = pixel_area_m2_raster(
            transform,
            height=forest.shape[0],
            width=forest.shape[1],
            crs=ds.crs,
        )

    forest_area_ha = float(np.sum(pixel_area_m2[forest], dtype=np.float64)) / 10_000.0
    forest_share = forest_area_ha / aoi_area_ha if aoi_area_ha > 0.0 else 0.0
    return Gfc2020BaselineMetrics(
        aoi_area_ha=round(aoi_area_ha, 6),
        gfc2020_forest_area_ha=round(forest_area_ha, 6),
        gfc2020_forest_share=round(forest_share, 6),
        area_method=f"{aoi_method}; raster_pixel_area_sum_for_gfc2020",
    )

