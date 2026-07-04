from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import rasterio

from eudr.evidence.gfc2020_baseline import load_aoi_geometry, mask_raster_to_aoi
from eudr_dmi_gil.geo.forest_area_core import pixel_area_m2_raster


@dataclass(frozen=True)
class HansenDisturbanceMetrics:
    loss_inside_gfc2020_ha: float
    loss_inside_gfc2020_ha_by_year: dict[str, float]


def _year_code(year: int) -> int:
    return year - 2000


def compute_hansen_loss_inside_gfc2020(
    *,
    aoi_geojson_path: Path,
    gfc2020_raster_path: Path,
    hansen_lossyear_raster_path: Path,
    start_year: int,
    end_year: int,
    forest_values: tuple[int, ...] = (1,),
) -> HansenDisturbanceMetrics:
    if start_year < 2021:
        raise ValueError("Post-cutoff Hansen evidence start year must be 2021 or later")
    if end_year < start_year:
        raise ValueError("end_year must be greater than or equal to start_year")

    aoi_geom = load_aoi_geometry(aoi_geojson_path)
    with rasterio.open(gfc2020_raster_path) as baseline_ds, rasterio.open(
        hansen_lossyear_raster_path
    ) as loss_ds:
        baseline_data, baseline_transform = mask_raster_to_aoi(baseline_ds, aoi_geom)
        loss_data, _ = mask_raster_to_aoi(loss_ds, aoi_geom)

        baseline_band = baseline_data[0]
        loss_band = loss_data[0]
        if baseline_band.shape != loss_band.shape:
            raise RuntimeError("GFC2020 and Hansen lossyear rasters must be co-registered")

        baseline_values = np.ma.filled(baseline_band, 0)
        loss_values = np.ma.filled(loss_band, 0)
        valid = (~np.ma.getmaskarray(baseline_band)) & (~np.ma.getmaskarray(loss_band))
        baseline_forest = np.isin(baseline_values, forest_values) & valid
        pixel_area_m2 = pixel_area_m2_raster(
            baseline_transform,
            height=baseline_forest.shape[0],
            width=baseline_forest.shape[1],
            crs=baseline_ds.crs,
        )

    by_year: dict[str, float] = {}
    total_m2 = np.float64(0.0)
    for year in range(start_year, end_year + 1):
        mask = baseline_forest & (loss_values == _year_code(year))
        area_m2 = np.sum(pixel_area_m2[mask], dtype=np.float64)
        total_m2 += area_m2
        by_year[str(year)] = round(float(area_m2) / 10_000.0, 6)

    return HansenDisturbanceMetrics(
        loss_inside_gfc2020_ha=round(float(total_m2) / 10_000.0, 6),
        loss_inside_gfc2020_ha_by_year=by_year,
    )

