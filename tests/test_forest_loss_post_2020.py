from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import rasterio
from pyproj import Geod
from rasterio.transform import from_bounds

from eudr_dmi_gil.tasks.forest_loss_post_2020 import (
    HansenConfig,
    compute_forest_loss_post_2020,
)


def _write_test_raster(path: Path, data: np.ndarray, transform, crs: str) -> None:
    profile = {
        "driver": "GTiff",
        "height": data.shape[0],
        "width": data.shape[1],
        "count": 1,
        "dtype": data.dtype,
        "crs": crs,
        "transform": transform,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)


def _pixel_area_ha_geographic(transform, row: int, col: int) -> float:
    geod = Geod(ellps="WGS84")
    x0, y0 = transform * (col, row)
    x1, y1 = transform * (col + 1, row + 1)
    lons = [x0, x1, x1, x0]
    lats = [y0, y0, y1, y1]
    area, _ = geod.polygon_area_perimeter(lons, lats)
    return abs(area) / 10000.0


def test_compute_forest_loss_post_2020(tmp_path: Path) -> None:
    tile_dir = tmp_path / "tiles"
    out_dir = tmp_path / "out"

    bounds = (24.0, 59.0, 24.02, 59.02)
    width = 2
    height = 2
    transform = from_bounds(*bounds, width, height)

    treecover = np.array([[0, 50], [60, 20]], dtype=np.uint8)
    lossyear = np.array([[0, 21], [0, 0]], dtype=np.uint8)

    _write_test_raster(tile_dir / "treecover2000.tif", treecover, transform, "EPSG:4326")
    _write_test_raster(tile_dir / "lossyear.tif", lossyear, transform, "EPSG:4326")

    aoi = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [bounds[0], bounds[1]],
                            [bounds[2], bounds[1]],
                            [bounds[2], bounds[3]],
                            [bounds[0], bounds[3]],
                            [bounds[0], bounds[1]],
                        ]
                    ],
                },
            }
        ],
    }
    aoi_path = tmp_path / "aoi.geojson"
    aoi_path.write_text(json.dumps(aoi), encoding="utf-8")

    result = compute_forest_loss_post_2020(
        aoi_geojson_path=aoi_path,
        output_dir=out_dir,
        config=HansenConfig(
            tile_dir=tile_dir,
            canopy_threshold_percent=30,
            cutoff_year=2020,
            write_masks=True,
        ),
    )

    # Expected areas based on mask logic.
    # Baseline pixels: (0,1) and (1,0)
    # Loss post 2020: (0,1)
    # Current cover: (1,0)
    baseline_area = _pixel_area_ha_geographic(transform, 0, 1) + _pixel_area_ha_geographic(
        transform, 1, 0
    )
    loss_area = _pixel_area_ha_geographic(transform, 0, 1)
    current_area = _pixel_area_ha_geographic(transform, 1, 0)

    assert result.summary_path.is_file()
    assert result.initial_tree_cover_ha == round(baseline_area, 6)
    assert result.forest_loss_post_2020_ha == round(loss_area, 6)
    assert result.current_tree_cover_ha == round(current_area, 6)
    assert result.mask_forest_loss_post_2020_path.is_file()
    assert result.mask_forest_current_path.is_file()
