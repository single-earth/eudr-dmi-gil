from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.transform import from_bounds

from eudr_dmi_gil.deps.radd_acquire import (
    RaddTileDescriptor,
    acquire_radd_alerts,
    select_alert_tiles,
)

AOI_BOUNDS = (-9.5, 6.0, -9.4, 6.1)


def _write_aoi(path: Path) -> None:
    west, south, east, north = AOI_BOUNDS
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"aoi_id": "fixture_aoi"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [[west, south], [east, south], [east, north], [west, north], [west, south]]
                    ],
                },
            }
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_select_alert_tiles_drops_baseline_domain_images() -> None:
    tiles = [
        RaddTileDescriptor("africa_20240103", "africa_20240103", "africa", "2024-01-08", ("Alert", "Date")),
        RaddTileDescriptor(
            "africa_radd_primaryhumidtropicalforest_baseline2018_v20201206",
            "africa_radd_primaryhumidtropicalforest_baseline2018_v20201206",
            "africa",
            None,
            ("constant",),
        ),
    ]
    selected = select_alert_tiles(tiles)
    assert [t.image_id for t in selected] == ["africa_20240103"]


def test_select_alert_tiles_orders_deterministically_by_system_index() -> None:
    tiles = [
        RaddTileDescriptor("africa_20240201", "africa_20240201", "africa", None, ("Alert", "Date")),
        RaddTileDescriptor("africa_20240103", "africa_20240103", "africa", None, ("Alert", "Date")),
    ]
    selected = select_alert_tiles(tiles)
    assert [t.system_index for t in selected] == ["africa_20240103", "africa_20240201"]


class FakeRaddAdapter:
    def __init__(self, *, tiles: list[RaddTileDescriptor]) -> None:
        self.tiles = tiles
        self.export_calls: list[dict[str, Any]] = []

    def list_tiles(self, *, geography, start_date, end_date, aoi_geojson):
        return self.tiles

    def export_alert_geotiff(self, *, tile_ids_ordered, aoi_geojson, scale_m, alert_out_path, date_out_path):
        self.export_calls.append({"tile_ids_ordered": tile_ids_ordered})
        for out_path, value, dtype in (
            (alert_out_path, 3, np.int32),
            (date_out_path, 24010, np.int32),
        ):
            data = np.full((3, 3), value, dtype=dtype)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with rasterio.open(
                out_path,
                "w",
                driver="GTiff",
                height=3,
                width=3,
                count=1,
                dtype=data.dtype,
                crs="EPSG:4326",
                transform=from_bounds(*AOI_BOUNDS, width=3, height=3),
            ) as dst:
                dst.write(data, 1)
        return {"output_dimensions": {"Alert": [3, 3], "Date": [3, 3]}}


def test_acquire_exports_alert_and_date_rasters(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi(aoi_path)
    adapter = FakeRaddAdapter(
        tiles=[
            RaddTileDescriptor("africa_20240103", "africa_20240103", "africa", "2024-01-08", ("Alert", "Date")),
        ]
    )

    metadata = acquire_radd_alerts(
        adapter=adapter,
        aoi_geojson_path=aoi_path,
        out_dir=tmp_path / "out",
        geography="africa",
        access_timestamp_utc="2026-08-15T00:00:00Z",
    )

    assert metadata["status"] == "exported"
    assert (tmp_path / "out" / "radd_alert.tif").is_file()
    assert (tmp_path / "out" / "radd_date.tif").is_file()
    assert metadata["alert_raster_sha256"]
    assert metadata["date_raster_sha256"]


def test_acquire_drops_baseline_tiles_from_export(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi(aoi_path)
    adapter = FakeRaddAdapter(
        tiles=[
            RaddTileDescriptor("africa_20240103", "africa_20240103", "africa", "2024-01-08", ("Alert", "Date")),
            RaddTileDescriptor(
                "africa_radd_primaryhumidtropicalforest_baseline2018_v20201206",
                "africa_radd_primaryhumidtropicalforest_baseline2018_v20201206",
                "africa",
                None,
                ("constant",),
            ),
        ]
    )

    metadata = acquire_radd_alerts(
        adapter=adapter, aoi_geojson_path=aoi_path, out_dir=tmp_path / "out", geography="africa"
    )

    assert metadata["tile_ids"] == ["africa_20240103"]
    assert metadata["dropped_non_alert_tile_ids"] == [
        "africa_radd_primaryhumidtropicalforest_baseline2018_v20201206"
    ]


def test_no_alert_tile_coverage_is_gap_not_silent_empty_export(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi(aoi_path)
    adapter = FakeRaddAdapter(tiles=[])

    metadata = acquire_radd_alerts(
        adapter=adapter, aoi_geojson_path=aoi_path, out_dir=tmp_path / "out", geography="africa"
    )

    assert metadata["status"] == "no_alert_tile_coverage"
    assert metadata["coverage_warning"]
    assert not (tmp_path / "out" / "radd_alert.tif").exists()
