from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.transform import from_bounds

from eudr_dmi_gil.deps.tmf_acquire import TmfTileDescriptor, acquire_tmf_change

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


class FakeTmfAdapter:
    def __init__(self, *, has_coverage: bool = True) -> None:
        self.has_coverage = has_coverage
        self.export_calls: list[dict[str, Any]] = []

    def list_tiles(self, *, asset_id, aoi_geojson):
        if not self.has_coverage:
            return []
        return [TmfTileDescriptor(image_id=f"{asset_id}/AFR", system_index="AFR", bands=("constant",))]

    def export_layer_geotiff(self, *, asset_id, band, tile_ids_ordered, aoi_geojson, scale_m, out_path):
        self.export_calls.append({"asset_id": asset_id, "tile_ids_ordered": tile_ids_ordered})
        data = np.full((3, 3), 2022, dtype=np.int32)
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
        return {"output_dimensions": [3, 3]}


def test_acquire_exports_deforestation_and_degradation_by_default(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi(aoi_path)
    adapter = FakeTmfAdapter()

    metadata = acquire_tmf_change(
        adapter=adapter,
        aoi_geojson_path=aoi_path,
        out_dir=tmp_path / "out",
        include_quality=False,
        access_timestamp_utc="2026-08-15T00:00:00Z",
    )

    assert set(metadata["layers"].keys()) == {"deforestation", "degradation"}
    assert metadata["layers"]["deforestation"]["status"] == "exported"
    assert (tmp_path / "out" / "tmf_deforestation.tif").is_file()
    assert (tmp_path / "out" / "tmf_degradation.tif").is_file()


def test_acquire_with_quality_exports_four_layers(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi(aoi_path)
    adapter = FakeTmfAdapter()

    metadata = acquire_tmf_change(
        adapter=adapter,
        aoi_geojson_path=aoi_path,
        out_dir=tmp_path / "out",
        include_quality=True,
    )

    assert set(metadata["layers"].keys()) == {"deforestation", "degradation", "duration", "intensity"}


def test_acquire_no_tile_coverage_is_recorded_not_fabricated(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi(aoi_path)
    adapter = FakeTmfAdapter(has_coverage=False)

    metadata = acquire_tmf_change(
        adapter=adapter, aoi_geojson_path=aoi_path, out_dir=tmp_path / "out", include_quality=False
    )

    assert metadata["layers"]["deforestation"]["status"] == "no_tile_coverage"
    assert not (tmp_path / "out" / "tmf_deforestation.tif").exists()


def test_acquire_freezes_checksums_and_dataset_version(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi(aoi_path)
    adapter = FakeTmfAdapter()

    metadata = acquire_tmf_change(
        adapter=adapter, aoi_geojson_path=aoi_path, out_dir=tmp_path / "out", include_quality=False
    )

    assert metadata["dataset_version"] == "v1_2025"
    assert metadata["layers"]["deforestation"]["output_sha256"]
    assert metadata["aoi_geojson_sha256"]
