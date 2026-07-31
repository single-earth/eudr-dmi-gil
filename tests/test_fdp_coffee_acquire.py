from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

from eudr_dmi_gil.deps.fdp_coffee_acquire import (
    FdpImageDescriptor,
    acquire_fdp_coffee_probability,
    select_deterministic_images,
)

AOI_BOUNDS = (-2.1, 6.55, -1.95, 6.7)


def _write_aoi(path: Path) -> None:
    west, south, east, north = AOI_BOUNDS
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"aoi_id": "ghana_west_africa_shared_aoi"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [west, south],
                            [east, south],
                            [east, north],
                            [west, north],
                            [west, south],
                        ]
                    ],
                },
            }
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


class FakeAdapter:
    """Deterministic fake `FdpCollectionAdapter` for offline testing (no live GEE)."""

    def __init__(self, images: list[FdpImageDescriptor], *, probability_value: float = 0.5) -> None:
        self.images = images
        self.probability_value = probability_value
        self.export_calls: list[dict[str, Any]] = []

    def list_images(self, *, asset_id, start_date, end_date, aoi_geojson):
        return self.images

    def export_probability_geotiff(
        self, *, asset_id, image_ids_ordered, band, aoi_geojson, scale_m, out_path
    ):
        self.export_calls.append({"image_ids_ordered": image_ids_ordered, "band": band})
        data = np.full((5, 5), self.probability_value, dtype=np.float32)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(
            out_path,
            "w",
            driver="GTiff",
            height=5,
            width=5,
            count=1,
            dtype=data.dtype,
            crs="EPSG:4326",
            transform=from_bounds(*AOI_BOUNDS, width=5, height=5),
        ) as dst:
            dst.write(data, 1)
        return {"projection": {"crs": "EPSG:4326"}, "output_dimensions": [5, 5]}


def test_select_deterministic_images_raises_on_empty() -> None:
    with pytest.raises(ValueError, match="zero images"):
        select_deterministic_images([])


def test_select_deterministic_images_sorts_by_system_index() -> None:
    images = [
        FdpImageDescriptor("id_b", "lng_-2_lat_6_b", 1704067200000, ("probability",), "EPSG:4326", None, None),
        FdpImageDescriptor("id_a", "lng_-3_lat_6_a", 1704067200000, ("probability",), "EPSG:4326", None, None),
    ]
    ordered = select_deterministic_images(images)
    assert [img.system_index for img in ordered] == ["lng_-2_lat_6_b", "lng_-3_lat_6_a"]


def test_select_deterministic_images_is_stable_regardless_of_input_order() -> None:
    a = FdpImageDescriptor("id_a", "aaa", None, (), None, None, None)
    b = FdpImageDescriptor("id_b", "bbb", None, (), None, None, None)
    assert select_deterministic_images([a, b]) == select_deterministic_images([b, a])


def test_single_image_used_as_is(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi(aoi_path)
    images = [
        FdpImageDescriptor(
            "projects/forestdatapartnership/assets/coffee/model_2025b/lng_-2_lat_6_abc",
            "lng_-2_lat_6_abc",
            1704067200000,
            ("probability",),
            "EPSG:4326",
            None,
            (11133, 11133),
        )
    ]
    adapter = FakeAdapter(images)
    metadata = acquire_fdp_coffee_probability(
        adapter=adapter,
        asset_id="projects/forestdatapartnership/assets/coffee/model_2025b",
        observation_year=2024,
        band="probability",
        aoi_geojson_path=aoi_path,
        out_raster_path=tmp_path / "out" / "probability.tif",
        out_metadata_path=tmp_path / "out" / "metadata.json",
        access_timestamp_utc="2026-07-31T00:00:00Z",
    )
    assert metadata["reduction"] == "single_image"
    assert metadata["selection_order"] == [images[0].image_id]
    assert metadata["date_interval"] == {"start": "2024-01-01", "end": "2025-01-01", "kind": "half_open"}
    assert metadata["source_acquisition_engine"] == "gee"
    assert metadata["execution_engine"] == "local-pinned-raster"
    assert metadata["output_sha256"]
    assert Path(metadata["output_path"]).is_file()


def test_multiple_images_are_mosaicked_in_recorded_order(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi(aoi_path)
    images = [
        FdpImageDescriptor("id_z", "lng_-3_lat_6_z", 1704067200000, ("probability",), "EPSG:4326", None, None),
        FdpImageDescriptor("id_a", "lng_-2_lat_6_a", 1704067200000, ("probability",), "EPSG:4326", None, None),
    ]
    adapter = FakeAdapter(images)
    metadata = acquire_fdp_coffee_probability(
        adapter=adapter,
        asset_id="projects/forestdatapartnership/assets/coffee/model_2025b",
        observation_year=2024,
        band="probability",
        aoi_geojson_path=aoi_path,
        out_raster_path=tmp_path / "out" / "probability.tif",
        out_metadata_path=tmp_path / "out" / "metadata.json",
    )
    assert metadata["reduction"] == "mosaic_ordered_by_system_index"
    # Sorted by system:index ("lng_-2_lat_6_a" < "lng_-3_lat_6_z"), not input order.
    assert metadata["selection_order"] == ["id_a", "id_z"]
    assert adapter.export_calls[0]["image_ids_ordered"] == ["id_a", "id_z"]


def test_zero_images_raises_and_does_not_write_output(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi(aoi_path)
    adapter = FakeAdapter([])
    out_raster = tmp_path / "out" / "probability.tif"
    with pytest.raises(ValueError, match="zero images"):
        acquire_fdp_coffee_probability(
            adapter=adapter,
            asset_id="projects/forestdatapartnership/assets/coffee/model_2025b",
            observation_year=2024,
            band="probability",
            aoi_geojson_path=aoi_path,
            out_raster_path=out_raster,
            out_metadata_path=tmp_path / "out" / "metadata.json",
        )
    assert not out_raster.is_file()


def test_out_of_range_exported_values_raise(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi(aoi_path)
    images = [FdpImageDescriptor("id_a", "a", None, ("probability",), "EPSG:4326", None, None)]
    adapter = FakeAdapter(images, probability_value=1.7)  # invalid
    with pytest.raises(ValueError, match=r"outside \[0, 1\]"):
        acquire_fdp_coffee_probability(
            adapter=adapter,
            asset_id="projects/forestdatapartnership/assets/coffee/model_2025b",
            observation_year=2024,
            band="probability",
            aoi_geojson_path=aoi_path,
            out_raster_path=tmp_path / "out" / "probability.tif",
            out_metadata_path=tmp_path / "out" / "metadata.json",
        )
