from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

from eudr_dmi_gil.analysis.jrc_post2020_loss import build_hansen_lossyear_metadata
from eudr_dmi_gil.commodities.analysis import run_commodity_assessment
from eudr_dmi_gil.commodities.config import CommodityConfig
from eudr_dmi_gil.providers.jrc_gfc2020 import LocalJrcGfc2020Provider


FIXED_TS = "2026-07-25T00:00:00+00:00"
BOUNDS = (-0.02, -0.02, 0.02, 0.02)
RASTER_BOUNDS = (-0.05, -0.05, 0.05, 0.05)


def _write_aoi(path: Path, *, country: str = "Brazil") -> None:
    west, south, east, north = BOUNDS
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "aoi_id": "commodity_fixture",
                    "country": country,
                    "commodity": "cocoa",
                },
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


def _write_raster(
    path: Path,
    data: np.ndarray,
    *,
    nodata: int | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "height": data.shape[0],
        "width": data.shape[1],
        "count": 1,
        "dtype": data.dtype,
        "crs": "EPSG:4326",
        "transform": from_bounds(*RASTER_BOUNDS, width=data.shape[1], height=data.shape[0]),
    }
    if nodata is not None:
        profile["nodata"] = nodata
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)


def _config(path: Path, *, class_values: tuple[int, ...] = (46,)) -> CommodityConfig:
    return CommodityConfig(
        id="coffee",
        display_name="Coffee",
        provider="mapbiomas_brazil",
        dataset_title="MapBiomas Brazil Land Cover",
        dataset_version="collection-9-2023",
        asset_id=path.as_posix(),
        local_path=path.as_posix(),
        class_values=class_values,
        class_labels=("Coffee plantations",),
        observation_year=2023,
        country_scope=("Brazil",),
        optional=True,
    )


def _run(
    tmp_path: Path,
    *,
    commodity: np.ndarray | None,
    country: str = "Brazil",
    class_values: tuple[int, ...] = (46,),
    nodata: int | None = None,
):
    aoi_path = tmp_path / "filename_says_cocoa.geojson"
    jrc_path = tmp_path / "jrc.tif"
    loss_path = tmp_path / "lossyear.tif"
    commodity_path = tmp_path / "coffee.tif"
    _write_aoi(aoi_path, country=country)
    _write_raster(jrc_path, np.ones((10, 10), dtype=np.uint8))
    loss = np.zeros((10, 10), dtype=np.uint8)
    loss[5, 5] = 21
    _write_raster(loss_path, loss)
    if commodity is not None:
        _write_raster(commodity_path, commodity.astype(np.int16), nodata=nodata)

    baseline_provider = LocalJrcGfc2020Provider(jrc_path, processed_at_utc=FIXED_TS)
    loss_metadata = build_hansen_lossyear_metadata(
        raster_path=loss_path,
        dataset_version="2025-v1.13",
        latest_available_year=2025,
        processed_at_utc=FIXED_TS,
        source_url="https://example.test/hansen",
        asset_identifier="UMD/hansen/global_forest_change_2025_v1_13",
    )
    return run_commodity_assessment(
        config=_config(commodity_path, class_values=class_values),
        aoi_geojson_path=aoi_path,
        aoi_country=country,
        jrc_gfc2020_raster_path=jrc_path,
        hansen_lossyear_raster_path=loss_path,
        output_dir=tmp_path / "out",
        baseline_metadata=baseline_provider.metadata(),
        loss_metadata=loss_metadata,
        requested_end_year=2025,
        target_resolution_m=500.0,
    )


def test_coffee_layer_present_with_overlap(tmp_path: Path) -> None:
    coffee = np.zeros((10, 10), dtype=np.uint8)
    coffee[5, 5] = 46
    result = _run(tmp_path, commodity=coffee)

    assert result.evidence_available is True
    assert result.coverage_status == "full"
    assert result.metrics.commodity_area_ha is not None
    assert result.metrics.post_2020_loss_and_commodity_overlap_ha is not None
    assert result.metrics.post_2020_loss_and_commodity_overlap_ha > 0.0
    assert any("human review required" in msg for msg in result.status_messages)


def test_coffee_layer_present_without_overlap(tmp_path: Path) -> None:
    coffee = np.zeros((10, 10), dtype=np.uint8)
    coffee[4, 4] = 46
    result = _run(tmp_path, commodity=coffee)

    assert result.metrics.commodity_area_ha is not None
    assert result.metrics.commodity_area_ha > 0.0
    assert result.metrics.post_2020_loss_and_commodity_overlap_ha == 0.0


def test_commodity_layer_unavailable_is_not_zero(tmp_path: Path) -> None:
    result = _run(tmp_path, commodity=None)

    assert result.evidence_available is False
    assert result.coverage_status == "unavailable"
    assert result.metrics.commodity_area_ha is None
    assert result.metrics.post_2020_loss_and_commodity_overlap_ha is None
    assert any(gap["code"] == "commodity_layer_unavailable" for gap in result.evidence_gaps)


def test_unsupported_country_provider_combination(tmp_path: Path) -> None:
    coffee = np.zeros((10, 10), dtype=np.uint8)
    coffee[5, 5] = 46
    result = _run(tmp_path, commodity=coffee, country="Vietnam")

    assert result.evidence_available is False
    assert result.coverage_status == "unsupported_country"
    assert result.metrics.commodity_area_ha is None
    assert any(
        gap["code"] == "commodity_provider_unsupported_country"
        for gap in result.evidence_gaps
    )


def test_missing_class_value_is_rejected(tmp_path: Path) -> None:
    coffee = np.zeros((10, 10), dtype=np.uint8)
    coffee[5, 5] = 46
    with pytest.raises(ValueError, match="class value"):
        _run(tmp_path, commodity=coffee, class_values=(99,))


def test_partial_coverage_is_distinct(tmp_path: Path) -> None:
    coffee = np.zeros((10, 10), dtype=np.int16)
    coffee[5, 5] = 46
    coffee[4, 4] = -1
    result = _run(tmp_path, commodity=coffee, nodata=-1)

    assert result.evidence_available is True
    assert result.coverage_status == "partial"
    assert result.metrics.commodity_area_ha is not None
    assert any(gap["code"] == "commodity_layer_partial_coverage" for gap in result.evidence_gaps)


def test_structured_commodity_overrides_aoi_filename_and_properties(tmp_path: Path) -> None:
    coffee = np.zeros((10, 10), dtype=np.uint8)
    coffee[5, 5] = 46
    result = _run(tmp_path, commodity=coffee)

    assert result.metadata.commodity_id == "coffee"
    assert result.metadata.display_name == "Coffee"
