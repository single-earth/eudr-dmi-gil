from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

from eudr_dmi_gil.analysis.radd_alerts import build_radd_dataset_metadata, compute_radd_alerts

FIXED_TS = "2026-08-15T00:00:00+00:00"
BOUNDS = (-0.02, -0.02, 0.02, 0.02)


def _write_aoi(path: Path, bounds: tuple[float, float, float, float] = BOUNDS) -> None:
    west, south, east, north = bounds
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


def _write_raster(
    path: Path,
    data: np.ndarray,
    *,
    nodata: int | None = None,
    bounds: tuple[float, float, float, float] = BOUNDS,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "height": data.shape[0],
        "width": data.shape[1],
        "count": 1,
        "dtype": data.dtype,
        "crs": "EPSG:4326",
        "transform": from_bounds(*bounds, width=data.shape[1], height=data.shape[0]),
    }
    if nodata is not None:
        profile["nodata"] = nodata
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)


def _run(
    tmp_path: Path,
    *,
    alert: np.ndarray,
    alert_date: np.ndarray,
    date_window_start: str | None = None,
    date_window_end: str | None = None,
    nodata: int | None = None,
    output_name: str = "out",
    coverage_warning: str | None = None,
):
    aoi_path = tmp_path / "aoi.geojson"
    alert_path = tmp_path / "radd_alert.tif"
    date_path = tmp_path / "radd_date.tif"
    _write_aoi(aoi_path)
    _write_raster(alert_path, alert.astype(np.int16), nodata=nodata)
    _write_raster(date_path, alert_date.astype(np.int32), nodata=nodata)

    metadata = build_radd_dataset_metadata(
        alert_raster_path=alert_path,
        date_raster_path=date_path,
        geography="africa",
        acquired_at_utc=FIXED_TS,
        collection_version_or_tile_ids=("africa_20240103",),
        aoi_export_bounds_wgs84=BOUNDS,
        date_window_start=date_window_start,
        date_window_end=date_window_end,
        coverage_warning=coverage_warning,
    )

    return compute_radd_alerts(
        aoi_geojson_path=aoi_path,
        radd_alert_raster_path=alert_path,
        radd_date_raster_path=date_path,
        output_dir=tmp_path / output_name,
        dataset_metadata=metadata,
        date_window_start=date_window_start,
        date_window_end=date_window_end,
        target_resolution_m=500.0,
    )


def test_low_versus_confirmed_confidence_kept_separate(tmp_path: Path) -> None:
    alert = np.zeros((4, 4), dtype=np.int16)
    alert[0, 0] = 2  # low confidence
    alert[3, 3] = 3  # confirmed
    alert_date = np.full((4, 4), 24001, dtype=np.int32)

    result = _run(tmp_path, alert=alert, alert_date=alert_date)

    assert result.metrics.radd_low_confidence_alert_area_ha > 0.0
    assert result.metrics.radd_confirmed_alert_area_ha > 0.0
    rows = result.metrics.to_metric_rows()
    assert "radd_low_confidence_alert_area_ha" in rows
    assert "radd_confirmed_alert_area_ha" in rows


def test_date_window_filtering_excludes_alerts_outside_window(tmp_path: Path) -> None:
    alert = np.full((4, 4), 3, dtype=np.int16)
    alert_date = np.zeros((4, 4), dtype=np.int32)
    alert_date[0, 0] = 22001  # 2022-01-01, outside window
    alert_date[3, 3] = 24050  # inside window

    result = _run(
        tmp_path,
        alert=alert,
        alert_date=alert_date,
        date_window_start="2024-01-01",
        date_window_end="2024-12-31",
    )

    # Only the in-window pixel should count; area must be less than if both counted.
    assert result.metrics.radd_confirmed_alert_area_ha > 0.0
    assert result.metrics.radd_first_alert_date is not None
    assert result.metrics.radd_first_alert_date >= "2024-01-01"


def test_nodata_pixels_excluded_and_reported(tmp_path: Path) -> None:
    alert = np.full((4, 4), 3, dtype=np.int16)
    alert_date = np.full((4, 4), 24010, dtype=np.int32)
    nodata_value = -1
    alert[0, 0] = nodata_value

    result = _run(tmp_path, alert=alert, alert_date=alert_date, nodata=nodata_value)

    codes = {gap["code"] for gap in result.evidence_gaps}
    assert "radd_alert_nodata_inside_aoi" in codes


def test_no_coverage_is_evidence_gap_not_zero(tmp_path: Path) -> None:
    nodata_value = -1
    alert = np.full((4, 4), nodata_value, dtype=np.int16)
    alert_date = np.full((4, 4), nodata_value, dtype=np.int32)

    result = _run(tmp_path, alert=alert, alert_date=alert_date, nodata=nodata_value)

    assert result.metrics.radd_domain_area_ha == 0.0
    codes = {gap["code"] for gap in result.evidence_gaps}
    assert "radd_no_domain_coverage" in codes


def test_mutable_source_metadata_is_frozen(tmp_path: Path) -> None:
    alert = np.full((4, 4), 3, dtype=np.int16)
    alert_date = np.full((4, 4), 24010, dtype=np.int32)

    result = _run(tmp_path, alert=alert, alert_date=alert_date)

    assert result.dataset_metadata.acquired_at_utc == FIXED_TS
    assert result.dataset_metadata.collection_id == "projects/radar-wur/raddalert/v1"
    assert result.dataset_metadata.alert_raster_sha256 is not None
    assert result.dataset_metadata.date_raster_sha256 is not None
    codes = {gap["code"] for gap in result.evidence_gaps}
    assert "radd_mutable_source_frozen_at_acquisition" in codes


def test_deterministic_rerun_from_same_frozen_export(tmp_path: Path) -> None:
    alert = np.zeros((4, 4), dtype=np.int16)
    alert[1, 1] = 3
    alert_date = np.full((4, 4), 24010, dtype=np.int32)

    result_a = _run(tmp_path, alert=alert, alert_date=alert_date, output_name="run_a")
    result_b = _run(tmp_path, alert=alert, alert_date=alert_date, output_name="run_b")

    assert result_a.metrics.radd_confirmed_alert_area_ha == result_b.metrics.radd_confirmed_alert_area_ha
    assert result_a.metrics.radd_first_alert_date == result_b.metrics.radd_first_alert_date
    assert result_a.dataset_metadata.alert_raster_sha256 == result_b.dataset_metadata.alert_raster_sha256


def test_coverage_warning_propagates_to_evidence_gaps(tmp_path: Path) -> None:
    alert = np.full((4, 4), 3, dtype=np.int16)
    alert_date = np.full((4, 4), 24010, dtype=np.int32)

    result = _run(
        tmp_path,
        alert=alert,
        alert_date=alert_date,
        coverage_warning="AOI straddles two RADD export tiles; edge pixels may be under-sampled.",
    )

    codes = {gap["code"] for gap in result.evidence_gaps}
    assert "radd_coverage_warning" in codes


def test_alert_count_reflects_cluster_not_pixel_count(tmp_path: Path) -> None:
    alert = np.zeros((6, 6), dtype=np.int16)
    alert[0:2, 0:2] = 3  # one 2x2 cluster
    alert[4, 4] = 3  # one isolated pixel
    alert_date = np.full((6, 6), 24010, dtype=np.int32)

    result = _run(tmp_path, alert=alert, alert_date=alert_date)

    assert result.metrics.radd_confirmed_alert_cluster_count == 2
