from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

from eudr_dmi_gil.analysis.tmf_change import build_tmf_layer_metadata, compute_tmf_change

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
    baseline: np.ndarray,
    defo_year: np.ndarray,
    deg_year: np.ndarray,
    requested_end_year: int = 2025,
    start_year: int = 2021,
    duration: np.ndarray | None = None,
    intensity: np.ndarray | None = None,
    nodata: int | None = None,
    output_name: str = "out",
):
    aoi_path = tmp_path / "aoi.geojson"
    baseline_path = tmp_path / "gfc2020.tif"
    defo_path = tmp_path / "tmf_defo.tif"
    deg_path = tmp_path / "tmf_deg.tif"
    _write_aoi(aoi_path)
    _write_raster(baseline_path, baseline.astype(np.uint8))
    _write_raster(defo_path, defo_year.astype(np.int16), nodata=nodata)
    _write_raster(deg_path, deg_year.astype(np.int16), nodata=nodata)

    duration_path = None
    intensity_path = None
    if duration is not None:
        duration_path = tmp_path / "tmf_duration.tif"
        _write_raster(duration_path, duration.astype(np.int16))
    if intensity is not None:
        intensity_path = tmp_path / "tmf_intensity.tif"
        _write_raster(intensity_path, intensity.astype(np.int16))

    defo_meta = build_tmf_layer_metadata(
        raster_path=defo_path,
        role="deforestation_year",
        asset_identifier="projects/JRC/TMF/v1_2025/DeforestationYear",
        dataset_version="v1_2025",
        processed_at_utc=FIXED_TS,
    )
    deg_meta = build_tmf_layer_metadata(
        raster_path=deg_path,
        role="degradation_year",
        asset_identifier="projects/JRC/TMF/v1_2025/DegradationYear",
        dataset_version="v1_2025",
        processed_at_utc=FIXED_TS,
    )

    return compute_tmf_change(
        aoi_geojson_path=aoi_path,
        tmf_deforestation_raster_path=defo_path,
        tmf_degradation_raster_path=deg_path,
        jrc_gfc2020_raster_path=baseline_path,
        output_dir=tmp_path / output_name,
        deforestation_metadata=defo_meta,
        degradation_metadata=deg_meta,
        requested_end_year=requested_end_year,
        start_year=start_year,
        tmf_duration_raster_path=duration_path,
        tmf_intensity_raster_path=intensity_path,
        target_resolution_m=500.0,
    )


def test_deforestation_and_degradation_kept_separate(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    defo_year = np.zeros((4, 4), dtype=np.int16)
    deg_year = np.zeros((4, 4), dtype=np.int16)
    defo_year[0, 0] = 2022
    deg_year[3, 3] = 2023

    result = _run(tmp_path, baseline=baseline, defo_year=defo_year, deg_year=deg_year)

    assert result.metrics.deforestation_on_gfc2020_baseline_ha > 0.0
    assert result.metrics.degradation_on_gfc2020_baseline_ha > 0.0
    rows = result.metrics.to_metric_rows()
    assert "tmf_deforestation_2021_2025_ha" in rows
    assert "tmf_degradation_2021_2025_ha" in rows
    assert rows["tmf_deforestation_2021_2025_ha"]["value"] > 0.0
    assert rows["tmf_degradation_2021_2025_ha"]["value"] > 0.0


def test_year_window_selection_excludes_outside_range(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    defo_year = np.zeros((4, 4), dtype=np.int16)
    defo_year[0, 0] = 2020  # before start_year
    defo_year[0, 1] = 2026  # after requested_end_year
    deg_year = np.zeros((4, 4), dtype=np.int16)

    result = _run(
        tmp_path,
        baseline=baseline,
        defo_year=defo_year,
        deg_year=deg_year,
        start_year=2021,
        requested_end_year=2025,
    )

    assert result.metrics.deforestation_on_gfc2020_baseline_ha == 0.0
    assert result.metrics.deforestation_on_tmf_domain_ha == 0.0


def test_both_denominators_preserved_and_distinct(tmp_path: Path) -> None:
    # Forest baseline only covers half the AOI; TMF domain covers the whole AOI.
    baseline = np.array(
        [[1, 1, 0, 0], [1, 1, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]], dtype=np.uint8
    )
    defo_year = np.full((4, 4), 2022, dtype=np.int16)
    deg_year = np.zeros((4, 4), dtype=np.int16)

    result = _run(tmp_path, baseline=baseline, defo_year=defo_year, deg_year=deg_year)

    assert result.metrics.tmf_domain_area_ha > result.metrics.gfc2020_forest_baseline_ha
    assert (
        result.metrics.deforestation_on_tmf_domain_ha
        > result.metrics.deforestation_on_gfc2020_baseline_ha
    )


def test_nodata_pixels_excluded_and_reported_as_gap(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    defo_year = np.full((4, 4), 2022, dtype=np.int16)
    deg_year = np.zeros((4, 4), dtype=np.int16)
    nodata_value = -1
    defo_year[0, 0] = nodata_value

    result = _run(
        tmp_path, baseline=baseline, defo_year=defo_year, deg_year=deg_year, nodata=nodata_value
    )

    codes = {gap["code"] for gap in result.evidence_gaps}
    assert "tmf_deforestation_nodata_inside_aoi" in codes


def test_no_tmf_domain_coverage_is_evidence_gap_not_zero(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    nodata_value = -1
    defo_year = np.full((4, 4), nodata_value, dtype=np.int16)
    deg_year = np.full((4, 4), nodata_value, dtype=np.int16)

    result = _run(
        tmp_path, baseline=baseline, defo_year=defo_year, deg_year=deg_year, nodata=nodata_value
    )

    assert result.metrics.tmf_domain_area_ha == 0.0
    codes = {gap["code"] for gap in result.evidence_gaps}
    assert "tmf_no_domain_coverage" in codes


def test_quality_context_fields_present_when_supplied(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    defo_year = np.zeros((4, 4), dtype=np.int16)
    defo_year[0, 0] = 2022
    deg_year = np.zeros((4, 4), dtype=np.int16)
    duration = np.full((4, 4), 42, dtype=np.int16)
    intensity = np.full((4, 4), 7, dtype=np.int16)

    result = _run(
        tmp_path,
        baseline=baseline,
        defo_year=defo_year,
        deg_year=deg_year,
        duration=duration,
        intensity=intensity,
    )

    assert result.metrics.quality_disturbance_duration_mean_raw == pytest.approx(42.0)
    assert result.metrics.quality_disturbance_intensity_mean_raw == pytest.approx(7.0)
    rows = result.metrics.to_metric_rows()
    assert "tmf_disturbance_duration_mean_raw" in rows
    assert "tmf_disturbance_intensity_mean_raw" in rows


def test_quality_context_absent_when_not_supplied_and_flagged_as_gap(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    defo_year = np.zeros((4, 4), dtype=np.int16)
    deg_year = np.zeros((4, 4), dtype=np.int16)

    result = _run(tmp_path, baseline=baseline, defo_year=defo_year, deg_year=deg_year)

    assert result.metrics.quality_disturbance_duration_mean_raw is None
    rows = result.metrics.to_metric_rows()
    assert "tmf_disturbance_duration_mean_raw" not in rows
    codes = {gap["code"] for gap in result.evidence_gaps}
    assert "tmf_quality_context_not_supplied" in codes


def test_valid_disruption_observation_counts_flagged_unavailable(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    defo_year = np.zeros((4, 4), dtype=np.int16)
    deg_year = np.zeros((4, 4), dtype=np.int16)

    result = _run(tmp_path, baseline=baseline, defo_year=defo_year, deg_year=deg_year)

    codes = {gap["code"] for gap in result.evidence_gaps}
    assert "tmf_valid_disruption_observation_counts_unavailable" in codes


def test_aoi_clipping_and_area_computation(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    defo_year = np.zeros((4, 4), dtype=np.int16)
    deg_year = np.zeros((4, 4), dtype=np.int16)

    result = _run(tmp_path, baseline=baseline, defo_year=defo_year, deg_year=deg_year)

    assert result.metrics.aoi_area_ha > 0.0
    assert 0.0 < result.metrics.tmf_domain_area_ha <= result.metrics.aoi_area_ha


def test_summary_and_debug_artifacts_written(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    defo_year = np.zeros((4, 4), dtype=np.int16)
    defo_year[0, 0] = 2022
    deg_year = np.zeros((4, 4), dtype=np.int16)

    result = _run(tmp_path, baseline=baseline, defo_year=defo_year, deg_year=deg_year)

    assert result.summary_path.is_file()
    assert result.debug_path.is_file()
    assert result.deforestation_mask_path.is_file()
    assert result.degradation_mask_path.is_file()
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert summary["deforestation_dataset"]["asset_identifier"] == (
        "projects/JRC/TMF/v1_2025/DeforestationYear"
    )
