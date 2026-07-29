from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

from eudr_dmi_gil.analysis.jrc_post2020_loss import (
    build_hansen_lossyear_metadata,
    compute_jrc_post2020_loss,
)
from eudr_dmi_gil.providers.jrc_gfc2020 import LocalJrcGfc2020Provider


FIXED_TS = "2026-07-25T00:00:00+00:00"
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


def _run_analysis(
    tmp_path: Path,
    *,
    baseline: np.ndarray,
    lossyear: np.ndarray,
    requested_end_year: int = 2025,
    latest_available_year: int = 2026,
    nodata: int | None = None,
    output_name: str = "out",
):
    aoi_path = tmp_path / "aoi.geojson"
    baseline_path = tmp_path / "jrc.tif"
    loss_path = tmp_path / "lossyear.tif"
    _write_aoi(aoi_path)
    _write_raster(baseline_path, baseline.astype(np.uint8), nodata=nodata)
    _write_raster(loss_path, lossyear.astype(np.uint8), nodata=nodata)

    baseline_provider = LocalJrcGfc2020Provider(
        baseline_path,
        processed_at_utc=FIXED_TS,
    )
    loss_metadata = build_hansen_lossyear_metadata(
        raster_path=loss_path,
        dataset_version="2026-v1.14",
        latest_available_year=latest_available_year,
        processed_at_utc=FIXED_TS,
        source_url="https://example.test/hansen",
        asset_identifier="UMD/hansen/global_forest_change_2026_v1_14",
    )
    return compute_jrc_post2020_loss(
        aoi_geojson_path=aoi_path,
        jrc_gfc2020_raster_path=baseline_path,
        hansen_lossyear_raster_path=loss_path,
        output_dir=tmp_path / output_name,
        baseline_metadata=baseline_provider.metadata(),
        loss_metadata=loss_metadata,
        requested_end_year=requested_end_year,
        target_resolution_m=500.0,
    )


def test_aoi_entirely_outside_baseline_forest(tmp_path: Path) -> None:
    result = _run_analysis(
        tmp_path,
        baseline=np.zeros((4, 4), dtype=np.uint8),
        lossyear=np.full((4, 4), 21, dtype=np.uint8),
    )

    assert result.metrics.forest_baseline_2020_ha == 0.0
    assert result.metrics.forest_loss_post_2020_on_baseline_ha == 0.0


def test_aoi_entirely_inside_baseline_forest_with_no_loss(tmp_path: Path) -> None:
    result = _run_analysis(
        tmp_path,
        baseline=np.ones((4, 4), dtype=np.uint8),
        lossyear=np.zeros((4, 4), dtype=np.uint8),
    )

    assert result.metrics.forest_baseline_2020_ha > 0.0
    assert result.metrics.forest_loss_post_2020_on_baseline_ha == 0.0
    assert result.metrics.baseline_forest_without_detected_loss_ha == pytest.approx(
        result.metrics.forest_baseline_2020_ha
    )


def test_loss_outside_jrc_forest_mask_is_excluded(tmp_path: Path) -> None:
    baseline = np.array(
        [
            [1, 1, 0, 0],
            [1, 1, 0, 0],
            [0, 0, 0, 0],
            [0, 0, 0, 0],
        ],
        dtype=np.uint8,
    )
    lossyear = np.array(
        [
            [0, 0, 21, 21],
            [0, 0, 21, 21],
            [21, 21, 21, 21],
            [21, 21, 21, 21],
        ],
        dtype=np.uint8,
    )
    result = _run_analysis(tmp_path, baseline=baseline, lossyear=lossyear)

    assert result.metrics.forest_baseline_2020_ha > 0.0
    assert result.metrics.forest_loss_post_2020_on_baseline_ha == 0.0


def test_loss_inside_jrc_forest_mask_is_included(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    lossyear = np.zeros((4, 4), dtype=np.uint8)
    lossyear[1, 1] = 21
    result = _run_analysis(tmp_path, baseline=baseline, lossyear=lossyear)

    assert result.metrics.forest_loss_post_2020_on_baseline_ha > 0.0
    assert result.metrics.forest_loss_post_2020_percent_of_baseline > 0.0


def test_loss_before_2021_is_excluded(tmp_path: Path) -> None:
    result = _run_analysis(
        tmp_path,
        baseline=np.ones((4, 4), dtype=np.uint8),
        lossyear=np.full((4, 4), 20, dtype=np.uint8),
    )

    assert result.metrics.forest_loss_post_2020_on_baseline_ha == 0.0


def test_loss_after_end_year_is_excluded(tmp_path: Path) -> None:
    result = _run_analysis(
        tmp_path,
        baseline=np.ones((4, 4), dtype=np.uint8),
        lossyear=np.full((4, 4), 26, dtype=np.uint8),
        requested_end_year=2025,
        latest_available_year=2026,
    )

    assert result.metrics.effective_end_year == 2025
    assert result.metrics.forest_loss_post_2020_on_baseline_ha == 0.0


def test_end_year_2025_and_2026_behave_differently(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    lossyear = np.zeros((4, 4), dtype=np.uint8)
    lossyear[0, 0] = 25
    lossyear[0, 1] = 26

    result_2025 = _run_analysis(
        tmp_path,
        baseline=baseline,
        lossyear=lossyear,
        requested_end_year=2025,
        latest_available_year=2026,
        output_name="out_2025",
    )
    result_2026 = _run_analysis(
        tmp_path,
        baseline=baseline,
        lossyear=lossyear,
        requested_end_year=2026,
        latest_available_year=2026,
        output_name="out_2026",
    )

    assert result_2025.metrics.effective_end_year == 2025
    assert result_2026.metrics.effective_end_year == 2026
    assert (
        result_2026.metrics.forest_loss_post_2020_on_baseline_ha
        > result_2025.metrics.forest_loss_post_2020_on_baseline_ha
    )


def test_requested_end_year_is_capped_to_dataset_coverage(tmp_path: Path) -> None:
    result = _run_analysis(
        tmp_path,
        baseline=np.ones((4, 4), dtype=np.uint8),
        lossyear=np.full((4, 4), 26, dtype=np.uint8),
        requested_end_year=2026,
        latest_available_year=2025,
    )

    assert result.metrics.requested_end_year == 2026
    assert result.metrics.effective_end_year == 2025
    assert result.metrics.forest_loss_post_2020_on_baseline_ha == 0.0
    assert any(gap["code"] == "requested_end_year_not_available" for gap in result.evidence_gaps)


def test_nodata_is_not_counted_as_forest_or_loss(tmp_path: Path) -> None:
    nodata = 255
    baseline = np.ones((4, 4), dtype=np.uint8)
    lossyear = np.zeros((4, 4), dtype=np.uint8)
    baseline[0, 0] = nodata
    lossyear[0, 0] = 21
    lossyear[0, 1] = nodata

    result = _run_analysis(
        tmp_path,
        baseline=baseline,
        lossyear=lossyear,
        nodata=nodata,
    )
    no_nodata = _run_analysis(
        tmp_path,
        baseline=np.ones((4, 4), dtype=np.uint8),
        lossyear=np.zeros((4, 4), dtype=np.uint8),
        output_name="out_no_nodata",
    )

    assert result.metrics.forest_baseline_2020_ha < no_nodata.metrics.forest_baseline_2020_ha
    assert result.metrics.forest_loss_post_2020_on_baseline_ha == 0.0
    assert any(gap["code"] == "jrc_nodata_inside_aoi" for gap in result.evidence_gaps)
    assert any(gap["code"] == "hansen_nodata_inside_aoi" for gap in result.evidence_gaps)


def test_repeated_execution_produces_identical_metrics_and_artifacts(tmp_path: Path) -> None:
    baseline = np.ones((4, 4), dtype=np.uint8)
    lossyear = np.zeros((4, 4), dtype=np.uint8)
    lossyear[2, 2] = 21

    first = _run_analysis(
        tmp_path,
        baseline=baseline,
        lossyear=lossyear,
        output_name="repeat_a",
    )
    second = _run_analysis(
        tmp_path,
        baseline=baseline,
        lossyear=lossyear,
        output_name="repeat_b",
    )

    assert first.metrics == second.metrics
    assert first.summary_path.read_bytes() == second.summary_path.read_bytes()
    assert first.loss_mask_path.read_bytes() == second.loss_mask_path.read_bytes()
    assert first.baseline_mask_path.read_bytes() == second.baseline_mask_path.read_bytes()


def test_legacy_baseline_metrics_do_not_replace_jrc_metric(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    jrc_path = tmp_path / "jrc.tif"
    loss_path = tmp_path / "lossyear.tif"
    _write_aoi(aoi_path)
    _write_raster(jrc_path, np.ones((4, 4), dtype=np.uint8))
    loss = np.zeros((4, 4), dtype=np.uint8)
    loss[0, 0] = 21
    _write_raster(loss_path, loss)

    evidence_root = tmp_path / "evidence"
    env = os.environ.copy()
    env["EUDR_DMI_EVIDENCE_ROOT"] = str(evidence_root)
    env["EUDR_DMI_GENERATED_AT_UTC"] = FIXED_TS
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "eudr_dmi_gil.reports.cli",
            "--aoi-id",
            "fixture_aoi",
            "--aoi-geojson",
            str(aoi_path),
            "--bundle-id",
            "bundle-jrc",
            "--out-format",
            "json",
            "--jrc-gfc2020-raster",
            str(jrc_path),
            "--hansen-lossyear-raster",
            str(loss_path),
            "--end-year",
            "2025",
            "--loss-dataset-end-year",
            "2025",
            "--analysis-target-resolution-m",
            "500",
            "--metric",
            "rfm_area_ha=999:ha:legacy:should_not_replace_jrc",
        ],
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )

    assert proc.returncode == 0, proc.stderr
    report_path = (
        evidence_root
        / "2026-07-25"
        / "bundle-jrc"
        / "reports"
        / "aoi_report_v2"
        / "fixture_aoi.json"
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    metrics = report["metrics"]

    assert metrics["rfm_area_ha"]["value"] == 999
    assert metrics["forest_baseline_2020_ha"]["value"] != 999
    assert metrics["forest_loss_post_2020_on_baseline_ha"]["value"] > 0.0
    compatibility = report["extensions"]["post_2020_loss_on_2020_forest"]["compatibility"]
    assert compatibility["canonical_baseline_metric"] == "forest_baseline_2020_ha"

