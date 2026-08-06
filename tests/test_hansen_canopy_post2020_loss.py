from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

from eudr_dmi_gil.analysis.hansen_canopy_post2020_loss import compute_hansen_canopy_post2020_loss
from eudr_dmi_gil.analysis.jrc_post2020_loss import build_hansen_lossyear_metadata
from eudr_dmi_gil.providers.hansen_treecover2000 import LocalHansenTreecoverProvider

FIXED_TS = "2026-08-06T00:00:00+00:00"
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
    treecover: np.ndarray,
    lossyear: np.ndarray,
    requested_end_year: int = 2025,
    latest_available_year: int = 2026,
    canopy_threshold_percent: int = 10,
    nodata: int | None = None,
    output_name: str = "out",
):
    aoi_path = tmp_path / "aoi.geojson"
    treecover_path = tmp_path / "treecover2000.tif"
    loss_path = tmp_path / "lossyear.tif"
    _write_aoi(aoi_path)
    _write_raster(treecover_path, treecover.astype(np.uint8), nodata=nodata)
    _write_raster(loss_path, lossyear.astype(np.uint8), nodata=nodata)

    baseline_provider = LocalHansenTreecoverProvider(
        treecover_path,
        canopy_threshold_percent=canopy_threshold_percent,
        dataset_version="2026-v1.14",
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
    return compute_hansen_canopy_post2020_loss(
        aoi_geojson_path=aoi_path,
        hansen_treecover_raster_path=treecover_path,
        hansen_lossyear_raster_path=loss_path,
        output_dir=tmp_path / output_name,
        baseline_metadata=baseline_provider.metadata(),
        loss_metadata=loss_metadata,
        requested_end_year=requested_end_year,
        canopy_threshold_percent=canopy_threshold_percent,
        target_resolution_m=500.0,
    )


def test_aoi_entirely_below_canopy_threshold(tmp_path: Path) -> None:
    result = _run_analysis(
        tmp_path,
        treecover=np.zeros((4, 4), dtype=np.uint8),
        lossyear=np.full((4, 4), 21, dtype=np.uint8),
    )
    assert result.metrics.forest_baseline_hansen10pct_2000_ha == 0.0
    assert result.metrics.forest_loss_post_2020_on_hansen10pct_baseline_ha == 0.0


def test_aoi_entirely_above_canopy_threshold_with_no_loss(tmp_path: Path) -> None:
    result = _run_analysis(
        tmp_path,
        treecover=np.full((4, 4), 50, dtype=np.uint8),
        lossyear=np.zeros((4, 4), dtype=np.uint8),
    )
    assert result.metrics.forest_baseline_hansen10pct_2000_ha > 0.0
    assert result.metrics.forest_loss_post_2020_on_hansen10pct_baseline_ha == 0.0
    assert result.metrics.hansen10pct_baseline_forest_without_detected_loss_ha == pytest.approx(
        result.metrics.forest_baseline_hansen10pct_2000_ha
    )


def test_canopy_below_threshold_is_excluded_from_baseline(tmp_path: Path) -> None:
    treecover = np.array(
        [
            [50, 50, 5, 5],
            [50, 50, 5, 5],
            [5, 5, 5, 5],
            [5, 5, 5, 5],
        ],
        dtype=np.uint8,
    )
    lossyear = np.full((4, 4), 21, dtype=np.uint8)
    result = _run_analysis(tmp_path, treecover=treecover, lossyear=lossyear)

    # Loss (uniform value 21 across the whole grid) only counts where canopy >= threshold, so
    # loss area must equal baseline area here, not the low-canopy pixels excluded from baseline.
    assert result.metrics.forest_baseline_hansen10pct_2000_ha > 0.0
    assert result.metrics.forest_loss_post_2020_on_hansen10pct_baseline_ha == pytest.approx(
        result.metrics.forest_baseline_hansen10pct_2000_ha
    )


def test_pre_2021_loss_excludes_pixel_from_baseline(tmp_path: Path) -> None:
    """A pixel with canopy>=threshold but Hansen-detected loss before 2021 is not baseline
    forest at all (it was already cleared before the 2020 cutoff), so post-2021 "loss" on that
    same pixel must not double-count -- distinct from `jrc_post2020_loss`'s baseline, which reads
    a single already-as-of-2020 categorical layer and has no separate pre-cutoff-loss exclusion."""
    treecover = np.full((4, 4), 50, dtype=np.uint8)
    lossyear = np.zeros((4, 4), dtype=np.uint8)
    lossyear[0, 0] = 15  # pre-2021 loss: pixel should be excluded from the baseline entirely
    lossyear[1, 1] = 21  # post-2020 loss on genuine baseline forest

    result = _run_analysis(tmp_path, treecover=treecover, lossyear=lossyear)

    assert result.metrics.forest_baseline_hansen10pct_2000_ha > 0.0
    assert result.metrics.forest_loss_post_2020_on_hansen10pct_baseline_ha > 0.0
    full_baseline = _run_analysis(
        tmp_path,
        treecover=np.full((4, 4), 50, dtype=np.uint8),
        lossyear=np.zeros((4, 4), dtype=np.uint8),
        output_name="full_baseline",
    )
    assert (
        result.metrics.forest_baseline_hansen10pct_2000_ha
        < full_baseline.metrics.forest_baseline_hansen10pct_2000_ha
    )


def test_loss_after_end_year_is_excluded(tmp_path: Path) -> None:
    result = _run_analysis(
        tmp_path,
        treecover=np.full((4, 4), 50, dtype=np.uint8),
        lossyear=np.full((4, 4), 26, dtype=np.uint8),
        requested_end_year=2025,
        latest_available_year=2026,
    )
    assert result.metrics.forest_loss_post_2020_on_hansen10pct_baseline_ha == 0.0


def test_canopy_threshold_is_configurable(tmp_path: Path) -> None:
    treecover = np.full((4, 4), 15, dtype=np.uint8)
    lossyear = np.zeros((4, 4), dtype=np.uint8)

    admitted = _run_analysis(
        tmp_path,
        treecover=treecover,
        lossyear=lossyear,
        canopy_threshold_percent=10,
        output_name="admitted",
    )
    excluded = _run_analysis(
        tmp_path,
        treecover=treecover,
        lossyear=lossyear,
        canopy_threshold_percent=20,
        output_name="excluded",
    )
    assert admitted.metrics.forest_baseline_hansen10pct_2000_ha > 0.0
    assert excluded.metrics.forest_baseline_hansen10pct_2000_ha == 0.0


def test_nodata_gap_codes_are_hansen_specific_not_jrc(tmp_path: Path) -> None:
    nodata = 255
    treecover = np.full((4, 4), 50, dtype=np.uint8)
    lossyear = np.zeros((4, 4), dtype=np.uint8)
    treecover[0, 0] = nodata
    lossyear[0, 1] = nodata

    result = _run_analysis(tmp_path, treecover=treecover, lossyear=lossyear, nodata=nodata)

    gap_codes = {gap["code"] for gap in result.evidence_gaps}
    assert "hansen_treecover2000_nodata_inside_aoi" in gap_codes
    assert "hansen_nodata_inside_aoi" in gap_codes
    assert "jrc_nodata_inside_aoi" not in gap_codes


def test_repeated_execution_produces_identical_metrics_and_artifacts(tmp_path: Path) -> None:
    treecover = np.full((4, 4), 50, dtype=np.uint8)
    lossyear = np.zeros((4, 4), dtype=np.uint8)
    lossyear[2, 2] = 21

    first = _run_analysis(tmp_path, treecover=treecover, lossyear=lossyear, output_name="repeat_a")
    second = _run_analysis(tmp_path, treecover=treecover, lossyear=lossyear, output_name="repeat_b")

    assert first.metrics == second.metrics
    assert first.summary_path.read_bytes() == second.summary_path.read_bytes()
    assert first.loss_mask_path.read_bytes() == second.loss_mask_path.read_bytes()
    assert first.baseline_mask_path.read_bytes() == second.baseline_mask_path.read_bytes()
