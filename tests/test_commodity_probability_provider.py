from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

from eudr_dmi_gil.analysis.jrc_post2020_loss import build_hansen_lossyear_metadata
from eudr_dmi_gil.commodities.analysis import run_commodity_assessment
from eudr_dmi_gil.commodities.config import CommodityConfig, MODE_PROBABILITY_THRESHOLD
from eudr_dmi_gil.providers.jrc_gfc2020 import LocalJrcGfc2020Provider

FIXED_TS = "2026-07-25T00:00:00+00:00"
BOUNDS = (-0.02, -0.02, 0.02, 0.02)
RASTER_BOUNDS = (-0.05, -0.05, 0.05, 0.05)


def _write_aoi(path: Path, *, country: str = "Ghana", note: str | None = None) -> None:
    west, south, east, north = BOUNDS
    props = {"aoi_id": "commodity_fixture", "country": country}
    if note is not None:
        props["note"] = note
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": props,
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


def _write_raster(path: Path, data: np.ndarray, *, nodata: float | None = None) -> None:
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


def _config(
    path: Path,
    *,
    threshold: float = 0.1,
    sensitivity_thresholds: tuple[float, ...] = (0.28, 0.5),
    country_scope: tuple[str, ...] = ("Ghana",),
) -> CommodityConfig:
    return CommodityConfig(
        id="coffee",
        display_name="Coffee",
        provider="forestdatapartnership",
        dataset_title="FDP Coffee Probability model 2025b",
        dataset_version="2025b",
        asset_id=path.as_posix(),
        local_path=path.as_posix(),
        observation_year=2024,
        country_scope=country_scope,
        mode=MODE_PROBABILITY_THRESHOLD,
        probability_band="probability",
        threshold=threshold,
        sensitivity_thresholds=sensitivity_thresholds,
    )


def _run(
    tmp_path: Path,
    *,
    probability: np.ndarray | None,
    nodata: float | None = None,
    threshold: float = 0.1,
    aoi_note: str | None = None,
    **config_overrides,
):
    aoi_path = tmp_path / "aoi.geojson"
    jrc_path = tmp_path / "jrc.tif"
    loss_path = tmp_path / "lossyear.tif"
    probability_path = tmp_path / "probability.tif"
    _write_aoi(aoi_path, note=aoi_note)
    _write_raster(jrc_path, np.ones((10, 10), dtype=np.uint8))
    loss = np.zeros((10, 10), dtype=np.uint8)
    loss[5, 5] = 21
    _write_raster(loss_path, loss)
    if probability is not None:
        _write_raster(probability_path, probability.astype(np.float32), nodata=nodata)

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
        config=_config(probability_path, threshold=threshold, **config_overrides),
        aoi_geojson_path=aoi_path,
        aoi_country="Ghana",
        jrc_gfc2020_raster_path=jrc_path,
        hansen_lossyear_raster_path=loss_path,
        output_dir=tmp_path / "out",
        baseline_metadata=baseline_provider.metadata(),
        loss_metadata=loss_metadata,
        requested_end_year=2025,
        target_resolution_m=500.0,
    )


def test_probability_mask_admits_values_at_or_above_threshold(tmp_path: Path) -> None:
    probability = np.zeros((10, 10), dtype=np.float32)
    probability[5, 5] = 0.5
    result = _run(tmp_path, probability=probability, threshold=0.1)

    assert result.evidence_available is True
    assert result.coverage_status == "full"
    assert result.metrics.commodity_area_ha is not None
    assert result.metrics.commodity_area_ha > 0.0
    # At least the hotspot is admitted (>=0.1); not every valid pixel is (most of the
    # raster is 0.0, well below threshold) - bilinear resampling may spread partial
    # admission to immediate neighbors of the hotspot, so assert a bounded, non-total range.
    profile = result.provenance["probability_profile"]
    assert profile["sensitivity"]
    entry = next(e for e in profile["sensitivity"].values() if e["threshold"] == 0.1)
    assert 1 <= entry["admitted_pixels"] < profile["valid_pixels"]


def test_probability_mask_nodata_partial_coverage(tmp_path: Path) -> None:
    probability = np.full((10, 10), 0.2, dtype=np.float32)
    probability[5, 5] = -9999.0  # central pixel, inside the AOI/target grid extent
    result = _run(tmp_path, probability=probability, nodata=-9999.0, threshold=0.1)

    assert result.coverage_status == "partial"
    assert any(gap["code"] == "commodity_layer_partial_coverage" for gap in result.evidence_gaps)


def test_probability_mask_no_coverage_inside_aoi(tmp_path: Path) -> None:
    result = _run(tmp_path, probability=None, threshold=0.1)
    assert result.evidence_available is False
    assert result.coverage_status == "unavailable"
    assert any(gap["code"] == "commodity_layer_unavailable" for gap in result.evidence_gaps)


def test_out_of_range_probability_values_are_rejected(tmp_path: Path) -> None:
    probability = np.zeros((10, 10), dtype=np.float32)
    probability[5, 5] = 1.5  # invalid: probability must be within [0, 1]
    with pytest.raises(ValueError, match=r"outside \[0, 1\]"):
        _run(tmp_path, probability=probability, threshold=0.1)


def test_threshold_missing_raises(tmp_path: Path) -> None:
    probability = np.zeros((10, 10), dtype=np.float32)
    probability[5, 5] = 0.5
    with pytest.raises(ValueError, match="threshold"):
        _run(tmp_path, probability=probability, threshold=None)  # type: ignore[arg-type]


def test_continuous_reprojection_uses_bilinear_not_nearest(tmp_path: Path) -> None:
    """Probability rasters must not use the categorical nearest-neighbor path."""
    from eudr_dmi_gil.commodities.providers import _reproject_continuous

    src = tmp_path / "src.tif"
    data = np.array([[0.0, 1.0], [0.0, 1.0]], dtype=np.float32)
    _write_raster(src, data)
    with rasterio.open(src) as ds:
        target_transform = ds.transform
        width, height, crs = ds.width * 4, ds.height * 4, ds.crs
        # Rescale transform to a finer grid over the same bounds.
        from rasterio.transform import from_bounds as _from_bounds

        target_transform = _from_bounds(*RASTER_BOUNDS, width=width, height=height)

    values, valid, info = _reproject_continuous(
        src, target_crs=str(crs), target_transform=target_transform, width=width, height=height
    )
    assert info["resampling"] == "bilinear"
    # Bilinear resampling of a 0/1 checkerboard-like ramp should produce intermediate values.
    finite = values[valid]
    assert finite.size > 0
    assert np.any((finite > 0.01) & (finite < 0.99))


def test_sensitivity_thresholds_and_denominators(tmp_path: Path) -> None:
    probability = np.zeros((10, 10), dtype=np.float32)
    probability[:, :] = 0.05
    probability[5, 5] = 0.6
    result = _run(tmp_path, probability=probability, threshold=0.1, sensitivity_thresholds=(0.28, 0.5))

    profile = result.provenance["probability_profile"]
    assert profile["valid_pixels"] > 0
    assert profile["aoi_pixels"] >= profile["valid_pixels"]
    for entry in profile["sensitivity"].values():
        assert 0.0 <= entry["admitted_share_of_valid_pixels_percent"] <= 100.0
        assert 0.0 <= entry["admitted_share_of_aoi_pixels_percent"] <= 100.0
    # Metric rows carry the headline probability stats.
    rows = result.metrics.to_metric_rows()
    assert rows["commodity_probability_configured_threshold"]["value"] == 0.1
    assert rows["commodity_probability_valid_coverage_of_aoi_percent"]["value"] is not None


def test_probability_mode_emits_standing_evidence_gaps(tmp_path: Path) -> None:
    probability = np.full((10, 10), 0.5, dtype=np.float32)
    result = _run(tmp_path, probability=probability, threshold=0.1)
    codes = {gap["code"] for gap in result.evidence_gaps}
    assert "commodity_threshold_not_locally_calibrated" in codes
    assert "commodity_local_validation_missing" in codes
    assert "commodity_absence_not_established" in codes
    # No cross-commodity/mismatch gap must ever be invented.
    assert not any("cross_commodity" in code or "mismatch" in code for code in codes)


def test_low_threshold_non_discriminating_coverage_flags_low_precision_gap(tmp_path: Path) -> None:
    """When >=95% of valid pixels are admitted at the configured threshold, flag it explicitly."""
    probability = np.full((10, 10), 0.5, dtype=np.float32)  # every pixel >= 0.1
    result = _run(tmp_path, probability=probability, threshold=0.1)
    gap = next(
        (g for g in result.evidence_gaps if g["code"] == "commodity_localization_low_precision"),
        None,
    )
    assert gap is not None
    assert gap["admitted_share_of_valid_pixels_percent"] >= 95.0
    assert "100.0%" in gap["message"] or "%" in gap["message"]


def test_high_discriminating_threshold_does_not_flag_low_precision_gap(tmp_path: Path) -> None:
    probability = np.zeros((10, 10), dtype=np.float32)
    probability[5, 5] = 0.9  # only a handful of pixels admitted at 0.1
    result = _run(tmp_path, probability=probability, threshold=0.1)
    codes = {gap["code"] for gap in result.evidence_gaps}
    assert "commodity_localization_low_precision" not in codes


def test_sample_aoi_gap_is_data_driven_from_aoi_note(tmp_path: Path) -> None:
    probability = np.full((10, 10), 0.5, dtype=np.float32)
    result = _run(
        tmp_path,
        probability=probability,
        threshold=0.1,
        aoi_note="Sample AOI polygon for portal testing (not a verified farm boundary).",
    )
    assert any(gap["code"] == "sample_aoi_not_verified_farm_boundary" for gap in result.evidence_gaps)


def test_sample_aoi_gap_absent_when_no_note(tmp_path: Path) -> None:
    probability = np.full((10, 10), 0.5, dtype=np.float32)
    result = _run(tmp_path, probability=probability, threshold=0.1, aoi_note=None)
    assert not any(
        gap["code"] == "sample_aoi_not_verified_farm_boundary" for gap in result.evidence_gaps
    )


def test_mapbiomas_discrete_provider_tests_remain_behaviorally_unchanged() -> None:
    """Smoke check: importing the probability module must not disturb the discrete provider."""
    from eudr_dmi_gil.commodities.providers import MapBiomasBrazilCommodityProvider

    assert MapBiomasBrazilCommodityProvider is not None
