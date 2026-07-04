from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

from eudr.evidence.dataset_registry import DATASET_REGISTRY, DatasetDefinition, resolve_end_year
from eudr.evidence.gfc2020_baseline import compute_gfc2020_baseline
from eudr.evidence.post2020_deforestation import build_post2020_evidence, main
from eudr_dmi_gil.geo.aoi_area import compute_aoi_geodesic_area_ha
from eudr_dmi_gil.geo.forest_area_core import pixel_area_m2_raster


def _write_raster(path: Path, data: np.ndarray, transform, crs: str = "EPSG:4326") -> None:
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


def _write_aoi(path: Path, bounds: tuple[float, float, float, float]) -> None:
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


@pytest.fixture()
def evidence_fixture(tmp_path: Path):
    bounds = (24.0, 59.0, 24.02, 59.02)
    transform = from_bounds(*bounds, width=2, height=2)
    aoi_path = tmp_path / "aoi.geojson"
    gfc_path = tmp_path / "gfc2020.tif"
    loss_path = tmp_path / "lossyear.tif"
    _write_aoi(aoi_path, bounds)

    # Forest pixels: top-left, top-right, bottom-right.
    # Hansen lossyear code 20 is pre-cutoff 2020 and must not count.
    # Code 21 is 2021 and counts. Code 26 is future availability and is excluded for auto=2025.
    gfc2020 = np.array([[1, 1], [0, 1]], dtype=np.uint8)
    lossyear = np.array([[20, 21], [22, 26]], dtype=np.uint8)
    _write_raster(gfc_path, gfc2020, transform)
    _write_raster(loss_path, lossyear, transform)
    return {
        "aoi": aoi_path,
        "gfc": gfc_path,
        "loss": loss_path,
        "transform": transform,
        "gfc_array": gfc2020,
        "loss_array": lossyear,
    }


def test_aoi_area_and_gfc2020_baseline_masking(evidence_fixture) -> None:
    baseline = compute_gfc2020_baseline(
        aoi_geojson_path=evidence_fixture["aoi"],
        gfc2020_raster_path=evidence_fixture["gfc"],
    )
    expected_aoi_area, _ = compute_aoi_geodesic_area_ha(evidence_fixture["aoi"])
    pixel_area_m2 = pixel_area_m2_raster(
        evidence_fixture["transform"],
        height=2,
        width=2,
        crs="EPSG:4326",
    )
    expected_forest_ha = float(
        np.sum(pixel_area_m2[evidence_fixture["gfc_array"] == 1], dtype=np.float64)
    ) / 10_000.0

    assert baseline.aoi_area_ha == round(expected_aoi_area, 6)
    assert baseline.gfc2020_forest_area_ha == round(expected_forest_ha, 6)
    assert baseline.gfc2020_forest_share == round(expected_forest_ha / expected_aoi_area, 6)


def test_auto_end_year_and_post_2020_filtering(evidence_fixture, monkeypatch) -> None:
    monkeypatch.setenv("EUDR_EVIDENCE_GENERATED_AT_UTC", "2026-07-04T00:00:00Z")
    payload = build_post2020_evidence(
        aoi_geojson_path=evidence_fixture["aoi"],
        gfc2020_raster_path=evidence_fixture["gfc"],
        hansen_lossyear_raster_path=evidence_fixture["loss"],
        out_path=None,
        start_year=2021,
        requested_end_year="auto",
        include_tmf=False,
        include_radd=False,
        include_sentinel_confirmation=False,
        min_report_area_ha=0.01,
    )

    yearly = payload["metrics_by_year"]["hansen_loss_inside_gfc2020_ha"]
    assert payload["inputs"]["resolved_end_year"] == 2025
    assert payload["inputs"]["end_year_resolution_mode"] == (
        "auto_latest_available_complete_evidence_year"
    )
    assert "2020" not in yearly
    assert set(yearly) == {"2021", "2022", "2023", "2024", "2025"}
    assert yearly["2021"] > 0.0
    assert yearly["2022"] == 0.0
    assert payload["metrics"]["hansen_loss_2021_resolved_end_year_inside_gfc2020_ha"] == (
        yearly["2021"]
    )


def test_explicit_end_year_pinning_writes_deterministic_json(
    evidence_fixture,
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("EUDR_EVIDENCE_GENERATED_AT_UTC", "2026-07-04T00:00:00Z")
    out_path = tmp_path / "metrics.json"
    payload = build_post2020_evidence(
        aoi_geojson_path=evidence_fixture["aoi"],
        gfc2020_raster_path=evidence_fixture["gfc"],
        hansen_lossyear_raster_path=evidence_fixture["loss"],
        out_path=out_path,
        start_year=2021,
        requested_end_year="2025",
        include_tmf=False,
        include_radd=False,
        include_sentinel_confirmation=False,
        min_report_area_ha=0.01,
    )

    written = json.loads(out_path.read_text(encoding="utf-8"))
    assert written == payload
    assert payload["inputs"]["requested_end_year"] == "2025"
    assert payload["inputs"]["resolved_end_year"] == 2025
    assert payload["inputs"]["end_year_resolution_mode"] == "user_specified"
    assert payload["dataset_versions"]["hansen_gfc"]["used_through_year"] == 2025


def test_cli_writes_metrics_file_with_auto_end_year(
    evidence_fixture,
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("EUDR_EVIDENCE_GENERATED_AT_UTC", "2026-07-04T00:00:00Z")
    out_path = tmp_path / "cli_metrics.json"

    exit_code = main(
        [
            "--aoi",
            evidence_fixture["aoi"].as_posix(),
            "--gfc2020-raster",
            evidence_fixture["gfc"].as_posix(),
            "--hansen-lossyear-raster",
            evidence_fixture["loss"].as_posix(),
            "--out",
            out_path.as_posix(),
            "--end-year",
            "auto",
            "--min-report-area-ha",
            "0.01",
        ]
    )

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["inputs"]["requested_end_year"] == "auto"
    assert payload["inputs"]["resolved_end_year"] == 2025
    assert payload["aoi_id"] == "fixture_aoi"


def test_requested_end_year_exceeding_mandatory_availability_raises(evidence_fixture) -> None:
    with pytest.raises(ValueError, match="exceeds latest available year"):
        build_post2020_evidence(
            aoi_geojson_path=evidence_fixture["aoi"],
            gfc2020_raster_path=evidence_fixture["gfc"],
            hansen_lossyear_raster_path=evidence_fixture["loss"],
            out_path=None,
            start_year=2021,
            requested_end_year="2026",
            include_tmf=False,
            include_radd=False,
            include_sentinel_confirmation=False,
            min_report_area_ha=0.01,
        )


def test_optional_layer_unavailable_for_requested_year_warning(monkeypatch) -> None:
    original = DATASET_REGISTRY["jrc_tmf"]
    DATASET_REGISTRY["jrc_tmf"] = DatasetDefinition(
        dataset_id=original.dataset_id,
        display_name=original.display_name,
        asset_id=original.asset_id,
        role=original.role,
        start_year=original.start_year,
        latest_available_year=2024,
        update_policy=original.update_policy,
        mandatory=False,
        notes=original.notes,
    )
    try:
        _, _, warnings = resolve_end_year(
            requested_end_year="2025",
            selected_datasets=[DATASET_REGISTRY["hansen_gfc"], DATASET_REGISTRY["jrc_tmf"]],
        )
    finally:
        DATASET_REGISTRY["jrc_tmf"] = original

    assert warnings
    assert warnings[0]["code"] == "optional_layer_unavailable_for_requested_year"


def test_optional_layer_used_through_year_is_capped_by_availability(
    evidence_fixture,
    monkeypatch,
) -> None:
    monkeypatch.setenv("EUDR_EVIDENCE_GENERATED_AT_UTC", "2026-07-04T00:00:00Z")
    original = DATASET_REGISTRY["jrc_tmf"]
    DATASET_REGISTRY["jrc_tmf"] = DatasetDefinition(
        dataset_id=original.dataset_id,
        display_name=original.display_name,
        asset_id=original.asset_id,
        role=original.role,
        start_year=original.start_year,
        latest_available_year=2024,
        update_policy=original.update_policy,
        mandatory=False,
        notes=original.notes,
    )
    try:
        payload = build_post2020_evidence(
            aoi_geojson_path=evidence_fixture["aoi"],
            gfc2020_raster_path=evidence_fixture["gfc"],
            hansen_lossyear_raster_path=evidence_fixture["loss"],
            out_path=None,
            start_year=2021,
            requested_end_year="2025",
            include_tmf=True,
            include_radd=False,
            include_sentinel_confirmation=False,
            min_report_area_ha=0.01,
        )
    finally:
        DATASET_REGISTRY["jrc_tmf"] = original

    assert payload["inputs"]["resolved_end_year"] == 2025
    assert payload["dataset_versions"]["jrc_tmf"]["latest_available_year"] == 2024
    assert payload["dataset_versions"]["jrc_tmf"]["used_through_year"] == 2024


def test_radd_unknown_year_warning_and_missing_agricultural_evidence_state(
    evidence_fixture,
    monkeypatch,
) -> None:
    monkeypatch.setenv("EUDR_EVIDENCE_GENERATED_AT_UTC", "2026-07-04T00:00:00Z")
    payload = build_post2020_evidence(
        aoi_geojson_path=evidence_fixture["aoi"],
        gfc2020_raster_path=evidence_fixture["gfc"],
        hansen_lossyear_raster_path=evidence_fixture["loss"],
        out_path=None,
        start_year=2021,
        requested_end_year="2025",
        include_tmf=False,
        include_radd=True,
        include_sentinel_confirmation=False,
        min_report_area_ha=0.01,
    )

    warning_codes = {warning["code"] for warning in payload["warnings"]}
    state = payload["evidence_state"]
    assert "optional_layer_year_unknown" in warning_codes
    assert payload["dataset_versions"]["radd"]["used_through_year"] is None
    assert state["post2020_disturbance_detected"] is True
    assert state["agricultural_conversion_evidence_present"] is False
    assert state["agricultural_conversion_evidence_missing"] is True
    assert state["eudr_interpretation_state"] == "insufficient_evidence"
    assert state["human_review_required"] is True
    assert "compliance_state" not in state
    assert "non_compliant" not in json.dumps(payload)


def test_small_detected_area_below_min_report_threshold(evidence_fixture, monkeypatch) -> None:
    monkeypatch.setenv("EUDR_EVIDENCE_GENERATED_AT_UTC", "2026-07-04T00:00:00Z")
    payload = build_post2020_evidence(
        aoi_geojson_path=evidence_fixture["aoi"],
        gfc2020_raster_path=evidence_fixture["gfc"],
        hansen_lossyear_raster_path=evidence_fixture["loss"],
        out_path=None,
        start_year=2021,
        requested_end_year="2025",
        include_tmf=False,
        include_radd=False,
        include_sentinel_confirmation=False,
        min_report_area_ha=10_000.0,
    )

    assert payload["metrics"]["union_post2020_disturbance_candidate_ha"] > 0.0
    assert payload["evidence_state"]["post2020_disturbance_detected"] is False
    assert payload["evidence_state"]["human_review_required"] is False
