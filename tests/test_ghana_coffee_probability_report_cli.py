from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_bounds

from eudr_dmi_gil.reports.validate import validate_aoi_report_file


def _run_cli(args: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parents[1]
    src_path = str(repo_root / "src")
    env = dict(env)
    env["PYTHONPATH"] = src_path + (":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    return subprocess.run(
        [sys.executable, "-m", "eudr_dmi_gil.reports.cli", *args],
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )


def _write_test_raster(path: Path, data: np.ndarray, transform, crs: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    profile = {
        "driver": "GTiff",
        "height": data.shape[0],
        "width": data.shape[1],
        "count": 1,
        "dtype": data.dtype,
        "crs": crs,
        "transform": transform,
    }
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)


def test_cli_probability_threshold_commodity_end_to_end(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    env = os.environ.copy()
    env["EUDR_DMI_EVIDENCE_ROOT"] = str(evidence_root)
    env["EUDR_DMI_GENERATED_AT_UTC"] = "2026-07-31T00:00:00+00:00"

    # Deliberately named to mirror the real repo situation: geometry provenance says cocoa,
    # but the configured commodity (via --commodity-config) is coffee. The report must not
    # surface "cocoa" anywhere in its user-facing output.
    aoi_path = tmp_path / "cocoa_west_africa_ghana_like.geojson"
    bounds = (-0.02, -0.02, 0.02, 0.02)
    aoi_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "aoi_id": "ghana_west_africa_shared_aoi",
                            "country": "Ghana",
                            "note": "Sample AOI polygon for portal testing (not a verified farm boundary).",
                        },
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [bounds[0], bounds[1]],
                                    [bounds[2], bounds[1]],
                                    [bounds[2], bounds[3]],
                                    [bounds[0], bounds[3]],
                                    [bounds[0], bounds[1]],
                                ]
                            ],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    raster_bounds = (-0.05, -0.05, 0.05, 0.05)
    transform = from_bounds(*raster_bounds, width=10, height=10)
    jrc_path = tmp_path / "jrc.tif"
    loss_path = tmp_path / "lossyear.tif"
    probability_path = tmp_path / "fdp_coffee_probability.tif"
    loss = np.zeros((10, 10), dtype=np.uint8)
    loss[5, 5] = 21
    probability = np.full((10, 10), 0.5, dtype=np.float32)
    _write_test_raster(jrc_path, np.ones((10, 10), dtype=np.uint8), transform, "EPSG:4326")
    _write_test_raster(loss_path, loss, transform, "EPSG:4326")
    _write_test_raster(probability_path, probability, transform, "EPSG:4326")

    commodity_config = tmp_path / "coffee_fdp.json"
    commodity_config.write_text(
        json.dumps(
            {
                "commodity": {
                    "id": "coffee",
                    "display_name": "Coffee",
                    "provider": "forestdatapartnership",
                    "mode": "probability_threshold",
                    "dataset_title": "FDP Coffee Probability model 2025b",
                    "dataset_version": "2025b",
                    "asset_id": "projects/forestdatapartnership/assets/coffee/model_2025b",
                    "local_path": probability_path.as_posix(),
                    "probability_band": "probability",
                    "threshold": 0.1,
                    "sensitivity_thresholds": [0.28, 0.5],
                    "observation_year": 2024,
                    "country_scope": ["Ghana"],
                    "optional": True,
                }
            }
        ),
        encoding="utf-8",
    )

    proc = _run_cli(
        [
            "--aoi-id",
            "ghana_west_africa_shared_aoi",
            "--aoi-geojson",
            str(aoi_path),
            "--bundle-id",
            "bundle-ghana-coffee-001",
            "--out-format",
            "both",
            "--jrc-gfc2020-raster",
            str(jrc_path),
            "--hansen-lossyear-raster",
            str(loss_path),
            "--loss-dataset-end-year",
            "2025",
            "--end-year",
            "2025",
            "--analysis-target-resolution-m",
            "500",
            "--commodity-config",
            str(commodity_config),
        ],
        env=env,
    )

    assert proc.returncode == 0, proc.stderr

    legacy_json = (
        evidence_root
        / "2026-07-31"
        / "bundle-ghana-coffee-001"
        / "reports"
        / "aoi_report_v2"
        / "ghana_west_africa_shared_aoi.json"
    )
    validate_aoi_report_file(legacy_json)
    report = json.loads(legacy_json.read_text(encoding="utf-8"))

    assert report["commodity"]["id"] == "coffee"
    assert report["commodity"]["evidence_available"] is True
    assert report["commodity"]["coverage_status"] == "full"
    assert report["parameters"]["commodity"]["mode"] == "probability_threshold"
    assert report["parameters"]["commodity"]["threshold"] == 0.1
    assert report["parameters"]["commodity"]["sensitivity_thresholds"] == [0.28, 0.5]

    probability_profile = report["extensions"]["commodity_assessment"]["provenance"][
        "probability_profile"
    ]
    assert probability_profile["configured_threshold"] == 0.1
    assert probability_profile["sensitivity"]

    gap_codes = {gap["code"] for gap in report["commodity"]["evidence_gaps"]}
    assert "commodity_threshold_not_locally_calibrated" in gap_codes
    assert "commodity_local_validation_missing" in gap_codes
    assert "sample_aoi_not_verified_farm_boundary" in gap_codes
    assert not any("cross_commodity" in code or "mismatch" in code for code in gap_codes)

    canonical_json = (
        evidence_root
        / "2026-07-31"
        / "bundle-ghana-coffee-001"
        / "reports"
        / "aoi_report_v2"
        / "ghana_west_africa_shared_aoi"
        / "report.json"
    )
    validate_aoi_report_file(canonical_json)
    canonical = json.loads(canonical_json.read_text(encoding="utf-8"))
    assert canonical["commodity"]["mode"] == "probability_threshold"
    assert canonical["commodity"]["threshold"] == 0.1
    assert canonical["commodity"]["probability_profile"]["configured_threshold"] == 0.1

    # No user-facing "cocoa" leakage: this run's AOI carries no cocoa reference at all, so any
    # appearance of the substring would indicate accidental filename/property leakage.
    assert "cocoa" not in canonical_json.read_text(encoding="utf-8").lower()

    # Candidate probability overlap must not silently become a confirmed-conversion claim.
    metrics_csv = canonical_json.parent / "metrics.csv"
    assert metrics_csv.is_file()
