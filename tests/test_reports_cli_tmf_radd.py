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


def _write_aoi(path: Path, bounds: tuple[float, float, float, float]) -> None:
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"aoi_id": "wood_fixture_aoi"},
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


def _base_rasters(tmp_path: Path) -> dict[str, Path]:
    bounds = (-0.05, -0.05, 0.05, 0.05)
    transform = from_bounds(*bounds, width=10, height=10)
    jrc_path = tmp_path / "jrc.tif"
    loss_path = tmp_path / "lossyear.tif"
    loss = np.zeros((10, 10), dtype=np.uint8)
    loss[5, 5] = 21
    _write_test_raster(jrc_path, np.ones((10, 10), dtype=np.uint8), transform, "EPSG:4326")
    _write_test_raster(loss_path, loss, transform, "EPSG:4326")
    return {"jrc": jrc_path, "loss": loss_path, "transform": transform, "bounds": bounds}


def _tmf_rasters(tmp_path: Path, transform, bounds) -> dict[str, Path]:
    defo_path = tmp_path / "tmf_defo.tif"
    deg_path = tmp_path / "tmf_deg.tif"
    defo = np.zeros((10, 10), dtype=np.int16)
    defo[5, 5] = 2022
    deg = np.zeros((10, 10), dtype=np.int16)
    deg[7, 7] = 2023
    _write_test_raster(defo_path, defo, transform, "EPSG:4326")
    _write_test_raster(deg_path, deg, transform, "EPSG:4326")
    return {"defo": defo_path, "deg": deg_path}


def _radd_rasters(tmp_path: Path, transform, bounds) -> dict[str, Path]:
    alert_path = tmp_path / "radd_alert.tif"
    date_path = tmp_path / "radd_date.tif"
    alert = np.zeros((10, 10), dtype=np.int32)
    alert[2, 2] = 3
    alert[3, 3] = 2
    date = np.zeros((10, 10), dtype=np.int32)
    date[2, 2] = 24050
    date[3, 3] = 24010
    _write_test_raster(alert_path, alert, transform, "EPSG:4326")
    _write_test_raster(date_path, date, transform, "EPSG:4326")
    return {"alert": alert_path, "date": date_path}


def test_cli_tmf_and_radd_produce_wood_evidence_state(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    env = os.environ.copy()
    env["EUDR_DMI_EVIDENCE_ROOT"] = str(evidence_root)
    env["EUDR_DMI_GENERATED_AT_UTC"] = "2026-08-15T00:00:00+00:00"

    aoi_path = tmp_path / "aoi.geojson"
    base = _base_rasters(tmp_path)
    _write_aoi(aoi_path, base["bounds"])
    tmf = _tmf_rasters(tmp_path, base["transform"], base["bounds"])
    radd = _radd_rasters(tmp_path, base["transform"], base["bounds"])

    proc = _run_cli(
        [
            "--aoi-id",
            "liberia_wood_fixture",
            "--aoi-geojson",
            str(aoi_path),
            "--bundle-id",
            "bundle-wood-001",
            "--out-format",
            "json",
            "--evidence-only-assessment",
            "--jrc-gfc2020-raster",
            str(base["jrc"]),
            "--hansen-lossyear-raster",
            str(base["loss"]),
            "--loss-dataset-end-year",
            "2025",
            "--end-year",
            "2025",
            "--analysis-target-resolution-m",
            "500",
            "--tmf-deforestation-raster",
            str(tmf["defo"]),
            "--tmf-degradation-raster",
            str(tmf["deg"]),
            "--radd-alert-raster",
            str(radd["alert"]),
            "--radd-date-raster",
            str(radd["date"]),
            "--radd-acquired-at",
            "2026-08-15T00:00:00Z",
            "--radd-geography",
            "africa",
        ],
        env=env,
    )

    assert proc.returncode == 0, proc.stderr
    report_json = (
        evidence_root
        / "2026-08-15"
        / "bundle-wood-001"
        / "reports"
        / "aoi_report_v2"
        / "liberia_wood_fixture.json"
    )
    validate_aoi_report_file(report_json)
    report = json.loads(report_json.read_text(encoding="utf-8"))

    # Deforestation and degradation are separate metrics, never collapsed.
    metrics = report["metrics"]
    assert metrics["tmf_deforestation_2021_2025_ha"]["value"] > 0.0
    assert metrics["tmf_degradation_2021_2025_ha"]["value"] > 0.0
    assert metrics["radd_confirmed_alert_area_ha"]["value"] > 0.0
    assert metrics["radd_low_confidence_alert_area_ha"]["value"] > 0.0

    # Provider semantics preserved verbatim: no relabeling as illegal logging or shipment proof.
    report_text = json.dumps(report)
    assert "illegal_logging" not in report_text
    assert "confirmed_eudr_deforestation" not in report_text
    assert "non_compliant" not in report_text

    change_observers = report["extensions"]["change_observers"]
    assert set(change_observers) == {"hansen", "jrc_tmf_deforestation", "jrc_tmf_degradation", "radd"}
    assert change_observers["radd"]["role"] == "alert"
    assert change_observers["jrc_tmf_degradation"]["role"] == "degradation_change"

    wood_state = report["extensions"]["wood_evidence_state"]
    assert wood_state["deforestation"]["state"] == "detected"
    assert set(wood_state["deforestation"]["area_by_source_ha"]) == {
        "hansen",
        "jrc_tmf_deforestation",
    }
    assert "probability" not in json.dumps(wood_state)

    layers = report["map_assets"]["layers"]
    assert layers["tmf_deforestation"] is not None
    assert layers["tmf_degradation"] is not None
    assert layers["radd_confirmed"] is not None
    assert layers["radd_provisional"] is not None
    assert layers["jrc_forest_baseline"] is not None


def test_cli_radd_requires_acquisition_timestamp(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    env = os.environ.copy()
    env["EUDR_DMI_EVIDENCE_ROOT"] = str(evidence_root)
    env["EUDR_DMI_GENERATED_AT_UTC"] = "2026-08-15T00:00:00+00:00"

    aoi_path = tmp_path / "aoi.geojson"
    base = _base_rasters(tmp_path)
    _write_aoi(aoi_path, base["bounds"])
    radd = _radd_rasters(tmp_path, base["transform"], base["bounds"])

    proc = _run_cli(
        [
            "--aoi-id",
            "missing_timestamp_fixture",
            "--aoi-geojson",
            str(aoi_path),
            "--bundle-id",
            "bundle-wood-002",
            "--out-format",
            "json",
            "--evidence-only-assessment",
            "--radd-alert-raster",
            str(radd["alert"]),
            "--radd-date-raster",
            str(radd["date"]),
        ],
        env=env,
    )

    assert proc.returncode != 0
    assert "radd-acquired-at" in proc.stderr


def test_cli_without_tmf_radd_args_is_unaffected(tmp_path: Path) -> None:
    """Optional observers must not change old results merely because the software knows about them."""
    evidence_root = tmp_path / "evidence"
    env = os.environ.copy()
    env["EUDR_DMI_EVIDENCE_ROOT"] = str(evidence_root)
    env["EUDR_DMI_GENERATED_AT_UTC"] = "2026-08-15T00:00:00+00:00"

    aoi_path = tmp_path / "aoi.geojson"
    base = _base_rasters(tmp_path)
    _write_aoi(aoi_path, base["bounds"])

    proc = _run_cli(
        [
            "--aoi-id",
            "no_wood_observers_fixture",
            "--aoi-geojson",
            str(aoi_path),
            "--bundle-id",
            "bundle-no-wood-001",
            "--out-format",
            "json",
            "--evidence-only-assessment",
            "--jrc-gfc2020-raster",
            str(base["jrc"]),
            "--hansen-lossyear-raster",
            str(base["loss"]),
            "--loss-dataset-end-year",
            "2025",
            "--end-year",
            "2025",
            "--analysis-target-resolution-m",
            "500",
        ],
        env=env,
    )

    assert proc.returncode == 0, proc.stderr
    report_json = (
        evidence_root
        / "2026-08-15"
        / "bundle-no-wood-001"
        / "reports"
        / "aoi_report_v2"
        / "no_wood_observers_fixture.json"
    )
    validate_aoi_report_file(report_json)
    report = json.loads(report_json.read_text(encoding="utf-8"))

    assert "change_observers" not in report["extensions"]
    assert "wood_evidence_state" not in report["extensions"]
    assert "jrc_tmf_change" not in report["extensions"]
    assert "radd_alerts" not in report["extensions"]
    assert not any(k.startswith("tmf_") for k in report["metrics"])
    assert not any(k.startswith("radd_") for k in report["metrics"])
