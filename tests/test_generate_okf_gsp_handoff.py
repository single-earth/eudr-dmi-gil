from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_bounds

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scripts.generate_okf_gsp_handoff import build_handoff  # noqa: E402


def _run_cli(args: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    src_path = str(REPO_ROOT / "src")
    env = dict(env)
    env["PYTHONPATH"] = src_path + (":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    return subprocess.run(
        [sys.executable, "-m", "eudr_dmi_gil.reports.cli", *args],
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )


def _write_test_raster(path: Path, data, transform, crs: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(data, 1)


def _generate_bundle(tmp_path: Path) -> Path:
    evidence_root = tmp_path / "evidence"
    env = os.environ.copy()
    env["EUDR_DMI_EVIDENCE_ROOT"] = str(evidence_root)
    env["EUDR_DMI_GENERATED_AT_UTC"] = "2026-07-31T00:00:00+00:00"

    aoi_path = tmp_path / "aoi.geojson"
    bounds = (-0.02, -0.02, 0.02, 0.02)
    aoi_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"country": "Ghana"},
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
    _write_test_raster(jrc_path, np.ones((10, 10), dtype=np.uint8), transform, "EPSG:4326")
    _write_test_raster(loss_path, np.zeros((10, 10), dtype=np.uint8), transform, "EPSG:4326")

    proc = _run_cli(
        [
            "--aoi-id",
            "handoff_smoke",
            "--aoi-geojson",
            str(aoi_path),
            "--bundle-id",
            "bundle-handoff-smoke",
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
        ],
        env=env,
    )
    assert proc.returncode == 0, proc.stderr
    return evidence_root / "2026-07-31" / "bundle-handoff-smoke"


def test_handoff_self_verifies_and_matches_disk(tmp_path: Path) -> None:
    bundle_dir = _generate_bundle(tmp_path)
    handoff = build_handoff(
        bundle_dir=bundle_dir,
        aoi_id="handoff_smoke",
        task_bundle_id="handoff-smoke-task",
        commodity="none",
        country="Ghana",
        country_code="GH",
        region="Test",
        generation_command="test",
        counterpart_repo="single-earth/eudr-dmi-gil",
        extra_datasets=[],
    )
    assert handoff["schema_version"] == "okf-gsp-evidence-handoff-v1"
    assert handoff["report_page_count"] == 12
    for artifact in handoff["artifacts"]:
        abs_path = bundle_dir / artifact["path"]
        assert abs_path.is_file()
        from eudr_dmi_gil.reports.bundle import compute_sha256

        assert compute_sha256(abs_path) == artifact["sha256"]


def test_handoff_fails_on_modified_artifact(tmp_path: Path) -> None:
    bundle_dir = _generate_bundle(tmp_path)
    manifest_path = bundle_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    tampered_relpath = manifest["artifacts"][0]["relpath"]
    tampered_path = bundle_dir / tampered_relpath
    tampered_path.write_bytes(tampered_path.read_bytes() + b"tampered")

    import pytest

    with pytest.raises(ValueError, match="sha256 mismatch"):
        build_handoff(
            bundle_dir=bundle_dir,
            aoi_id="handoff_smoke",
            task_bundle_id="handoff-smoke-task",
            commodity="none",
            country="Ghana",
            country_code="GH",
            region="Test",
            generation_command="test",
            counterpart_repo="single-earth/eudr-dmi-gil",
            extra_datasets=[],
        )


def test_handoff_fails_on_missing_artifact(tmp_path: Path) -> None:
    bundle_dir = _generate_bundle(tmp_path)
    manifest_path = bundle_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    removed_relpath = manifest["artifacts"][-1]["relpath"]
    (bundle_dir / removed_relpath).unlink()

    import pytest

    with pytest.raises(FileNotFoundError):
        build_handoff(
            bundle_dir=bundle_dir,
            aoi_id="handoff_smoke",
            task_bundle_id="handoff-smoke-task",
            commodity="none",
            country="Ghana",
            country_code="GH",
            region="Test",
            generation_command="test",
            counterpart_repo="single-earth/eudr-dmi-gil",
            extra_datasets=[],
        )
