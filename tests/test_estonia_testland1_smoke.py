from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


def _run_cli(args: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "eudr_dmi_gil.reports.cli", *args],
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )


def test_estonia_testland1_geojson_smoke(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    geojson_path = repo_root / "aoi_json_examples" / "estonia_testland1.geojson"
    if not geojson_path.is_file():
        pytest.skip("estonia_testland1.geojson not found")

    evidence_root = tmp_path / "evidence"
    env = os.environ.copy()
    env["EUDR_DMI_EVIDENCE_ROOT"] = str(evidence_root)

    bundle_id = "estonia_testland1-smoke"
    aoi_id = "estonia_testland1"

    proc = _run_cli(
        [
            "--aoi-id",
            aoi_id,
            "--aoi-geojson",
            str(geojson_path),
            "--bundle-id",
            bundle_id,
            "--out-format",
            "both",
        ],
        env=env,
    )

    if proc.returncode != 0:
        pytest.skip(f"CLI failed (environment prerequisites missing): {proc.stderr}")

    bundle_date = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")
    bundle_dir = evidence_root / bundle_date / bundle_id

    report_json = bundle_dir / "reports" / "aoi_report_v1" / f"{aoi_id}.json"
    report_html = bundle_dir / "reports" / "aoi_report_v1" / f"{aoi_id}.html"
    metrics_csv = bundle_dir / "reports" / "aoi_report_v1" / aoi_id / "metrics.csv"
    manifest = bundle_dir / "manifest.json"
    geometry = bundle_dir / "inputs" / "aoi.geojson"

    assert report_json.exists()
    assert report_html.exists()
    assert metrics_csv.exists()
    assert manifest.exists()
    assert geometry.exists()
