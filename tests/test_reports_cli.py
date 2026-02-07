from __future__ import annotations

import os
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
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


def test_cli_help() -> None:
    proc = _run_cli(["--help"], env=os.environ.copy())
    assert proc.returncode == 0
    assert "Generate a deterministic AOI report bundle" in proc.stdout


def test_cli_golden_run_creates_bundle(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    env = os.environ.copy()
    env["EUDR_DMI_EVIDENCE_ROOT"] = str(evidence_root)

    bundle_id = "bundle-001"
    aoi_id = "aoi-123"

    policy_ref_file = tmp_path / "policy_refs.txt"
    policy_ref_file.write_text(
        "# comment line\npolicy-spine:eudr/article-3\nplaceholder:TODO\n\n",
        encoding="utf-8",
    )

    proc = _run_cli(
        [
            "--aoi-id",
            aoi_id,
            "--aoi-wkt",
            "POINT (0 0)",
            "--bundle-id",
            bundle_id,
            "--out-format",
            "both",
            "--metric",
            "b_metric=2:count:dummy_source:note b",
            "--metric",
            "a_metric=1:count:dummy_source:note a",
            "--policy-mapping-ref",
            "policy-spine:eudr/article-9",
            "--policy-mapping-ref-file",
            str(policy_ref_file),
        ],
        env=env,
    )

    assert proc.returncode == 0, proc.stderr

    bundle_date = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")
    bundle_dir = evidence_root / bundle_date / bundle_id

    assert bundle_dir.exists()

    report_json = bundle_dir / "reports" / "aoi_report_v2" / f"{aoi_id}.json"
    report_html = bundle_dir / "reports" / "aoi_report_v2" / f"{aoi_id}.html"
    metrics_csv = bundle_dir / "reports" / "aoi_report_v2" / aoi_id / "metrics.csv"
    manifest = bundle_dir / "manifest.json"
    geometry = bundle_dir / "inputs" / "aoi.wkt"

    assert report_json.exists()
    assert report_html.exists()
    assert metrics_csv.exists()
    assert manifest.exists()
    assert geometry.exists()

    # Contract validation.
    validate_aoi_report_file(report_json)

    # report.html should link to declared HTML artifacts if present.
    report_obj = json.loads(report_json.read_text(encoding="utf-8"))
    html_relpaths = [
        item.get("relpath")
        for item in report_obj.get("evidence_artifacts", [])
        if isinstance(item, dict)
        and (item.get("meta") or {}).get("role") == "report_html"
    ]
    if html_relpaths:
        html_text = report_html.read_text(encoding="utf-8")
        for relpath in html_relpaths:
            assert relpath in html_text

    # metrics.csv header and stable row ordering.
    lines = metrics_csv.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "variable,value,unit,source,notes"
    assert lines[1].startswith("a_metric,")
    assert lines[2].startswith("b_metric,")

    # HTML links should be portable (no absolute paths, no schemes).
    html = report_html.read_text(encoding="utf-8")
    hrefs = re.findall(r'href="([^"]+)"', html)
    assert hrefs, "expected at least one link"
    for href in hrefs:
        assert not href.startswith("/")
        assert "://" not in href
        assert str(tmp_path) not in href

    # manifest.json includes metrics.csv
    manifest_obj = json.loads(manifest.read_text(encoding="utf-8"))
    relpaths = [a["relpath"] for a in manifest_obj.get("artifacts", [])]
    assert f"reports/aoi_report_v2/{aoi_id}/metrics.csv" in relpaths


def test_cli_hansen_external_dependencies(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    env = os.environ.copy()
    env["EUDR_DMI_EVIDENCE_ROOT"] = str(evidence_root)

    bundle_id = "bundle-hansen-001"
    aoi_id = "aoi-456"

    aoi_geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [20.0, 50.0],
                            [40.0, 50.0],
                            [40.0, 60.0],
                            [20.0, 60.0],
                            [20.0, 50.0],
                        ]
                    ],
                },
            }
        ],
    }
    aoi_path = tmp_path / "aoi.geojson"
    aoi_path.write_text(json.dumps(aoi_geojson), encoding="utf-8")

    tile_dir = tmp_path / "tiles"
    tile_bounds = [
        (20.0, 50.0, 30.0, 60.0, "N50_E020"),
        (30.0, 50.0, 40.0, 60.0, "N50_E030"),
    ]
    for minx, miny, maxx, maxy, tile_id in tile_bounds:
        transform = from_bounds(minx, miny, maxx, maxy, 2, 2)
        treecover = np.array([[50, 50], [50, 50]], dtype=np.uint8)
        lossyear = np.array([[0, 21], [0, 0]], dtype=np.uint8)
        _write_test_raster(tile_dir / tile_id / "treecover2000.tif", treecover, transform, "EPSG:4326")
        _write_test_raster(tile_dir / tile_id / "lossyear.tif", lossyear, transform, "EPSG:4326")

    proc = _run_cli(
        [
            "--aoi-id",
            aoi_id,
            "--aoi-geojson",
            str(aoi_path),
            "--bundle-id",
            bundle_id,
            "--out-format",
            "json",
            "--enable-hansen-post-2020-loss",
            "--hansen-tile-dir",
            str(tile_dir),
        ],
        env=env,
    )

    assert proc.returncode == 0, proc.stderr

    bundle_date = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")
    bundle_dir = evidence_root / bundle_date / bundle_id
    report_json = bundle_dir / "reports" / "aoi_report_v2" / f"{aoi_id}.json"
    report = json.loads(report_json.read_text(encoding="utf-8"))

    deps = report.get("external_dependencies")
    assert isinstance(deps, list) and deps
    dep = deps[0]
    tiles_used = dep.get("tiles_used")
    assert isinstance(tiles_used, list)
    ordered = [(t["tile_id"], t["layer"], t["local_path"]) for t in tiles_used]
    assert ordered == sorted(ordered)
    assert all(t.get("source_url") for t in tiles_used)

    manifest_rel = dep.get("tiles_manifest", {}).get("relpath")
    assert isinstance(manifest_rel, str)
    assert (bundle_dir / manifest_rel).is_file()
    evidence_relpaths = {item["relpath"] for item in report.get("evidence_artifacts", [])}
    assert manifest_rel in evidence_relpaths

    manifest = bundle_dir / "manifest.json"
    manifest_obj = json.loads(manifest.read_text(encoding="utf-8"))
    manifest_entries = {
        item["relpath"]: item for item in manifest_obj.get("artifacts", []) if "relpath" in item
    }
    assert manifest_rel in manifest_entries
    assert manifest_entries[manifest_rel].get("content_type") == "application/json"

    validation = report.get("validation")
    assert isinstance(validation, dict)
    crosscheck = validation.get("forest_area_crosscheck")
    assert isinstance(crosscheck, dict)
    assert crosscheck.get("outcome") in {"pass", "fail", "not_comparable"}
    assert "reference" in crosscheck
    assert "computed" in crosscheck
    assert "comparison" in crosscheck
    csv_ref = crosscheck.get("csv_ref", {})
    summary_ref = crosscheck.get("summary_ref", {})
    assert (bundle_dir / csv_ref.get("relpath", "")).is_file()
    assert (bundle_dir / summary_ref.get("relpath", "")).is_file()
