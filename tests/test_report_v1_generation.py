from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from eudr_dmi.reports.build_report import build_report_v1


def _write_demo_geojson(path: Path) -> None:
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"plot_code": "P01", "owner": "Demo Operator"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[25.0, 58.0], [25.02, 58.0], [25.02, 58.02], [25.0, 58.02], [25.0, 58.0]]],
                },
            },
            {
                "type": "Feature",
                "properties": {"plot_code": "P02"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[25.03, 58.0], [25.05, 58.0], [25.05, 58.02], [25.03, 58.02], [25.03, 58.0]]],
                },
            },
            {
                "type": "Feature",
                "properties": {"plot_code": "P03"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[25.06, 58.0], [25.08, 58.0], [25.08, 58.02], [25.06, 58.02], [25.06, 58.0]]],
                },
            },
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_missing_kyc_coerces_to_na(tmp_path: Path) -> None:
    geojson_path = tmp_path / "demo.geojson"
    _write_demo_geojson(geojson_path)

    report = build_report_v1(
        run_id="demo_2026-02-20",
        plot_id="demo_plot_01",
        aoi_geojson_path=geojson_path,
        kyc_json=None,
        analysis_json=None,
    )
    payload = report.to_dict()

    assert payload["company"]["operator"] == "N/A"
    assert payload["company"]["address"] == "N/A"
    assert payload["commodity"]["commodity_type"] == "N/A"
    assert payload["commodity"]["country_of_production"] == "N/A"


def test_generated_out_structure_exists(tmp_path: Path) -> None:
    geojson_path = tmp_path / "demo.geojson"
    _write_demo_geojson(geojson_path)

    repo_root = Path(__file__).resolve().parents[1]
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "generate_report_v1.py"),
        "--run-id",
        "demo_2026-02-20",
        "--plot-id",
        "demo_plot_01",
        "--aoi-geojson",
        str(geojson_path),
        "--out-dir",
        str(tmp_path / "out" / "reports"),
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    assert proc.returncode == 0, proc.stderr

    out = tmp_path / "out" / "reports" / "demo_2026-02-20" / "demo_plot_01"
    assert (out / "report.json").is_file()
    assert (out / "report.html").is_file()
    assert (out / "report.pdf").is_file()
    assert (out / "manifest.sha256").is_file()


def test_manifest_has_three_entries(tmp_path: Path) -> None:
    geojson_path = tmp_path / "demo.geojson"
    _write_demo_geojson(geojson_path)

    repo_root = Path(__file__).resolve().parents[1]
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "generate_report_v1.py"),
        "--run-id",
        "demo_2026-02-20",
        "--plot-id",
        "demo_plot_01",
        "--aoi-geojson",
        str(geojson_path),
        "--out-dir",
        str(tmp_path / "out" / "reports"),
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    assert proc.returncode == 0, proc.stderr

    manifest = (
        tmp_path
        / "out"
        / "reports"
        / "demo_2026-02-20"
        / "demo_plot_01"
        / "manifest.sha256"
    )
    lines = [line.strip() for line in manifest.read_text(encoding="utf-8").splitlines() if line.strip()]

    assert len(lines) == 3
    assert lines[0].endswith("  report.json")
    assert lines[1].endswith("  report.html")
    assert lines[2].endswith("  report.pdf")


def test_forest_metrics_maps_into_deforestation_assessment(tmp_path: Path) -> None:
    geojson_path = tmp_path / "demo.geojson"
    _write_demo_geojson(geojson_path)

    report = build_report_v1(
        run_id="demo_2026-02-20",
        plot_id="demo_plot_01",
        aoi_geojson_path=geojson_path,
        analysis_json={
            "forest_metrics": {
                "forest_end_year_area_ha": 416.06087365026985,
                "loss_2021_2024_ha": 16.771785975099796,
            },
            "metrics": {
                "aoi_area_ha": {"value": 15775.219873859023},
            },
        },
    )
    payload = report.to_dict()
    summary = payload["deforestation_assessment"]["summary_metrics"]

    assert payload["deforestation_assessment"]["deforestation_detected"] == "Yes"
    assert summary["area_forest_ha"] == 416.06087365026985
    assert summary["area_loss_post_2020_ha"] == 16.771785975099796
    assert summary["area_aoi_ha"] == 15775.219873859023


def test_generate_report_with_analysis_writes_static_map_svg(tmp_path: Path) -> None:
    geojson_path = tmp_path / "demo.geojson"
    _write_demo_geojson(geojson_path)

    run_root = tmp_path / "run"
    map_dir = run_root / "reports" / "aoi_report_v2" / "demo_aoi" / "map"
    map_dir.mkdir(parents=True, exist_ok=True)

    aoi_layer = run_root / "inputs" / "aoi.geojson"
    aoi_layer.parent.mkdir(parents=True, exist_ok=True)
    _write_demo_geojson(aoi_layer)

    forest_layer = run_root / "reports" / "aoi_report_v2" / "demo_aoi" / "hansen" / "forest_current_tree_cover_mask.geojson"
    forest_layer.parent.mkdir(parents=True, exist_ok=True)
    forest_layer.write_text(geojson_path.read_text(encoding="utf-8"), encoding="utf-8")

    loss_layer = run_root / "reports" / "aoi_report_v2" / "demo_aoi" / "hansen" / "forest_loss_post_2020_mask.geojson"
    loss_layer.write_text(geojson_path.read_text(encoding="utf-8"), encoding="utf-8")

    map_config = {
        "aoi_bbox": {"min_lat": 58.0, "min_lon": 25.0, "max_lat": 58.02, "max_lon": 25.08},
        "latest_year": 2024,
        "layers": {
            "aoi_boundary": "../../../../inputs/aoi.geojson",
            "forest_end_year": "../hansen/forest_current_tree_cover_mask.geojson",
            "forest_loss_post_2020": "../hansen/forest_loss_post_2020_mask.geojson",
        },
    }
    (map_dir / "map_config.json").write_text(json.dumps(map_config), encoding="utf-8")

    analysis_path = run_root / "demo_aoi_report.json"
    analysis_payload = {
        "map_assets": {
            "config_relpath": "reports/aoi_report_v2/demo_aoi/map/map_config.json",
        },
        "forest_metrics": {
            "forest_end_year_area_ha": 10.0,
            "loss_2021_2024_ha": 1.0,
        },
        "metrics": {
            "aoi_area_ha": {"value": 20.0},
        },
    }
    analysis_path.write_text(json.dumps(analysis_payload), encoding="utf-8")

    repo_root = Path(__file__).resolve().parents[1]
    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "generate_report_v1.py"),
        "--run-id",
        "demo_2026-02-20",
        "--plot-id",
        "demo_plot_01",
        "--aoi-geojson",
        str(geojson_path),
        "--analysis-json",
        str(analysis_path),
        "--out-dir",
        str(tmp_path / "out" / "reports"),
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    assert proc.returncode == 0, proc.stderr

    out = tmp_path / "out" / "reports" / "demo_2026-02-20" / "demo_plot_01"
    assert (out / "deforestation_map.svg").is_file()
    report_payload = json.loads((out / "report.json").read_text(encoding="utf-8"))
    assert "deforestation_map.svg" in report_payload["deforestation_assessment"]["evidence_maps"]
    assert "deforestation_map.svg" in report_payload["artifacts"]
