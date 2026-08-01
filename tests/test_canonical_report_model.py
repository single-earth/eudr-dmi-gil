from __future__ import annotations

import json
import shutil
import subprocess
import struct
import zlib
from pathlib import Path

import pytest

from eudr_dmi_gil.reports.report_model import (
    ArtifactRef,
    _render_gap_rows,
    canonical_report_from_aoi_report_v2,
    materialize_evidence_pngs,
    render_canonical_html,
    render_canonical_pdf,
    update_canonical_artifact_hashes,
    write_canonical_metrics_csv,
    write_canonical_report_json,
    write_sha256_manifest,
)
from eudr_dmi_gil.reports.validate import validate_aoi_report


def _mask_geojson(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [0.0, 0.0],
                                    [1.0, 0.0],
                                    [1.0, 1.0],
                                    [0.0, 1.0],
                                    [0.0, 0.0],
                                ]
                            ],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _aoi_geojson(path: Path, *, country: str = "Brazil") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"country": country},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [0.0, 0.0],
                                    [1.0, 0.0],
                                    [1.0, 1.0],
                                    [0.0, 1.0],
                                    [0.0, 0.0],
                                ]
                            ],
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _source_report(bundle_root: Path, *, aoi_id: str = "coffee_named_aoi") -> dict:
    baseline = bundle_root / "reports" / "aoi_report_v2" / aoi_id / "jrc" / "jrc_forest_2020_mask.geojson"
    loss = (
        bundle_root
        / "reports"
        / "aoi_report_v2"
        / aoi_id
        / "jrc"
        / "forest_loss_2021_2024_on_jrc_forest_2020_mask.geojson"
    )
    _mask_geojson(baseline)
    _mask_geojson(loss)
    return {
        "report_version": "aoi_report_v2",
        "generated_at_utc": "2026-07-25T00:00:00+00:00",
        "bundle_id": "bundle-001",
        "aoi_id": aoi_id,
        "aoi_geometry_ref": {"kind": "geojson", "value": "inputs/aoi.geojson"},
        "metrics": {
            "aoi_area_ha": {"value": 100.0, "unit": "ha"},
            "forest_baseline_2020_ha": {"value": 80.0, "unit": "ha"},
            "forest_baseline_2020_percent_of_aoi": {"value": 80.0, "unit": "percent"},
            "forest_loss_post_2020_on_baseline_ha": {"value": 2.5, "unit": "ha"},
            "forest_loss_post_2020_percent_of_aoi": {"value": 2.5, "unit": "percent"},
            "forest_loss_post_2020_percent_of_baseline": {"value": 3.125, "unit": "percent"},
        },
        "parameters": {
            "assessment_end_year": {
                "evidence_start_year": 2021,
                "requested_end_year": 2025,
                "effective_end_year": 2024,
            },
            "post_2020_loss_on_2020_forest": {
                "baseline_dataset_id": "JRC/GFC2020/V3"
            },
        },
        "datasets": [],
        "methodology": {
            "post_2020_loss_on_2020_forest": {
                "calculation": {"method": "deterministic_categorical_raster_intersection"}
            }
        },
        "computed_outputs": {
            "post_2020_loss_on_2020_forest": {
                "baseline_mask_ref": {
                    "relpath": str(baseline.relative_to(bundle_root)).replace("\\", "/")
                },
                "loss_mask_ref": {
                    "relpath": str(loss.relative_to(bundle_root)).replace("\\", "/")
                },
            }
        },
        "extensions": {},
    }


def _artifact(path: str | None, *, available: bool = True, status: str = "available") -> ArtifactRef:
    return ArtifactRef(
        path,
        available,
        status,
        checksum_sha256="0" * 64 if available and path else None,
        content_type="image/png" if available and path else None,
    )


def _write_fake_png(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    width = 160
    height = 100
    rows = []
    for y in range(height):
        row = bytearray()
        for x in range(width):
            row.extend((28 + x % 48, 80 + y % 70, 45 + (x + y) % 50, 255))
        rows.append(b"\x00" + bytes(row))

    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
        )

    path.write_bytes(
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(b"".join(rows), level=9))
        + chunk(b"IEND", b"")
    )


def _pdf_fixture_report(
    tmp_path: Path,
    *,
    aoi_id: str,
    loss_ha: float,
    effective_end_year: int,
    commodity_available: bool,
) -> tuple[Path, Path, object]:
    bundle_root = tmp_path / f"bundle-{aoi_id}"
    report_root = bundle_root / "reports" / "aoi_report_v2" / aoi_id
    _aoi_geojson(bundle_root / "inputs" / "aoi.geojson", country="Ghana")
    source = _source_report(bundle_root, aoi_id=aoi_id)
    source["parameters"]["assessment_end_year"] = {
        "evidence_start_year": 2021,
        "requested_end_year": effective_end_year,
        "effective_end_year": effective_end_year,
    }
    source["metrics"]["forest_loss_post_2020_on_baseline_ha"]["value"] = loss_ha
    source["metrics"]["forest_loss_post_2020_percent_of_aoi"]["value"] = loss_ha
    source["metrics"]["forest_loss_post_2020_percent_of_baseline"]["value"] = loss_ha / 80.0 * 100.0
    if commodity_available:
        source["commodity"] = {
            "id": "coffee",
            "display_name": "Coffee",
            "provider": "configured_fixture",
            "dataset": "Configured Coffee Evidence",
            "version": "fixture-v1",
            "observation_year": 2024,
            "class_values": [46],
            "coverage_status": "fixture",
            "evidence_available": True,
            "evidence_gaps": [],
        }
        source["metrics"]["commodity_area_ha"] = {"value": 41.0, "unit": "ha"}
        source["metrics"]["post_2020_loss_and_commodity_overlap_ha"] = {
            "value": min(loss_ha, 1.25),
            "unit": "ha",
        }

    loss_name = f"evidence/03_forest_loss_2021_{effective_end_year}.png"
    artifact_specs = {
        "aoi_satellite": "evidence/01_aoi_satellite.png",
        "jrc_forest_2020": "evidence/02_jrc_forest_2020.png",
        "forest_loss": loss_name,
        "intersection": "evidence/05_intersection.png",
        "before_after": "evidence/06_before_after.png" if commodity_available else None,
        "legend": "evidence/legend.png",
    }
    artifacts = {}
    for key, relpath in artifact_specs.items():
        if relpath is None:
            artifacts[key] = _artifact(None, available=False, status=f"{key}_unavailable")
        else:
            _write_fake_png(report_root / relpath)
            artifacts[key] = _artifact(relpath)
    if commodity_available:
        _write_fake_png(report_root / "evidence/04_commodity_layer.png")
        artifacts["commodity_layer"] = _artifact("evidence/04_commodity_layer.png")
    else:
        artifacts["commodity_layer"] = _artifact(
            None,
            available=False,
            status="usable_commodity_layer_not_available",
        )

    report = canonical_report_from_aoi_report_v2(
        source,
        bundle_root=bundle_root,
        report_root=bundle_root / "reports" / "aoi_report_v2",
        generated_artifacts=artifacts,
    )
    return bundle_root, report_root, report


def _pdf_page_count(pdf_path: Path) -> int:
    pdfinfo = shutil.which("pdfinfo")
    if not pdfinfo:
        pytest.skip("pdfinfo is not available")
    proc = subprocess.run([pdfinfo, str(pdf_path)], text=True, capture_output=True, check=True)
    for line in proc.stdout.splitlines():
        if line.startswith("Pages:"):
            return int(line.split(":", 1)[1].strip())
    raise AssertionError("pdfinfo did not report page count")


def _pdf_text(pdf_path: Path) -> str:
    pdftotext = shutil.which("pdftotext")
    if not pdftotext:
        pytest.skip("pdftotext is not available")
    proc = subprocess.run([pdftotext, str(pdf_path), "-"], text=True, capture_output=True, check=True)
    return proc.stdout


def test_render_gap_rows_shows_code_message_schema_not_literal_gap_placeholder() -> None:
    # commodities/analysis.py and commodities/providers.py declare gaps as {code, severity,
    # message}, not {gap_id/artifact_id, status/reason/description} — the renderer must read
    # both schemas or every such gap silently collapses to a blank "gap" row (regression this
    # test guards against).
    gaps = [
        {
            "code": "commodity_threshold_not_locally_calibrated",
            "severity": "warning",
            "message": "The configured probability threshold (0.01) is a generic model cutoff.",
        },
        {
            "gap_id": "missing_regional_overview_png",
            "artifact_id": "regional_overview_png",
            "status": "not_available_in_local_bundle",
            "path": None,
        },
    ]
    html = _render_gap_rows(gaps)
    assert "commodity_threshold_not_locally_calibrated" in html
    assert "generic model cutoff" in html
    assert "regional_overview_png" in html
    assert "not_available_in_local_bundle" in html
    assert "<th>gap</th>" not in html


def test_canonical_schema_and_missing_optional_commodity_artifact(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundle"
    source = _source_report(bundle_root)
    artifacts = {
        "aoi_satellite": ArtifactRef(None, False, "satellite_unavailable"),
        "jrc_forest_2020": ArtifactRef(None, False, "baseline_unavailable"),
        "forest_loss": ArtifactRef(None, False, "loss_unavailable"),
        "commodity_layer": ArtifactRef(None, False, "usable_commodity_layer_not_available"),
        "intersection": ArtifactRef(None, False, "intersection_unavailable"),
        "before_after": ArtifactRef(None, False, "before_after_unavailable"),
        "legend": ArtifactRef(None, False, "legend_unavailable"),
    }
    report = canonical_report_from_aoi_report_v2(
        source,
        bundle_root=bundle_root,
        report_root=bundle_root / "reports" / "aoi_report_v2",
        generated_artifacts=artifacts,
    )

    validate_aoi_report(report.to_dict())
    assert report.commodity["id"] is None
    assert report.layers["commodity"]["path"] is None
    assert report.layers["commodity"]["available"] is False
    assert artifacts["commodity_layer"].availability_status == "usable_commodity_layer_not_available"


def test_customer_html_is_json_first_accessible_and_non_decision_language(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundle"
    aoi_id = "coffee_named_aoi_with_a_very_long_dataset_name"
    report_root = bundle_root / "reports" / "aoi_report_v2" / aoi_id
    _aoi_geojson(bundle_root / "inputs" / "aoi.geojson", country="Brazil")
    source = _source_report(bundle_root, aoi_id=aoi_id)
    source["commodity"] = {
        "id": "coffee",
        "display_name": "Coffee",
        "provider": "mapbiomas_brazil",
        "dataset": "MapBiomas Brazil Land Cover And Land Use Collection With Long Name",
        "version": "collection-9-2023",
        "observation_year": 2023,
        "class_values": [46],
        "coverage_status": "full",
        "evidence_available": True,
        "evidence_gaps": [],
    }
    source["metrics"]["post_2020_loss_and_commodity_overlap_ha"] = {
        "value": 1.25,
        "unit": "ha",
    }
    for relpath in [
        "evidence/01_satellite.png",
        "evidence/02_jrc_forest_2020.png",
        "evidence/03_forest_loss_2021_2024.png",
        "evidence/04_commodity_layer.png",
        "evidence/05_intersection.png",
    ]:
        _write_fake_png(report_root / relpath)
    artifacts = {
        "aoi_satellite": _artifact("evidence/01_satellite.png"),
        "jrc_forest_2020": _artifact("evidence/02_jrc_forest_2020.png"),
        "forest_loss": _artifact("evidence/03_forest_loss_2021_2024.png"),
        "commodity_layer": _artifact("evidence/04_commodity_layer.png"),
        "intersection": _artifact("evidence/05_intersection.png"),
        "before_after": _artifact(None, available=False, status="before_after_unavailable"),
        "legend": _artifact(None, available=False, status="legend_unavailable"),
    }
    report = canonical_report_from_aoi_report_v2(
        source,
        bundle_root=bundle_root,
        report_root=bundle_root / "reports" / "aoi_report_v2",
        generated_artifacts=artifacts,
    )

    html_path = report_root / "report.html"
    render_canonical_html(report, html_path)
    html_text = html_path.read_text(encoding="utf-8")

    assert '<script type="application/json" id="report-data">' in html_text
    assert 'role="tablist"' in html_text
    assert 'role="tab"' in html_text
    assert 'data-layer="commodity"' in html_text
    assert "MapBiomas Brazil Land Cover And Land Use Collection With Long Name" in html_text
    assert "Coffee - Brazil" in html_text
    assert "2021-2024" in html_text
    assert "JSON download" in html_text
    assert "PDF download" in html_text
    assert "Before/after imagery is not available in this bundle. No comparison has been fabricated." in html_text
    assert "This report contains satellite-derived and geospatial evidence." in html_text
    for forbidden in [
        "EUDR compliant",
        "EUDR non-compliant",
        "Compliance status",
        "Compliance you can prove",
        "deforestation caused by coffee",
        "forest conversion detected",
    ]:
        assert forbidden not in html_text


def test_customer_html_hides_missing_optional_commodity_and_flags_broken_layer_path(
    tmp_path: Path,
) -> None:
    bundle_root = tmp_path / "bundle"
    report_root = bundle_root / "reports" / "aoi_report_v2" / "aoi-1"
    source = _source_report(bundle_root, aoi_id="aoi-1")
    artifacts = {
        "aoi_satellite": _artifact(None, available=False, status="satellite_unavailable"),
        "jrc_forest_2020": _artifact(
            "evidence/missing_jrc.png",
            available=True,
            status="available",
        ),
        "forest_loss": _artifact(None, available=False, status="loss_unavailable"),
        "commodity_layer": _artifact(
            None,
            available=False,
            status="usable_commodity_layer_not_available",
        ),
        "intersection": _artifact(None, available=False, status="intersection_unavailable"),
        "before_after": _artifact(None, available=False, status="before_after_unavailable"),
        "legend": _artifact(None, available=False, status="legend_unavailable"),
    }
    report = canonical_report_from_aoi_report_v2(
        source,
        bundle_root=bundle_root,
        report_root=bundle_root / "reports" / "aoi_report_v2",
        generated_artifacts=artifacts,
    )

    html_path = report_root / "report.html"
    render_canonical_html(report, html_path)
    html_text = html_path.read_text(encoding="utf-8")

    assert 'data-layer="commodity"' not in html_text
    assert "Declared artifact path was not found in the generated bundle." in html_text
    assert 'data-path-status="missing"' in html_text


def test_canonical_mandatory_artifacts_dynamic_end_year_and_relative_paths(tmp_path: Path) -> None:
    pytest.importorskip("rasterio")
    bundle_root = tmp_path / "bundle"
    report_root = bundle_root / "reports" / "aoi_report_v2" / "aoi-1"
    source = _source_report(bundle_root, aoi_id="aoi-1")
    artifacts = materialize_evidence_pngs(source, bundle_root=bundle_root, report_root=report_root)
    report = canonical_report_from_aoi_report_v2(
        source,
        bundle_root=bundle_root,
        report_root=bundle_root / "reports" / "aoi_report_v2",
        generated_artifacts=artifacts,
    )

    assert report.layers["jrc_forest_2020"]["path"] == "evidence/02_jrc_forest_2020.png"
    assert report.layers["forest_loss"]["path"] == "evidence/03_forest_loss_2021_2024.png"
    assert report.layers["intersection"]["path"] == "evidence/05_intersection.png"
    assert report.layers["legend"]["path"] == "evidence/legend.png"
    for layer in report.layers.values():
        path = layer["path"]
        if path is not None:
            assert not path.startswith("/")
            assert "://" not in path


def test_canonical_manifest_ordering_and_repeatability(tmp_path: Path) -> None:
    pytest.importorskip("rasterio")
    bundle_root = tmp_path / "bundle"
    report_root = bundle_root / "reports" / "aoi_report_v2" / "aoi-1"
    source = _source_report(bundle_root, aoi_id="aoi-1")
    artifacts = materialize_evidence_pngs(source, bundle_root=bundle_root, report_root=report_root)
    report = canonical_report_from_aoi_report_v2(
        source,
        bundle_root=bundle_root,
        report_root=bundle_root / "reports" / "aoi_report_v2",
        generated_artifacts=artifacts,
    )

    json_path = report_root / "report.json"
    html_path = report_root / "report.html"
    pdf_path = report_root / "report.pdf"
    metrics_path = report_root / "metrics.csv"
    write_canonical_metrics_csv(metrics_path, report.metrics)
    write_canonical_report_json(json_path, report)
    render_canonical_html(report, html_path)
    pytest.importorskip("reportlab")
    render_canonical_pdf(report, pdf_path, report_root=report_root)

    relpaths = [
        str(path.relative_to(bundle_root)).replace("\\", "/")
        for path in [pdf_path, html_path, json_path, metrics_path]
    ]
    for artifact in artifacts.values():
        if artifact.path:
            relpaths.append(str((report_root / artifact.path).relative_to(bundle_root)).replace("\\", "/"))
    report = update_canonical_artifact_hashes(
        report,
        report_root=report_root,
        bundle_root=bundle_root,
        artifact_relpaths=relpaths,
    )
    write_canonical_report_json(json_path, report)
    first_json = json_path.read_bytes()
    write_canonical_report_json(json_path, report)
    assert json_path.read_bytes() == first_json

    manifest_path = report_root / "manifest.sha256"
    write_sha256_manifest(bundle_root, manifest_path, list(reversed(relpaths)))
    lines = manifest_path.read_text(encoding="utf-8").splitlines()
    assert lines == sorted(lines, key=lambda line: line.split("  ", 1)[1])

    html_text = html_path.read_text(encoding="utf-8")
    pdf_bytes = pdf_path.read_bytes()
    assert report.report_id in html_text
    assert report.report_id.encode("latin-1") in pdf_bytes


def test_canonical_pdf_renderer_a4_twelve_pages_headings_values_and_repeatability(
    tmp_path: Path,
) -> None:
    _, report_root, report = _pdf_fixture_report(
        tmp_path,
        aoi_id="coffee-loss-positive-2025",
        loss_ha=2.5,
        effective_end_year=2025,
        commodity_available=True,
    )
    json_path = report_root / "report.json"
    pdf_path = report_root / "report.pdf"
    pdf_again = report_root / "report-again.pdf"
    write_canonical_report_json(json_path, report)
    render_canonical_pdf(report, pdf_path, report_root=report_root)
    render_canonical_pdf(report, pdf_again, report_root=report_root)

    assert _pdf_page_count(pdf_path) == 12
    assert pdf_path.read_bytes() == pdf_again.read_bytes()
    text = _pdf_text(pdf_path)
    assert "EUDR\nEvidence\nPackage" in text
    for heading in [
        "EXECUTIVE SUMMARY",
        "ASSESSMENT WORKFLOW",
        "REGIONAL OVERVIEW",
        "FOREST BASELINE 2020",
        "FOREST LOSS AFTER 2020",
        "SATELLITE EVIDENCE",
        "INTERPRETATION",
        "DATA AND METHODS",
        "AUDIT TRAIL",
        "DETERMINISTIC ARTIFACTS",
        "APPENDIX",
    ]:
        assert heading in text
    report_obj = json.loads(json_path.read_text(encoding="utf-8"))
    assert report_obj["report_id"] in text
    assert "Coffee" in text
    assert "Ghana" in text
    assert "100.0 ha" in text
    assert "80 ha" in text
    assert "2.5 ha" in text
    assert "No compliance determination is made by this report." in text
    assert "EUDR certificate" not in text
    assert "forest conversion detected" not in text
    assert report.audit["pdf_generator"]["engine"] == "ReportLab"
    assert report.audit["pdf_generator"]["version"]


def test_canonical_pdf_renderer_loss_zero_and_missing_commodity_degrade_gracefully(
    tmp_path: Path,
) -> None:
    _, report_root, report = _pdf_fixture_report(
        tmp_path,
        aoi_id="loss-zero-no-commodity-2026",
        loss_ha=0.0,
        effective_end_year=2026,
        commodity_available=False,
    )
    pdf_path = report_root / "report.pdf"
    render_canonical_pdf(report, pdf_path, report_root=report_root)

    assert report.layers["forest_loss"]["path"] == "evidence/03_forest_loss_2021_2026.png"
    assert report.layers["commodity"]["path"] is None
    text = _pdf_text(pdf_path)
    assert "No review trigger" in text
    assert "Before/after evidence unavailable" in text
    assert "usable_commodity_layer_not_available" in text
    assert "coffee map placeholder" not in text
    assert _pdf_page_count(pdf_path) == 12


def test_canonical_manifest_includes_report_pdf(tmp_path: Path) -> None:
    bundle_root, report_root, report = _pdf_fixture_report(
        tmp_path,
        aoi_id="manifest-pdf",
        loss_ha=2.5,
        effective_end_year=2025,
        commodity_available=True,
    )
    pdf_path = report_root / "report.pdf"
    html_path = report_root / "report.html"
    json_path = report_root / "report.json"
    metrics_path = report_root / "metrics.csv"
    manifest_path = report_root / "manifest.sha256"
    write_canonical_metrics_csv(metrics_path, report.metrics)
    write_canonical_report_json(json_path, report)
    render_canonical_html(report, html_path)
    render_canonical_pdf(report, pdf_path, report_root=report_root)

    relpaths = [
        str(path.relative_to(bundle_root)).replace("\\", "/")
        for path in [json_path, html_path, pdf_path, metrics_path]
    ]
    for artifact in report.artifacts.values():
        if artifact["path"]:
            relpaths.append(
                str((report_root / artifact["path"]).relative_to(bundle_root)).replace("\\", "/")
            )
    write_sha256_manifest(bundle_root, manifest_path, relpaths)
    manifest = manifest_path.read_text(encoding="utf-8")
    assert "reports/aoi_report_v2/manifest-pdf/report.pdf" in manifest
    assert "reports/aoi_report_v2/manifest-pdf/metrics.csv" in manifest


def test_canonical_pdf_rasterized_pages_are_nonblank_when_supported(tmp_path: Path) -> None:
    pdftoppm = shutil.which("pdftoppm")
    if not pdftoppm:
        pytest.skip("pdftoppm is not available")
    Image = pytest.importorskip("PIL.Image")
    ImageStat = pytest.importorskip("PIL.ImageStat")
    _, report_root, report = _pdf_fixture_report(
        tmp_path,
        aoi_id="visual-check",
        loss_ha=2.5,
        effective_end_year=2025,
        commodity_available=True,
    )
    pdf_path = report_root / "report.pdf"
    render_canonical_pdf(report, pdf_path, report_root=report_root)

    prefix = tmp_path / "rasterized-page"
    subprocess.run([pdftoppm, "-png", "-r", "40", str(pdf_path), str(prefix)], check=True)
    rasters = sorted(tmp_path.glob("rasterized-page-*.png"))
    assert len(rasters) == 12
    for raster in rasters:
        with Image.open(raster).convert("L") as im:
            stat = ImageStat.Stat(im)
            assert stat.stddev[0] > 0.5, f"{raster.name} appears blank"
