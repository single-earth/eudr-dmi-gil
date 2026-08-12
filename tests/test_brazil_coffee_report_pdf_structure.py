"""Deterministic structural audit of the Brazil coffee (Minas Gerais) report.pdf.

This test runs the supported report CLI against the repository's committed
`coffee_brazil_minas_gerais` AOI and evidence inputs (the same inputs used by
`scripts/run_brazil_coffee_report_clean.sh`) and then verifies the generated
`report.pdf` structure using text extraction only (`pdfinfo`/`pdftotext`,
i.e. the PDF's own embedded text layer) -- no OCR/rasterization is used here.
"""

from __future__ import annotations

import hashlib
import csv
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from eudr_dmi_gil.reports.validate import validate_aoi_report_file

# "intersection" is deliberately excluded: against the real pinned Hansen/JRC/MapBiomas rasters
# this AOI has post-2020 forest loss that does not overlap the coffee commodity layer
# (post_2020_loss_and_commodity_overlap_ha == 0.0), so evidence/05_intersection.png is genuinely
# absent -- see scripts/run_brazil_coffee_report_clean.sh's verify_zero_loss_commodity_overlap step
# and the sibling run_brazil_coffee_eudr_compliant_report_clean.sh, which documents the same
# "absent when there is no overlap geometry" behavior for its own zero-loss case.
REQUIRED_CANONICAL_LAYERS = [
    "satellite",
    "jrc_forest_2020",
    "forest_loss",
    "commodity",
    "before_after",
    "regional_overview",
    "legend",
]

REQUIRED_DETERMINISTIC_ARTIFACTS_PAGE_FILES = [
    "report.json",
    "report.html",
    "report.pdf",
    "metrics.csv",
    "manifest.sha256",
    "evidence/01_aoi_satellite.png",
    "evidence/02_jrc_forest_2020.png",
    "evidence/04_commodity_layer.png",
    "evidence/06_before_after.png",
    "evidence/legend.png",
]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

REPO_ROOT = Path(__file__).resolve().parents[1]
AOI_ID = "coffee_brazil_minas_gerais"
AOI_PATH = REPO_ROOT / "aoi_json_examples" / f"{AOI_ID}.geojson"
INPUTS_DIR = REPO_ROOT / f"out/{AOI_ID}_inputs"
COMMODITY_CONFIG = INPUTS_DIR / "coffee_config_brazil_cerrado_mineiro.json"

# Required page order per the EUDR evidence package acceptance requirement.
EXPECTED_PAGE_HEADINGS: list[tuple[str, str]] = [
    ("Cover", "EUDR"),
    ("Executive Summary", "EXECUTIVE SUMMARY"),
    ("Assessment Workflow", "ASSESSMENT WORKFLOW"),
    ("Regional Overview", "REGIONAL OVERVIEW"),
    ("Forest Baseline 2020", "FOREST BASELINE 2020"),
    ("Forest Loss After 2020", "FOREST LOSS AFTER 2020"),
    ("Satellite Evidence", "SATELLITE EVIDENCE"),
    ("Interpretation", "INTERPRETATION"),
    ("Data and Methods", "DATA AND METHODS"),
    ("Audit Trail", "AUDIT TRAIL"),
    ("Deterministic Artifacts", "DETERMINISTIC ARTIFACTS"),
    ("Appendix", "APPENDIX"),
]


def _require_poppler() -> tuple[str, str]:
    pdfinfo = shutil.which("pdfinfo")
    pdftotext = shutil.which("pdftotext")
    if not pdfinfo or not pdftotext:
        pytest.skip("pdfinfo/pdftotext (poppler-utils) not available")
    return pdfinfo, pdftotext


def _pdf_page_count(pdfinfo: str, pdf_path: Path) -> int:
    proc = subprocess.run([pdfinfo, str(pdf_path)], text=True, capture_output=True, check=True)
    for line in proc.stdout.splitlines():
        if line.startswith("Pages:"):
            return int(line.split(":", 1)[1].strip())
    raise AssertionError("pdfinfo did not report a page count")


def _pdf_page_text(pdftotext: str, pdf_path: Path, page: int) -> str:
    proc = subprocess.run(
        [pdftotext, "-f", str(page), "-l", str(page), "-layout", str(pdf_path), "-"],
        text=True,
        capture_output=True,
        check=True,
    )
    return proc.stdout


@pytest.fixture(scope="module")
def brazil_bundle_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    for required in [
        AOI_PATH,
        COMMODITY_CONFIG,
        INPUTS_DIR / "jrc_gfc2020_v3.tif",
        INPUTS_DIR / "hansen_lossyear_2025_v1_13.tif",
        INPUTS_DIR / "hansen_treecover2000_2025_v1_13.tif",
        INPUTS_DIR / "sentinel2_baseline_2020.tif",
        INPUTS_DIR / "sentinel2_recent_2025.tif",
        INPUTS_DIR / "sentinel2_regional_overview.tif",
        INPUTS_DIR / "regional_admin_boundaries.geojson",
    ]:
        if not required.is_file():
            pytest.skip(f"missing committed Brazil coffee input: {required}")

    tmp_path = tmp_path_factory.mktemp("brazil_coffee_pdf")
    evidence_root = tmp_path / "evidence"
    bundle_id = "brazil-coffee-pdf-structure-test"

    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "src") + (
        ":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    env["EUDR_DMI_EVIDENCE_ROOT"] = str(evidence_root)
    env["EUDR_DMI_GENERATED_AT_UTC"] = "2026-07-29T00:00:00+00:00"

    args = [
        sys.executable,
        "-m",
        "eudr_dmi_gil.reports.cli",
        "--aoi-id",
        AOI_ID,
        "--aoi-geojson",
        str(AOI_PATH),
        "--bundle-id",
        bundle_id,
        "--out-format",
        "both",
        "--jrc-gfc2020-raster",
        str(INPUTS_DIR / "jrc_gfc2020_v3.tif"),
        "--hansen-lossyear-raster",
        str(INPUTS_DIR / "hansen_lossyear_2025_v1_13.tif"),
        "--hansen-treecover2000-raster",
        str(INPUTS_DIR / "hansen_treecover2000_2025_v1_13.tif"),
        "--commodity",
        "coffee",
        "--commodity-config",
        str(COMMODITY_CONFIG),
        "--satellite-baseline-raster",
        str(INPUTS_DIR / "sentinel2_baseline_2020.tif"),
        "--satellite-baseline-date",
        "2020-09-26/2020-10-01",
        "--satellite-recent-raster",
        str(INPUTS_DIR / "sentinel2_recent_2025.tif"),
        "--satellite-recent-date",
        "2025-07-02/2025-09-30",
        "--satellite-regional-raster",
        str(INPUTS_DIR / "sentinel2_regional_overview.tif"),
        "--satellite-regional-date",
        "2023-09-21/2025-09-30",
        "--regional-admin-boundaries-geojson",
        str(INPUTS_DIR / "regional_admin_boundaries.geojson"),
        "--analysis-target-crs",
        "EPSG:6933",
        "--analysis-target-resolution-m",
        "30",
    ]
    proc = subprocess.run(args, text=True, capture_output=True, env=env, check=False)
    assert proc.returncode == 0, f"CLI run failed:\nstdout={proc.stdout}\nstderr={proc.stderr}"

    bundle_dir = Path(proc.stdout.strip().splitlines()[-1])
    assert bundle_dir.is_dir(), f"CLI did not print a valid bundle directory: {bundle_dir}"
    return bundle_dir


def test_brazil_report_json_validates_against_canonical_schema(brazil_bundle_dir: Path) -> None:
    report_json = brazil_bundle_dir / "reports" / "aoi_report_v2" / AOI_ID / "report.json"
    assert report_json.is_file()
    validate_aoi_report_file(report_json)


def test_brazil_report_pdf_has_exactly_twelve_pages(brazil_bundle_dir: Path) -> None:
    pdfinfo, _ = _require_poppler()
    pdf_path = brazil_bundle_dir / "reports" / "aoi_report_v2" / AOI_ID / "report.pdf"
    assert pdf_path.is_file()
    assert _pdf_page_count(pdfinfo, pdf_path) == 12


@pytest.mark.parametrize("page_index", range(1, 13))
def test_brazil_report_pdf_page_heading(brazil_bundle_dir: Path, page_index: int) -> None:
    pdfinfo, pdftotext = _require_poppler()
    pdf_path = brazil_bundle_dir / "reports" / "aoi_report_v2" / AOI_ID / "report.pdf"
    assert pdf_path.is_file()
    assert _pdf_page_count(pdfinfo, pdf_path) == 12

    expected_name, expected_marker = EXPECTED_PAGE_HEADINGS[page_index - 1]
    text = _pdf_page_text(pdftotext, pdf_path, page_index)
    assert expected_marker in text, (
        f"Page {page_index} expected heading '{expected_name}' "
        f"(marker '{expected_marker}') not found. Actual extracted text:\n{text}"
    )


def test_brazil_bundle_root_manifest_is_complete_and_hash_verified(brazil_bundle_dir: Path) -> None:
    manifest = json.loads((brazil_bundle_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["artifacts"], "bundle-level manifest.json declares no artifacts"
    for entry in manifest["artifacts"]:
        relpath = entry["relpath"]
        assert not relpath.startswith("/"), f"artifact path is absolute: {relpath}"
        assert ".." not in Path(relpath).parts, f"artifact path escapes bundle root: {relpath}"
        full_path = brazil_bundle_dir / relpath
        assert full_path.is_file(), f"manifest.json declares missing file: {relpath}"
        assert _sha256(full_path) == entry["sha256"], f"sha256 mismatch for {relpath}"
        assert full_path.stat().st_size == entry["size_bytes"], f"size mismatch for {relpath}"


def test_brazil_canonical_manifest_sha256_sorted_and_verified(brazil_bundle_dir: Path) -> None:
    canonical_dir = brazil_bundle_dir / "reports" / "aoi_report_v2" / AOI_ID
    lines = (canonical_dir / "manifest.sha256").read_text(encoding="utf-8").splitlines()
    assert lines, "canonical manifest.sha256 is empty"
    relpaths = []
    for line in lines:
        digest, _, relpath = line.partition("  ")
        relpath = relpath.strip()
        relpaths.append(relpath)
        assert relpath != "manifest.sha256", "canonical manifest.sha256 must not include itself"
        full_path = brazil_bundle_dir / relpath
        assert full_path.is_file(), f"canonical manifest.sha256 declares missing file: {relpath}"
        assert _sha256(full_path) == digest.strip(), f"sha256 mismatch for {relpath}"
    assert relpaths == sorted(relpaths), "canonical manifest.sha256 is not sorted by relative path"


def test_brazil_required_canonical_layers_are_all_available(brazil_bundle_dir: Path) -> None:
    canonical_dir = brazil_bundle_dir / "reports" / "aoi_report_v2" / AOI_ID
    report = json.loads((canonical_dir / "report.json").read_text(encoding="utf-8"))
    layers = report["layers"]
    for key in REQUIRED_CANONICAL_LAYERS:
        layer = layers.get(key)
        assert layer is not None, f"required layer missing from report.json layers: {key}"
        assert layer.get("available") is True, f"required layer not available: {key} -> {layer}"
        path = layer.get("path")
        assert path, f"required layer {key} has no path"
        assert (canonical_dir / path).is_file(), f"required layer {key} artifact missing on disk: {path}"

    cover_hero = layers.get("cover_hero")
    assert cover_hero is not None, "cover_hero layer key missing from report.json layers"
    if cover_hero.get("available") is True:
        path = cover_hero.get("path")
        assert path, f"available cover_hero has no path: {cover_hero}"
        assert (canonical_dir / path).is_file(), f"cover_hero artifact missing on disk: {path}"
    else:
        assert cover_hero.get("availability_status") in {
            "esri_satellite_imagery_could_not_be_fetched_or_rendered",
            "cover_hero_not_materialized",
        }, f"unexpected cover_hero unavailability: {cover_hero}"
        assert cover_hero.get("path") is None, f"unavailable cover_hero should not declare a path: {cover_hero}"

    intersection = layers.get("intersection")
    assert intersection is not None, "intersection layer key missing from report.json layers"
    assert intersection.get("available") is False, (
        f"expected intersection layer unavailable (real data has zero loss/commodity overlap "
        f"for this AOI), got: {intersection}"
    )
    assert intersection.get("path") is None, f"expected no intersection layer path, got: {intersection}"


def test_brazil_dual_baseline_loss_commodity_overlap(brazil_bundle_dir: Path) -> None:
    """Pins this AOI's real, corrected evidentiary finding: the JRC closed-canopy baseline shows
    zero loss/commodity overlap, but the Hansen tree-canopy>=10% baseline (the FAO/EUDR Art.2
    forest definition) shows real, majority-overlapping post-2020 forest-to-coffee conversion.
    Regression-guards the fabricated-fixture defect and the JRC-only-baseline blind spot both
    being fixed at once (see scripts/run_brazil_coffee_report_clean.sh's own equivalent check)."""
    canonical_dir = brazil_bundle_dir / "reports" / "aoi_report_v2" / AOI_ID
    report = json.loads((canonical_dir / "report.json").read_text(encoding="utf-8"))
    metrics = report["metrics"]

    jrc_loss = metrics["forest_loss_post_2020_on_baseline_ha"]["value"]
    jrc_overlap = metrics["post_2020_loss_and_commodity_overlap_ha"]["value"]
    hansen_loss = metrics["forest_loss_post_2020_on_hansen10pct_baseline_ha"]["value"]
    hansen_overlap = metrics[
        "forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_ha"
    ]["value"]

    assert jrc_loss > 0.0
    assert jrc_overlap == 0.0
    assert hansen_loss > 0.0
    assert hansen_overlap > 0.0
    assert hansen_overlap > jrc_loss, (
        "Hansen-canopy-baseline loss/commodity overlap should be a real, substantial signal, "
        "not a rounding artifact"
    )

    summary = report["assessment"]["summary"]
    assert "tree-canopy" in summary.lower() or "canopy" in summary.lower(), (
        f"assessment summary should surface the Hansen-canopy-baseline finding, got: {summary!r}"
    )


def test_brazil_metrics_agree_between_json_and_csv(brazil_bundle_dir: Path) -> None:
    canonical_dir = brazil_bundle_dir / "reports" / "aoi_report_v2" / AOI_ID
    report = json.loads((canonical_dir / "report.json").read_text(encoding="utf-8"))
    metrics = report["metrics"]
    assert "dummy_metric" not in metrics, "placeholder dummy_metric leaked into a JRC/commodity report"

    with (canonical_dir / "metrics.csv").open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == ["variable", "value", "unit", "source", "notes"]
        csv_values = {row["variable"]: row["value"] for row in reader}

    for name, entry in metrics.items():
        assert name in csv_values, f"metric {name} missing from metrics.csv"
        value = entry["value"]
        csv_value = csv_values[name]
        if isinstance(value, bool):
            assert csv_value.lower() == str(value).lower(), (
                f"metric {name} disagrees between report.json ({value}) and metrics.csv ({csv_value})"
            )
        else:
            assert abs(float(csv_value) - float(value)) < 1e-6, (
                f"metric {name} disagrees between report.json ({value}) and metrics.csv ({csv_value})"
            )


def test_brazil_headline_metrics_agree_in_pdf_text(brazil_bundle_dir: Path) -> None:
    _, pdftotext = _require_poppler()
    canonical_dir = brazil_bundle_dir / "reports" / "aoi_report_v2" / AOI_ID
    report = json.loads((canonical_dir / "report.json").read_text(encoding="utf-8"))
    metrics = report["metrics"]

    proc = subprocess.run(
        [pdftotext, str(canonical_dir / "report.pdf"), "-"],
        text=True,
        capture_output=True,
        check=True,
    )
    pdf_text = proc.stdout

    headline_metric_names = [
        "aoi_area_ha",
        "forest_baseline_2020_ha",
        "forest_loss_post_2020_on_baseline_ha",
        "post_2020_loss_and_commodity_overlap_ha",
    ]
    for name in headline_metric_names:
        value = metrics[name]["value"]
        rendered = f"{value:,.1f}"
        assert rendered in pdf_text, (
            f"headline metric {name}={value} (expected '{rendered}') not found verbatim in report.pdf text"
        )


def test_brazil_deterministic_artifacts_page_rows_are_all_in_root_manifest(
    brazil_bundle_dir: Path,
) -> None:
    manifest = json.loads((brazil_bundle_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest_relpaths = {entry["relpath"] for entry in manifest["artifacts"]}
    canonical_prefix = f"reports/aoi_report_v2/{AOI_ID}/"
    for fname in REQUIRED_DETERMINISTIC_ARTIFACTS_PAGE_FILES:
        relpath = canonical_prefix + fname
        assert relpath in manifest_relpaths, (
            f"page-11 (Deterministic Artifacts) file not represented in root manifest.json: {relpath}"
        )
