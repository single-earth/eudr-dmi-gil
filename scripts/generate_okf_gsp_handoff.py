#!/usr/bin/env python3
"""Assemble + self-verify an `okf-gsp-evidence-handoff-v1` JSON for a generated AOI bundle.

There is no committed handoff-writer in this repo (the existing Brazil coffee handoff at
`out/okf_handoffs/eudr-coffee-brazil-minas-gerais.json` was hand-assembled, not produced by
checked-in code). This script derives the smallest compatible contract from that existing
integration and self-verifies every path/hash/page-count against the files actually on disk,
refusing to write a handoff that references anything stale or missing.

Artifact `role`/`required` classification here is a simplified, regex-based version of the
richer, hand-curated Brazil handoff (which also records exact PDF page numbers per artifact).
This script does not attempt to re-derive per-artifact PDF page numbers; `report_pages` is left
empty except for the canonical report.pdf itself, which lists every page.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from eudr_dmi_gil.reports.bundle import compute_sha256  # noqa: E402

_ROLE_RULES: list[tuple[str, str]] = [
    ("inputs/aoi.geojson", "aoi_geometry"),
    ("inputs/aoi.wkt", "aoi_geometry"),
    ("inputs/satellite_baseline.tif", "satellite_baseline_raster"),
    ("inputs/satellite_recent.tif", "satellite_recent_raster"),
    ("inputs/satellite_regional.tif", "satellite_regional_raster"),
    ("inputs/regional_admin_boundaries.geojson", "regional_admin_boundaries"),
    ("/report.json", "canonical_report_json"),
    ("/report.html", "canonical_report_html"),
    ("/report.pdf", "canonical_report_pdf"),
    ("/metrics.csv", "canonical_metrics_csv"),
    ("/manifest.sha256", "canonical_manifest"),
    ("_commodity_summary.json", "commodity_summary"),
    ("_commodity_mask.geojson", "commodity_mask"),
    ("_post2020_loss_overlap_mask.geojson", "commodity_post2020_loss_overlap_mask"),
    ("_commodity_debug.json", "commodity_debug"),
    ("_on_jrc_forest_2020_mask.geojson", "post_2020_loss_on_2020_forest_mask"),
    ("jrc_forest_2020_mask.geojson", "jrc_forest_2020_mask"),
    ("jrc_post2020_loss_2021", "post_2020_loss_on_2020_forest_summary"),
    ("jrc_post2020_loss_debug.json", "post_2020_loss_on_2020_forest_debug"),
    ("evidence/01_aoi_satellite.png", "aoi_satellite_basemap"),
    ("evidence/02_jrc_forest_2020.png", "jrc_forest_2020_mask_png"),
    ("evidence/03_forest_loss", "post_2020_loss_on_2020_forest_mask_png"),
    ("evidence/04_commodity_layer.png", "commodity_mask_png"),
    ("evidence/05_intersection.png", "commodity_post2020_loss_overlap_mask_png"),
    ("evidence/06_before_after.png", "before_after_satellite_png"),
    ("evidence/07_regional_overview.png", "regional_overview_png"),
    ("evidence/08_cover_hero.png", "cover_hero_png"),
    ("evidence/legend.png", "map_legend_png"),
    ("fdp_coffee_acquisition_metadata.json", "commodity_source_acquisition_metadata"),
]

_REQUIRED_ROLES = {
    "aoi_geometry",
    "canonical_report_json",
    "canonical_report_html",
    "canonical_report_pdf",
    "canonical_metrics_csv",
    "canonical_manifest",
    "commodity_summary",
    "commodity_mask",
    "commodity_post2020_loss_overlap_mask",
    "commodity_debug",
}


def _mime_type(path: Path) -> str:
    suffix = path.suffix.lower()
    return {
        ".json": "application/json",
        ".geojson": "application/geo+json",
        ".csv": "text/csv",
        ".html": "text/html",
        ".pdf": "application/pdf",
        ".png": "image/png",
        ".tif": "image/tiff",
        ".tiff": "image/tiff",
        ".sha256": "text/plain",
        ".wkt": "text/plain",
    }.get(suffix, "application/octet-stream")


def _role_for(relpath: str) -> str | None:
    for pattern, role in _ROLE_RULES:
        if pattern in relpath:
            return role
    return None


def _git_commit(repo_dir: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo_dir), "rev-parse", "HEAD"], text=True
    ).strip()


def _git_dirty(repo_dir: Path) -> bool:
    out = subprocess.check_output(
        ["git", "-C", str(repo_dir), "status", "--porcelain"], text=True
    )
    return bool(out.strip())


# Fixed page sequence rendered by `report_model.render_canonical_pdf` (see its `new_page(n, ...)`
# calls plus the implicit cover page); identical for every report regardless of whether a
# commodity is configured, so it is safe to state here rather than re-deriving it at runtime.
_CANONICAL_REPORT_PAGE_TITLES = [
    "Cover",
    "Executive Summary",
    "Assessment Workflow",
    "Regional Overview",
    "Forest Baseline 2020",
    "Forest Loss After 2020",
    "Satellite Evidence",
    "Interpretation",
    "Data And Methods",
    "Audit Trail",
    "Deterministic Artifacts",
    "Appendix",
]


def _pdf_page_count(pdf_path: Path) -> int:
    try:
        import pypdf

        return len(pypdf.PdfReader(str(pdf_path)).pages)
    except Exception:
        pass
    out = subprocess.check_output(["pdfinfo", str(pdf_path)], text=True)
    for line in out.splitlines():
        if line.lower().startswith("pages:"):
            return int(line.split(":", 1)[1].strip())
    raise RuntimeError(f"Could not determine page count for {pdf_path}")


def build_handoff(
    *,
    bundle_dir: Path,
    aoi_id: str,
    task_bundle_id: str,
    commodity: str,
    country: str,
    country_code: str,
    region: str,
    generation_command: str,
    counterpart_repo: str,
    extra_datasets: list[dict[str, Any]],
) -> dict[str, Any]:
    manifest_path = bundle_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Bundle manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    canonical_report_root = bundle_dir / "reports" / "aoi_report_v2" / aoi_id
    canonical_report_json_path = canonical_report_root / "report.json"
    canonical_report_pdf_path = canonical_report_root / "report.pdf"
    if not canonical_report_json_path.is_file():
        raise FileNotFoundError(f"Canonical report.json not found: {canonical_report_json_path}")
    if not canonical_report_pdf_path.is_file():
        raise FileNotFoundError(f"Canonical report.pdf not found: {canonical_report_pdf_path}")

    canonical_report = json.loads(canonical_report_json_path.read_text(encoding="utf-8"))
    page_count = _pdf_page_count(canonical_report_pdf_path)
    report_pdf_relpath = str(canonical_report_pdf_path.relative_to(bundle_dir)).replace("\\", "/")
    report_pdf_sha256 = compute_sha256(canonical_report_pdf_path)

    artifacts: list[dict[str, Any]] = []
    for record in manifest.get("artifacts", manifest.get("records", [])):
        relpath = record["relpath"]
        abs_path = bundle_dir / relpath
        if not abs_path.is_file():
            raise FileNotFoundError(f"Manifest references missing artifact: {relpath}")
        actual_sha256 = compute_sha256(abs_path)
        if actual_sha256 != record["sha256"]:
            raise ValueError(
                f"Manifest sha256 mismatch for {relpath}: "
                f"manifest={record['sha256']} actual={actual_sha256}"
            )
        role = _role_for(relpath)
        report_pages = list(range(1, page_count + 1)) if relpath == report_pdf_relpath else []
        artifacts.append(
            {
                "path": relpath,
                "mime_type": record.get("content_type") or _mime_type(abs_path),
                "sha256": actual_sha256,
                "size_bytes": abs_path.stat().st_size,
                "role": role or "unclassified_artifact",
                "required": role in _REQUIRED_ROLES,
                "report_pages": [str(p) for p in report_pages],
                "description": f"Bundle artifact ({role or 'unclassified'}): {relpath}",
            }
        )
    artifacts.sort(key=lambda a: a["path"])

    counterpart_commit = _git_commit(REPO_ROOT)
    counterpart_dirty = _git_dirty(REPO_ROOT)

    datasets = [
        {
            "dataset_id": "aoi_geometry_input",
            "license": "user_supplied",
            "source_url": "inputs/aoi.geojson",
            "version": "user_supplied",
        }
    ] + extra_datasets

    handoff = {
        "schema_version": "okf-gsp-evidence-handoff-v1",
        "task_bundle_id": task_bundle_id,
        "aoi_id": aoi_id,
        "commodity": commodity,
        "country": country,
        "country_code": country_code,
        "region": region,
        "purpose": "eudr",
        "generation_command": generation_command,
        "counterpart_repository": counterpart_repo,
        "counterpart_commit": counterpart_commit,
        "counterpart_dirty": counterpart_dirty,
        "evidence_bundle_id": bundle_dir.name,
        "evidence_bundle_path": str(bundle_dir.resolve()),
        "bundle_manifest_relpath": "manifest.json",
        "bundle_manifest_sha256": compute_sha256(manifest_path),
        "report_root_relpath": str(canonical_report_root.relative_to(bundle_dir)).replace("\\", "/"),
        "report_pdf_relpath": report_pdf_relpath,
        "report_pdf_sha256": report_pdf_sha256,
        "report_page_count": page_count,
        "report_page_titles": _CANONICAL_REPORT_PAGE_TITLES[:page_count],
        "resolved_end_year": canonical_report.get("temporal_scope", {}).get("effective_end_year"),
        "datasets": datasets,
        "artifacts": artifacts,
    }
    return handoff


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bundle-dir", required=True, type=Path)
    p.add_argument("--aoi-id", required=True)
    p.add_argument("--task-bundle-id", required=True)
    p.add_argument("--commodity", default="coffee")
    p.add_argument("--country", required=True)
    p.add_argument("--country-code", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--generation-command", required=True)
    p.add_argument("--counterpart-repository", default="single-earth/eudr-dmi-gil")
    p.add_argument("--extra-dataset", action="append", default=[], help="JSON object, repeatable")
    p.add_argument("--out", required=True, type=Path)
    args = p.parse_args(argv)

    extra_datasets = [json.loads(s) for s in args.extra_dataset]
    handoff = build_handoff(
        bundle_dir=args.bundle_dir,
        aoi_id=args.aoi_id,
        task_bundle_id=args.task_bundle_id,
        commodity=args.commodity,
        country=args.country,
        country_code=args.country_code,
        region=args.region,
        generation_command=args.generation_command,
        counterpart_repo=args.counterpart_repository,
        extra_datasets=extra_datasets,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote {args.out}")
    print(f"counterpart_commit: {handoff['counterpart_commit']} (dirty={handoff['counterpart_dirty']})")
    print(f"report_page_count: {handoff['report_page_count']}")
    print(f"artifacts: {len(handoff['artifacts'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
