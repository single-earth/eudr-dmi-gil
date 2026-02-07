from __future__ import annotations

import argparse
import os
import re
import sys
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import csv

from .bundle import bundle_dir as compute_bundle_dir
from .bundle import compute_sha256
from .bundle import resolve_evidence_root, write_manifest
from .determinism import canonical_json_bytes, sha256_bytes, write_bytes
from eudr_dmi_gil.deps.hansen_acquire import build_entries_from_provenance
from eudr_dmi_gil.geo.aoi_area import compute_aoi_geodesic_area_ha
from .policy_refs import collect_policy_mapping_refs


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _utc_now_compact() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def _git_commit() -> str:
    override = os.environ.get("EUDR_DMI_GIT_COMMIT")
    if override:
        return override.strip()
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out
    except Exception:
        return ""


def _sanitize_id(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("empty id")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def _rel_href(from_path: Path, to_path: Path) -> str:
    rel = os.path.relpath(to_path, start=from_path.parent)
    return Path(rel).as_posix()


def _content_type_for_path(path: Path) -> str | None:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return "application/json"
    if suffix == ".geojson":
        return "application/geo+json"
    if suffix == ".csv":
        return "text/csv"
    if suffix == ".html":
        return "text/html"
    if suffix == ".wkt":
        return "text/plain"
    return None


def _render_html_summary(report: dict[str, Any], *, html_path: Path, artifact_paths: list[Path]) -> str:
    def row(k: str, v: str) -> str:
        return f"<tr><th>{k}</th><td>{v}</td></tr>"

    def link_row(label: str, relpath: str) -> str:
        return f"<tr><th>{label}</th><td><a href=\"{relpath}\">{relpath}</a></td></tr>"

    summary = report.get("results_summary", {})
    aoi_area = summary.get("aoi_area", {})
    deforestation = summary.get("deforestation_free_post_2020", {})

    aoi_id = report.get("aoi_id", "(unknown)")
    bundle_id = report.get("bundle_id", "(unknown)")
    generated = report.get("generated_at_utc", "(unknown)")
    version = report.get("report_version", "(unknown)")
    geom_ref = report.get("aoi_geometry_ref", {})

    bundle_root = html_path.parents[2]
    evidence_artifacts = report.get("evidence_artifacts", [])
    evidence_rows = []
    for item in evidence_artifacts:
        if not isinstance(item, dict):
            continue
        relpath = item.get("relpath")
        if not isinstance(relpath, str):
            continue
        abs_path = bundle_root / relpath
        href = _rel_href(html_path, abs_path)
        role = (item.get("meta") or {}).get("role") if isinstance(item.get("meta"), dict) else ""
        evidence_rows.append(
            f"<tr><td><a href=\"{href}\">{relpath}</a></td><td><code>{item.get('sha256')}</code></td><td>{item.get('size_bytes','')}</td><td>{role}</td></tr>"
        )

    evidence_table = "\n".join(evidence_rows) if evidence_rows else "<tr><td colspan=\"4\">(none)</td></tr>"

    datasets_rows = []
    for ds in report.get("datasets", []) or []:
        if not isinstance(ds, dict):
            continue
        datasets_rows.append(
            f"<tr><td>{ds.get('dataset_id')}</td><td>{ds.get('version','')}</td><td>{ds.get('retrieved_at_utc','')}</td><td>{ds.get('license','')}</td><td>{ds.get('source_url','')}</td></tr>"
        )
    datasets_table = "\n".join(datasets_rows) if datasets_rows else "<tr><td colspan=\"5\">(none)</td></tr>"

    params_rows = []
    for key, value in sorted((report.get("parameters") or {}).items(), key=lambda kv: kv[0]):
        params_rows.append(row(key, str(value)))
    params_table = "\n".join(params_rows) if params_rows else row("(none)", "")

    mapping_rows = []
    for entry in report.get("policy_mapping", []) or []:
        if not isinstance(entry, dict):
            continue
        evidence_fields = ", ".join(entry.get("evidence_fields") or [])
        artifacts = ", ".join(
            f"<a href=\"{_rel_href(html_path, bundle_root / rel)}\">{rel}</a>"
            for rel in entry.get("artifact_relpaths") or []
        )
        mapping_rows.append(
            f"<tr><td>{entry.get('article_ref')}</td><td>{entry.get('requirement')}</td><td>{evidence_fields}</td><td>{artifacts}</td><td>{entry.get('status')}</td></tr>"
        )
    mapping_table = "\n".join(mapping_rows) if mapping_rows else "<tr><td colspan=\"5\">(none)</td></tr>"

    deforestation_rows = []
    if deforestation:
        deforestation_rows.extend(
            [
                row("Definition", "Forest loss after 2020-12-31 (pixel-wise intersection)"),
                row("Forest loss (ha)", str(deforestation.get("forest_loss_post_2020_ha"))),
                row("Percent of AOI", str(deforestation.get("percent_of_aoi"))),
                row("Threshold (ha)", str(deforestation.get("threshold_ha"))),
                row("Status", str(deforestation.get("status"))),
                row("Uncertainty", str(deforestation.get("uncertainty"))),
            ]
        )
    deforestation_table = "\n".join(deforestation_rows) if deforestation_rows else row("(none)", "")

    artifacts_for_links = "\n".join(
        f"<li><a href=\"{_rel_href(html_path, p)}\">{_rel_href(html_path, p)}</a></li>"
        for p in sorted(artifact_paths, key=lambda p: p.as_posix())
    )

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>AOI Report — {aoi_id}</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 24px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
    th {{ background: #f6f6f6; text-align: left; width: 240px; }}
    h2 {{ margin-top: 28px; }}
    code {{ background: #f6f6f6; padding: 1px 4px; border-radius: 4px; }}
    .muted {{ color: #666; }}
  </style>
</head>
<body>
  <h1>AOI Report</h1>
  <table>
    {row('AOI', str(aoi_id))}
    {row('Bundle', str(bundle_id))}
    {row('Generated (UTC)', str(generated))}
    {row('Report Version', str(version))}
    {row('Geometry Ref', f"{geom_ref.get('kind')}: {geom_ref.get('value')}")}
    {row('AOI area (ha)', str(aoi_area.get('area_ha')))}
    {row('AOI area method', str(aoi_area.get('method')))}
  </table>

  <h2>Deforestation-free (post-2020)</h2>
  <table>
    {deforestation_table}
  </table>

  <h2>Forest baseline / forest mask</h2>
  <table>
    {row('Tree cover threshold (%)', str((report.get('parameters', {}).get('forest_loss_post_2020') or {}).get('canopy_threshold_percent', '')))}
    {row('Baseline year', '2000')}
    {row('Cutoff year', str((report.get('parameters', {}).get('forest_loss_post_2020') or {}).get('cutoff_year', '')))}
  </table>

  <h2>Data sources & provenance</h2>
  <table>
    <tr><th>dataset_id</th><th>version</th><th>retrieved_at_utc</th><th>license</th><th>source_url</th></tr>
    {datasets_table}
  </table>

  <h2>Methods & parameters</h2>
  <table>
    {params_table}
  </table>

  <h2>Traceability to EUDR Articles</h2>
  <table>
    <tr><th>Article</th><th>Requirement</th><th>Evidence fields</th><th>Artifacts</th><th>Status</th></tr>
    {mapping_table}
  </table>

  <h2>Evidence artifact index</h2>
  <table>
    <tr><th>relpath</th><th>sha256</th><th>size_bytes</th><th>role</th></tr>
    {evidence_table}
  </table>

  <h2>Bundle artifacts</h2>
  <ul>
    {artifacts_for_links}
  </ul>
</body>
</html>
"""


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m eudr_dmi_gil.reports.cli",
        description="Generate a deterministic AOI report bundle (JSON + HTML + manifest).",
    )

    p.add_argument("--aoi-id", required=True, help="AOI identifier")

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--aoi-geojson", help="Path to AOI geometry GeoJSON")
    g.add_argument("--aoi-wkt", help="AOI geometry WKT string")

    p.add_argument(
        "--bundle-id",
        help=(
            "Optional bundle id. If omitted, a deterministic id is derived from aoi-id + UTC timestamp."
        ),
    )

    p.add_argument(
        "--out-format",
        choices=["json", "html", "both"],
        default="both",
        help="Which outputs to write into the bundle.",
    )

    p.add_argument(
        "--policy-mapping-ref",
        action="append",
        default=[],
        help="Optional policy-to-evidence spine reference (repeatable).",
    )

    p.add_argument(
        "--policy-mapping-ref-file",
        action="append",
        default=[],
        help=(
            "Path to a file containing newline-separated policy-to-evidence spine references "
            "(repeatable). Lines starting with '#' are ignored."
        ),
    )

    p.add_argument(
        "--dummy-metric",
        default="dummy_metric=1:count",
        help="Metric in the form name=value:unit (for scaffolding).",
    )

    p.add_argument(
        "--metric",
        action="append",
        default=[],
        help=(
            "Metric row in the form variable=value:unit[:source[:notes]] (repeatable). "
            "If omitted, --dummy-metric is used."
        ),
    )

    p.add_argument(
        "--enable-hansen-post-2020-loss",
        action="store_true",
        help="Enable Hansen post-2020 forest loss pixel-mask computation.",
    )

    p.add_argument(
        "--hansen-tile-dir",
        help="Optional Hansen tile directory (defaults to EUDR_DMI_HANSEN_TILE_DIR).",
    )

    p.add_argument(
        "--hansen-canopy-threshold",
        type=int,
        default=30,
        help="Tree cover canopy threshold percent for baseline forest mask (default: 30).",
    )

    p.add_argument(
        "--hansen-cutoff-year",
        type=int,
        default=2020,
        help="Cutoff year for post-loss filter (default: 2020).",
    )

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    policy_mapping_refs = collect_policy_mapping_refs(
        refs=list(args.policy_mapping_ref or []),
        ref_files=list(args.policy_mapping_ref_file or []),
    )

    aoi_id = _sanitize_id(args.aoi_id)

    generated_at_utc = _utc_now_iso()

    bundle_id = args.bundle_id
    if not bundle_id:
        # Deterministic derivation from AOI id + timestamp (timestamp is explicit).
        stamp = _utc_now_compact()
        bundle_id = f"{aoi_id}-{stamp}"
    bundle_id = _sanitize_id(bundle_id)

    # Bundle date is UTC date.
    bundle_date = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")

    bdir = compute_bundle_dir(bundle_id=bundle_id, bundle_date=bundle_date)
    resolve_evidence_root()

    # Write geometry into the bundle for portability.
    inputs_dir = bdir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)

    if args.aoi_geojson:
        geojson_src = Path(args.aoi_geojson)
        geo_bytes = geojson_src.read_bytes()
        geo_rel = Path("inputs") / "aoi.geojson"
        geo_kind = "geojson"
    else:
        geo_bytes = (args.aoi_wkt.strip() + "\n").encode("utf-8")
        geo_rel = Path("inputs") / "aoi.wkt"
        geo_kind = "wkt"

    geo_path = bdir / geo_rel
    write_bytes(geo_path, geo_bytes)
    geo_sha = sha256_bytes(geo_bytes)

    aoi_area_ha: float | None = None
    aoi_area_method = ""
    if geo_kind == "geojson":
        try:
            aoi_area_ha, aoi_area_method = compute_aoi_geodesic_area_ha(geo_path)
        except Exception:
            aoi_area_ha = None
            aoi_area_method = ""

    fallback_dummy = None if args.enable_hansen_post_2020_loss else args.dummy_metric
    metric_rows = _parse_metric_rows(args.metric, fallback_dummy=fallback_dummy)
    if aoi_area_ha is not None:
        metric_rows.append(
            MetricRow(
                variable="aoi_area_ha",
                value=aoi_area_ha,
                unit="ha",
                source="geometry",
                notes=aoi_area_method or "",
            )
        )
        metric_rows = sorted(metric_rows, key=lambda r: r.variable)

    forest_loss_threshold_ha = 0.0
    forest_loss_percent_of_aoi: float | None = None
    forest_loss_status = "na"

    hansen_result = None
    hansen_analysis = None
    hansen_methodology_block: dict[str, Any] | None = None
    hansen_computed_block: dict[str, Any] | None = None
    hansen_computed_outputs_block: dict[str, Any] | None = None
    hansen_acceptance_criteria_block: dict[str, Any] | None = None
    hansen_result_block: dict[str, Any] | None = None
    hansen_external_dependencies: list[dict[str, Any]] | None = None
    if args.enable_hansen_post_2020_loss:
        from eudr_dmi_gil.analysis.forest_loss_post_2020 import run_forest_loss_post_2020
        from eudr_dmi_gil.tasks.forest_loss_post_2020 import load_hansen_config

        if geo_kind != "geojson":
            raise RuntimeError("Hansen post-2020 loss requires --aoi-geojson input")

        hansen_config = load_hansen_config(
            tile_dir=Path(args.hansen_tile_dir) if args.hansen_tile_dir else None,
            canopy_threshold_percent=args.hansen_canopy_threshold,
            cutoff_year=args.hansen_cutoff_year,
            write_masks=True,
            aoi_geojson_path=geo_path,
        )
        hansen_output_dir = bdir / "reports" / "aoi_report_v2" / aoi_id / "hansen"
        hansen_analysis = run_forest_loss_post_2020(
            aoi_geojson_path=geo_path,
            output_dir=hansen_output_dir,
            config=hansen_config,
            aoi_id=aoi_id,
            run_id=bundle_id,
        )
        hansen_result = hansen_analysis.raw

        if aoi_area_ha:
            forest_loss_percent_of_aoi = (
                hansen_result.forest_loss_post_2020_ha / aoi_area_ha
            ) * 100.0
            forest_loss_status = (
                "pass"
                if hansen_result.forest_loss_post_2020_ha <= forest_loss_threshold_ha
                else "fail"
            )

        metric_rows.extend(
            [
                MetricRow(
                    variable="pixel_forest_loss_post_2020_ha",
                    value=hansen_result.forest_loss_post_2020_ha,
                    unit="ha",
                    source="hansen_gfc",
                    notes="pixel_mask",
                ),
                MetricRow(
                    variable="pixel_initial_tree_cover_ha",
                    value=hansen_result.initial_tree_cover_ha,
                    unit="ha",
                    source="hansen_gfc",
                    notes="pixel_mask",
                ),
                MetricRow(
                    variable="pixel_current_tree_cover_ha",
                    value=hansen_result.current_tree_cover_ha,
                    unit="ha",
                    source="hansen_gfc",
                    notes="pixel_mask",
                ),
            ]
        )
        if forest_loss_percent_of_aoi is not None:
            metric_rows.append(
                MetricRow(
                    variable="forest_loss_post_2020_percent_of_aoi",
                    value=forest_loss_percent_of_aoi,
                    unit="percent",
                    source="hansen_gfc",
                    notes="forest_loss_post_2020_ha / aoi_area_ha",
                )
            )
        metric_rows = sorted(metric_rows, key=lambda r: r.variable)

        hansen_methodology_block = {
            "forest_loss_post_2020": {
                "data_sources": ["hansen_global_forest_change"],
                "dataset_version": hansen_config.dataset_version,
                "forest_definition": {
                    "tree_cover_threshold_percent": hansen_config.canopy_threshold_percent
                },
                "calculation": {
                    "method": "pixel_wise_intersection",
                    "cutoff_date": f"{hansen_config.cutoff_year}-12-31",
                    "area_units": "ha",
                },
                "resolution": {"pixel_size_m": 30},
                "tile_source": hansen_config.tile_source,
                "tile_source_url_template": hansen_config.url_template,
                "is_placeholder": False,
            }
        }

        hansen_computed_block = {
            "forest_loss_post_2020": {
                "pixel_initial_tree_cover_ha": hansen_result.initial_tree_cover_ha,
                "pixel_forest_loss_post_2020_ha": hansen_result.forest_loss_post_2020_ha,
                "pixel_current_tree_cover_ha": hansen_result.current_tree_cover_ha,
                "mask_forest_loss_post_2020": str(
                    hansen_result.mask_forest_loss_post_2020_path.relative_to(bdir)
                ).replace("\\", "/"),
                "mask_forest_current_year": str(
                    hansen_result.mask_forest_current_path.relative_to(bdir)
                ).replace("\\", "/"),
                "tiles_manifest": str(
                    hansen_analysis.tiles_manifest_path.relative_to(bdir)
                ).replace("\\", "/"),
            }
        }

        hansen_computed_outputs_block = {
            "forest_loss_post_2020": {
                "area_ha": hansen_analysis.computed.area_ha,
                "pixel_size_m": hansen_analysis.computed.pixel_size_m,
                "mask_geojson_ref": {
                    "relpath": str(hansen_analysis.loss_mask_path.relative_to(bdir)).replace(
                        "\\", "/"
                    ),
                    "sha256": compute_sha256(hansen_analysis.loss_mask_path),
                    "content_type": "application/geo+json",
                },
                "tiles_manifest_ref": {
                    "relpath": str(hansen_analysis.tiles_manifest_path.relative_to(bdir)).replace(
                        "\\", "/"
                    ),
                    "sha256": compute_sha256(hansen_analysis.tiles_manifest_path),
                    "content_type": "application/json",
                },
            }
        }

        hansen_acceptance_criteria_block = {
            "criteria_id": "forest_loss_post_2020_max_ha",
            "description": "Forest loss post-2020 must be <= 0 ha.",
            "evidence_classes": ["forest_loss_post_2020"],
            "decision_type": "threshold",
        }
        hansen_result_block = {
            "result_id": "forest_loss_post_2020_max_ha",
            "criteria_ids": ["forest_loss_post_2020_max_ha"],
            "evidence_classes": ["forest_loss_post_2020"],
            "status": forest_loss_status,
            "observed_value": hansen_result.forest_loss_post_2020_ha,
            "threshold_value": forest_loss_threshold_ha,
            "unit": "ha",
        }

        entries = hansen_config.tile_entries or build_entries_from_provenance(
            hansen_analysis.raw.tile_provenance,
            tile_dir=hansen_config.tile_dir,
            url_template=hansen_config.url_template,
        )
        tiles_used = sorted(
            [
                {
                    "tile_id": e.tile_id,
                    "layer": e.layer,
                    "local_path": e.local_path,
                    "sha256": e.sha256,
                    "size_bytes": e.size_bytes,
                    "source_url": e.source_url,
                }
                for e in entries
            ],
            key=lambda item: (item.get("tile_id", ""), item.get("layer", ""), item.get("local_path", "")),
        )

        hansen_external_dependencies = [
            {
                "dependency_id": "hansen_gfc_2024_v1_12",
                "dataset_version": hansen_config.dataset_version,
                "tile_source": hansen_config.tile_source,
                "aoi_geojson_sha256": geo_sha,
                "tiles_manifest": {
                    "relpath": str(hansen_analysis.tiles_manifest_path.relative_to(bdir)).replace(
                        "\\", "/"
                    ),
                    "sha256": compute_sha256(hansen_analysis.tiles_manifest_path),
                },
                "tiles_used": tiles_used,
            }
        ]

    policy_mapping_refs = policy_mapping_refs or [
        "policy-spine:eudr/article-3",
        "policy-spine:eudr/article-9",
    ]

    datasets: list[dict[str, Any]] = [
        {
            "dataset_id": "aoi_geometry_input",
            "version": "user_supplied",
            "retrieved_at_utc": generated_at_utc,
            "license": "user_supplied",
            "source_url": geo_rel.as_posix(),
        }
    ]
    if hansen_result is not None:
        datasets.append(
            {
                "dataset_id": "hansen_gfc_2024_v1_12",
                "version": hansen_config.dataset_version,
                "retrieved_at_utc": generated_at_utc,
                "license": "Hansen GFC (public)",
                "source_url": "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2024-v1.12/",
            }
        )

    parameters: dict[str, Any] = {
        "aoi_area_method": aoi_area_method or "unknown",
        "implementation": {
            "forest_loss_post_2020": "v1",
            "git_commit": _git_commit(),
        },
    }
    if hansen_result is not None:
        parameters["forest_loss_post_2020"] = {
            "canopy_threshold_percent": hansen_config.canopy_threshold_percent,
            "cutoff_year": hansen_config.cutoff_year,
            "acceptance_threshold_ha": forest_loss_threshold_ha,
            "pixel_area_method": "geodesic_wgs84_pyproj",
        }

    results_summary: dict[str, Any] = {
        "aoi_area": {
            "area_ha": aoi_area_ha if aoi_area_ha is not None else 0.0,
            "method": aoi_area_method or "unknown",
        }
    }
    if hansen_result is not None and forest_loss_percent_of_aoi is not None:
        results_summary["deforestation_free_post_2020"] = {
            "forest_loss_post_2020_ha": hansen_result.forest_loss_post_2020_ha,
            "percent_of_aoi": forest_loss_percent_of_aoi,
            "threshold_ha": forest_loss_threshold_ha,
            "status": forest_loss_status,
            "uncertainty": {
                "pixel_area_method": "geodesic_wgs84_pyproj",
                "nodata": "masked_as_no_loss",
                "projection": "EPSG:4326",
                "conservative_bounds": "area estimates are lower-bound for masked/no-data pixels",
            },
        }

    policy_mapping: list[dict[str, Any]] = [
        {
            "article_ref": "EUDR Article 9",
            "requirement": "AOI geometry is declared and traceable",
            "evidence_fields": ["aoi_geometry_ref", "inputs.sources"],
            "artifact_relpaths": [geo_rel.as_posix()],
            "status": "pass",
        }
    ]
    if hansen_result is not None and hansen_analysis is not None:
        policy_mapping.append(
            {
                "article_ref": "EUDR Article 3",
                "requirement": "Deforestation-free after 2020-12-31",
                "evidence_fields": [
                    "results_summary.deforestation_free_post_2020",
                    "computed.forest_loss_post_2020.pixel_forest_loss_post_2020_ha",
                ],
                "artifact_relpaths": [
                    str(hansen_analysis.loss_mask_path.relative_to(bdir)).replace("\\", "/"),
                    str(hansen_analysis.tiles_manifest_path.relative_to(bdir)).replace("\\", "/"),
                ],
                "status": forest_loss_status,
            }
        )

    in_scope_articles = ["article-3"] if hansen_result is not None else []
    report: dict[str, Any] = {
        "report_version": "aoi_report_v2",
        "generated_at_utc": generated_at_utc,
        "bundle_id": bundle_id,
        "report_metadata": {
            "report_type": "example",
            "regulatory_context": {
                "regulation": "EUDR",
                "in_scope_articles": in_scope_articles,
                "out_of_scope_articles": [],
            },
            "assessment_capability": "inspectable_only",
        },
        "computed": {},
        "computed_outputs": {},
        "validation": {},
        "methodology": {},
        "external_dependencies": [],
        "aoi_id": aoi_id,
        "aoi_geometry_ref": {
            "kind": geo_kind,
            "value": geo_rel.as_posix(),
            "sha256": geo_sha,
        },
        "inputs": {
            "sources": [
                {
                    "source_id": "aoi_geometry",
                    "sha256": geo_sha,
                    "uri": geo_rel.as_posix(),
                    "content_type": "application/geo+json" if geo_kind == "geojson" else "text/plain",
                }
            ]
        },
        "metrics": _metrics_from_rows(metric_rows),
        "evidence_artifacts": [],
        "parameters": parameters,
        "datasets": datasets,
        "policy_mapping": policy_mapping,
        "results_summary": results_summary,
        "evidence_registry": {
            "evidence_classes": [
                {
                    "class_id": "aoi_geometry",
                    "mandatory": True,
                    "status": "present",
                }
            ]
        },
        "acceptance_criteria": [
            {
                "criteria_id": "aoi_geometry_present",
                "description": "AOI geometry is present and referenced in inputs.",
                "evidence_classes": ["aoi_geometry"],
                "decision_type": "presence",
            }
        ],
        "results": [
            {
                "result_id": "result-001",
                "criteria_ids": ["aoi_geometry_present"],
                "status": "pass",
            }
        ],
        "assumptions": [],
        "regulatory_traceability": [
            {
                "regulation": "EUDR",
                "article_ref": "article-3",
                "evidence_class": "aoi_geometry",
                "acceptance_criteria": "aoi_geometry_present",
                "result_ref": "result-001",
            }
        ],
        "policy_mapping_refs": policy_mapping_refs,
        "extensions": {
            "metrics_rows_v1": [
                {
                    "variable": r.variable,
                    "value": r.value,
                    "unit": r.unit,
                    "source": r.source,
                    "notes": r.notes,
                }
                for r in metric_rows
            ]
        },
    }

    artifact_paths: list[Path] = [geo_path]
    if hansen_result is not None and hansen_analysis is not None:
        if hansen_methodology_block is not None:
            report["methodology"] = hansen_methodology_block
        if hansen_computed_block is not None:
            report["computed"] = hansen_computed_block
        if hansen_computed_outputs_block is not None:
            report["computed_outputs"] = hansen_computed_outputs_block
        if hansen_acceptance_criteria_block is not None:
            report["acceptance_criteria"].append(hansen_acceptance_criteria_block)
        if hansen_result_block is not None:
            report["results"].append(hansen_result_block)
            report["regulatory_traceability"].append(
                {
                    "regulation": "EUDR",
                    "article_ref": "article-3",
                    "evidence_class": "forest_loss_post_2020",
                    "acceptance_criteria": "forest_loss_post_2020_max_ha",
                    "result_ref": "forest_loss_post_2020_max_ha",
                }
            )
        if hansen_external_dependencies is not None:
            report["external_dependencies"] = hansen_external_dependencies

        artifact_paths.append(hansen_analysis.summary_path)
        artifact_paths.extend(
            [
                hansen_analysis.loss_mask_path,
                hansen_analysis.current_mask_path,
                hansen_analysis.tiles_manifest_path,
            ]
        )

        loss_present = hansen_analysis.loss_mask_path.is_file()
        current_present = hansen_analysis.current_mask_path.is_file()
        summary_present = hansen_analysis.summary_path.is_file()
        tiles_manifest_present = hansen_analysis.tiles_manifest_path.is_file()

        report["evidence_registry"]["evidence_classes"].extend(
            [
                {
                    "class_id": "forest_loss_post_2020",
                    "mandatory": True,
                    "status": "present"
                    if loss_present and current_present and summary_present
                    else "missing",
                },
                {
                    "class_id": "hansen_tiles_provenance",
                    "mandatory": True,
                    "status": "present" if tiles_manifest_present else "missing",
                },
            ]
        )

    if geo_kind == "geojson":
        from eudr_dmi_gil.analysis.maaamet_validation import run_maaamet_crosscheck

        maaamet_dir = bdir / "reports" / "aoi_report_v2" / aoi_id / "maaamet"
        computed_current_forest = (
            hansen_result.current_tree_cover_ha if hansen_result is not None else None
        )
        maaamet_result = run_maaamet_crosscheck(
            aoi_geojson_path=geo_path,
            output_dir=maaamet_dir,
            computed_forest_area_ha=computed_current_forest,
        )
        crosscheck_block = {
            "source": "maaamet",
            "fields_used": maaamet_result.fields_used,
            "outcome": maaamet_result.outcome,
            "reference": {
                "source": maaamet_result.reference_source,
                "method": maaamet_result.reference_method,
                "value_ha": maaamet_result.reference_value_ha,
            },
            "computed": {"forest_area_ha": maaamet_result.computed_forest_area_ha},
            "comparison": {
                "tolerance_percent": maaamet_result.tolerance_percent,
                "diff_pct": maaamet_result.diff_pct,
            },
            "csv_ref": {
                "relpath": str(maaamet_result.csv_path.relative_to(bdir)).replace("\\", "/"),
                "sha256": compute_sha256(maaamet_result.csv_path),
                "content_type": "text/csv",
            },
            "summary_ref": {
                "relpath": str(maaamet_result.summary_path.relative_to(bdir)).replace("\\", "/"),
                "sha256": compute_sha256(maaamet_result.summary_path),
                "content_type": "application/json",
            },
        }
        if maaamet_result.reason:
            crosscheck_block["reason"] = maaamet_result.reason
        report["validation"]["forest_area_crosscheck"] = crosscheck_block
        artifact_paths.extend([maaamet_result.csv_path, maaamet_result.summary_path])

    # metrics.csv (portable, deterministic) lives alongside the report outputs.
    metrics_csv_path = bdir / "reports" / "aoi_report_v2" / aoi_id / "metrics.csv"
    _write_metrics_csv(metrics_csv_path, metric_rows)
    artifact_paths.append(metrics_csv_path)

    # JSON output
    report_json_path = bdir / "reports" / "aoi_report_v2" / f"{aoi_id}.json"
    if args.out_format in ("json", "both"):
        report_json_path.parent.mkdir(parents=True, exist_ok=True)
        report_json_bytes = canonical_json_bytes(report) + b"\n"
        report_json_path.write_bytes(report_json_bytes)
        artifact_paths.append(report_json_path)

    # HTML output
    report_html_path = bdir / "reports" / "aoi_report_v2" / f"{aoi_id}.html"
    if args.out_format in ("html", "both"):
        report_html_path.parent.mkdir(parents=True, exist_ok=True)
        # Link to whatever artifacts are already known; report JSON is included if produced.
        known_artifacts_for_html = list(artifact_paths)
        html = _render_html_summary(report, html_path=report_html_path, artifact_paths=known_artifacts_for_html)
        report_html_path.write_text(html, encoding="utf-8")
        artifact_paths.append(report_html_path)

    def _artifact_role(relpath: str) -> str | None:
        if relpath == geo_rel.as_posix():
            return "aoi_geometry"
        if relpath.endswith(f"/{aoi_id}.json"):
            return "report_json"
        if relpath.endswith(f"/{aoi_id}.html"):
            return "report_html"
        if relpath.endswith("/metrics.csv"):
            return "metrics_csv"
        if relpath.endswith("forest_loss_post_2020_mask.geojson"):
            return "forest_loss_mask"
        if relpath.endswith("forest_current_tree_cover_mask.geojson"):
            return "forest_current_mask"
        if relpath.endswith("forest_loss_post_2020_tiles.json"):
            return "hansen_tiles_manifest"
        if relpath.endswith("forest_loss_post_2020_summary.json"):
            return "forest_loss_summary"
        if relpath.endswith("maaamet_forest_area_crosscheck.csv"):
            return "maaamet_crosscheck_csv"
        if relpath.endswith("maaamet_forest_area_crosscheck_summary.json"):
            return "maaamet_crosscheck_summary"
        return None

    # Populate evidence_artifacts in report JSON (exclude manifest to avoid circularity).
    report["evidence_artifacts"] = []
    for p in sorted(set(artifact_paths), key=lambda p: p.as_posix()):
        relpath = str(p.relative_to(bdir)).replace("\\\\", "/")
        entry = {
            "relpath": relpath,
            "sha256": compute_sha256(p),
            "size_bytes": p.stat().st_size,
        }
        content_type = _content_type_for_path(p)
        if content_type:
            entry["content_type"] = content_type
        role = _artifact_role(relpath)
        if role:
            entry["meta"] = {"role": role}
        report["evidence_artifacts"].append(entry)

    # If we wrote report JSON, rewrite it now that evidence_artifacts is populated.
    if args.out_format in ("json", "both"):
        report_json_path.write_bytes(canonical_json_bytes(report) + b"\n")

    # Validate contract.
    from .validate import validate_aoi_report

    validate_aoi_report(report)

    # Manifest written by bundle writer.
    # Exclude manifest itself from artifacts passed to the writer.
    write_manifest(bdir, sorted(set(artifact_paths), key=lambda p: p.as_posix()))

    print(str(bdir))
    return 0


@dataclass(frozen=True)
class MetricRow:
    variable: str
    value: int | float
    unit: str
    source: str
    notes: str


def _parse_metric_rows(raw_metrics: list[str], *, fallback_dummy: str | None) -> list[MetricRow]:
    rows: list[MetricRow] = []

    if raw_metrics:
        for raw in raw_metrics:
            rows.append(_parse_metric_row(raw))
    elif fallback_dummy is not None:
        # Backwards-compat scaffolding.
        name, value, unit = _parse_dummy_metric(fallback_dummy)
        rows.append(MetricRow(variable=name, value=value, unit=unit, source="", notes=""))

    # Stable ordering.
    return sorted(rows, key=lambda r: r.variable)


def _parse_metric_row(raw: str) -> MetricRow:
    # variable=value:unit[:source[:notes]]
    if "=" not in raw or ":" not in raw:
        raise ValueError("--metric must be variable=value:unit[:source[:notes]]")

    variable, rest = raw.split("=", 1)
    parts = rest.split(":")
    if len(parts) < 2:
        raise ValueError("--metric must be variable=value:unit[:source[:notes]]")

    value_str = parts[0]
    unit = parts[1]
    source = parts[2] if len(parts) >= 3 else ""
    notes = ":".join(parts[3:]) if len(parts) >= 4 else ""

    variable = variable.strip()
    unit = unit.strip()
    source = source.strip()
    notes = notes.strip()

    if not variable or not unit:
        raise ValueError("--metric must be variable=value:unit[:source[:notes]]")

    value: int | float
    try:
        if "." in value_str:
            value = float(value_str)
        else:
            value = int(value_str)
    except ValueError:
        raise ValueError("--metric value must be int or float") from None

    return MetricRow(variable=variable, value=value, unit=unit, source=source, notes=notes)


def _parse_dummy_metric(raw: str) -> tuple[str, int | float, str]:
    # name=value:unit
    if "=" not in raw or ":" not in raw:
        raise ValueError("--dummy-metric must be name=value:unit")

    name, rest = raw.split("=", 1)
    value_str, unit = rest.split(":", 1)
    name = name.strip()
    unit = unit.strip()

    if not name or not unit:
        raise ValueError("--dummy-metric must be name=value:unit")

    value: int | float
    try:
        if "." in value_str:
            value = float(value_str)
        else:
            value = int(value_str)
    except ValueError:
        raise ValueError("--dummy-metric value must be int or float") from None

    return name, value, unit


def _metrics_from_rows(rows: list[MetricRow]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        entry: dict[str, Any] = {"value": r.value, "unit": r.unit}
        if r.notes:
            entry["notes"] = r.notes
        out[r.variable] = entry
    return out


def _write_metrics_csv(path: Path, rows: list[MetricRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure stable row ordering.
    ordered = sorted(rows, key=lambda r: r.variable)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["variable", "value", "unit", "source", "notes"])
        for r in ordered:
            writer.writerow([r.variable, _stable_value_str(r.value), r.unit, r.source, r.notes])


def _stable_value_str(value: int | float) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    # Use a stable representation; keep it simple for now.
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
