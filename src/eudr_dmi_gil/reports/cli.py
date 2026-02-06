from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import csv

from .bundle import bundle_dir as compute_bundle_dir
from .bundle import compute_sha256
from .bundle import resolve_evidence_root, write_manifest
from .determinism import canonical_json_bytes, sha256_bytes, write_bytes
from .policy_refs import collect_policy_mapping_refs
from .validate import validate_aoi_report_v1


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _utc_now_compact() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def _sanitize_id(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("empty id")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def _rel_href(from_path: Path, to_path: Path) -> str:
    rel = os.path.relpath(to_path, start=from_path.parent)
    return Path(rel).as_posix()


def _render_html_summary(report: dict[str, Any], *, html_path: Path, artifact_paths: list[Path]) -> str:
    def row(k: str, v: str) -> str:
        return f"<tr><th>{k}</th><td>{v}</td></tr>"

    report_json_path = next((p for p in artifact_paths if p.suffix == ".json"), None)

    links_html = "\n".join(
        f'<li><a href="{_rel_href(html_path, p)}">{_rel_href(html_path, p)}</a></li>'
        for p in sorted(artifact_paths, key=lambda p: p.as_posix())
    )

    metrics_rows = "\n".join(
        row(k, f"{v.get('value')} {v.get('unit')}")
        for k, v in sorted((report.get("metrics") or {}).items(), key=lambda kv: kv[0])
    )

    inputs_rows = "\n".join(
        row(
            src.get("source_id", "(unknown)"),
            " ".join(
                part
                for part in [
                    (f"version={src.get('version')}" if src.get("version") else ""),
                    (f"sha256={src.get('sha256')}" if src.get("sha256") else ""),
                    (f"uri={src.get('uri')}" if src.get("uri") else ""),
                ]
                if part
            ),
        )
        for src in (report.get("inputs") or {}).get("sources", [])
    )

    policy_refs = report.get("policy_mapping_refs") or []
    policy_html = "\n".join(f"<li>{ref}</li>" for ref in policy_refs)

    report_link = ""
    if report_json_path is not None:
        report_link = (
            f'<p><a href="{_rel_href(html_path, report_json_path)}">Open report JSON</a></p>'
        )

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>AOI Report Summary — {report.get('aoi_id')}</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 24px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
    th {{ background: #f6f6f6; text-align: left; width: 240px; }}
    h2 {{ margin-top: 28px; }}
    code {{ background: #f6f6f6; padding: 1px 4px; border-radius: 4px; }}
  </style>
</head>
<body>
  <h1>AOI Report Summary</h1>
  <table>
    {row('AOI', str(report.get('aoi_id')))}
    {row('Bundle', str(report.get('bundle_id')))}
    {row('Generated (UTC)', str(report.get('generated_at_utc')))}
    {row('Report Version', str(report.get('report_version')))}
    {row('Geometry Ref', f"{report.get('aoi_geometry_ref', {}).get('kind')}: {report.get('aoi_geometry_ref', {}).get('value')}")}
  </table>

  {report_link}

  <h2>Inputs</h2>
  <table>
    {inputs_rows or row('(none)', '')}
  </table>

  <h2>Metrics</h2>
  <table>
    {metrics_rows or row('(none)', '')}
  </table>

  <h2>Evidence Artifacts</h2>
  <ul>
    {links_html}
  </ul>

  <h2>Policy Mapping References</h2>
  <ul>
    {policy_html or '<li>(none)</li>'}
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

    metric_rows = _parse_metric_rows(args.metric, fallback_dummy=args.dummy_metric)

    hansen_result = None
    hansen_analysis = None
    hansen_methodology_block: dict[str, Any] | None = None
    hansen_computed_block: dict[str, Any] | None = None
    hansen_computed_outputs_block: dict[str, Any] | None = None
    hansen_acceptance_criteria_block: dict[str, Any] | None = None
    hansen_result_block: dict[str, Any] | None = None
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
        hansen_output_dir = bdir / "reports" / "aoi_report_v1" / aoi_id / "hansen"
        hansen_analysis = run_forest_loss_post_2020(
            aoi_geojson_path=geo_path,
            output_dir=hansen_output_dir,
            config=hansen_config,
            aoi_id=aoi_id,
            run_id=bundle_id,
        )
        hansen_result = hansen_analysis.raw

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
            "criteria_id": "forest_loss_post_2020_computed",
            "description": "Forest loss post-2020 computed from Hansen tiles.",
            "evidence_classes": ["forest_loss_post_2020"],
            "decision_type": "presence",
        }
        hansen_result_block = {
            "result_id": "forest_loss_post_2020_computed",
            "criteria_ids": ["forest_loss_post_2020_computed"],
            "evidence_classes": ["forest_loss_post_2020"],
            "status": "computed",
        }

    report: dict[str, Any] = {
        "report_version": "aoi_report_v1",
        "generated_at_utc": generated_at_utc,
        "bundle_id": bundle_id,
        "report_metadata": {
            "report_type": "example",
            "regulatory_context": {
                "regulation": "EUDR",
                "in_scope_articles": [],
                "out_of_scope_articles": [],
            },
            "assessment_capability": "inspectable_only",
        },
        "computed": {},
        "computed_outputs": {},
        "validation": {},
        "methodology": {},
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
                "status": "placeholder",
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
                    "acceptance_criteria": "forest_loss_post_2020_computed",
                    "result_ref": "forest_loss_post_2020_computed",
                }
            )

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

        maaamet_dir = bdir / "reports" / "aoi_report_v1" / aoi_id / "maaamet"
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
            "comparison": {"tolerance_percent": maaamet_result.tolerance_percent},
            "csv_ref": {
                "relpath": str(maaamet_result.csv_path.relative_to(bdir)).replace("\\", "/"),
                "sha256": compute_sha256(maaamet_result.csv_path),
                "content_type": "text/csv",
            },
        }
        if maaamet_result.reason:
            crosscheck_block["reason"] = maaamet_result.reason
        report["validation"]["forest_area_crosscheck"] = crosscheck_block
        artifact_paths.extend([maaamet_result.csv_path, maaamet_result.summary_path])

    # metrics.csv (portable, deterministic) lives alongside the report outputs.
    metrics_csv_path = bdir / "reports" / "aoi_report_v1" / aoi_id / "metrics.csv"
    _write_metrics_csv(metrics_csv_path, metric_rows)
    artifact_paths.append(metrics_csv_path)

    # JSON output
    report_json_path = bdir / "reports" / "aoi_report_v1" / f"{aoi_id}.json"
    if args.out_format in ("json", "both"):
        report_json_path.parent.mkdir(parents=True, exist_ok=True)
        report_json_bytes = canonical_json_bytes(report) + b"\n"
        report_json_path.write_bytes(report_json_bytes)
        artifact_paths.append(report_json_path)

    # HTML output
    report_html_path = bdir / "reports" / "aoi_report_v1" / f"{aoi_id}.html"
    if args.out_format in ("html", "both"):
        report_html_path.parent.mkdir(parents=True, exist_ok=True)
        # Link to whatever artifacts are already known; report JSON is included if produced.
        known_artifacts_for_html = list(artifact_paths)
        html = _render_html_summary(report, html_path=report_html_path, artifact_paths=known_artifacts_for_html)
        report_html_path.write_text(html, encoding="utf-8")
        artifact_paths.append(report_html_path)

    # Populate evidence_artifacts in report JSON (exclude manifest to avoid circularity).
    report["evidence_artifacts"] = [
        {
            "relpath": str(p.relative_to(bdir)).replace("\\\\", "/"),
            "sha256": compute_sha256(p),
            "size_bytes": p.stat().st_size,
        }
        for p in sorted(set(artifact_paths), key=lambda p: p.as_posix())
    ]

    # If we wrote report JSON, rewrite it now that evidence_artifacts is populated.
    if args.out_format in ("json", "both"):
        report_json_path.write_bytes(canonical_json_bytes(report) + b"\n")

    # Validate contract.
    validate_aoi_report_v1(report)

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


def _parse_metric_rows(raw_metrics: list[str], *, fallback_dummy: str) -> list[MetricRow]:
    rows: list[MetricRow] = []

    if raw_metrics:
        for raw in raw_metrics:
            rows.append(_parse_metric_row(raw))
    else:
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
