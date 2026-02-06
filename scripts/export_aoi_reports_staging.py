#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
EVIDENCE_ROOT_DEFAULT = REPO_ROOT / "audit" / "evidence"
OUTPUT_ROOT_DEFAULT = REPO_ROOT / "out" / "site_bundle" / "aoi_reports"
EXAMPLE_RUN_ID = "example"


@dataclass(frozen=True)
class RunEntry:
    run_id: str
    report_html_path: Path
    report_json_path: Path
    summary_present: bool


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_single_example_run(output_root: Path) -> None:
    runs_dir = output_root / "runs"
    if not runs_dir.is_dir():
        raise SystemExit(f"Runs dir not found: {runs_dir}")

    run_dirs = [p for p in runs_dir.iterdir() if p.is_dir()]
    if len(run_dirs) != 1:
        raise SystemExit(f"Expected exactly one run dir under {runs_dir}, found {len(run_dirs)}")

    if run_dirs[0].name != EXAMPLE_RUN_ID:
        raise SystemExit(f"Expected only '{EXAMPLE_RUN_ID}' run dir, found: {run_dirs[0].name}")


def _render_index(entry: RunEntry) -> str:
    summary_link = (
        f' <span class="muted">(</span><a href="runs/{entry.run_id}/summary.json">summary.json</a>'
        f'<span class="muted">)</span>'
        if entry.summary_present
        else ""
    )
    rows = (
        f'<li><a href="runs/{entry.run_id}/report.html">{entry.run_id}</a> '
        f'<span class="muted">(</span><a href="runs/{entry.run_id}/aoi_report.json">aoi_report.json</a>'
        f'<span class="muted">)</span>{summary_link}</li>'
    )

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>AOI Reports</title>
  <style>
    :root {{ --fg:#111; --bg:#fff; --muted:#666; --card:#f6f7f9; --link:#0b5fff; }}
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
           color: var(--fg); background: var(--bg); margin: 0; }}
    header {{ border-bottom: 1px solid #e7e7e7; background: #fff; position: sticky; top: 0; }}
    .wrap {{ max-width: 980px; margin: 0 auto; padding: 16px 20px; }}
    nav a {{ margin-right: 14px; text-decoration: none; color: var(--link); font-weight: 600; }}
    nav a.active {{ color: var(--fg); }}
    main {{ padding: 18px 20px 40px; }}
    h1 {{ margin: 0 0 6px; font-size: 22px; }}
    p {{ line-height: 1.5; }}
    .muted {{ color: var(--muted); }}
    .card {{ background: var(--card); border: 1px solid #e8eaee; border-radius: 12px; padding: 14px 14px; }}
    ul {{ padding-left: 18px; }}
  </style>
</head>
<body>
  <header>
    <div class=\"wrap\">
      <nav>
        <a href=\"../index.html\">Home</a>
        <a href=\"../articles/index.html\">Articles</a>
        <a href=\"../dependencies/index.html\">Dependencies</a>
        <a href=\"../regulation/links.html\">Regulation</a>
        <a href=\"../regulation/sources.html\">Sources</a>
        <a href=\"../regulation/policy_to_evidence_spine.html\">Spine</a>
        <a href=\"../views/index.html\">Views</a>
        <a href=\"index.html\" class=\"active\">AOI Reports</a>
        <a href=\"../dao_stakeholders/index.html\">DAO (Stakeholders)</a>
        <a href=\"../dao_dev/index.html\">DAO (Developers)</a>
      </nav>
    </div>
  </header>
  <main>
    <div class=\"wrap\">
      <h1>AOI Reports</h1>
      <p class=\"muted\">Portable bundle. Links assume this folder is mounted at <code>docs/site/aoi_reports/</code>.</p>
      <div class=\"card\">
        <h2>Runs</h2>
        <ul>
          {rows or '<li>(none)</li>'}
        </ul>
      </div>
    </div>
  </main>
</body>
</html>
"""


def _render_report_html(report: dict[str, Any], *, rel_artifacts: list[str]) -> str:
    aoi_id = report.get("aoi_id", "(unknown)")
    bundle_id = report.get("bundle_id", "(unknown)")
    generated = report.get("generated_at_utc", "(unknown)")

    links = "\n".join(
        f'<li><a href="{path}">{path}</a></li>' for path in rel_artifacts
    )

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>AOI Report — {aoi_id}</title>
  <style>
    :root {{ --fg:#111; --bg:#fff; --muted:#666; --card:#f6f7f9; --link:#0b5fff; }}
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
           color: var(--fg); background: var(--bg); margin: 0; }}
    header {{ border-bottom: 1px solid #e7e7e7; background: #fff; position: sticky; top: 0; }}
    .wrap {{ max-width: 980px; margin: 0 auto; padding: 16px 20px; }}
    nav a {{ margin-right: 14px; text-decoration: none; color: var(--link); font-weight: 600; }}
    nav a.active {{ color: var(--fg); }}
    main {{ padding: 18px 20px 40px; }}
    h1 {{ margin: 0 0 6px; font-size: 22px; }}
    p {{ line-height: 1.5; }}
    .muted {{ color: var(--muted); }}
    .card {{ background: var(--card); border: 1px solid #e8eaee; border-radius: 12px; padding: 14px 14px; }}
    ul {{ padding-left: 18px; }}
    code {{ background: #f1f1f1; padding: 1px 4px; border-radius: 6px; }}
  </style>
</head>
<body>
  <header>
    <div class=\"wrap\">
      <nav>
        <a href=\"../../../index.html\">Home</a>
        <a href=\"../../../articles/index.html\">Articles</a>
        <a href=\"../../../dependencies/index.html\">Dependencies</a>
        <a href=\"../../../regulation/links.html\">Regulation</a>
        <a href=\"../../../regulation/sources.html\">Sources</a>
        <a href=\"../../../regulation/policy_to_evidence_spine.html\">Spine</a>
        <a href=\"../../../views/index.html\">Views</a>
        <a href=\"../../index.html\" class=\"active\">AOI Reports</a>
        <a href=\"../../../dao_stakeholders/index.html\">DAO (Stakeholders)</a>
        <a href=\"../../../dao_dev/index.html\">DAO (Developers)</a>
      </nav>
    </div>
  </header>
  <main>
    <div class=\"wrap\">
      <p class=\"muted\"><a href=\"../../index.html\">Back to AOI runs</a></p>
      <h1>AOI Report</h1>
      <p><b>AOI</b>: <code>{aoi_id}</code><br />
         <b>Bundle</b>: <code>{bundle_id}</code><br />
         <b>Generated (UTC)</b>: <code>{generated}</code></p>
      <div class=\"card\">
        <h2>Artifacts</h2>
        <ul>
          {links or '<li>(none)</li>'}
        </ul>
      </div>
    </div>
  </main>
</body>
</html>
"""


def export_aoi_reports(*, evidence_root: Path, output_root: Path) -> None:
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    report_jsons = sorted(evidence_root.rglob("reports/aoi_report_v1/*.json"))

    if len(report_jsons) != 1:
        raise SystemExit(
            f"Expected exactly one report JSON under {evidence_root}, found {len(report_jsons)}"
        )

    report_json = report_jsons[0]
    report = _load_json(report_json)

    # report_json is expected at: <bundle_root>/reports/aoi_report_v1/<aoi_id>.json
    bundle_root = report_json.parent.parent.parent
    run_dir = output_root / "runs" / EXAMPLE_RUN_ID
    run_dir.mkdir(parents=True, exist_ok=True)

    rel_artifacts: list[str] = []
    rel_artifacts_set: set[str] = set()

    def _add_relpath(relpath: str) -> None:
      if relpath in rel_artifacts_set:
        return
      rel_artifacts.append(relpath)
      rel_artifacts_set.add(relpath)

    # Copy declared input artefacts into run dir (preserve relative paths)
    input_relpaths: set[str] = set()
    aoi_ref = report.get("aoi_geometry_ref", {}).get("value")
    if isinstance(aoi_ref, str) and aoi_ref:
      input_relpaths.add(aoi_ref)
    for src_entry in report.get("inputs", {}).get("sources", []):
      if not isinstance(src_entry, dict):
        continue
      uri = src_entry.get("uri")
      if isinstance(uri, str) and uri:
        input_relpaths.add(uri)

    for relpath in sorted(input_relpaths):
      src = bundle_root / relpath
      if not src.exists():
        continue
      dest = run_dir / relpath
      dest.parent.mkdir(parents=True, exist_ok=True)
      shutil.copy2(src, dest)
      _add_relpath(dest.relative_to(run_dir).as_posix())

    # Copy evidence artifacts into run dir (preserve relative paths)
    for artifact in report.get("evidence_artifacts", []):
        relpath = artifact.get("relpath")
        if not relpath:
            continue
        src = bundle_root / relpath
        if not src.exists():
            continue
        dest = run_dir / relpath
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        _add_relpath(dest.relative_to(run_dir).as_posix())

    # Write canonical report JSON name
    report_json_out = run_dir / "aoi_report.json"
    report_json_out.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if "aoi_report.json" not in rel_artifacts_set:
      rel_artifacts.insert(0, "aoi_report.json")
      rel_artifacts_set.add("aoi_report.json")

    # Render a portable report.html (relative links)
    report_html_out = run_dir / "report.html"
    report_html_out.write_text(
        _render_report_html(report, rel_artifacts=rel_artifacts), encoding="utf-8"
    )

    summary_present = (run_dir / "summary.json").is_file()
    entry = RunEntry(
        run_id=EXAMPLE_RUN_ID,
        report_html_path=report_html_out,
        report_json_path=report_json_out,
        summary_present=summary_present,
    )
    index_html = _render_index(entry)
    (output_root / "index.html").write_text(index_html, encoding="utf-8")

    _ensure_single_example_run(output_root)


def main() -> int:
    evidence_root = Path(os.environ.get("EUDR_DMI_EVIDENCE_ROOT", str(EVIDENCE_ROOT_DEFAULT)))
    output_root = Path(os.environ.get("EUDR_DMI_AOI_STAGING_DIR", str(OUTPUT_ROOT_DEFAULT)))
    export_aoi_reports(evidence_root=evidence_root, output_root=output_root)
    print(str(output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
