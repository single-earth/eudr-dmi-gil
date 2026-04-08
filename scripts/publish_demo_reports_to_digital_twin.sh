#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

RUN_ID="demo_2026-02-20"
DIGITAL_TWIN_ROOT="/Users/server/projects/eudr-dmi-gil-digital-twin"

usage() {
  cat <<EOF
Usage: scripts/publish_demo_reports_to_digital_twin.sh [--run-id <run_id>] [--digital-twin-root <path>]

Publishes demo reports generated in:
  out/reports/<run_id>/

to Digital Twin path:
  <digital-twin-root>/docs/site/sample_reports/runs/<run_id>/

Then regenerates:
  <digital-twin-root>/docs/site/sample_reports/index.html
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)
      RUN_ID="$2"
      shift 2
      ;;
    --digital-twin-root)
      DIGITAL_TWIN_ROOT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

SOURCE_RUN_DIR="$REPO_ROOT/out/reports/$RUN_ID"
DT_SITE_ROOT="$DIGITAL_TWIN_ROOT/docs/site"
DT_DAO_ROOT="$DT_SITE_ROOT/sample_reports"
DT_RUNS_DIR="$DT_DAO_ROOT/runs"
DT_TARGET_RUN_DIR="$DT_RUNS_DIR/$RUN_ID"

if [[ ! -d "$SOURCE_RUN_DIR" ]]; then
  echo "ERROR: source run directory not found: $SOURCE_RUN_DIR" >&2
  exit 2
fi

if [[ ! -d "$DIGITAL_TWIN_ROOT/.git" ]]; then
  echo "ERROR: digital twin repo not found (missing .git): $DIGITAL_TWIN_ROOT" >&2
  exit 2
fi

for plot in demo_plot_01 demo_plot_02 demo_plot_03; do
  for artifact in report.html report.pdf report.json; do
    if [[ ! -f "$SOURCE_RUN_DIR/$plot/$artifact" ]]; then
      echo "ERROR: missing source artifact: $SOURCE_RUN_DIR/$plot/$artifact" >&2
      exit 2
    fi
  done
done

mkdir -p "$DT_RUNS_DIR"
rsync -a --delete "$SOURCE_RUN_DIR/" "$DT_TARGET_RUN_DIR/"

python3 - "$DT_DAO_ROOT" <<'PY'
from __future__ import annotations

import html
import os
import sys
from pathlib import Path

dao_root = Path(sys.argv[1])
runs_dir = dao_root / "runs"
index_path = dao_root / "index.html"

runs: list[str] = []
if runs_dir.is_dir():
    runs = sorted([p.name for p in runs_dir.iterdir() if p.is_dir()])

plot_entries = [
    ("demo_plot_01", "Demo Plot 01"),
    ("demo_plot_02", "Demo Plot 02"),
    ("demo_plot_03", "Demo Plot 03"),
]

def run_block(run_id: str) -> str:
    rows: list[str] = []
    for plot_id, label in plot_entries:
        plot_dir = runs_dir / run_id / plot_id
        html_exists = (plot_dir / "report.html").is_file()
        pdf_exists = (plot_dir / "report.pdf").is_file()
        json_exists = (plot_dir / "report.json").is_file()
        html_link = f"runs/{run_id}/{plot_id}/report.html" if html_exists else "#"
        pdf_link = f"runs/{run_id}/{plot_id}/report.pdf" if pdf_exists else "#"
        json_link = f"runs/{run_id}/{plot_id}/report.json" if json_exists else "#"
        rows.append(
            "<li>"
            f"<strong>{html.escape(label)}</strong> — "
            f"<a href=\"{html.escape(html_link)}\">HTML</a> · "
            f"<a href=\"{html.escape(pdf_link)}\">PDF</a> · "
            f"<a href=\"{html.escape(json_link)}\">JSON</a>"
            "</li>"
        )
    return (
        "<div class=\"card\">"
        f"<h2>{html.escape(run_id)}</h2>"
        "<ul>"
        + "\n".join(rows)
        + "</ul>"
        "</div>"
    )

if runs:
    runs_html = "\n".join(run_block(run_id) for run_id in runs)
else:
    runs_html = "<p class=\"muted\">No runs published.</p>"

content = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>DAO Reports</title>
  <style>
    :root {{ --fg:#111; --bg:#fff; --muted:#666; --card:#f6f7f9; --link:#0b5fff; }}
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; color:var(--fg); background:var(--bg); margin:0; }}
    header {{ border-bottom:1px solid #e7e7e7; background:#fff; position:sticky; top:0; }}
    .wrap {{ max-width:980px; margin:0 auto; padding:16px 20px; }}
    nav a {{ margin-right:14px; text-decoration:none; color:var(--link); font-weight:600; }}
    nav a.active {{ color:var(--fg); }}
    main {{ padding:18px 20px 40px; }}
    h1 {{ margin:0 0 6px; font-size:22px; }}
    .muted {{ color:var(--muted); }}
    .card {{ background:var(--card); border:1px solid #e8eaee; border-radius:12px; padding:14px; margin-top:12px; }}
    ul {{ padding-left:18px; }}
  </style>
</head>
<body>
  <header>
    <div class=\"wrap\">
      <nav>
        <a href=\"../index.html\">Home</a>
        <a href=\"../bundles/index.html\">AOI Reports</a>
        <a href=\"index.html\" class=\"active\">DAO Reports</a>
        <a href=\"../dao_stakeholders/index.html\">DAO (Stakeholders)</a>
        <a href=\"../dao_dev/index.html\">DAO (Developers)</a>
      </nav>
    </div>
  </header>
  <main>
    <div class=\"wrap\">
      <h1>DAO Reports</h1>
      <p class=\"muted\">Published deterministic demo runs from the authoritative generation repository.</p>
      {runs_html}
    </div>
  </main>
</body>
</html>
"""

index_path.parent.mkdir(parents=True, exist_ok=True)
index_path.write_text(content, encoding="utf-8")
PY

echo "Published run: $RUN_ID"
echo "Target run dir: $DT_TARGET_RUN_DIR"
echo "Index: $DT_DAO_ROOT/index.html"
