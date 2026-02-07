#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -x "$REPO_ROOT/scripts/clean_aoi_reports.sh" ]]; then
  echo "ERROR: missing or non-executable scripts/clean_aoi_reports.sh" >&2
  echo "HINT: ensure you are in repo root and up-to-date (git pull)" >&2
  exit 2
fi

"$REPO_ROOT/scripts/clean_aoi_reports.sh"
INPUT_GEOJSON="$REPO_ROOT/aoi_json_examples/estonia_testland1.geojson"
EVIDENCE_ROOT="$REPO_ROOT/audit/evidence"
OUTPUT_ROOT="$REPO_ROOT/out/site_bundle/aoi_reports"

if [[ ! -f "$INPUT_GEOJSON" ]]; then
  echo "ERROR: input file not found: $INPUT_GEOJSON" >&2
  exit 2
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
AOI_ID="estonia_testland1"
BUNDLE_ID="${AOI_ID}-${STAMP}"
BUNDLE_DATE="$(date -u +%F)"
BUNDLE_DIR="$EVIDENCE_ROOT/$BUNDLE_DATE/$BUNDLE_ID"

export EUDR_DMI_EVIDENCE_ROOT="$EVIDENCE_ROOT"

python -m eudr_dmi_gil.reports.cli \
  --aoi-id "$AOI_ID" \
  --aoi-geojson "$INPUT_GEOJSON" \
  --bundle-id "$BUNDLE_ID" \
  --out-format both \
  --metric area_ha=12.34:ha:operator:placeholder \
  --metric forest_cover_fraction=0.56:fraction:operator:placeholder

python scripts/export_aoi_reports_staging.py

REPORT_JSON="$BUNDLE_DIR/reports/aoi_report_v2/${AOI_ID}.json"
REPORT_HTML="$BUNDLE_DIR/reports/aoi_report_v2/${AOI_ID}.html"
METRICS_CSV="$BUNDLE_DIR/reports/aoi_report_v2/${AOI_ID}/metrics.csv"
MANIFEST_JSON="$BUNDLE_DIR/manifest.json"
INPUT_COPY="$BUNDLE_DIR/inputs/aoi.geojson"

DT_INDEX="$OUTPUT_ROOT/index.html"
DT_RUN_REPORT="$OUTPUT_ROOT/runs/$BUNDLE_ID/report.html"
DT_RUN_JSON="$OUTPUT_ROOT/runs/$BUNDLE_ID/aoi_report.json"

missing=0
for f in "$REPORT_JSON" "$REPORT_HTML" "$METRICS_CSV" "$MANIFEST_JSON" "$INPUT_COPY" "$DT_INDEX" "$DT_RUN_REPORT" "$DT_RUN_JSON"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: missing expected file: $f" >&2
    missing=1
  fi
done

if [[ "$missing" -ne 0 ]]; then
  exit 2
fi

echo "Input file: $INPUT_GEOJSON"
echo "Bundle output: $BUNDLE_DIR"
echo "DT staging output: $OUTPUT_ROOT"

echo "Generated files (bundle):"
for f in "$REPORT_JSON" "$REPORT_HTML" "$METRICS_CSV" "$MANIFEST_JSON" "$INPUT_COPY"; do
  echo "- $f"
done

echo "Generated files (DT staging):"
for f in "$DT_INDEX" "$DT_RUN_REPORT" "$DT_RUN_JSON"; do
  echo "- $f"
done
