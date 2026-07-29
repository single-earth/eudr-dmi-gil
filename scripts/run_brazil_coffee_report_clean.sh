#!/usr/bin/env bash
# Supported clean-run wrapper for the Brazil coffee (Minas Gerais) EUDR evidence package.
#
# This script only calls the ordinary public report CLI
# (`python -m eudr_dmi_gil.reports.cli`) with the inputs and flags documented in
# docs/reports/README.md's "Single-commodity evidence" section. It does not
# reproduce any business/report-rendering logic itself.
#
# It:
#   1. removes only stale derived scratch output for this task (never audit/evidence/**);
#   2. runs the complete JRC GFC2020 + Hansen post-2020 + coffee-commodity + satellite
#      evidence analysis via the supported CLI;
#   3. lets the CLI validate schemas, render JSON/HTML/PDF/CSV/evidence imagery, and
#      build the bundle manifest + canonical manifest.sha256;
#   4. exits non-zero if any required Brazil coffee artifact is missing afterward.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="$REPO_ROOT/.venv/bin/python"
test -x "$PYTHON" || { echo "ERROR: missing venv python at $PYTHON" >&2; exit 2; }
export PYTHONPATH="$REPO_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

log() { printf "[%s] %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

AOI_ID="coffee_brazil_minas_gerais"
AOI_PATH="$REPO_ROOT/aoi_json_examples/${AOI_ID}.geojson"
INPUTS_DIR="$REPO_ROOT/out/${AOI_ID}_inputs"
COMMODITY_CONFIG="$INPUTS_DIR/coffee_config_brazil_cerrado_mineiro.json"

BUNDLE_ID="${BUNDLE_ID:-${AOI_ID}_evidence_freeze_$(date -u +%Y%m%dT%H%M%SZ)}"
SCRATCH_DIR="$REPO_ROOT/.tmp/${AOI_ID}_run"

for f in "$AOI_PATH" "$COMMODITY_CONFIG" \
  "$INPUTS_DIR/jrc_gfc2020_fixture.tif" "$INPUTS_DIR/hansen_lossyear_fixture.tif" \
  "$INPUTS_DIR/sentinel2_baseline_2020.tif" "$INPUTS_DIR/sentinel2_recent_2025.tif" \
  "$INPUTS_DIR/sentinel2_regional_overview.tif" "$INPUTS_DIR/regional_admin_boundaries.geojson"; do
  test -f "$f" || { echo "ERROR: missing required input: $f" >&2; exit 2; }
done

log "START: clean_task_scratch (does not touch audit/evidence/**)"
rm -rf "$SCRATCH_DIR"
mkdir -p "$SCRATCH_DIR"
log "DONE: clean_task_scratch"

export EUDR_DMI_GENERATED_AT_UTC="${EUDR_DMI_GENERATED_AT_UTC:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
export EUDR_DMI_GIT_COMMIT="${EUDR_DMI_GIT_COMMIT:-$(git -C "$REPO_ROOT" rev-parse HEAD)}"

log "START: run_report_cli (bundle_id=$BUNDLE_ID)"
"$PYTHON" -m eudr_dmi_gil.reports.cli \
  --aoi-id "$AOI_ID" \
  --aoi-geojson "$AOI_PATH" \
  --bundle-id "$BUNDLE_ID" \
  --out-format both \
  --jrc-gfc2020-raster "$INPUTS_DIR/jrc_gfc2020_fixture.tif" \
  --hansen-lossyear-raster "$INPUTS_DIR/hansen_lossyear_fixture.tif" \
  --commodity coffee \
  --commodity-config "$COMMODITY_CONFIG" \
  --satellite-baseline-raster "$INPUTS_DIR/sentinel2_baseline_2020.tif" \
  --satellite-baseline-date "2020-09-26/2020-10-01 (lowest-cloud-cover Sentinel-2 L2A TCI mosaic)" \
  --satellite-recent-raster "$INPUTS_DIR/sentinel2_recent_2025.tif" \
  --satellite-recent-date "2025-07-02/2025-09-30 (lowest-cloud-cover Sentinel-2 L2A TCI mosaic)" \
  --satellite-regional-raster "$INPUTS_DIR/sentinel2_regional_overview.tif" \
  --satellite-regional-date "2023-09-21/2025-09-30 (wide-context Sentinel-2 L2A TCI mosaic)" \
  --regional-admin-boundaries-geojson "$INPUTS_DIR/regional_admin_boundaries.geojson" \
  --satellite-dataset-title "Sentinel-2 L2A true-color (TCI) visual composite" \
  --satellite-dataset-version "sentinel-2-l2a" \
  --satellite-source-url "https://earth-search.aws.element84.com/v1" \
  --satellite-license "Contains modified Copernicus Sentinel data" \
  --satellite-selection-method "Per intersecting MGRS tile, selected the scene with the lowest eo:cloud_cover (AOI-scale) or lowest AOI-local cloud fraction verified via the SCL band (regional-scale) within the target window; tiles mosaicked to cover the AOI/region. No per-pixel multi-scene cloud masking/median was performed." \
  --analysis-target-crs "EPSG:6933" \
  --analysis-target-resolution-m 30
log "DONE: run_report_cli"

BUNDLE_DIR="$REPO_ROOT/audit/evidence/$(date -u +%Y-%m-%d)/${BUNDLE_ID}"
CANONICAL_DIR="$BUNDLE_DIR/reports/aoi_report_v2/${AOI_ID}"

REQUIRED_FILES=(
  "$CANONICAL_DIR/report.json"
  "$CANONICAL_DIR/report.html"
  "$CANONICAL_DIR/report.pdf"
  "$CANONICAL_DIR/metrics.csv"
  "$CANONICAL_DIR/manifest.sha256"
  "$BUNDLE_DIR/manifest.json"
  "$CANONICAL_DIR/evidence/02_jrc_forest_2020.png"
  "$CANONICAL_DIR/evidence/04_commodity_layer.png"
  "$CANONICAL_DIR/evidence/05_intersection.png"
  "$CANONICAL_DIR/evidence/06_before_after.png"
  "$CANONICAL_DIR/evidence/legend.png"
)

log "START: verify_required_brazil_artifacts"
missing=0
for f in "${REQUIRED_FILES[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: missing required Brazil artifact: $f" >&2
    missing=1
  fi
done
loss_png_count="$(find "$CANONICAL_DIR/evidence" -maxdepth 1 -name '03_forest_loss_2021_*.png' 2>/dev/null | wc -l | tr -d ' ')"
if [[ "$loss_png_count" != "1" ]]; then
  echo "ERROR: expected exactly one evidence/03_forest_loss_2021_<end_year>.png, found $loss_png_count" >&2
  missing=1
fi
if [[ "$missing" != "0" ]]; then
  exit 2
fi
log "DONE: verify_required_brazil_artifacts"

log "START: validate_schema"
"$PYTHON" -c "
from eudr_dmi_gil.reports.validate import validate_aoi_report_file
validate_aoi_report_file('$CANONICAL_DIR/report.json')
print('OK: eudr_evidence_report_v3 schema validation passed')
"
log "DONE: validate_schema"

log "START: verify_manifest_sha256"
(cd "$BUNDLE_DIR" && shasum -a 256 -c "reports/aoi_report_v2/${AOI_ID}/manifest.sha256")
log "DONE: verify_manifest_sha256"

printf "\nBundle: %s\n" "$BUNDLE_DIR"
printf "Bundle ID: %s\n" "$BUNDLE_ID"
printf "Canonical report: %s\n" "$CANONICAL_DIR/report.json"
