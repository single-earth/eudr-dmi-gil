#!/usr/bin/env bash
# Supported clean-run wrapper for the Brazil coffee (Minas Gerais) EUDR-compliant-example
# evidence package. Sibling of scripts/run_brazil_coffee_report_clean.sh, same conventions,
# targeting the coffee_brazil_minas_gerais_eudr_compliant AOI instead.
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

AOI_ID="coffee_brazil_minas_gerais_eudr_compliant"
AOI_PATH="$REPO_ROOT/aoi_json_examples/${AOI_ID}.geojson"
INPUTS_DIR="$REPO_ROOT/out/${AOI_ID}_inputs"
COMMODITY_CONFIG="$INPUTS_DIR/coffee_config_brazil_cerrado_mineiro_eudr_compliant.json"

BUNDLE_ID="${BUNDLE_ID:-${AOI_ID}_evidence_freeze_$(date -u +%Y%m%dT%H%M%SZ)}"
SCRATCH_DIR="$REPO_ROOT/.tmp/${AOI_ID}_run"

for f in "$AOI_PATH" "$COMMODITY_CONFIG" \
  "$INPUTS_DIR/jrc_gfc2020_v3.tif" "$INPUTS_DIR/hansen_lossyear_2025_v1_13.tif" \
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
  --jrc-gfc2020-raster "$INPUTS_DIR/jrc_gfc2020_v3.tif" \
  --hansen-lossyear-raster "$INPUTS_DIR/hansen_lossyear_2025_v1_13.tif" \
  --commodity coffee \
  --commodity-config "$COMMODITY_CONFIG" \
  --satellite-baseline-raster "$INPUTS_DIR/sentinel2_baseline_2020.tif" \
  --satellite-baseline-date "2020-01-01/2020-12-31 (lowest-cloud-cover Sentinel-2 L2A TCI mosaic)" \
  --satellite-recent-raster "$INPUTS_DIR/sentinel2_recent_2025.tif" \
  --satellite-recent-date "2025-06-01/2025-09-30 (lowest-cloud-cover Sentinel-2 L2A TCI mosaic)" \
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

# Unlike scripts/run_brazil_coffee_report_clean.sh (which reproduces an AOI with detected
# post-2020 loss), this AOI is constructed as a no-loss example. Round 26 (report_model.py):
# evidence/03_forest_loss_2021_<end_year>.png is no longer solely a loss-mask render - it is a
# stacked composite (current forest net of any detected loss, plus commodity, plus loss, plus
# loss-on-commodity) that renders whenever a forest baseline and/or commodity layer exists for the
# AOI, so it now exists even for this zero-loss AOI (showing forest + coffee context, nothing
# loss-colored). evidence/05_intersection.png remains correctly absent: there is genuinely no
# loss-and-commodity overlap geometry to show. That is verified below by asserting the PNG now
# exists, the zero-loss metric, and the single remaining `missing_intersection` evidence-gap code
# (`missing_forest_loss` no longer applies now that the artifact renders).
REQUIRED_FILES=(
  "$CANONICAL_DIR/report.json"
  "$CANONICAL_DIR/report.html"
  "$CANONICAL_DIR/report.pdf"
  "$CANONICAL_DIR/metrics.csv"
  "$CANONICAL_DIR/manifest.sha256"
  "$BUNDLE_DIR/manifest.json"
  "$CANONICAL_DIR/evidence/02_jrc_forest_2020.png"
  "$CANONICAL_DIR/evidence/04_commodity_layer.png"
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
  echo "ERROR: expected exactly one evidence/03_forest_loss_2021_<end_year>.png (forest/commodity context composite) for this no-loss AOI, found $loss_png_count" >&2
  missing=1
fi
if [[ -f "$CANONICAL_DIR/evidence/05_intersection.png" ]]; then
  echo "ERROR: expected no evidence/05_intersection.png for this no-loss AOI" >&2
  missing=1
fi
if [[ "$missing" != "0" ]]; then
  exit 2
fi
log "DONE: verify_required_brazil_artifacts"

log "START: verify_zero_post_2020_loss"
"$PYTHON" -c "
import json
report = json.load(open('$CANONICAL_DIR/report.json'))
loss_ha = report['metrics']['forest_loss_post_2020_on_baseline_ha']['value']
status = report['assessment']['status']
gap_codes = {g.get('gap_id') for g in report['assessment']['limitations'] if 'gap_id' in g}
assert loss_ha == 0.0, f'expected zero post-2020 forest loss, got {loss_ha}'
assert status == 'no_post_2020_baseline_loss_detected', f'unexpected assessment status: {status}'
assert 'missing_forest_loss' not in gap_codes, f'forest_loss artifact should now render for a zero-loss AOI: {gap_codes}'
assert 'missing_intersection' in gap_codes, f'unexpected evidence gap codes: {gap_codes}'
print('OK: zero post-2020 forest loss confirmed, EUDR-compliant-example verdict as expected')
"
log "DONE: verify_zero_post_2020_loss"

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
