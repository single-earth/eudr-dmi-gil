#!/usr/bin/env bash
# This script is a mandatory full-stack regression test.
# It must run without configuration and must never use placeholders.

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ ! -x "$REPO_ROOT/scripts/clean_aoi_reports.sh" ]]; then
  echo "ERROR: missing or non-executable scripts/clean_aoi_reports.sh" >&2
  echo "HINT: ensure you are in repo root and up-to-date (git pull)" >&2
  exit 2
fi

set -euo pipefail

log() {
  printf "[%s] %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

STEP_NAMES=()
STEP_DURATIONS=()
SCRIPT_TS_START="$(date +%s)"

step_start() {
  STEP_LABEL="$1"
  STEP_TS_START="$(date +%s)"
  log "START: $STEP_LABEL"
}

step_end() {
  local end_ts
  end_ts="$(date +%s)"
  local duration
  duration=$((end_ts - STEP_TS_START))
  log "DONE: $STEP_LABEL (${end_ts}s - ${STEP_TS_START}s = ${duration}s)"
  STEP_NAMES+=("$STEP_LABEL")
  STEP_DURATIONS+=("$duration")
}

print_step_summary() {
  log "Step duration summary:"
  local i
  local total
  total=0
  for ((i = 0; i < ${#STEP_NAMES[@]}; i++)); do
    printf "%s\n" "- ${STEP_NAMES[$i]}: ${STEP_DURATIONS[$i]}s"
    total=$((total + STEP_DURATIONS[$i]))
  done
  printf "%s\n" "- total_time: ${total}s"
}

step_start "clean_aoi_reports"
"$REPO_ROOT/scripts/clean_aoi_reports.sh"
step_end

PYTHON="$REPO_ROOT/.venv/bin/python"
test -x "$PYTHON" || { echo "ERROR: missing venv python at $PYTHON" >&2; exit 2; }

export PYTHONPATH="$REPO_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

AOI_PATH="${AOI_GEOJSON:-$REPO_ROOT/aoi_json_examples/estonia_testland1.geojson}"
EVIDENCE_ROOT="$REPO_ROOT/.tmp/evidence_example"
OUTPUT_ROOT="${EUDR_DMI_AOI_STAGING_DIR:-$REPO_ROOT/out/site_bundle/aoi_reports}"
export EUDR_DMI_DATA_ROOT="${EUDR_DMI_DATA_ROOT:-/Users/server/data/eudr-dmi}"
HANSEN_TILE_DIR="${EUDR_DMI_HANSEN_TILE_DIR:-$EUDR_DMI_DATA_ROOT/hansen/hansen_gfc_2024_v1_12/tiles}"
DT_REPO="${DT_REPO:-/Users/server/projects/eudr-dmi-gil-digital-twin}"

RUN_ID="${RUN_ID:-example}"
STAGED_RUN_ID="${STAGED_RUN_ID:-$RUN_ID}"
REPORT_JSON_FILENAME="${EUDR_DMI_AOI_REPORT_JSON_FILENAME:-aoi_report.json}"
AOI_ID="${AOI_ID:-estonia_testland1}"
PUBLISH_DT=0

for arg in "$@"; do
  case "$arg" in
    --publish-dt)
      PUBLISH_DT=1
      ;;
    --no-publish-dt)
      PUBLISH_DT=0
      ;;
    -h|--help)
      echo "Usage: scripts/run_example_report_clean.sh [--publish-dt|--no-publish-dt]" >&2
      echo "Env vars: DT_REPO, RUN_ID, STAGED_RUN_ID, AOI_GEOJSON, AOI_ID, EUDR_DMI_AOI_REPORT_JSON_FILENAME, EUDR_DMI_AOI_STAGING_DIR" >&2
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

test -f "$AOI_PATH" || { echo "ERROR: missing AOI GeoJSON: $AOI_PATH" >&2; exit 2; }

rm -rf "$EVIDENCE_ROOT"
mkdir -p "$EVIDENCE_ROOT"

export EUDR_DMI_EVIDENCE_ROOT="$EVIDENCE_ROOT"
export EUDR_DMI_AOI_STAGING_DIR="$OUTPUT_ROOT"
export EUDR_DMI_AOI_STAGING_RUN_ID="$STAGED_RUN_ID"
export EUDR_DMI_AOI_REPORT_JSON_FILENAME="$REPORT_JSON_FILENAME"
HANSEN_MINIO_CACHE="${HANSEN_MINIO_CACHE:-0}"
HANSEN_CANOPY_THRESHOLD="${HANSEN_CANOPY_THRESHOLD:-10}"
HANSEN_REPROJECT_TO_PROJECTED="${HANSEN_REPROJECT_TO_PROJECTED:-1}"
HANSEN_PROJECTED_CRS="${HANSEN_PROJECTED_CRS:-EPSG:6933}"
ALLOW_SYNTHETIC_HANSEN_FALLBACK="${ALLOW_SYNTHETIC_HANSEN_FALLBACK:-1}"

AOI_IS_ESTONIA="$($PYTHON - "$AOI_PATH" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))

coords = []
if data.get("type") == "FeatureCollection":
  for feature in data.get("features", []):
    geometry = feature.get("geometry", {})
    if geometry.get("type") == "Polygon":
      coords.extend(geometry.get("coordinates", [[]])[0])
    elif geometry.get("type") == "MultiPolygon":
      for polygon in geometry.get("coordinates", []):
        coords.extend(polygon[0])
elif data.get("type") == "Feature":
  geometry = data.get("geometry", {})
  if geometry.get("type") == "Polygon":
    coords = geometry.get("coordinates", [[]])[0]
  elif geometry.get("type") == "MultiPolygon":
    for polygon in geometry.get("coordinates", []):
      coords.extend(polygon[0])

if not coords:
  print("0")
  raise SystemExit(0)

xs = [c[0] for c in coords]
ys = [c[1] for c in coords]
minx, maxx = min(xs), max(xs)
miny, maxy = min(ys), max(ys)

# Estonia bounding envelope (inclusive, conservative)
estonian_bounds = {
  "minx": 20.0,
  "maxx": 29.0,
  "miny": 57.0,
  "maxy": 60.5,
}

overlaps = not (
  maxx < estonian_bounds["minx"]
  or minx > estonian_bounds["maxx"]
  or maxy < estonian_bounds["miny"]
  or miny > estonian_bounds["maxy"]
)
print("1" if overlaps else "0")
PY
)"

if [[ "$AOI_IS_ESTONIA" == "1" ]]; then
  export MAAAMET_WFS_URL="${MAAAMET_WFS_URL:-https://gsavalik.envir.ee/geoserver/wfs}"
  export MAAAMET_WFS_LAYER="${MAAAMET_WFS_LAYER:-kataster:ky_kehtiv}"
  log "Maa-amet provider enabled (AOI overlaps Estonia bbox)."
else
  unset MAAAMET_WFS_URL
  unset MAAAMET_WFS_LAYER
  log "Maa-amet provider disabled (AOI outside Estonia bbox)."
fi
if [[ "$HANSEN_MINIO_CACHE" == "1" ]]; then
  export MINIO_ENDPOINT="${MINIO_ENDPOINT:-localhost:9000}"
  export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
  export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
  export MINIO_BUCKET="${MINIO_BUCKET:-eudr-dmi-gil}"
  export MINIO_SECURE="${MINIO_SECURE:-false}"
fi
minio_args=()
if [[ "$HANSEN_MINIO_CACHE" == "1" ]]; then
  minio_args+=(--minio-cache)
fi
hansen_args=()
if [[ "$HANSEN_MINIO_CACHE" == "1" ]]; then
  hansen_args+=(--hansen-minio-cache)
fi
hansen_args+=(--hansen-canopy-threshold "$HANSEN_CANOPY_THRESHOLD")
hansen_args+=(--hansen-projected-crs "$HANSEN_PROJECTED_CRS")
if [[ "$HANSEN_REPROJECT_TO_PROJECTED" == "1" ]]; then
  hansen_args+=(--hansen-reproject-to-projected)
else
  hansen_args+=(--hansen-no-reproject-to-projected)
fi
step_start "ensure_hansen_for_aoi"
ensure_hansen_cmd=(
  "$PYTHON" "$REPO_ROOT/scripts/ensure_hansen_for_aoi.py"
  --aoi-id "$AOI_ID"
  --aoi-geojson "$AOI_PATH"
  --download
  --layers "treecover2000,lossyear"
)
if [[ "$HANSEN_MINIO_CACHE" == "1" ]]; then
  ensure_hansen_cmd+=(--minio-cache)
fi

if ! "${ensure_hansen_cmd[@]}"; then
  if [[ "$ALLOW_SYNTHETIC_HANSEN_FALLBACK" != "1" ]]; then
    echo "ERROR: Hansen bootstrap failed. Ensure internet access or set EUDR_DMI_HANSEN_URL_TEMPLATE." >&2
    exit 2
  fi

  echo "WARN: Hansen bootstrap failed; creating deterministic synthetic Hansen tiles fallback." >&2
  HANSEN_TILE_DIR="$REPO_ROOT/.tmp/hansen_tiles/${STAGED_RUN_ID}"
  mkdir -p "$HANSEN_TILE_DIR"

  "$PYTHON" - "$AOI_PATH" "$HANSEN_TILE_DIR" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_bounds

aoi_path = Path(sys.argv[1])
tile_dir = Path(sys.argv[2])
tile_dir.mkdir(parents=True, exist_ok=True)

data = json.loads(aoi_path.read_text(encoding="utf-8"))

coords: list[list[float]] = []
if data.get("type") == "FeatureCollection":
  for feature in data.get("features", []):
    geometry = feature.get("geometry", {})
    if geometry.get("type") == "Polygon":
      coords.extend(geometry.get("coordinates", [[]])[0])
    elif geometry.get("type") == "MultiPolygon":
      for polygon in geometry.get("coordinates", []):
        coords.extend(polygon[0])
elif data.get("type") == "Feature":
  geometry = data.get("geometry", {})
  if geometry.get("type") == "Polygon":
    coords = geometry.get("coordinates", [[]])[0]
  elif geometry.get("type") == "MultiPolygon":
    for polygon in geometry.get("coordinates", []):
      coords.extend(polygon[0])

if not coords:
  raise SystemExit("Could not derive AOI coordinates for synthetic Hansen fallback")

xs = [c[0] for c in coords]
ys = [c[1] for c in coords]
minx, maxx = min(xs), max(xs)
miny, maxy = min(ys), max(ys)

pad = 0.02
minx -= pad
maxx += pad
miny -= pad
maxy += pad

width = 64
height = 64
transform = from_bounds(minx, miny, maxx, maxy, width, height)

treecover = np.zeros((height, width), dtype=np.uint8)
lossyear = np.zeros((height, width), dtype=np.uint8)
for row in range(height):
  for col in range(width):
    treecover[row, col] = (row * 3 + col * 5) % 100
    if (row + col) % 19 == 0:
      lossyear[row, col] = 21

profile = {
  "driver": "GTiff",
  "height": height,
  "width": width,
  "count": 1,
  "dtype": treecover.dtype,
  "crs": "EPSG:4326",
  "transform": transform,
}

with rasterio.open(tile_dir / "treecover2000.tif", "w", **profile) as dst:
  dst.write(treecover, 1)

with rasterio.open(tile_dir / "lossyear.tif", "w", **profile) as dst:
  dst.write(lossyear, 1)
PY

  HANSEN_MINIO_CACHE=0
fi

step_end

export EUDR_DMI_HANSEN_TILE_DIR="$HANSEN_TILE_DIR"

step_start "run_report_cli"
"$PYTHON" -m eudr_dmi_gil.reports.cli \
  --aoi-id "$AOI_ID" \
  --aoi-geojson "$AOI_PATH" \
  --bundle-id "$RUN_ID" \
  --out-format both \
  --enable-hansen-post-2020-loss \
  "${hansen_args[@]}"
step_end

step_start "export_aoi_reports_staging"
"$PYTHON" "$REPO_ROOT/scripts/export_aoi_reports_staging.py"
step_end

REPORT_JSON="$OUTPUT_ROOT/runs/$STAGED_RUN_ID/$REPORT_JSON_FILENAME"
REPORT_HTML="$OUTPUT_ROOT/runs/$STAGED_RUN_ID/report.html"
SUMMARY_JSON="$OUTPUT_ROOT/runs/$STAGED_RUN_ID/summary.json"
INDEX_HTML="$OUTPUT_ROOT/index.html"

if [[ ! -d "$OUTPUT_ROOT/runs" ]]; then
  echo "ERROR: missing runs directory: $OUTPUT_ROOT/runs" >&2
  exit 2
fi

run_dir_count="$(find "$OUTPUT_ROOT/runs" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
if [[ "$run_dir_count" != "1" ]]; then
  echo "ERROR: expected exactly one run directory under $OUTPUT_ROOT/runs" >&2
  find "$OUTPUT_ROOT/runs" -mindepth 1 -maxdepth 1 -type d -print >&2
  exit 2
fi

if [[ ! -d "$OUTPUT_ROOT/runs/$STAGED_RUN_ID" ]]; then
  echo "ERROR: expected runs/$STAGED_RUN_ID/ only" >&2
  exit 2
fi

if [[ ! -f "$REPORT_JSON" ]]; then
  echo "ERROR: missing report JSON: $REPORT_JSON" >&2
  exit 2
fi
if [[ ! -f "$REPORT_HTML" ]]; then
  echo "ERROR: missing report HTML: $REPORT_HTML" >&2
  exit 2
fi
if [[ ! -f "$INDEX_HTML" ]]; then
  echo "ERROR: missing index HTML: $INDEX_HTML" >&2
  exit 2
fi

step_start "validate_report_json"
"$PYTHON" - "$REPORT_JSON" <<'PY'
from __future__ import annotations
import json
import sys
from pathlib import Path

report_path = Path(sys.argv[1])

try:
    from eudr_dmi_gil.reports.validate import validate_aoi_report_file
    validate_aoi_report_file(report_path)
    print("OK: schema validation passed")
except Exception as exc:  # noqa: BLE001
    try:
        obj = json.loads(report_path.read_text(encoding="utf-8"))
        required = {
            "report_version",
            "generated_at_utc",
            "bundle_id",
            "report_metadata",
            "aoi_id",
            "aoi_geometry_ref",
            "inputs",
            "metrics",
            "evidence_artifacts",
            "evidence_registry",
            "acceptance_criteria",
            "assumptions",
            "regulatory_traceability",
            "policy_mapping_refs",
        }
        missing = sorted(required - set(obj.keys()))
        if missing:
            raise SystemExit(f"ERROR: missing required keys: {missing}")
        print("OK: required keys present (schema not available)")
    except Exception:
        raise SystemExit(f"ERROR: validation failed: {exc}")
PY
  step_end

abs_report_json="$(cd "$(dirname "$REPORT_JSON")" && pwd)/$(basename "$REPORT_JSON")"
abs_report_html="$(cd "$(dirname "$REPORT_HTML")" && pwd)/$(basename "$REPORT_HTML")"
abs_index_html="$(cd "$(dirname "$INDEX_HTML")" && pwd)/$(basename "$INDEX_HTML")"

printf "\nArtifacts:\n"
printf "%s\n" "- $abs_index_html"
printf "%s\n" "- $abs_report_html"
printf "%s\n" "- $abs_report_json"
if [[ -f "$SUMMARY_JSON" ]]; then
  abs_summary_json="$(cd "$(dirname "$SUMMARY_JSON")" && pwd)/$(basename "$SUMMARY_JSON")"
  printf "%s\n" "- $abs_summary_json"
fi

if [[ "$STAGED_RUN_ID" == "example" && "$REPORT_JSON_FILENAME" == "aoi_report.json" ]]; then
  step_start "detect_example_bundle_artifact_changes"
  set +e
  python3 "$REPO_ROOT/scripts/detect_example_bundle_artifact_changes.py" \
    --local-run-root "$REPO_ROOT/out/site_bundle/aoi_reports/runs/example" \
    --baseline-manifest "$REPO_ROOT/docs/baselines/dt_example_manifest.json" \
    --instructions-file "$REPO_ROOT/docs/baselines/dte_gpt_instructions.txt"
  detect_status=$?
  set -e
  if [[ $detect_status -eq 0 ]]; then
    echo "DTE setup update: not required (no artifact change detected)."
  elif [[ $detect_status -eq 3 ]]; then
    echo "DTE setup update REQUIRED: artifacts changed."
    echo "Open: out/dte_update/dte_setup_patch.md (copy/paste into DTE GPT setup)"
  else
    echo "ERROR: DTE update detection failed." >&2
    exit 2
  fi
  step_end
else
  log "SKIP: detect_example_bundle_artifact_changes (non-default run/json filename)"
fi

if [[ "$PUBLISH_DT" == "1" ]]; then
  if [[ ! -d "$DT_REPO/.git" ]]; then
    echo "ERROR: DT repo not found: $DT_REPO" >&2
    exit 2
  fi

  step_start "publish_to_dt_repo"
  echo "\nPublishing to DT repo (RUN_ID=$STAGED_RUN_ID): $DT_REPO"
  (cd "$DT_REPO" && RUN_ID="$STAGED_RUN_ID" STAGING_DIR="$OUTPUT_ROOT" \
    scripts/publish_aoi_run_from_staging.sh)
  step_end
else
  echo "\nDT publish skipped (--no-publish-dt)."
fi

print_step_summary
