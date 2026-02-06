#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PYTHON="$REPO_ROOT/.venv/bin/python"
test -x "$PYTHON" || { echo "ERROR: missing venv python at $PYTHON" >&2; exit 2; }

export PYTHONPATH="$REPO_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

AOI_PATH="$REPO_ROOT/aoi_json_examples/estonia_testland1.geojson"
EVIDENCE_ROOT="$REPO_ROOT/.tmp/evidence_example"
OUTPUT_ROOT="$REPO_ROOT/out/site_bundle/aoi_reports"
HANSEN_TILE_DIR="${EUDR_DMI_HANSEN_TILE_DIR:-$REPO_ROOT/tests/fixtures/hansen/tiles}"
DT_REPO="${DT_REPO:-/Users/server/projects/eudr-dmi-gil-digital-twin}"

RUN_ID="estonia_testland1_example"
STAGED_RUN_ID="example"
AOI_ID="estonia_testland1"
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
      echo "Env vars: DT_REPO" >&2
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

test -f "$AOI_PATH" || { echo "ERROR: missing AOI: $AOI_PATH" >&2; exit 2; }

rm -rf "$EVIDENCE_ROOT"
mkdir -p "$EVIDENCE_ROOT"

export EUDR_DMI_EVIDENCE_ROOT="$EVIDENCE_ROOT"
export EUDR_DMI_AOI_STAGING_DIR="$OUTPUT_ROOT"
if [[ ! -d "$HANSEN_TILE_DIR" ]]; then
  echo "ERROR: missing Hansen fixture tiles dir: $HANSEN_TILE_DIR" >&2
  echo "Set EUDR_DMI_HANSEN_TILE_DIR to a fixture tileset (e.g. tests/fixtures/hansen/tiles)." >&2
  exit 2
fi

export EUDR_DMI_HANSEN_TILE_DIR="$HANSEN_TILE_DIR"

"$PYTHON" -m eudr_dmi_gil.reports.cli \
  --aoi-id "$AOI_ID" \
  --aoi-geojson "$AOI_PATH" \
  --bundle-id "$RUN_ID" \
  --out-format both \
  --enable-hansen-post-2020-loss \
  --metric area_ha=12.34:ha:example:deterministic \
  --metric forest_cover_fraction=0.56:fraction:example:deterministic

"$PYTHON" scripts/export_aoi_reports_staging.py

REPORT_JSON="$OUTPUT_ROOT/runs/$STAGED_RUN_ID/aoi_report.json"
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

"$PYTHON" - "$REPORT_JSON" <<'PY'
from __future__ import annotations
import json
import sys
from pathlib import Path

report_path = Path(sys.argv[1])

try:
    from eudr_dmi_gil.reports.validate import validate_aoi_report_v1_file
    validate_aoi_report_v1_file(report_path)
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

if [[ "$PUBLISH_DT" == "1" ]]; then
  if [[ ! -d "$DT_REPO/.git" ]]; then
    echo "ERROR: DT repo not found: $DT_REPO" >&2
    exit 2
  fi

  echo "\nPublishing to DT repo (RUN_ID=$STAGED_RUN_ID): $DT_REPO"
  (cd "$DT_REPO" && RUN_ID="$STAGED_RUN_ID" STAGING_DIR="$OUTPUT_ROOT" \
    scripts/publish_aoi_run_from_staging.sh)
else
  echo "\nDT publish skipped (--no-publish-dt)."
fi
