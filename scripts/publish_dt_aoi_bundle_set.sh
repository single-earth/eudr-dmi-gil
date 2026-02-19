#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DT_REPO="${DT_REPO:-/Users/server/projects/eudr-dmi-gil-digital-twin}"
DT_SITE_ROOT="${DT_REPO}/docs/site"
DT_RUNS_DIR="${DT_SITE_ROOT}/aoi_reports/runs"

WORK_ROOT="${REPO_ROOT}/out/site_bundle/dt_publish_work"
FINAL_BUNDLE_ROOT="${REPO_ROOT}/out/site_bundle/dt_publish_final/aoi_reports"

RUN_SPECS=(
  "example|aoi_json_examples/estonia_testland1.geojson|estonia_testland1|estonia_aoi_report.json"
  "latin_america|aoi_json_examples/mixed_crop_latin_america_colombia.geojson|mixed_crop_latin_america_colombia|latin_america_aoi_report.json"
  "se_asia|aoi_json_examples/coffee_se_asia_vietnam.geojson|coffee_se_asia_vietnam|se_asia_aoi_report.json"
  "west_africa|aoi_json_examples/cocoa_west_africa_ghana.geojson|cocoa_west_africa_ghana|west_africa_aoi_report.json"
)

log() {
  printf "[%s] %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

require_clean_repo() {
  local repo_path="$1"
  local label="$2"
  if [[ -n "$(git -C "$repo_path" status --porcelain)" ]]; then
    echo "ERROR: ${label} repo working tree is not clean: $repo_path" >&2
    git -C "$repo_path" status --porcelain >&2
    exit 1
  fi
}

main() {
  if [[ ! -d "$DT_REPO/.git" ]]; then
    echo "ERROR: DT repo not found: $DT_REPO" >&2
    exit 2
  fi

  require_clean_repo "$DT_REPO" "Digital Twin"

  rm -rf "$WORK_ROOT" "$FINAL_BUNDLE_ROOT"
  mkdir -p "$WORK_ROOT" "$FINAL_BUNDLE_ROOT/runs"

  for spec in "${RUN_SPECS[@]}"; do
    IFS="|" read -r run_id aoi_rel aoi_id report_json_name <<< "$spec"

    aoi_path="${REPO_ROOT}/${aoi_rel}"
    stage_root="${WORK_ROOT}/${run_id}"

    if [[ ! -f "$aoi_path" ]]; then
      echo "ERROR: missing AOI GeoJSON: $aoi_path" >&2
      exit 2
    fi

    log "Generating staged run: ${run_id} (${report_json_name})"
    EUDR_DMI_AOI_STAGING_DIR="$stage_root" \
    RUN_ID="$run_id" \
    STAGED_RUN_ID="$run_id" \
    AOI_ID="$aoi_id" \
    AOI_GEOJSON="$aoi_path" \
    EUDR_DMI_AOI_REPORT_JSON_FILENAME="$report_json_name" \
    "$REPO_ROOT/scripts/run_example_report_clean.sh" --no-publish-dt

    run_dir="${stage_root}/runs/${run_id}"
    if [[ ! -d "$run_dir" ]]; then
      echo "ERROR: expected run directory missing: $run_dir" >&2
      exit 2
    fi

    if [[ ! -f "${run_dir}/${report_json_name}" ]]; then
      echo "ERROR: expected report JSON missing: ${run_dir}/${report_json_name}" >&2
      exit 2
    fi

    rsync -a --delete "$run_dir/" "${FINAL_BUNDLE_ROOT}/runs/${run_id}/"
  done

  log "Publishing four AOI runs to DT repo"
  (
    cd "$DT_REPO"
    bash scripts/clean_aoi_reports.sh
    mkdir -p "$DT_RUNS_DIR"

    for spec in "${RUN_SPECS[@]}"; do
      IFS="|" read -r run_id _aoi_rel _aoi_id _report_json_name <<< "$spec"
      rsync -a --delete "${FINAL_BUNDLE_ROOT}/runs/${run_id}/" "${DT_RUNS_DIR}/${run_id}/"
    done

    python3 scripts/rebuild_aoi_reports_index.py --site-root "$DT_SITE_ROOT"
    python3 scripts/validate_aoi_run_artifacts.py --runs-dir "$DT_RUNS_DIR"

    for spec in "${RUN_SPECS[@]}"; do
      IFS="|" read -r run_id _aoi_rel _aoi_id _report_json_name <<< "$spec"
      python3 scripts/check_nav_links.py --site-root "$DT_SITE_ROOT" --run-id "$run_id"
    done
  )

  log "Completed generation + DT publish sync for run IDs: example, latin_america, se_asia, west_africa"
}

main "$@"
