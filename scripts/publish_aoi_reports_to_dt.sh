#!/usr/bin/env bash
set -euo pipefail

# Deterministic publisher for AOI reports to the Digital Twin repo.
#
# Requirements enforced:
# - Source runs live in out/site_bundle/aoi_reports/runs/
# - Exactly two run directories must exist
# - DT target path: /Users/server/projects/eudr-dmi-gil-digital-twin/docs/site/aoi_reports/runs/
# - Do not touch audit/evidence/

SOURCE_RUNS_DIR="/Users/server/projects/eudr-dmi-gil/out/site_bundle/aoi_reports/runs"
DT_REPO="/Users/server/projects/eudr-dmi-gil-digital-twin"
DT_SITE_ROOT="${DT_REPO}/docs/site"
DT_RUNS_DIR="${DT_SITE_ROOT}/aoi_reports/runs"

if [[ ! -d "$SOURCE_RUNS_DIR" ]]; then
  echo "ERROR: source runs dir not found: $SOURCE_RUNS_DIR" >&2
  exit 2
fi

# Collect run directories (exactly two required)
mapfile -t run_dirs < <(find "$SOURCE_RUNS_DIR" -mindepth 1 -maxdepth 1 -type d | sort)

if [[ ${#run_dirs[@]} -ne 2 ]]; then
  echo "ERROR: expected exactly 2 run directories in $SOURCE_RUNS_DIR; found ${#run_dirs[@]}" >&2
  for d in "${run_dirs[@]}"; do
    echo "- $d" >&2
  done
  exit 2
fi

# Validate each run has report.html and aoi_report.json
for d in "${run_dirs[@]}"; do
  if [[ ! -f "$d/report.html" ]]; then
    echo "ERROR: missing report.html in $d" >&2
    exit 2
  fi
  if [[ ! -f "$d/aoi_report.json" ]]; then
    echo "ERROR: missing aoi_report.json in $d" >&2
    exit 2
  fi
done

# Ensure DT repo exists and is clean
if [[ ! -d "$DT_REPO/.git" ]]; then
  echo "ERROR: DT repo not found: $DT_REPO" >&2
  exit 2
fi

cd "$DT_REPO"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "ERROR: DT repo working tree is not clean" >&2
  git status --porcelain >&2
  exit 1
fi

# Clean existing AOI reports in DT repo
scripts/clean_aoi_reports.sh

# Copy runs into DT
mkdir -p "$DT_RUNS_DIR"
for d in "${run_dirs[@]}"; do
  run_id="$(basename "$d")"
  rsync -a --delete "$d/" "$DT_RUNS_DIR/$run_id/"
done

# Rebuild AOI reports index
scripts/rebuild_aoi_reports_index.py --site-root "$DT_SITE_ROOT"

# Link check: index -> each run -> report.html must resolve
index_file="$DT_SITE_ROOT/aoi_reports/index.html"
if [[ ! -f "$index_file" ]]; then
  echo "ERROR: missing index file: $index_file" >&2
  exit 2
fi

for d in "${run_dirs[@]}"; do
  run_id="$(basename "$d")"
  report_file="$DT_RUNS_DIR/$run_id/report.html"
  if [[ ! -f "$report_file" ]]; then
    echo "ERROR: missing DT report: $report_file" >&2
    exit 2
  fi
  if ! grep -q "runs/$run_id/report.html" "$index_file"; then
    echo "ERROR: index missing link to run $run_id" >&2
    exit 2
  fi
done

# Stage, commit, and push changes

git add docs/site/aoi_reports/

if git diff --cached --quiet; then
  echo "ERROR: no changes staged (nothing to publish)" >&2
  exit 1
fi

git commit -m "Publish AOI reports (2 runs)"

git push origin main

# Return to original repo
cd - >/dev/null
