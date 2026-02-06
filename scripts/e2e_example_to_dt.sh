#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DT_REPO_DEFAULT="/Users/server/projects/eudr-dmi-gil-digital-twin"

DT_REPO="${DT_REPO:-$DT_REPO_DEFAULT}"
PUBLISH=0

for arg in "$@"; do
  case "$arg" in
    --publish)
      PUBLISH=1
      ;;
    -h|--help)
      echo "Usage: scripts/e2e_example_to_dt.sh [--publish]" >&2
      echo "Env vars: DT_REPO" >&2
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

cd "$REPO_ROOT"

scripts/run_example_report_clean.sh

if [[ "$PUBLISH" == "1" ]]; then
  if [[ ! -d "$DT_REPO/.git" ]]; then
    echo "ERROR: DT repo not found: $DT_REPO" >&2
    exit 2
  fi
  echo "\nPublishing to DT repo (RUN_ID=example): $DT_REPO"
  (cd "$DT_REPO" && RUN_ID="example" STAGING_DIR="out/site_bundle/aoi_reports" \
    scripts/publish_aoi_run_from_staging.sh)

  echo "\nNext steps (run in DT repo):"
  echo "cd $DT_REPO"
  echo "git status"
  echo "git add docs/site/aoi_reports/"
  echo "git commit -m \"Publish AOI run example\""
  echo "git push"
fi

scripts/verify_dt_links.sh --dt-repo "$DT_REPO"
