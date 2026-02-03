#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DT_REPO_DEFAULT="/Users/server/projects/eudr-dmi-gil-digital-twin"
DT_AOI_DIR_DEFAULT="docs/site/aoi_reports"
SOURCE_DIR_DEFAULT="out/site_bundle/aoi_reports"
KEEP_DEFAULT="2"

usage() {
  cat <<EOF
Usage: scripts/publish_latest_aoi_reports_to_dt.sh [--dt-repo PATH] [--dt-aoi-dir REL] [--source-dir REL] [--keep N]

Publishes the latest AOI report runs into the Digital Twin repo.
Defaults:
  --dt-repo   $DT_REPO_DEFAULT
  --dt-aoi-dir $DT_AOI_DIR_DEFAULT
  --source-dir $SOURCE_DIR_DEFAULT
  --keep       $KEEP_DEFAULT
EOF
}

DT_REPO="$DT_REPO_DEFAULT"
DT_AOI_DIR="$DT_AOI_DIR_DEFAULT"
SOURCE_DIR="$SOURCE_DIR_DEFAULT"
KEEP="$KEEP_DEFAULT"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dt-repo)
      DT_REPO="$2"
      shift 2
      ;;
    --dt-aoi-dir)
      DT_AOI_DIR="$2"
      shift 2
      ;;
    --source-dir)
      SOURCE_DIR="$2"
      shift 2
      ;;
    --keep)
      KEEP="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

python "$REPO_ROOT/tools/publish_latest_aoi_reports_to_dt.py" \
  --dt-repo "$DT_REPO" \
  --dt-aoi-dir "$DT_AOI_DIR" \
  --source-dir "$SOURCE_DIR" \
  --keep "$KEEP"
