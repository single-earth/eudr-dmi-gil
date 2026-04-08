#!/usr/bin/env bash
set -euo pipefail

# Publish AOI report site bundle into the Digital Twin portal repo.
#
# This script intentionally:
# - does NOT push any git remotes
# - assumes no credentials
# - performs a local filesystem sync only

SRC_DIR_DEFAULT="docs/site_bundle_reports"
DT_TARGET_DEFAULT="../eudr-dmi-gil-digital-twin/site/bundles"

SRC_DIR="${EUDR_DMI_SITE_BUNDLE_DIR:-$SRC_DIR_DEFAULT}"
DT_TARGET_DIR="${EUDR_DMI_DIGITAL_TWIN_AOI_REPORTS_DIR:-$DT_TARGET_DEFAULT}"

DRY_RUN="${EUDR_DMI_PUBLISH_DRY_RUN:-0}"

usage() {
  cat <<EOF
Usage: scripts/publish_reports_to_digital_twin.sh

Copies the portable AOI report site bundle into the Digital Twin repo checkout.

Env vars:
  EUDR_DMI_SITE_BUNDLE_DIR              Source folder (default: $SRC_DIR_DEFAULT)
  EUDR_DMI_DIGITAL_TWIN_AOI_REPORTS_DIR Target folder (default: $DT_TARGET_DEFAULT)
  EUDR_DMI_PUBLISH_DRY_RUN              If '1', only prints what would change

Notes:
- This script does not git add/commit/push.
- The intended workflow is human-in-the-loop: review the DT repo diff, then commit/push from that repo.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ ! -d "$SRC_DIR" ]]; then
  echo "ERROR: source directory not found: $SRC_DIR" >&2
  echo "Hint: run the exporter first, e.g.: python scripts/export_reports_site_bundle.py --date YYYY-MM-DD" >&2
  exit 2
fi

mkdir -p "$DT_TARGET_DIR"

# Try to detect if target lives inside a git repo (DT repo checkout).
# Not strictly required, but gives better operator feedback.
DT_GIT_ROOT=""
_probe="$DT_TARGET_DIR"
for _ in {1..8}; do
  if [[ -d "$_probe/.git" ]]; then
    DT_GIT_ROOT="$_probe"
    break
  fi
  parent="$(cd "$_probe"/.. && pwd)"
  current="$(cd "$_probe" && pwd)"
  if [[ "$parent" == "$current" ]]; then
    break
  fi
  _probe="$parent"
done

if [[ -z "$DT_GIT_ROOT" ]]; then
  echo "WARN: target does not appear to be inside a git repo (.git not found in ancestors)." >&2
  echo "      Target: $DT_TARGET_DIR" >&2
  echo "      Continuing anyway." >&2
fi

RSYNC_ARGS=(-a --delete --itemize-changes)
if [[ "$DRY_RUN" == "1" ]]; then
  RSYNC_ARGS+=(--dry-run)
fi

echo "Publishing AOI reports site bundle"
echo "- Source: $SRC_DIR"
echo "- Target: $DT_TARGET_DIR"

# Sync content (stable end result; copy order not important).
# Trailing '/.' ensures we copy contents, not the directory itself.
set +e
rsync_output="$(rsync "${RSYNC_ARGS[@]}" "$SRC_DIR/." "$DT_TARGET_DIR/" 2>&1)"
status=$?
set -e

if [[ $status -ne 0 ]]; then
  echo "ERROR: rsync failed ($status)" >&2
  echo "$rsync_output" >&2
  exit $status
fi

# Summary
changed_lines="$(echo "$rsync_output" | sed '/^$/d' | wc -l | tr -d ' ')"
file_count_src="$(find "$SRC_DIR" -type f | wc -l | tr -d ' ')"

echo ""
echo "Summary"
echo "- Source files: $file_count_src"
if [[ "$DRY_RUN" == "1" ]]; then
  echo "- Dry run: yes"
else
  echo "- Dry run: no"
fi

echo ""
echo "Rsync itemized changes ($changed_lines lines):"
# Print itemized changes deterministically (rsync already stable enough);
# sort to make output stable when rsync output ordering differs.
if [[ -n "$rsync_output" ]]; then
  echo "$rsync_output" | sed '/^$/d' | sort
fi

echo ""
echo "Next steps (human-in-the-loop):"
if [[ -n "$DT_GIT_ROOT" ]]; then
  echo "- Review changes: (cd \"$DT_GIT_ROOT\" && git status && git diff)"
  echo "- Commit/push from the DT repo when ready."
else
  echo "- Review the target folder contents and commit/push from your DT repo checkout." 
fi
