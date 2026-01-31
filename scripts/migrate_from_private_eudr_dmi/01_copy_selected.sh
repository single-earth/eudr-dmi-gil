#!/usr/bin/env bash
set -euo pipefail

# Copies selected directories/files from a private `eudr_dmi` working tree into
# this repository's snapshot folder, enforcing hard exclusions.
#
# Required env:
#   PRIVATE_EUDR_DMI_SRC=/absolute/path/to/private/eudr_dmi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SRC_ROOT="${PRIVATE_EUDR_DMI_SRC:-}"
DEST_ROOT="$REPO_ROOT/adopted/private_eudr_dmi_snapshot"

if [[ -z "$SRC_ROOT" ]]; then
  echo "ERROR: PRIVATE_EUDR_DMI_SRC is not set" >&2
  exit 2
fi

if [[ ! -d "$SRC_ROOT" ]]; then
  echo "ERROR: PRIVATE_EUDR_DMI_SRC is not a directory: $SRC_ROOT" >&2
  exit 2
fi

mkdir -p "$DEST_ROOT"

RSYNC_EXCLUDES=(
  "--exclude=.git/"
  "--exclude=audit/"
  "--exclude=outputs/"
  "--exclude=.venv/"
  "--exclude=__pycache__/"
  "--exclude=*.pyc"
  "--exclude=.env*"
  "--exclude=keys.yml"
)

copy_one() {
  local rel="$1"
  local src="$SRC_ROOT/$rel"
  local dest="$DEST_ROOT/$rel"

  if [[ ! -e "$src" ]]; then
    # Not all private repos will have all optional paths.
    return 0
  fi

  mkdir -p "$(dirname "$dest")"

  # Use rsync idempotently (-a) and remove files that no longer exist in source (--delete).
  # Trailing slashes matter: for directories, we want the directory itself replicated.
  if [[ -d "$src" ]]; then
    rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$src/" "$dest/"
  else
    rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$src" "$dest"
  fi
}

# Minimum adoption scope.
copy_one "src"
copy_one "tests"
copy_one "tools"
copy_one "scripts"
copy_one "docs"
copy_one ".github"

# Common dependency descriptors.
shopt -s nullglob
for f in "$SRC_ROOT"/requirements*.txt "$SRC_ROOT"/requirements*.in; do
  base="$(basename "$f")"
  copy_one "$base"
done

# Conservative allowlist for top-level run scripts (if present).
copy_one "run.sh"
copy_one "run.py"

# Copy top-level run* scripts without pulling in other top-level files.
# This is limited to run*.sh/run*.py for safety.
for f in "$SRC_ROOT"/run*.sh "$SRC_ROOT"/run*.py; do
  base="$(basename "$f")"
  copy_one "$base"
done

# Fail fast if forbidden files exist in destination.
# This check is intentionally post-copy to catch any unexpected paths.
if find "$DEST_ROOT" -type f \( -name '.env' -o -name '.env.*' -o -name 'keys.yml' \) -print -quit | grep -q .; then
  echo "ERROR: forbidden files (.env* or keys.yml) detected in $DEST_ROOT" >&2
  find "$DEST_ROOT" -type f \( -name '.env' -o -name '.env.*' -o -name 'keys.yml' \) -print >&2
  exit 3
fi

echo "Snapshot copied to: $DEST_ROOT"
