#!/usr/bin/env bash
set -euo pipefail

# Convenience wrapper for bootstrapping the repo-local DuckDB catalogue.
# Keeps everything repo-relative.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [ -f .venv/bin/activate ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

python scripts/bootstrap_data_db.py "$@"
