#!/usr/bin/env bash
set -euo pipefail

DT_REPO_DEFAULT="../eudr-dmi-gil-digital-twin"

usage() {
  cat <<EOF
Usage: scripts/verify_dt_links.sh --dt-repo /path/to/eudr-dmi-gil-digital-twin

Runs the portable link checker and verifies AOI report navigation:
- docs/site/index.html -> docs/site/aoi_reports/index.html
- docs/site/aoi_reports/index.html -> runs/*/report.html
EOF
}

DT_REPO="$DT_REPO_DEFAULT"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dt-repo)
      DT_REPO="$2"
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

if [[ ! -d "$DT_REPO" ]]; then
  echo "ERROR: DT repo not found: $DT_REPO" >&2
  exit 2
fi

DT_SITE_DIR="$DT_REPO/docs/site"
if [[ ! -d "$DT_SITE_DIR" ]]; then
  echo "ERROR: DT site dir not found: $DT_SITE_DIR" >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECKER_PATH="$REPO_ROOT/../eudr_dmi/tools/site/check_site_links.py"

if [[ ! -f "$CHECKER_PATH" ]]; then
  echo "ERROR: link checker not found: $CHECKER_PATH" >&2
  echo "Hint: expected sibling checkout at ../eudr_dmi" >&2
  exit 2
fi

python "$CHECKER_PATH" --root "$DT_SITE_DIR" --out "$DT_REPO/docs/link_check.json"

python - "$DT_SITE_DIR" <<'PY'
from __future__ import annotations

import sys
from html.parser import HTMLParser
from pathlib import Path

site_root = Path(sys.argv[1]).resolve()
index_path = site_root / "index.html"
aoi_index_path = site_root / "aoi_reports" / "index.html"

if not index_path.is_file():
    raise SystemExit(f"ERROR: missing {index_path}")
if not aoi_index_path.is_file():
    raise SystemExit(f"ERROR: missing {aoi_index_path}")

class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for k, v in attrs:
            if k == "href" and v:
                self.links.append(v)


def _extract_links(path: Path) -> list[str]:
    parser = LinkExtractor()
    parser.feed(path.read_text(encoding="utf-8", errors="replace"))
    return parser.links

index_links = _extract_links(index_path)
aoi_links = _extract_links(aoi_index_path)

has_aoi_link = False
for href in index_links:
    if "aoi_reports/index.html" in href:
        target = (index_path.parent / href).resolve()
        if target == aoi_index_path:
            has_aoi_link = True
            break

if not has_aoi_link:
    raise SystemExit("ERROR: AOI Reports link missing or broken in docs/site/index.html")

report_links = [h for h in aoi_links if "runs/" in h and h.endswith("/report.html")]
for href in report_links:
    target = (aoi_index_path.parent / href).resolve()
    if not target.is_file():
        raise SystemExit(f"ERROR: report link missing: {href}")

print("OK: AOI report navigation links verified")
PY
