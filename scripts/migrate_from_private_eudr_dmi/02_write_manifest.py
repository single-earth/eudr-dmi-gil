#!/usr/bin/env python3

from __future__ import annotations

import hashlib
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT_ROOT = REPO_ROOT / "adopted" / "private_eudr_dmi_snapshot"
MANIFEST_PATH = SNAPSHOT_ROOT / "latest_manifest.sha256"

EXCLUDED_DIR_NAMES = {".git", "audit", "outputs", ".venv", "__pycache__"}
EXCLUDED_FILE_SUFFIXES = {".pyc"}
EXCLUDED_BASENAMES = {"keys.yml"}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def is_excluded(rel: Path) -> bool:
    if any(part in EXCLUDED_DIR_NAMES for part in rel.parts):
        return True

    name = rel.name

    if name == ".env" or name.startswith(".env."):
        return True

    if name in EXCLUDED_BASENAMES:
        return True

    lowered = name.lower()
    if any(lowered.endswith(suffix) for suffix in EXCLUDED_FILE_SUFFIXES):
        return True

    return False


def main(argv: list[str]) -> int:
    if not SNAPSHOT_ROOT.exists() or not SNAPSHOT_ROOT.is_dir():
        print(f"ERROR: snapshot root not found: {SNAPSHOT_ROOT}", file=sys.stderr)
        return 2

    entries: list[tuple[str, str]] = []

    for path in SNAPSHOT_ROOT.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue

        rel = path.relative_to(SNAPSHOT_ROOT)
        if is_excluded(rel):
            continue

        entries.append((rel.as_posix(), sha256_file(path)))

    entries.sort(key=lambda x: x[0])

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        for relpath, digest in entries:
            f.write(f"{digest} {relpath}\n")

    print(f"Wrote manifest: {MANIFEST_PATH}")
    print(f"Entries: {len(entries)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
