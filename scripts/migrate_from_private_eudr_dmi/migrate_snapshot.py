#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import shutil
import sys
from typing import Iterable


DEFAULT_DEST_DIR = Path("adopted/private_eudr_dmi_snapshot")
MANIFEST_PATH = DEFAULT_DEST_DIR / "latest_manifest.sha256"

# Hard excludes (policy): do not copy these.
EXACT_BASENAMES = {
    "keys.yml",
}

# Treat any of these directory names as excluded wherever they appear.
EXCLUDED_DIR_NAMES = {
    "audit",
    "outputs",
}

# Exclude env files by pattern.
ENV_PREFIX = ".env"

# Exclude common secret/key material by suffix.
EXCLUDED_SUFFIXES = {
    ".pem",
    ".key",
}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Adopt a sanitized snapshot from the private eudr_dmi repo into this repository. "
            "This tool intentionally excludes secrets, runtime data, and generated artefacts."
        )
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Path to a local working tree of the private eudr_dmi repository.",
    )
    parser.add_argument(
        "--dest",
        default=str(DEFAULT_DEST_DIR),
        help="Destination directory under this repo (default: adopted/private_eudr_dmi_snapshot).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List actions without copying files.",
    )
    return parser.parse_args(argv)


def is_excluded_path(relative_path: Path) -> bool:
    parts = relative_path.parts

    # Directory exclusions (any level)
    if any(part in EXCLUDED_DIR_NAMES for part in parts):
        return True

    basename = relative_path.name

    # Exact filename exclusions
    if basename in EXACT_BASENAMES:
        return True

    # Env file exclusions
    if basename == ENV_PREFIX or basename.startswith(ENV_PREFIX + "."):
        return True

    # Suffix-based exclusions
    lowered = basename.lower()
    if any(lowered.endswith(suffix) for suffix in EXCLUDED_SUFFIXES):
        return True

    return False


def iter_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if path.is_file() and not path.is_symlink():
            yield path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_relpath(path: Path, root: Path) -> Path:
    return path.resolve().relative_to(root.resolve())


def ensure_empty_dir(path: Path, dry_run: bool) -> None:
    if dry_run:
        return
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def write_manifest(entries: list[tuple[str, str]], manifest_path: Path, dry_run: bool) -> None:
    lines = [f"{digest}  {relpath}\n" for relpath, digest in entries]
    if dry_run:
        return
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("".join(lines), encoding="utf-8")


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    source = Path(args.source).expanduser()
    dest = Path(args.dest)

    if not source.exists() or not source.is_dir():
        print(f"ERROR: source not found or not a directory: {source}", file=sys.stderr)
        return 2

    # Basic guardrail: do not accept the runtime data plane as a source.
    # This is a coarse safety check; the hard excludes below remain authoritative.
    if str(source.resolve()).startswith(str(Path("/Users/server/data/dmi").resolve())):
        print("ERROR: source appears to be within the runtime data plane (/Users/server/data/dmi).", file=sys.stderr)
        return 2

    # Ensure destination is inside the repo working tree, when possible.
    # (This tool is expected to be run from repo root.)
    repo_root = Path.cwd().resolve()
    dest_abs = (repo_root / dest).resolve()
    if repo_root not in dest_abs.parents and dest_abs != repo_root:
        print("ERROR: dest must be within the current working directory (repo root).", file=sys.stderr)
        return 2

    if args.dry_run:
        print(f"DRY RUN: would replace destination directory: {dest_abs}")
    ensure_empty_dir(dest_abs, dry_run=args.dry_run)

    copied: list[tuple[str, str]] = []

    for file_path in iter_files(source):
        rel = safe_relpath(file_path, source)

        if is_excluded_path(rel):
            continue

        dest_file = dest_abs / rel

        if args.dry_run:
            print(f"COPY {rel}")
            continue

        dest_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_path, dest_file)

    # Build deterministic manifest from destination contents.
    if args.dry_run:
        print(f"DRY RUN: would write manifest to {MANIFEST_PATH}")
        return 0

    for dest_file in iter_files(dest_abs):
        rel = dest_file.relative_to(dest_abs)
        # Defensive: ensure excluded paths do not slip in via pre-existing state.
        if is_excluded_path(rel):
            continue
        copied.append((rel.as_posix(), sha256_file(dest_file)))

    copied.sort(key=lambda x: x[0])
    write_manifest(copied, dest_abs / MANIFEST_PATH.name, dry_run=False)

    print(f"Wrote snapshot to: {dest_abs}")
    print(f"Wrote manifest to: {dest_abs / MANIFEST_PATH.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
