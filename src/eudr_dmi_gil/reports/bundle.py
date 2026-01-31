from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

from .determinism import canonical_json_bytes


EVIDENCE_ROOT_ENV = "EUDR_DMI_EVIDENCE_ROOT"
DEFAULT_EVIDENCE_ROOT = Path("audit") / "evidence"


def resolve_evidence_root(explicit: str | os.PathLike[str] | None = None) -> Path:
    """Resolve the evidence root directory.

    Conventions (grounded in upstream `eudr_dmi` README and this repo ADRs):
    - evidence root defaults to `audit/evidence/`
    - it is overrideable via `EUDR_DMI_EVIDENCE_ROOT`
    - bundles are written under: <root>/<YYYY-MM-DD>/<bundle_id>/

    The default path is intentionally repo-relative and should be gitignored.
    """

    if explicit is not None:
        return Path(explicit)

    env_value = os.environ.get(EVIDENCE_ROOT_ENV)
    if env_value:
        return Path(env_value)

    return DEFAULT_EVIDENCE_ROOT


def utc_today_yyyy_mm_dd() -> str:
    return date.today().strftime("%Y-%m-%d")


@dataclass(frozen=True)
class ArtifactRecord:
    relpath: str
    sha256: str
    size_bytes: int


def compute_sha256(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Compute sha256 hex digest for a file."""

    import hashlib

    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def bundle_dir(
    *,
    bundle_id: str,
    bundle_date: str | None = None,
    evidence_root: str | Path | None = None,
) -> Path:
    """Compute the bundle directory path.

    Layout: <root>/<YYYY-MM-DD>/<bundle_id>/

    If bundle_date is omitted, uses the current UTC date.
    """

    root = resolve_evidence_root(evidence_root)
    if bundle_date is None:
        # Explicit UTC date (not local time).
        bundle_date = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")

    return root / bundle_date / bundle_id


def write_manifest(bundle_dir: str | Path, artifacts: Iterable[str | Path]) -> bytes:
    """Write `manifest.json` in bundle_dir and return the bytes written.

    - stable ordering (sorted by relpath)
    - stable JSON formatting

    `artifacts` should be a list of files inside `bundle_dir` (or paths that can
    be made relative to it).
    """

    bdir = Path(bundle_dir)
    records: list[ArtifactRecord] = []

    for artifact in artifacts:
        p = Path(artifact)
        relpath = str(p.relative_to(bdir))
        records.append(
            ArtifactRecord(
                relpath=relpath,
                sha256=compute_sha256(p),
                size_bytes=p.stat().st_size,
            )
        )

    records_sorted = sorted(records, key=lambda r: r.relpath)

    manifest_obj = {
        "manifest_version": "evidence_manifest_v1",
        "bundle_dir": str(bdir.as_posix()),
        "artifacts": [
            {"relpath": r.relpath, "sha256": r.sha256, "size_bytes": r.size_bytes}
            for r in records_sorted
        ],
    }

    manifest_bytes = canonical_json_bytes(manifest_obj) + b"\n"

    bdir.mkdir(parents=True, exist_ok=True)
    (bdir / "manifest.json").write_bytes(manifest_bytes)
    return manifest_bytes
