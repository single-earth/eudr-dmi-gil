from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


AUDIT_ROOT_ENV = "EUDR_AUDIT_ROOT"
DEFAULT_AUDIT_ROOT = Path("audit")


def resolve_audit_root(explicit: str | os.PathLike[str] | None = None) -> Path:
    """Resolve the evidence/audit root directory.

    Conventions (grounded in the upstream eudr_dmi README):
    - an "audit root" can be overridden by operator configuration
    - bundles are written under: <AUDIT_ROOT>/<YYYY-MM-DD>/<bundle_id>/

    This repo defaults to a local repo-relative `audit/` folder, which is
    intentionally gitignored.
    """

    if explicit is not None:
        return Path(explicit)

    env_value = os.environ.get(AUDIT_ROOT_ENV)
    if env_value:
        return Path(env_value)

    return DEFAULT_AUDIT_ROOT


@dataclass(frozen=True)
class BundleRef:
    bundle_date: str  # YYYY-MM-DD
    bundle_id: str


@dataclass(frozen=True)
class BundleLayout:
    """Filesystem layout for an evidence bundle."""

    audit_root: Path
    ref: BundleRef

    @property
    def bundle_root(self) -> Path:
        return self.audit_root / self.ref.bundle_date / self.ref.bundle_id

    @property
    def reports_dir(self) -> Path:
        return self.bundle_root / "reports"

    @property
    def manifest_path(self) -> Path:
        return self.bundle_root / "bundle_manifest.json"

    @property
    def site_bundle_zip_path(self) -> Path:
        return self.bundle_root / "site_bundle.zip"
