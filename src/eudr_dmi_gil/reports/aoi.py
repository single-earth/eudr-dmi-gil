from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .determinism import canonical_json_bytes, create_deterministic_zip, write_bytes
from .layout import BundleLayout


def write_aoi_summary_report(
    *,
    layout: BundleLayout,
    aoi_id: str,
    summary: Mapping[str, Any],
) -> Path:
    """Write a minimal AOI summary report.

    This is a scaffold: it writes deterministic JSON to:
    <bundle_root>/reports/aoi_summary_v1/<aoi_id>.json

    Concrete pipelines will add additional outputs (maps, stats, evidence index).
    """

    out_path = layout.reports_dir / "aoi_summary_v1" / f"{aoi_id}.json"
    write_bytes(out_path, canonical_json_bytes({"aoi_id": aoi_id, **dict(summary)}) + b"\n")
    return out_path


def write_site_bundle_zip(
    *,
    layout: BundleLayout,
    index_html: str,
    extra_files: Mapping[str, bytes] | None = None,
) -> Path:
    """Create a portable site bundle zip.

    Convention (grounded in upstream eudr_dmi README): a bundle can include a
    portable, self-contained zip for publication.

    The portal repo is responsible for hosting/publishing the zip; this repo is
    responsible for generating it deterministically.
    """

    files: dict[str, bytes] = {"index.html": index_html.encode("utf-8")}
    if extra_files:
        files.update(dict(extra_files))

    create_deterministic_zip(layout.site_bundle_zip_path, files)
    return layout.site_bundle_zip_path
