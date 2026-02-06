from __future__ import annotations

import os
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from eudr_dmi_gil.io import data_plane
from eudr_dmi_gil.reports.determinism import sha256_file, write_json

DATASET_VERSION_DEFAULT = "2024-v1.12"
HANSEN_BASE_DIR_NAME = "hansen_gfc_2024_v1_12"
HANSEN_URL_TEMPLATE_ENV = "EUDR_DMI_HANSEN_URL_TEMPLATE"


@dataclass(frozen=True)
class HansenLayerEntry:
    tile_id: str
    layer: str
    local_path: str
    sha256: str
    size_bytes: int
    source_url: str
    status: str


def hansen_default_base_dir() -> Path:
    return data_plane.external_root() / "hansen" / HANSEN_BASE_DIR_NAME


def resolve_tile_dir(tile_id: str) -> Path:
    return hansen_default_base_dir() / "tiles" / tile_id


def _format_url(template: str, *, tile_id: str, layer: str) -> str:
    return template.format(layer=layer, tile_id=tile_id)


def _download_to_path(url: str, dest_path: Path) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
    with urllib.request.urlopen(url) as response, tmp_path.open("wb") as fh:  # noqa: S310
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            fh.write(chunk)
    tmp_path.replace(dest_path)


def ensure_hansen_layers_present(
    tile_id: str,
    layers: Iterable[str],
    *,
    download: bool,
) -> list[HansenLayerEntry]:
    entries: list[HansenLayerEntry] = []
    url_template = os.environ.get(HANSEN_URL_TEMPLATE_ENV, "").strip()

    for layer in layers:
        tile_dir = resolve_tile_dir(tile_id)
        local_path = tile_dir / f"{layer}.tif"
        source_url = _format_url(url_template, tile_id=tile_id, layer=layer) if url_template else ""

        if local_path.is_file():
            entries.append(
                HansenLayerEntry(
                    tile_id=tile_id,
                    layer=layer,
                    local_path=str(local_path.resolve()),
                    sha256=sha256_file(local_path),
                    size_bytes=local_path.stat().st_size,
                    source_url=source_url,
                    status="present",
                )
            )
            continue

        if not download:
            entries.append(
                HansenLayerEntry(
                    tile_id=tile_id,
                    layer=layer,
                    local_path=str(local_path.resolve()),
                    sha256="",
                    size_bytes=0,
                    source_url=source_url,
                    status="missing",
                )
            )
            continue

        if not url_template:
            raise RuntimeError(
                "Missing Hansen URL template. Set EUDR_DMI_HANSEN_URL_TEMPLATE to enable downloads."
            )

        _download_to_path(source_url, local_path)
        entries.append(
            HansenLayerEntry(
                tile_id=tile_id,
                layer=layer,
                local_path=str(local_path.resolve()),
                sha256=sha256_file(local_path),
                size_bytes=local_path.stat().st_size,
                source_url=source_url,
                status="downloaded",
            )
        )

    return entries


def _infer_tile_id_from_path(path: Path) -> str:
    if path.parent.name and path.parent.name != "tiles":
        return path.parent.name
    return "unknown"


def build_entries_from_provenance(
    provenance: Iterable[object],
    *,
    tile_dir: Path,
) -> list[HansenLayerEntry]:
    entries: list[HansenLayerEntry] = []
    for item in provenance:
        layer = getattr(item, "layer", None)
        relpath = getattr(item, "relpath", None)
        sha256 = getattr(item, "sha256", "")
        if not layer or not relpath:
            continue
        local_path = (tile_dir / relpath).resolve()
        size_bytes = local_path.stat().st_size if local_path.exists() else 0
        tile_id = _infer_tile_id_from_path(Path(relpath))
        entries.append(
            HansenLayerEntry(
                tile_id=tile_id,
                layer=str(layer),
                local_path=str(local_path),
                sha256=str(sha256),
                size_bytes=size_bytes,
                source_url="",
                status="present" if local_path.exists() else "missing",
            )
        )
    return entries


def write_tiles_manifest(
    manifest_path: Path,
    *,
    entries: Iterable[HansenLayerEntry],
    dataset_version: str,
    tile_source: str,
    aoi_id: str,
    run_id: str,
    tile_ids: Iterable[str],
    derived_relpaths: dict[str, str],
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    created_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    ordered_entries = sorted(
        entries, key=lambda e: (e.tile_id, e.layer, e.local_path)
    )
    payload = {
        "dataset_version": dataset_version,
        "tile_source": tile_source,
        "aoi_id": aoi_id,
        "run_id": run_id,
        "tile_ids": sorted(set(tile_ids)),
        "created_utc": created_utc,
        "derived_relpaths": dict(sorted(derived_relpaths.items())),
        "entries": [
            {
                "tile_id": e.tile_id,
                "layer": e.layer,
                "local_path": e.local_path,
                "sha256": e.sha256,
                "size_bytes": e.size_bytes,
                "source_url": e.source_url,
                "status": e.status,
            }
            for e in ordered_entries
        ],
    }
    write_json(manifest_path, payload)
