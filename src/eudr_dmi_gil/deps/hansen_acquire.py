from __future__ import annotations

import os
import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from eudr_dmi_gil.io import data_plane
from eudr_dmi_gil.reports.determinism import sha256_file, write_json

DATASET_VERSION_DEFAULT = "2025-v1.13"
HANSEN_BASE_DIR_NAME = "hansen_gfc_2025_v1_13"
HANSEN_URL_TEMPLATE_ENV = "EUDR_DMI_HANSEN_URL_TEMPLATE"
DEFAULT_HANSEN_URL_TEMPLATE = (
    "https://storage.googleapis.com/earthenginepartners-hansen/"
    "GFC-2025-v1.13/Hansen_GFC-2025-v1.13_{layer}_{url_tile_id}.tif"
)


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
    url_tile_id = tile_id
    m = re.fullmatch(r"([NS])(\d{2})_([EW])(\d{3})", tile_id)
    if m:
        lat_h, lat_v, lon_h, lon_v = m.groups()
        url_tile_id = f"{lat_v}{lat_h}_{lon_v}{lon_h}"
    return template.format(layer=layer, tile_id=tile_id, url_tile_id=url_tile_id)


def resolve_hansen_url_template() -> str:
    url_template = os.environ.get(HANSEN_URL_TEMPLATE_ENV, "").strip()
    return url_template or DEFAULT_HANSEN_URL_TEMPLATE


def infer_hansen_latest_year(
    *,
    dataset_version: str | None = None,
    tile_dir: Path | None = None,
    external_root: Path | None = None,
) -> int:
    """Infer the most recent Hansen year from local context.

    Preference order:
    1) Explicit dataset_version (e.g., "2025-v1.13")
    2) Tile directory path parts (e.g., hansen_gfc_2025_v1_13)
    3) External root hansen directories
    4) DATASET_VERSION_DEFAULT
    """

    def _extract_year(value: str) -> int | None:
        match = re.search(r"(?<!\d)(20\d{2})(?!\d)", value)
        if not match:
            return None
        return int(match.group(1))

    candidates: list[int] = []

    if dataset_version:
        year = _extract_year(dataset_version)
        if year is not None:
            candidates.append(year)

    if tile_dir is not None:
        for part in tile_dir.resolve().parts:
            year = _extract_year(str(part))
            if year is not None:
                candidates.append(year)

    if not candidates:
        root = external_root or data_plane.external_root()
        hansen_root = Path(root) / "hansen"
        if hansen_root.is_dir():
            for entry in hansen_root.iterdir():
                if not entry.is_dir():
                    continue
                year = _extract_year(entry.name)
                if year is not None:
                    candidates.append(year)

    fallback = _extract_year(DATASET_VERSION_DEFAULT) or 2025
    return max(candidates) if candidates else fallback


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
    url_template = resolve_hansen_url_template()

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
    url_template: str | None = None,
) -> list[HansenLayerEntry]:
    entries: list[HansenLayerEntry] = []
    resolved_template = (url_template or resolve_hansen_url_template()).strip()
    for item in provenance:
        layer = getattr(item, "layer", None)
        relpath = getattr(item, "relpath", None)
        sha256 = getattr(item, "sha256", "")
        if not layer or not relpath:
            continue
        local_path = (tile_dir / relpath).resolve()
        size_bytes = local_path.stat().st_size if local_path.exists() else 0
        tile_id = _infer_tile_id_from_path(Path(relpath))
        source_url = (
            _format_url(resolved_template, tile_id=tile_id, layer=str(layer))
            if resolved_template and tile_id != "unknown"
            else ""
        )
        entries.append(
            HansenLayerEntry(
                tile_id=tile_id,
                layer=str(layer),
                local_path=str(local_path),
                sha256=str(sha256),
                size_bytes=size_bytes,
                source_url=source_url,
                status="present" if local_path.exists() else "missing",
            )
        )
    return entries


def portable_hansen_local_path(local_path: str) -> str:
    path = Path(local_path)
    if path.parent.name and path.name:
        return f"{path.parent.name}/{path.name}"
    return path.as_posix()


def _manifest_created_utc() -> str:
    override = os.environ.get("EUDR_DMI_GENERATED_AT_UTC", "").strip()
    if override:
        try:
            created = datetime.fromisoformat(override.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                "EUDR_DMI_GENERATED_AT_UTC must be an ISO-8601 timestamp"
            ) from exc
        if created.tzinfo is None:
            raise ValueError("EUDR_DMI_GENERATED_AT_UTC must include a timezone")
        return created.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
            "+00:00", "Z"
        )

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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
    created_utc = _manifest_created_utc()
    ordered_entries = sorted(
        entries, key=lambda e: (e.tile_id, e.layer, portable_hansen_local_path(e.local_path))
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
                "local_path": portable_hansen_local_path(e.local_path),
                "sha256": e.sha256,
                "size_bytes": e.size_bytes,
                "source_url": e.source_url,
                "status": e.status,
            }
            for e in ordered_entries
        ],
    }
    write_json(manifest_path, payload)
