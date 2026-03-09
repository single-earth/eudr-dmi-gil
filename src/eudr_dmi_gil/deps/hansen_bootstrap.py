from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

from eudr_dmi_gil.io import data_plane
from eudr_dmi_gil.reports.determinism import sha256_file, write_json

from .hansen_acquire import (
    DATASET_VERSION_DEFAULT,
    HANSEN_BASE_DIR_NAME,
    HANSEN_URL_TEMPLATE_ENV,
    HansenLayerEntry,
    ensure_hansen_layers_present,
    hansen_default_base_dir,
    resolve_tile_dir,
)
from . import minio_cache

DEFAULT_HANSEN_URL_TEMPLATE = (
    "https://storage.googleapis.com/earthenginepartners-hansen/"
    "GFC-2024-v1.12/Hansen_GFC-2024-v1.12_{layer}_{url_tile_id}.tif"
)


try:
    from .hansen_tiles import hansen_tile_ids_for_bbox, load_aoi_bbox
except Exception:  # pragma: no cover - fallback for import errors
    hansen_tile_ids_for_bbox = None
    load_aoi_bbox = None


def _ensure_default_url_template() -> str:
    url_template = os.environ.get(HANSEN_URL_TEMPLATE_ENV, "").strip()
    if not url_template:
        os.environ[HANSEN_URL_TEMPLATE_ENV] = DEFAULT_HANSEN_URL_TEMPLATE
        return DEFAULT_HANSEN_URL_TEMPLATE
    return url_template


def _resolve_tile_ids(aoi_geojson_path: Path) -> list[str]:
    if load_aoi_bbox is None or hansen_tile_ids_for_bbox is None:
        raise RuntimeError("Hansen tile utilities are unavailable")
    bbox = load_aoi_bbox(aoi_geojson_path)
    tile_ids = hansen_tile_ids_for_bbox(bbox)
    if not tile_ids:
        raise ValueError("No Hansen tiles intersect AOI bbox")
    return tile_ids


def _manifest_path_for_aoi(aoi_id: str) -> Path:
    return (
        data_plane.external_root()
        / "hansen"
        / HANSEN_BASE_DIR_NAME
        / "manifests"
        / aoi_id
        / "tiles_manifest.json"
    )


def _minio_env() -> tuple[str, str, str, str]:
    endpoint = os.environ.get("MINIO_ENDPOINT", "").strip()
    access_key = os.environ.get("MINIO_ACCESS_KEY", "").strip()
    secret_key = os.environ.get("MINIO_SECRET_KEY", "").strip()
    bucket = os.environ.get("MINIO_BUCKET", "").strip()
    if not endpoint or not access_key or not secret_key or not bucket:
        raise RuntimeError("Missing MINIO_* env vars (MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET)")
    return endpoint, access_key, secret_key, bucket


def _cache_key(tile_id: str, layer: str) -> str:
    return f"deps/hansen/gfc_2024_v1_12/tiles/{tile_id}/{layer}.tif"


def _manifest_key(aoi_id: str) -> str:
    return f"deps/hansen/gfc_2024_v1_12/manifests/{aoi_id}/tiles_manifest.json"


def _entry_from_local(tile_id: str, layer: str, local_path: Path, *, status: str, source_url: str) -> HansenLayerEntry:
    return HansenLayerEntry(
        tile_id=tile_id,
        layer=layer,
        local_path=str(local_path.resolve()),
        sha256=sha256_file(local_path),
        size_bytes=local_path.stat().st_size,
        source_url=source_url,
        status=status,
    )


def ensure_hansen_for_aoi(
    *,
    aoi_id: str,
    aoi_geojson_path: Path,
    layers: Iterable[str],
    download: bool,
    minio_cache_enabled: bool = False,
    offline: bool = False,
) -> Path:
    if not aoi_id.strip():
        raise ValueError("aoi_id must be non-empty")
    if not aoi_geojson_path.is_file():
        raise FileNotFoundError(f"AOI GeoJSON not found: {aoi_geojson_path}")

    tile_ids = _resolve_tile_ids(aoi_geojson_path)
    layers_list = sorted({layer.strip() for layer in layers if layer.strip()})
    if not layers_list:
        raise ValueError("At least one layer is required")

    url_template = _ensure_default_url_template()

    endpoint = access_key = secret_key = bucket = ""
    if minio_cache_enabled:
        endpoint, access_key, secret_key, bucket = _minio_env()
        minio_cache.ensure_bucket(endpoint, access_key, secret_key, bucket)

    entries: list[HansenLayerEntry] = []
    effective_download = download and not offline

    for tile_id in tile_ids:
        missing_layers: list[str] = []
        if minio_cache_enabled:
            for layer in layers_list:
                local_path = resolve_tile_dir(tile_id) / f"{layer}.tif"
                source_url = url_template.format(layer=layer, tile_id=tile_id)
                key = _cache_key(tile_id, layer)
                # get_file_if_exists handles all three cases:
                #   1. Local file present and SHA-256 matches stored metadata → skip download.
                #   2. Local file present but SHA-256 mismatch → delete and re-download.
                #   3. Local file absent → download from MinIO.
                if minio_cache.get_file_if_exists(bucket, key, local_path):
                    entries.append(
                        _entry_from_local(
                            tile_id,
                            layer,
                            local_path,
                            status="cached",
                            source_url=source_url,
                        )
                    )
                    continue
                missing_layers.append(layer)
        else:
            missing_layers = list(layers_list)

        if missing_layers:
            if not effective_download:
                raise RuntimeError(
                    f"Missing Hansen tiles for {tile_id} (offline/minio-only mode enabled)."
                )
            downloaded_entries = ensure_hansen_layers_present(
                tile_id, missing_layers, download=effective_download
            )
            entries.extend(downloaded_entries)
            if minio_cache_enabled:
                for entry in downloaded_entries:
                    if entry.status in {"downloaded", "present"}:
                        key = _cache_key(entry.tile_id, entry.layer)
                        minio_cache.put_file(
                            bucket,
                            key,
                            Path(entry.local_path),
                            content_type="image/tiff",
                        )

    ordered_entries = sorted(
        entries, key=lambda e: (e.tile_id, e.layer, e.local_path)
    )

    manifest_path = _manifest_path_for_aoi(aoi_id)
    payload = {
        "schema_version": "v1",
        "dataset_version": DATASET_VERSION_DEFAULT,
        "aoi_id": aoi_id,
        "tile_ids": tile_ids,
        "layers": layers_list,
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
    if minio_cache_enabled:
        minio_cache.put_file(
            bucket,
            _manifest_key(aoi_id),
            manifest_path,
            content_type="application/json",
        )
    return manifest_path


def hansen_tiles_root() -> Path:
    return hansen_default_base_dir() / "tiles"
