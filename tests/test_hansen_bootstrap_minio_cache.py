from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from eudr_dmi_gil.deps import hansen_acquire
from eudr_dmi_gil.deps.hansen_bootstrap import ensure_hansen_for_aoi
from eudr_dmi_gil.deps import minio_cache


def _fake_urlopen_factory(payload: bytes):
    class _Response:
        def __init__(self, data: bytes) -> None:
            self._buf = io.BytesIO(data)

        def read(self, size: int = -1) -> bytes:
            return self._buf.read(size)

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def _fake_urlopen(url: str):  # noqa: ANN001
        return _Response(payload)

    return _fake_urlopen


def _write_aoi_geojson(path: Path) -> None:
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [20.1, 50.1],
                            [20.9, 50.1],
                            [20.9, 50.9],
                            [20.1, 50.9],
                            [20.1, 50.1],
                        ]
                    ],
                },
            }
        ],
    }
    path.write_text(json.dumps(geojson), encoding="utf-8")


def test_hansen_bootstrap_minio_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("EUDR_DMI_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv(
        "EUDR_DMI_HANSEN_URL_TEMPLATE",
        "https://storage.googleapis.com/earthenginepartners-hansen/"
        "GFC-2025-v1.13/Hansen_GFC-2025-v1.13_{layer}_{tile_id}.tif",
    )
    monkeypatch.setenv("MINIO_ENDPOINT", "minio.local")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "access")
    monkeypatch.setenv("MINIO_SECRET_KEY", "secret")
    monkeypatch.setenv("MINIO_BUCKET", "cache")

    monkeypatch.setattr(
        hansen_acquire.urllib.request,
        "urlopen",
        _fake_urlopen_factory(b"tile-bytes"),
    )

    calls: dict[str, list[tuple[str, str]]] = {"put": [], "get": [], "bucket": []}

    def _ensure_bucket(endpoint: str, access_key: str, secret_key: str, bucket: str) -> None:
        calls["bucket"].append((endpoint, bucket))

    def _get_file(bucket: str, key: str, dest_path: Path) -> bool:
        calls["get"].append((bucket, key))
        return False

    def _put_file(bucket: str, key: str, local_path: Path, content_type: str | None = None) -> None:
        calls["put"].append((bucket, key))

    monkeypatch.setattr(minio_cache, "ensure_bucket", _ensure_bucket)
    monkeypatch.setattr(minio_cache, "get_file_if_exists", _get_file)
    monkeypatch.setattr(minio_cache, "put_file", _put_file)

    aoi_path = tmp_path / "aoi.geojson"
    _write_aoi_geojson(aoi_path)

    manifest_path = ensure_hansen_for_aoi(
        aoi_id="test_aoi",
        aoi_geojson_path=aoi_path,
        layers=["treecover2000", "lossyear"],
        download=True,
        minio_cache_enabled=True,
        offline=False,
    )

    assert manifest_path.is_file()
    assert calls["bucket"]
    assert any("tiles/N50_E020/lossyear.tif" in key for _, key in calls["put"])
    assert any("tiles/N50_E020/treecover2000.tif" in key for _, key in calls["put"])
    assert any("manifests/test_aoi/tiles_manifest.json" in key for _, key in calls["put"])