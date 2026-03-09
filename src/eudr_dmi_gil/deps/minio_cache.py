from __future__ import annotations

import hashlib
import mimetypes
import os
from pathlib import Path

from minio import Minio
from minio.error import S3Error

_METADATA_SHA256_KEY = "x-amz-meta-sha256"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _client_from_env() -> Minio:
    endpoint = os.environ.get("MINIO_ENDPOINT", "").strip()
    access_key = os.environ.get("MINIO_ACCESS_KEY", "").strip()
    secret_key = os.environ.get("MINIO_SECRET_KEY", "").strip()
    secure_env = os.environ.get("MINIO_SECURE", "true").strip().lower()
    secure = secure_env not in {"0", "false", "no"}

    if not endpoint or not access_key or not secret_key:
        raise RuntimeError("Missing MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY")

    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def ensure_bucket(endpoint: str, access_key: str, secret_key: str, bucket: str) -> None:
    secure_env = os.environ.get("MINIO_SECURE", "true").strip().lower()
    secure = secure_env not in {"0", "false", "no"}
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def put_file(bucket: str, key: str, local_path: Path, content_type: str | None = None) -> None:
    client = _client_from_env()
    if content_type is None:
        content_type, _ = mimetypes.guess_type(str(local_path))
    sha256 = _sha256_file(local_path)
    client.fput_object(
        bucket,
        key,
        str(local_path),
        content_type=content_type,
        metadata={"sha256": sha256},
    )


def get_file_if_exists(bucket: str, key: str, dest_path: Path) -> bool:
    client = _client_from_env()
    try:
        stat = client.stat_object(bucket, key)
    except S3Error as exc:
        if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
            return False
        raise

    stored_sha256 = (stat.metadata or {}).get(_METADATA_SHA256_KEY, "").strip().lower()

    # If a local file already exists, check SHA-256 before downloading.
    if dest_path.is_file():
        if stored_sha256 and _sha256_file(dest_path) == stored_sha256:
            # Local file is intact — skip download.
            return True
        # Local file is stale or corrupted — remove before re-downloading.
        dest_path.unlink()

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    client.fget_object(bucket, key, str(dest_path))

    # Verify integrity of the downloaded file.
    if stored_sha256:
        downloaded_sha256 = _sha256_file(dest_path)
        if downloaded_sha256 != stored_sha256:
            dest_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"SHA-256 mismatch after download for {key}: "
                f"expected {stored_sha256}, got {downloaded_sha256}"
            )

    return True