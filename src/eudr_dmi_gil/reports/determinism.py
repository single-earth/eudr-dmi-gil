from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipInfo, ZipFile


EPOCH_ZIP_DT = (1980, 1, 1, 0, 0, 0)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def canonical_json_bytes(obj: object) -> bytes:
    """Encode JSON deterministically.

    - UTF-8
    - stable key ordering
    - no insignificant whitespace
    """

    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def write_json(path: Path, obj: object) -> None:
    write_bytes(path, canonical_json_bytes(obj) + b"\n")


def create_deterministic_zip(zip_path: Path, files: dict[str, bytes]) -> None:
    """Create a deterministic zip (stable ordering + stable timestamps).

    Note: determinism can still be affected by zip metadata and compression
    implementation differences across Python versions; this function minimizes
    variation in practice by controlling ordering and timestamps.
    """

    zip_path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure stable ordering.
    ordered_items = sorted(files.items(), key=lambda kv: kv[0])

    with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED) as zf:
        for relpath, content in ordered_items:
            info = ZipInfo(relpath)
            info.date_time = EPOCH_ZIP_DT
            info.compress_type = ZIP_DEFLATED
            zf.writestr(info, content)


def file_size_bytes(path: Path) -> int:
    return path.stat().st_size
