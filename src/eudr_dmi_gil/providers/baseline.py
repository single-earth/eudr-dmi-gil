from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class BaselineProviderMetadata:
    provider_id: str
    dataset_title: str
    dataset_id: str
    asset_identifier: str
    dataset_version: str
    source_url: str
    cutoff_date: str
    band: str
    forest_value: int
    spatial_resolution_m: int | float | None
    native_crs: str | None
    retrieved_or_processed_at_utc: str
    checksum: str | None
    source_fingerprint: str | None
    local_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ForestBaselineProvider(Protocol):
    def metadata(self) -> BaselineProviderMetadata:
        raise NotImplementedError

    def raster_path(self) -> Path:
        raise NotImplementedError

