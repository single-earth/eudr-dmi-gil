from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import rasterio

from eudr_dmi_gil.reports.bundle import compute_sha256

from .baseline import BaselineProviderMetadata


JRC_GFC2020_DATASET_ID = "jrc_gfc2020_v3"
JRC_GFC2020_TITLE = "JRC Global Forest Cover 2020"
JRC_GFC2020_ASSET_ID = "JRC/GFC2020/V3"
JRC_GFC2020_VERSION = "V3"
JRC_GFC2020_SOURCE_URL = "https://forobs.jrc.ec.europa.eu/GFC"
JRC_GFC2020_CUTOFF_DATE = "2020-12-31"
JRC_GFC2020_BAND = "Map"
JRC_GFC2020_FOREST_VALUE = 1


class LocalJrcGfc2020Provider:
    """JRC GFC2020 baseline provider backed by a local raster.

    This intentionally avoids adding Google Earth Engine as a runtime dependency.
    The provider records the Earth Engine asset identifier as catalogue metadata,
    while analysis consumes downloaded/provider-supplied raster data.
    """

    def __init__(
        self,
        raster_path: str | Path,
        *,
        processed_at_utc: str | None = None,
        source_fingerprint: str | None = None,
    ) -> None:
        self._raster_path = Path(raster_path)
        self._processed_at_utc = processed_at_utc or _utc_now_iso()
        self._source_fingerprint = source_fingerprint

    def raster_path(self) -> Path:
        return self._raster_path

    def metadata(self) -> BaselineProviderMetadata:
        checksum = compute_sha256(self._raster_path) if self._raster_path.is_file() else None
        native_crs: str | None = None
        resolution: int | float | None = 10
        if self._raster_path.is_file():
            with rasterio.open(self._raster_path) as ds:
                native_crs = ds.crs.to_string() if ds.crs is not None else None
                if ds.res:
                    resolution = max(abs(float(ds.res[0])), abs(float(ds.res[1])))

        fingerprint = self._source_fingerprint or checksum
        return BaselineProviderMetadata(
            provider_id="local_jrc_gfc2020",
            dataset_title=JRC_GFC2020_TITLE,
            dataset_id=JRC_GFC2020_DATASET_ID,
            asset_identifier=JRC_GFC2020_ASSET_ID,
            dataset_version=JRC_GFC2020_VERSION,
            source_url=JRC_GFC2020_SOURCE_URL,
            cutoff_date=JRC_GFC2020_CUTOFF_DATE,
            band=JRC_GFC2020_BAND,
            forest_value=JRC_GFC2020_FOREST_VALUE,
            spatial_resolution_m=resolution,
            native_crs=native_crs,
            retrieved_or_processed_at_utc=self._processed_at_utc,
            checksum=checksum,
            source_fingerprint=fingerprint,
            local_path=self._raster_path.as_posix(),
        )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

