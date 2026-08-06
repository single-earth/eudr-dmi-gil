from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import rasterio

from eudr_dmi_gil.reports.bundle import compute_sha256

from .baseline import BaselineProviderMetadata

HANSEN_TREECOVER2000_DATASET_ID = "hansen_treecover2000"
HANSEN_TREECOVER2000_TITLE = "Hansen Global Forest Change treecover2000"
HANSEN_TREECOVER2000_ASSET_ID_PREFIX = "UMD/hansen/global_forest_change_"
HANSEN_TREECOVER2000_SOURCE_URL = "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2024-v1.12/download.html"
HANSEN_TREECOVER2000_CUTOFF_DATE = "2000-12-31"
HANSEN_TREECOVER2000_BAND = "treecover2000"


class LocalHansenTreecoverProvider:
    """Hansen treecover2000 baseline provider backed by a local raster.

    Mirrors `providers.jrc_gfc2020.LocalJrcGfc2020Provider`: the Earth Engine asset id is
    recorded as catalogue metadata, analysis consumes a downloaded local raster. Unlike JRC's
    categorical `Map == forest_value` baseline, this dataset's forest baseline is defined by a
    canopy-cover threshold (`treecover2000 >= forest_value`), matching the FAO/EUDR Article 2
    forest definition (>10% canopy cover) rather than JRC's stricter closed-canopy criterion.
    `forest_value` here holds that threshold (percent), not an equality value.
    """

    def __init__(
        self,
        raster_path: str | Path,
        *,
        canopy_threshold_percent: int,
        dataset_version: str,
        processed_at_utc: str | None = None,
        source_fingerprint: str | None = None,
    ) -> None:
        self._raster_path = Path(raster_path)
        self._canopy_threshold_percent = canopy_threshold_percent
        self._dataset_version = dataset_version
        self._processed_at_utc = processed_at_utc or _utc_now_iso()
        self._source_fingerprint = source_fingerprint

    def raster_path(self) -> Path:
        return self._raster_path

    def metadata(self) -> BaselineProviderMetadata:
        checksum = compute_sha256(self._raster_path) if self._raster_path.is_file() else None
        native_crs: str | None = None
        resolution: int | float | None = 30
        if self._raster_path.is_file():
            with rasterio.open(self._raster_path) as ds:
                native_crs = ds.crs.to_string() if ds.crs is not None else None
                if ds.res:
                    resolution = max(abs(float(ds.res[0])), abs(float(ds.res[1])))

        fingerprint = self._source_fingerprint or checksum
        asset_identifier = (
            HANSEN_TREECOVER2000_ASSET_ID_PREFIX
            + self._dataset_version.replace("-", "_").replace(".", "_")
        )
        return BaselineProviderMetadata(
            provider_id="local_hansen_treecover2000",
            dataset_title=HANSEN_TREECOVER2000_TITLE,
            dataset_id=HANSEN_TREECOVER2000_DATASET_ID,
            asset_identifier=asset_identifier,
            dataset_version=self._dataset_version,
            source_url=HANSEN_TREECOVER2000_SOURCE_URL,
            cutoff_date=HANSEN_TREECOVER2000_CUTOFF_DATE,
            band=HANSEN_TREECOVER2000_BAND,
            forest_value=self._canopy_threshold_percent,
            spatial_resolution_m=resolution,
            native_crs=native_crs,
            retrieved_or_processed_at_utc=self._processed_at_utc,
            checksum=checksum,
            source_fingerprint=fingerprint,
            local_path=self._raster_path.as_posix(),
        )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
