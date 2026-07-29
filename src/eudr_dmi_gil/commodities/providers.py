from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import rasterio

from eudr_dmi_gil.reports.bundle import compute_sha256

from .config import CommodityConfig


@dataclass(frozen=True)
class CommodityProviderMetadata:
    commodity_id: str
    display_name: str
    provider_id: str
    dataset_title: str
    dataset_id: str
    dataset_version: str
    asset_identifier: str
    source_url: str | None
    observation_year: int
    class_values: tuple[int, ...]
    class_labels: tuple[str, ...]
    country_scope: tuple[str, ...]
    local_path: str | None
    checksum: str | None

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["class_values"] = list(self.class_values)
        out["class_labels"] = list(self.class_labels)
        out["country_scope"] = list(self.country_scope)
        return out


@dataclass(frozen=True)
class AlignedCommodityMask:
    mask: np.ndarray | None
    valid: np.ndarray | None
    values: np.ndarray | None
    metadata: CommodityProviderMetadata
    coverage_status: str
    nodata: dict[str, Any]
    provenance: dict[str, Any]
    evidence_gaps: list[dict[str, Any]]
    evidence_available: bool


class CommodityProvider(Protocol):
    def metadata(self) -> CommodityProviderMetadata:
        raise NotImplementedError

    def aligned_mask(
        self,
        *,
        target_crs: str,
        target_transform: rasterio.Affine,
        width: int,
        height: int,
        aoi_mask: np.ndarray,
    ) -> AlignedCommodityMask:
        raise NotImplementedError

    def validate_country_scope(self, aoi_country: str | None) -> list[dict[str, Any]]:
        raise NotImplementedError


class MapBiomasBrazilCommodityProvider:
    def __init__(self, config: CommodityConfig) -> None:
        self.config = config

    def metadata(self) -> CommodityProviderMetadata:
        raster_path = _source_path_if_local(self.config.raster_source)
        checksum = compute_sha256(raster_path) if raster_path is not None and raster_path.is_file() else None
        return CommodityProviderMetadata(
            commodity_id=self.config.id,
            display_name=self.config.display_name,
            provider_id=self.config.provider,
            dataset_title=self.config.dataset_title,
            dataset_id=_dataset_id(self.config.provider, self.config.dataset_version),
            dataset_version=self.config.dataset_version,
            asset_identifier=self.config.asset_id,
            source_url=self.config.source_url,
            observation_year=self.config.observation_year,
            class_values=self.config.class_values,
            class_labels=self.config.class_labels,
            country_scope=self.config.country_scope,
            local_path=raster_path.as_posix() if raster_path is not None else None,
            checksum=checksum,
        )

    def validate_country_scope(self, aoi_country: str | None) -> list[dict[str, Any]]:
        if not aoi_country:
            return [
                {
                    "code": "aoi_country_unavailable",
                    "severity": "warning",
                    "message": (
                        "AOI country was not supplied in structured AOI properties; "
                        "commodity provider geographic compatibility cannot be validated."
                    ),
                }
            ]
        supported = {c.casefold() for c in self.config.country_scope}
        if aoi_country.casefold() not in supported:
            return [
                {
                    "code": "commodity_provider_unsupported_country",
                    "severity": "warning",
                    "message": (
                        f"{self.config.provider} is configured for "
                        f"{', '.join(self.config.country_scope)}, not {aoi_country}."
                    ),
                    "aoi_country": aoi_country,
                    "country_scope": list(self.config.country_scope),
                }
            ]
        return []

    def aligned_mask(
        self,
        *,
        target_crs: str,
        target_transform: rasterio.Affine,
        width: int,
        height: int,
        aoi_mask: np.ndarray,
    ) -> AlignedCommodityMask:
        from eudr_dmi_gil.analysis.jrc_post2020_loss import _reproject_categorical

        metadata = self.metadata()
        gaps: list[dict[str, Any]] = []
        raster_path = _source_path_if_local(self.config.raster_source)
        if raster_path is None or not raster_path.is_file():
            gaps.append(
                {
                    "code": "commodity_layer_unavailable",
                    "severity": "warning",
                    "message": "Configured commodity raster source is not available locally.",
                    "asset_id": self.config.asset_id,
                    "local_path": self.config.local_path,
                }
            )
            return AlignedCommodityMask(
                mask=None,
                valid=None,
                values=None,
                metadata=metadata,
                coverage_status="unavailable",
                nodata={"aoi_pixels": int(np.count_nonzero(aoi_mask)), "nodata_pixels": None},
                provenance={"source": self.config.raster_source, "checksum": None},
                evidence_gaps=gaps,
                evidence_available=False,
            )

        _validate_classes_exist(raster_path, self.config.class_values)
        values, valid, info = _reproject_categorical(
            raster_path,
            target_crs=target_crs,
            target_transform=target_transform,
            width=width,
            height=height,
        )
        aoi_pixels = int(np.count_nonzero(aoi_mask))
        valid_inside_aoi = aoi_mask & valid
        valid_pixels = int(np.count_nonzero(valid_inside_aoi))
        nodata_pixels = int(np.count_nonzero(aoi_mask & (~valid)))
        if valid_pixels == 0:
            gaps.append(
                {
                    "code": "commodity_layer_no_coverage_inside_aoi",
                    "severity": "warning",
                    "message": "Commodity raster has no valid pixels inside the AOI.",
                    "aoi_pixels": aoi_pixels,
                }
            )
            coverage_status = "unavailable"
            evidence_available = False
        elif nodata_pixels:
            gaps.append(
                {
                    "code": "commodity_layer_partial_coverage",
                    "severity": "warning",
                    "message": "Commodity raster has nodata pixels inside the AOI.",
                    "aoi_pixels": aoi_pixels,
                    "nodata_pixels": nodata_pixels,
                }
            )
            coverage_status = "partial"
            evidence_available = True
        else:
            coverage_status = "full"
            evidence_available = True

        class_values = np.array(self.config.class_values, dtype=values.dtype)
        mask = aoi_mask & valid & np.isin(values, class_values)
        return AlignedCommodityMask(
            mask=mask,
            valid=valid,
            values=values,
            metadata=metadata,
            coverage_status=coverage_status,
            nodata={
                "aoi_pixels": aoi_pixels,
                "valid_pixels": valid_pixels,
                "nodata_pixels": nodata_pixels,
            },
            provenance={
                "source": self.config.raster_source,
                "checksum": compute_sha256(raster_path),
                "alignment": info,
            },
            evidence_gaps=gaps,
            evidence_available=evidence_available,
        )


def provider_for_config(config: CommodityConfig) -> CommodityProvider:
    if config.provider != "mapbiomas_brazil":
        raise ValueError(f"Unsupported commodity provider: {config.provider}")
    if config.id != "coffee":
        raise ValueError(
            "mapbiomas_brazil provider currently supports the configured coffee implementation only"
        )
    return MapBiomasBrazilCommodityProvider(config)


def _dataset_id(provider: str, version: str) -> str:
    safe_version = version.strip().lower().replace(" ", "_").replace(".", "_").replace("-", "_")
    return f"{provider}_{safe_version}" if safe_version else provider


def _source_path_if_local(source: str) -> Path | None:
    if not source:
        return None
    path = Path(source)
    return path if path.is_file() or path.parent.exists() else None


def _validate_classes_exist(raster_path: Path, class_values: tuple[int, ...]) -> None:
    with rasterio.open(raster_path) as ds:
        band = ds.read(1, masked=True)
    values = np.unique(np.ma.compressed(band))
    present = {int(v) for v in values.tolist()}
    missing = [value for value in class_values if int(value) not in present]
    if missing:
        raise ValueError(
            "Configured commodity class value(s) missing from raster "
            f"{raster_path}: {json.dumps(missing)}"
        )
