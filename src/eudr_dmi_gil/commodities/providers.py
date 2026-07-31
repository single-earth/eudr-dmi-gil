from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.transform import array_bounds
from rasterio.warp import reproject

from eudr_dmi_gil.reports.bundle import compute_sha256

from .config import MODE_DISCRETE_CLASSES, MODE_PROBABILITY_THRESHOLD, CommodityConfig


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
    mode: str = MODE_DISCRETE_CLASSES
    probability_band: str | None = None
    threshold: float | None = None
    sensitivity_thresholds: tuple[float, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["class_values"] = list(self.class_values)
        out["class_labels"] = list(self.class_labels)
        out["country_scope"] = list(self.country_scope)
        out["sensitivity_thresholds"] = list(self.sensitivity_thresholds)
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
            mode=self.config.mode,
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


class ProbabilityThresholdCommodityProvider:
    """Continuous probability-surface commodity provider (e.g. FDP coffee probability).

    `mode` (not the `provider` string) selects this implementation: `provider` only records
    provenance (which organization/model produced the raster), while `mode` defines the raster
    semantics (continuous probability vs. discrete class membership).
    """

    RESAMPLING = "bilinear"

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
            mode=self.config.mode,
            probability_band=self.config.probability_band,
            threshold=self.config.threshold,
            sensitivity_thresholds=self.config.sensitivity_thresholds,
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
        metadata = self.metadata()
        gaps: list[dict[str, Any]] = []
        threshold = self.config.threshold
        if threshold is None:
            raise ValueError(
                "probability_threshold commodity provider requires config.threshold to be set"
            )

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

        values, valid, info = _reproject_continuous(
            raster_path,
            target_crs=target_crs,
            target_transform=target_transform,
            width=width,
            height=height,
            resampling=self.RESAMPLING,
        )

        aoi_pixels = int(np.count_nonzero(aoi_mask))
        valid_inside_aoi = aoi_mask & valid
        valid_pixels = int(np.count_nonzero(valid_inside_aoi))
        nodata_pixels = int(np.count_nonzero(aoi_mask & (~valid)))

        if valid_pixels:
            finite_values = values[valid_inside_aoi]
            out_of_range = finite_values[(finite_values < 0.0) | (finite_values > 1.0)]
            if out_of_range.size:
                raise ValueError(
                    "Configured commodity probability raster "
                    f"{raster_path} contains {out_of_range.size} value(s) outside [0, 1] "
                    f"(min={float(finite_values.min())!r}, max={float(finite_values.max())!r}); "
                    "refusing to silently clamp material out-of-range probability values."
                )

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

        candidate_mask = valid_inside_aoi & (values >= threshold)

        probability_profile = None
        if evidence_available:
            probability_profile = _probability_profile(
                values=values,
                valid_inside_aoi=valid_inside_aoi,
                aoi_pixels=aoi_pixels,
                valid_pixels=valid_pixels,
                threshold=threshold,
                sensitivity_thresholds=self.config.sensitivity_thresholds,
            )

        return AlignedCommodityMask(
            mask=candidate_mask,
            valid=valid_inside_aoi,
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
                "probability_band": self.config.probability_band,
                "threshold": threshold,
                "resampling": self.RESAMPLING,
                "probability_profile": probability_profile,
            },
            evidence_gaps=gaps,
            evidence_available=evidence_available,
        )


def provider_for_config(config: CommodityConfig) -> CommodityProvider:
    """Select a provider implementation from `config.mode` (raster semantics).

    `config.provider` is provenance metadata only (which organization/model produced the
    raster) and must not be used as a substitute for mode-based dispatch.
    """
    if config.mode == MODE_PROBABILITY_THRESHOLD:
        return ProbabilityThresholdCommodityProvider(config)
    if config.mode == MODE_DISCRETE_CLASSES:
        if config.provider != "mapbiomas_brazil":
            raise ValueError(
                f"Unsupported commodity provider for discrete_classes mode: {config.provider}"
            )
        if config.id != "coffee":
            raise ValueError(
                "mapbiomas_brazil provider currently supports the configured coffee implementation only"
            )
        return MapBiomasBrazilCommodityProvider(config)
    raise ValueError(f"Unsupported commodity mode: {config.mode}")


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


_CONTINUOUS_RESAMPLING = {
    "bilinear": Resampling.bilinear,
    "average": Resampling.average,
    "nearest": Resampling.nearest,
}


def _reproject_continuous(
    raster_path: Path,
    *,
    target_crs: str,
    target_transform: rasterio.Affine,
    width: int,
    height: int,
    resampling: str = "bilinear",
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    """Reproject a continuous (e.g. probability) raster band, preserving float values.

    Mirrors `eudr_dmi_gil.analysis.jrc_post2020_loss._reproject_categorical`'s alignment/
    provenance shape, but resamples with a continuous-surface method (bilinear/average)
    instead of nearest-neighbor, which is only appropriate for discrete class rasters.
    """
    if resampling not in _CONTINUOUS_RESAMPLING:
        raise ValueError(
            f"Unsupported continuous resampling method: {resampling!r} "
            f"(expected one of {sorted(_CONTINUOUS_RESAMPLING)})"
        )
    nodata_value = float("nan")
    destination = np.full((height, width), nodata_value, dtype=np.float32)
    valid_destination = np.zeros((height, width), dtype=np.uint8)
    with rasterio.open(raster_path) as ds:
        band = ds.read(1, masked=True)
        valid = (~np.ma.getmaskarray(band)).astype(np.uint8)
        values = np.ma.filled(band.astype(np.float32), nodata_value).astype(np.float32)
        reproject(
            source=values,
            destination=destination,
            src_transform=ds.transform,
            src_crs=ds.crs,
            src_nodata=nodata_value,
            dst_transform=target_transform,
            dst_crs=target_crs,
            dst_nodata=nodata_value,
            resampling=_CONTINUOUS_RESAMPLING[resampling],
        )
        reproject(
            source=valid,
            destination=valid_destination,
            src_transform=ds.transform,
            src_crs=ds.crs,
            src_nodata=0,
            dst_transform=target_transform,
            dst_crs=target_crs,
            dst_nodata=0,
            resampling=Resampling.nearest,
        )
        dst_bounds = array_bounds(height, width, target_transform)
        transform_applied = (
            ds.crs is None
            or ds.crs.to_string() != target_crs
            or tuple(round(v, 9) for v in ds.transform[:6])
            != tuple(round(v, 9) for v in target_transform[:6])
            or ds.width != width
            or ds.height != height
        )
        info = {
            "path": raster_path.as_posix(),
            "source_crs": ds.crs.to_string() if ds.crs is not None else None,
            "source_width": ds.width,
            "source_height": ds.height,
            "source_nodata": ds.nodata,
            "target_crs": target_crs,
            "target_width": width,
            "target_height": height,
            "target_bounds": {
                "left": dst_bounds[0],
                "bottom": dst_bounds[1],
                "right": dst_bounds[2],
                "top": dst_bounds[3],
            },
            "resampling": resampling,
            "grid_alignment_transformation_applied": transform_applied,
        }
    valid_bool = valid_destination.astype(bool) & ~np.isnan(destination)
    return destination, valid_bool, info


def _percentile(values: np.ndarray, q: float) -> float:
    return round(float(np.percentile(values, q)), 6)


def _probability_profile(
    *,
    values: np.ndarray,
    valid_inside_aoi: np.ndarray,
    aoi_pixels: int,
    valid_pixels: int,
    threshold: float,
    sensitivity_thresholds: tuple[float, ...],
) -> dict[str, Any]:
    """Probability statistics + threshold sensitivity table over valid AOI pixels.

    `valid_pixels` is the primary denominator for admitted shares (per the reporting
    contract); `aoi_pixels` is also reported so valid coverage of the AOI is explicit.
    """
    finite = values[valid_inside_aoi]
    stats = {
        "min": _percentile(finite, 0),
        "p05": _percentile(finite, 5),
        "p25": _percentile(finite, 25),
        "median": _percentile(finite, 50),
        "p75": _percentile(finite, 75),
        "p95": _percentile(finite, 95),
        "max": _percentile(finite, 100),
        "mean": round(float(np.mean(finite)), 6),
    }

    all_thresholds = sorted({round(float(threshold), 6), *(round(float(t), 6) for t in sensitivity_thresholds)})
    sensitivity: dict[str, Any] = {}
    for t in all_thresholds:
        admitted_pixels = int(np.count_nonzero(valid_inside_aoi & (values >= t)))
        sensitivity[f"{t:.6g}"] = {
            "threshold": t,
            "admitted_pixels": admitted_pixels,
            "admitted_share_of_valid_pixels_percent": (
                round(100.0 * admitted_pixels / valid_pixels, 4) if valid_pixels else None
            ),
            "admitted_share_of_aoi_pixels_percent": (
                round(100.0 * admitted_pixels / aoi_pixels, 4) if aoi_pixels else None
            ),
        }

    return {
        "valid_pixels": valid_pixels,
        "aoi_pixels": aoi_pixels,
        "valid_coverage_of_aoi_percent": (
            round(100.0 * valid_pixels / aoi_pixels, 4) if aoi_pixels else None
        ),
        "stats": stats,
        "configured_threshold": round(float(threshold), 6),
        "sensitivity_thresholds": [round(float(t), 6) for t in sensitivity_thresholds],
        "sensitivity": sensitivity,
    }
