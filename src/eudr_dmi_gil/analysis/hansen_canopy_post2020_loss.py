"""Second, parallel forest-baseline analysis: Hansen `treecover2000 >= canopy_threshold`.

`jrc_post2020_loss.py` computes post-2020 forest loss against a single baseline: JRC/GFC2020's
categorical `Map == 1` (a strict closed-canopy definition). This module computes the same class of
metrics against a different baseline: Hansen `treecover2000 >= canopy_threshold` (default 10%),
excluding any pixel with Hansen-detected loss before 2021 -- the FAO forest definition referenced by
EUDR Article 2 (land with >10% canopy cover), not JRC's stricter criterion. The two baselines can
disagree materially on the same AOI: a real post-2020 loss/commodity overlap can be present under
the Hansen canopy baseline while reading as zero under the JRC baseline, because JRC does not
classify the disturbed area as forest at all. Both baselines are reported side by side rather than
choosing one, per this repo's provable-vs-conjecture evidentiary stance.

Reuses `jrc_post2020_loss.py`'s private grid/reprojection/mask-write helpers, the same way
`commodities/analysis.py` already does.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from eudr_dmi_gil.analysis.jrc_post2020_loss import (
    LossDatasetMetadata,
    _load_aoi_geometry,
    _percent,
    _rasterize_aoi,
    _reproject_categorical,
    _round6,
    _target_grid,
    _write_mask_geojson,
)
from eudr_dmi_gil.commodities.config import CommodityConfig
from eudr_dmi_gil.commodities.providers import provider_for_config
from eudr_dmi_gil.providers.baseline import BaselineProviderMetadata
from eudr_dmi_gil.reports.determinism import write_json


@dataclass(frozen=True)
class HansenCanopyPost2020Metrics:
    forest_baseline_hansen10pct_2000_ha: float
    forest_baseline_hansen10pct_2000_percent_of_aoi: float
    forest_loss_post_2020_on_hansen10pct_baseline_ha: float
    forest_loss_post_2020_on_hansen10pct_baseline_percent_of_aoi: float
    forest_loss_post_2020_percent_of_hansen10pct_baseline: float
    hansen10pct_baseline_forest_without_detected_loss_ha: float
    hansen10pct_baseline_canopy_threshold_percent: int

    def to_metric_rows(self) -> dict[str, dict[str, Any]]:
        return {
            "forest_baseline_hansen10pct_2000_ha": {
                "value": self.forest_baseline_hansen10pct_2000_ha,
                "unit": "ha",
                "notes": "AOI & Hansen treecover2000 >= threshold & no Hansen loss in 2001-2020",
            },
            "forest_baseline_hansen10pct_2000_percent_of_aoi": {
                "value": self.forest_baseline_hansen10pct_2000_percent_of_aoi,
                "unit": "percent",
            },
            "forest_loss_post_2020_on_hansen10pct_baseline_ha": {
                "value": self.forest_loss_post_2020_on_hansen10pct_baseline_ha,
                "unit": "ha",
                "notes": "AOI & Hansen treecover2000 canopy baseline & Hansen lossyear range",
            },
            "forest_loss_post_2020_on_hansen10pct_baseline_percent_of_aoi": {
                "value": self.forest_loss_post_2020_on_hansen10pct_baseline_percent_of_aoi,
                "unit": "percent",
            },
            "forest_loss_post_2020_percent_of_hansen10pct_baseline": {
                "value": self.forest_loss_post_2020_percent_of_hansen10pct_baseline,
                "unit": "percent",
            },
            "hansen10pct_baseline_forest_without_detected_loss_ha": {
                "value": self.hansen10pct_baseline_forest_without_detected_loss_ha,
                "unit": "ha",
            },
            "hansen10pct_baseline_canopy_threshold_percent": {
                "value": self.hansen10pct_baseline_canopy_threshold_percent,
                "unit": "percent",
                "notes": "canopy-cover threshold defining the Hansen forest baseline (FAO/EUDR Art.2 definition)",
            },
        }


@dataclass(frozen=True)
class HansenCanopyPost2020AnalysisResult:
    metrics: HansenCanopyPost2020Metrics
    baseline_metadata: BaselineProviderMetadata
    loss_metadata: LossDatasetMetadata
    summary_path: Path
    baseline_mask_path: Path
    loss_mask_path: Path
    debug_path: Path
    evidence_gaps: list[dict[str, Any]]
    grid: dict[str, Any]
    baseline_forest_mask: np.ndarray
    loss_on_baseline_mask: np.ndarray
    target_transform: Any
    target_crs: str


def compute_hansen_canopy_post2020_loss(
    *,
    aoi_geojson_path: Path,
    hansen_treecover_raster_path: Path,
    hansen_lossyear_raster_path: Path,
    output_dir: Path,
    baseline_metadata: BaselineProviderMetadata,
    loss_metadata: LossDatasetMetadata,
    requested_end_year: int,
    canopy_threshold_percent: int = 10,
    target_crs: str = "EPSG:6933",
    target_resolution_m: float = 30.0,
    all_touched: bool = True,
) -> HansenCanopyPost2020AnalysisResult:
    if requested_end_year < 2021:
        raise ValueError("end_year must be >= 2021")
    if loss_metadata.latest_available_year < 2021:
        raise ValueError("Loss dataset temporal coverage must include 2021 or later")

    effective_end_year = min(requested_end_year, loss_metadata.latest_available_year)
    end_code = effective_end_year - 2000

    aoi_geom_wgs84 = _load_aoi_geometry(aoi_geojson_path)
    target_transform, width, height, target_bounds = _target_grid(
        aoi_geom_wgs84,
        target_crs=target_crs,
        resolution=target_resolution_m,
    )
    zone_mask = _rasterize_aoi(
        aoi_geom_wgs84,
        target_crs=target_crs,
        out_shape=(height, width),
        transform=target_transform,
        all_touched=all_touched,
    )

    treecover_values, treecover_valid, treecover_info = _reproject_categorical(
        hansen_treecover_raster_path,
        target_crs=target_crs,
        target_transform=target_transform,
        width=width,
        height=height,
    )
    loss_values, loss_valid, loss_info = _reproject_categorical(
        hansen_lossyear_raster_path,
        target_crs=target_crs,
        target_transform=target_transform,
        width=width,
        height=height,
    )

    pixel_area_ha = abs(float(target_transform.a) * float(target_transform.e)) / 10_000.0
    aoi_area_ha = _round6(float(np.count_nonzero(zone_mask)) * pixel_area_ha)

    baseline_nodata = zone_mask & (~treecover_valid)
    loss_nodata = zone_mask & (~loss_valid)
    pre2021_loss = loss_valid & (loss_values >= 1) & (loss_values <= 20)
    baseline_forest = (
        zone_mask
        & treecover_valid
        & (treecover_values >= canopy_threshold_percent)
        & (~pre2021_loss)
    )
    loss_range = zone_mask & loss_valid & (loss_values >= 21) & (loss_values <= end_code)
    loss_on_baseline = baseline_forest & loss_range

    baseline_area = _round6(float(np.count_nonzero(baseline_forest)) * pixel_area_ha)
    loss_area = _round6(float(np.count_nonzero(loss_on_baseline)) * pixel_area_ha)
    baseline_without_loss = _round6(max(baseline_area - loss_area, 0.0))

    metrics = HansenCanopyPost2020Metrics(
        forest_baseline_hansen10pct_2000_ha=baseline_area,
        forest_baseline_hansen10pct_2000_percent_of_aoi=_percent(baseline_area, aoi_area_ha),
        forest_loss_post_2020_on_hansen10pct_baseline_ha=loss_area,
        forest_loss_post_2020_on_hansen10pct_baseline_percent_of_aoi=_percent(loss_area, aoi_area_ha),
        forest_loss_post_2020_percent_of_hansen10pct_baseline=_percent(loss_area, baseline_area),
        hansen10pct_baseline_forest_without_detected_loss_ha=baseline_without_loss,
        hansen10pct_baseline_canopy_threshold_percent=canopy_threshold_percent,
    )

    evidence_gaps = _hansen_canopy_evidence_gaps(
        requested_end_year=requested_end_year,
        effective_end_year=effective_end_year,
        baseline_nodata_pixels=int(np.count_nonzero(baseline_nodata)),
        loss_nodata_pixels=int(np.count_nonzero(loss_nodata)),
        aoi_pixels=int(np.count_nonzero(zone_mask)),
        baseline_info=treecover_info,
        loss_info=loss_info,
        baseline_metadata=baseline_metadata,
        loss_metadata=loss_metadata,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"2021_{effective_end_year}"
    baseline_mask_path = output_dir / "hansen10pct_forest_2000_mask.geojson"
    loss_mask_path = output_dir / f"forest_loss_{suffix}_on_hansen10pct_forest_2000_mask.geojson"
    debug_path = output_dir / "hansen_canopy_post2020_loss_debug.json"
    summary_path = output_dir / f"hansen_canopy_post2020_loss_{suffix}_summary.json"

    _write_mask_geojson(baseline_mask_path, baseline_forest, target_transform, target_crs)
    _write_mask_geojson(loss_mask_path, loss_on_baseline, target_transform, target_crs)

    grid = {
        "target_crs": target_crs,
        "target_resolution_m": target_resolution_m,
        "target_bounds": {
            "left": target_bounds[0],
            "bottom": target_bounds[1],
            "right": target_bounds[2],
            "top": target_bounds[3],
        },
        "width": width,
        "height": height,
        "pixel_area_ha": _round6(pixel_area_ha),
        "resampling": "nearest",
        "boundary_rule": "rasterize_polygon_all_touched"
        if all_touched
        else "rasterize_polygon_pixel_center",
    }

    debug = {
        "grid": grid,
        "canopy_threshold_percent": canopy_threshold_percent,
        "mask_true_pixels": {
            "aoi": int(np.count_nonzero(zone_mask)),
            "hansen10pct_forest_2000": int(np.count_nonzero(baseline_forest)),
            "loss_on_hansen10pct_forest_2000": int(np.count_nonzero(loss_on_baseline)),
        },
        "nodata_pixels_inside_aoi": {
            "hansen_treecover2000": int(np.count_nonzero(baseline_nodata)),
            "hansen_lossyear": int(np.count_nonzero(loss_nodata)),
        },
        "source_alignment": {
            "baseline": treecover_info,
            "loss": loss_info,
        },
    }
    write_json(debug_path, debug)

    summary = {
        "analysis_id": "post_2020_loss_on_hansen_treecover2000_baseline_v1",
        "formula": (
            f"AOI AND hansen_treecover2000 >= {canopy_threshold_percent} AND NOT hansen_lossyear in [1,20] "
            "AND hansen_lossyear >= 21 AND hansen_lossyear <= effective_end_year - 2000"
        ),
        "metrics": metrics.to_metric_rows(),
        "baseline_provider": baseline_metadata.to_dict(),
        "loss_dataset": loss_metadata.to_dict(),
        "evidence_gaps": evidence_gaps,
        "grid": grid,
        "artifacts": {
            "baseline_mask": baseline_mask_path.name,
            "loss_mask": loss_mask_path.name,
            "debug": debug_path.name,
        },
    }
    write_json(summary_path, summary)

    return HansenCanopyPost2020AnalysisResult(
        metrics=metrics,
        baseline_metadata=baseline_metadata,
        loss_metadata=loss_metadata,
        summary_path=summary_path,
        baseline_mask_path=baseline_mask_path,
        loss_mask_path=loss_mask_path,
        debug_path=debug_path,
        evidence_gaps=evidence_gaps,
        grid=grid,
        baseline_forest_mask=baseline_forest,
        loss_on_baseline_mask=loss_on_baseline,
        target_transform=target_transform,
        target_crs=target_crs,
    )


@dataclass(frozen=True)
class HansenCanopyCommodityOverlapMetrics:
    baseline_forest_and_commodity_overlap_hansen10pct_baseline_ha: float | None
    forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_ha: float | None
    forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_percent_of_aoi: float | None
    forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_percent_of_loss: float | None
    overlap_mask_path: Path

    def to_metric_rows(self) -> dict[str, dict[str, Any]]:
        return {
            "baseline_forest_and_commodity_overlap_hansen10pct_baseline_ha": {
                "value": self.baseline_forest_and_commodity_overlap_hansen10pct_baseline_ha,
                "unit": "ha",
                "notes": "Hansen treecover2000 canopy forest baseline intersected with observed commodity layer",
            },
            "forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_ha": {
                "value": self.forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_ha,
                "unit": "ha",
                "notes": (
                    "post-2020 Hansen loss inside the Hansen treecover2000 canopy forest baseline "
                    "intersected with commodity layer"
                ),
            },
            "forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_percent_of_aoi": {
                "value": self.forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_percent_of_aoi,
                "unit": "percent",
            },
            "forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_percent_of_loss": {
                "value": self.forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_percent_of_loss,
                "unit": "percent",
            },
        }


def compute_hansen_canopy_commodity_overlap(
    *,
    config: CommodityConfig,
    aoi_geojson_path: Path,
    aoi_country: str | None,
    hansen_canopy_result: HansenCanopyPost2020AnalysisResult,
    output_dir: Path,
) -> HansenCanopyCommodityOverlapMetrics | None:
    """Intersect an already-computed Hansen-canopy loss mask with the configured commodity layer.

    Re-derives the commodity provider's aligned mask (cheap relative to raster reprojection) on
    the same grid `hansen_canopy_result` was computed on, rather than changing
    `commodities.analysis.run_commodity_assessment`'s existing return contract to expose its
    internal mask array. Returns `None` when the commodity provider has no evidence available for
    this AOI (out-of-scope country, missing layer, etc.) -- mirroring
    `run_commodity_assessment`'s own "unavailable" handling.
    """
    provider = provider_for_config(config)
    if provider.validate_country_scope(aoi_country):
        return None

    target_transform = hansen_canopy_result.target_transform
    target_crs = hansen_canopy_result.target_crs
    baseline_forest = hansen_canopy_result.baseline_forest_mask
    loss_on_baseline = hansen_canopy_result.loss_on_baseline_mask
    height, width = baseline_forest.shape

    aoi_geom_wgs84 = _load_aoi_geometry(aoi_geojson_path)
    aoi_mask = _rasterize_aoi(
        aoi_geom_wgs84,
        target_crs=target_crs,
        out_shape=(height, width),
        transform=target_transform,
        all_touched=True,
    )
    commodity = provider.aligned_mask(
        target_crs=target_crs,
        target_transform=target_transform,
        width=width,
        height=height,
        aoi_mask=aoi_mask,
    )
    if not commodity.evidence_available or commodity.mask is None:
        return None

    pixel_area_ha = abs(float(target_transform.a) * float(target_transform.e)) / 10_000.0
    aoi_area_ha = _round6(float(np.count_nonzero(aoi_mask)) * pixel_area_ha)
    commodity_mask = commodity.mask
    baseline_and_commodity = baseline_forest & commodity_mask
    loss_and_commodity = loss_on_baseline & commodity_mask

    loss_area = _round6(float(np.count_nonzero(loss_on_baseline)) * pixel_area_ha)
    overlap_area = _round6(float(np.count_nonzero(loss_and_commodity)) * pixel_area_ha)
    baseline_overlap_area = _round6(float(np.count_nonzero(baseline_and_commodity)) * pixel_area_ha)

    output_dir.mkdir(parents=True, exist_ok=True)
    overlap_mask_path = output_dir / f"{config.id}_hansen10pct_overlap_mask.geojson"
    _write_mask_geojson(overlap_mask_path, loss_and_commodity, target_transform, target_crs)

    return HansenCanopyCommodityOverlapMetrics(
        baseline_forest_and_commodity_overlap_hansen10pct_baseline_ha=baseline_overlap_area,
        forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_ha=overlap_area,
        forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_percent_of_aoi=_percent(
            overlap_area, aoi_area_ha
        ),
        forest_loss_post_2020_and_commodity_overlap_hansen10pct_baseline_percent_of_loss=_percent(
            overlap_area, loss_area
        ),
        overlap_mask_path=overlap_mask_path,
    )


def _hansen_canopy_evidence_gaps(
    *,
    requested_end_year: int,
    effective_end_year: int,
    baseline_nodata_pixels: int,
    loss_nodata_pixels: int,
    aoi_pixels: int,
    baseline_info: dict[str, Any],
    loss_info: dict[str, Any],
    baseline_metadata: BaselineProviderMetadata,
    loss_metadata: LossDatasetMetadata,
) -> list[dict[str, Any]]:
    """Same shape as `jrc_post2020_loss._evidence_gaps`, with codes naming the Hansen
    treecover2000 baseline instead of JRC -- reusing that function's codes here would mislabel
    a treecover2000 nodata gap as a JRC nodata gap.
    """
    gaps: list[dict[str, Any]] = []
    if requested_end_year != effective_end_year:
        gaps.append(
            {
                "code": "requested_end_year_not_available",
                "severity": "warning",
                "message": (
                    f"Requested end_year {requested_end_year} exceeds selected loss "
                    f"dataset coverage through {loss_metadata.latest_available_year}; "
                    f"effective_end_year is {effective_end_year}."
                ),
                "requested_end_year": requested_end_year,
                "effective_end_year": effective_end_year,
                "latest_available_year": loss_metadata.latest_available_year,
            }
        )
    if baseline_nodata_pixels:
        gaps.append(
            {
                "code": "hansen_treecover2000_nodata_inside_aoi",
                "severity": "warning",
                "message": (
                    "Hansen treecover2000 nodata pixels inside AOI were excluded from the "
                    "canopy-cover forest baseline metrics."
                ),
                "pixels": baseline_nodata_pixels,
                "aoi_pixels": aoi_pixels,
            }
        )
    if loss_nodata_pixels:
        gaps.append(
            {
                "code": "hansen_nodata_inside_aoi",
                "severity": "warning",
                "message": "Hansen lossyear nodata pixels inside AOI were excluded from loss metrics.",
                "pixels": loss_nodata_pixels,
                "aoi_pixels": aoi_pixels,
            }
        )
    if baseline_info.get("grid_alignment_transformation_applied") or loss_info.get(
        "grid_alignment_transformation_applied"
    ):
        gaps.append(
            {
                "code": "grid_alignment_transformation_applied",
                "severity": "info",
                "message": (
                    "Categorical rasters were aligned to the deterministic target grid "
                    "with nearest-neighbour resampling."
                ),
                "target_crs": baseline_info.get("target_crs"),
            }
        )
    if baseline_metadata.checksum is None:
        gaps.append(
            {
                "code": "baseline_checksum_missing",
                "severity": "warning",
                "message": "Checksum or source fingerprint is missing for the baseline raster.",
            }
        )
    if loss_metadata.checksum is None:
        gaps.append(
            {
                "code": "loss_checksum_missing",
                "severity": "warning",
                "message": "Checksum or source fingerprint is missing for the loss raster.",
            }
        )
    return gaps
