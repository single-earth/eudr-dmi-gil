from __future__ import annotations

import json
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
from eudr_dmi_gil.providers.baseline import BaselineProviderMetadata
from eudr_dmi_gil.reports.bundle import compute_sha256
from eudr_dmi_gil.reports.determinism import write_json

from .config import CommodityConfig
from .providers import CommodityProviderMetadata, provider_for_config


@dataclass(frozen=True)
class CommodityMetrics:
    commodity_area_ha: float | None
    commodity_percent_of_aoi: float | None
    baseline_forest_and_commodity_overlap_ha: float | None
    post_2020_loss_and_commodity_overlap_ha: float | None
    post_2020_loss_and_commodity_percent_of_aoi: float | None
    post_2020_loss_and_commodity_percent_of_loss: float | None
    commodity_observation_year: int

    def to_metric_rows(self) -> dict[str, dict[str, Any]]:
        return {
            "commodity_area_ha": {"value": self.commodity_area_ha, "unit": "ha"},
            "commodity_percent_of_aoi": {
                "value": self.commodity_percent_of_aoi,
                "unit": "percent",
            },
            "baseline_forest_and_commodity_overlap_ha": {
                "value": self.baseline_forest_and_commodity_overlap_ha,
                "unit": "ha",
                "notes": "JRC 2020 forest baseline intersected with observed commodity layer",
            },
            "post_2020_loss_and_commodity_overlap_ha": {
                "value": self.post_2020_loss_and_commodity_overlap_ha,
                "unit": "ha",
                "notes": "post-2020 Hansen loss inside JRC 2020 forest intersected with commodity layer",
            },
            "post_2020_loss_and_commodity_percent_of_aoi": {
                "value": self.post_2020_loss_and_commodity_percent_of_aoi,
                "unit": "percent",
            },
            "post_2020_loss_and_commodity_percent_of_loss": {
                "value": self.post_2020_loss_and_commodity_percent_of_loss,
                "unit": "percent",
            },
            "commodity_observation_year": {
                "value": self.commodity_observation_year,
                "unit": "year",
            },
        }


@dataclass(frozen=True)
class CommodityAssessmentResult:
    config: CommodityConfig
    metadata: CommodityProviderMetadata
    metrics: CommodityMetrics
    coverage_status: str
    evidence_available: bool
    evidence_gaps: list[dict[str, Any]]
    status_messages: list[str]
    summary_path: Path | None
    commodity_mask_path: Path | None
    overlap_mask_path: Path | None
    debug_path: Path | None
    provenance: dict[str, Any]
    nodata: dict[str, Any]
    grid: dict[str, Any] | None


def run_commodity_assessment(
    *,
    config: CommodityConfig,
    aoi_geojson_path: Path,
    aoi_country: str | None,
    jrc_gfc2020_raster_path: Path,
    hansen_lossyear_raster_path: Path,
    output_dir: Path,
    baseline_metadata: BaselineProviderMetadata,
    loss_metadata: LossDatasetMetadata,
    requested_end_year: int,
    target_crs: str = "EPSG:6933",
    target_resolution_m: float = 30.0,
    all_touched: bool = True,
    forest_value: int = 1,
) -> CommodityAssessmentResult:
    provider = provider_for_config(config)
    scope_gaps = provider.validate_country_scope(aoi_country)
    metadata = provider.metadata()
    if scope_gaps:
        return _unavailable_result(
            config=config,
            metadata=metadata,
            coverage_status="unsupported_country",
            gaps=scope_gaps,
            status_messages=["Commodity evidence is outside its supported geographic scope."],
            provenance={"country_scope": list(config.country_scope), "aoi_country": aoi_country},
            nodata={},
        )

    effective_end_year = min(requested_end_year, loss_metadata.latest_available_year)
    end_code = effective_end_year - 2000
    aoi_geom_wgs84 = _load_aoi_geometry(aoi_geojson_path)
    target_transform, width, height, target_bounds = _target_grid(
        aoi_geom_wgs84,
        target_crs=target_crs,
        resolution=target_resolution_m,
    )
    aoi_mask = _rasterize_aoi(
        aoi_geom_wgs84,
        target_crs=target_crs,
        out_shape=(height, width),
        transform=target_transform,
        all_touched=all_touched,
    )

    commodity = provider.aligned_mask(
        target_crs=target_crs,
        target_transform=target_transform,
        width=width,
        height=height,
        aoi_mask=aoi_mask,
    )
    if not commodity.evidence_available or commodity.mask is None:
        return _unavailable_result(
            config=config,
            metadata=metadata,
            coverage_status=commodity.coverage_status,
            gaps=commodity.evidence_gaps,
            status_messages=["Commodity evidence unavailable."],
            provenance=commodity.provenance,
            nodata=commodity.nodata,
        )

    baseline_values, baseline_valid, baseline_info = _reproject_categorical(
        jrc_gfc2020_raster_path,
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
    aoi_area_ha = _round6(float(np.count_nonzero(aoi_mask)) * pixel_area_ha)
    baseline_forest = aoi_mask & baseline_valid & (baseline_values == forest_value)
    post_2020_loss_on_baseline = (
        baseline_forest
        & loss_valid
        & (loss_values >= 21)
        & (loss_values <= end_code)
    )
    commodity_mask = commodity.mask
    baseline_and_commodity = baseline_forest & commodity_mask
    loss_and_commodity = post_2020_loss_on_baseline & commodity_mask

    commodity_area = _area_ha(commodity_mask, pixel_area_ha)
    loss_area = _area_ha(post_2020_loss_on_baseline, pixel_area_ha)
    overlap_area = _area_ha(loss_and_commodity, pixel_area_ha)
    metrics = CommodityMetrics(
        commodity_area_ha=commodity_area,
        commodity_percent_of_aoi=_percent(commodity_area, aoi_area_ha),
        baseline_forest_and_commodity_overlap_ha=_area_ha(
            baseline_and_commodity, pixel_area_ha
        ),
        post_2020_loss_and_commodity_overlap_ha=overlap_area,
        post_2020_loss_and_commodity_percent_of_aoi=_percent(overlap_area, aoi_area_ha),
        post_2020_loss_and_commodity_percent_of_loss=_percent(overlap_area, loss_area),
        commodity_observation_year=config.observation_year,
    )

    status_messages = _status_messages(loss_area=loss_area, overlap_area=overlap_area, config=config)

    output_dir.mkdir(parents=True, exist_ok=True)
    commodity_mask_path = output_dir / f"{config.id}_commodity_mask.geojson"
    overlap_mask_path = output_dir / f"{config.id}_post2020_loss_overlap_mask.geojson"
    debug_path = output_dir / f"{config.id}_commodity_debug.json"
    summary_path = output_dir / f"{config.id}_commodity_summary.json"
    _write_mask_geojson(commodity_mask_path, commodity_mask, target_transform, target_crs)
    _write_mask_geojson(overlap_mask_path, loss_and_commodity, target_transform, target_crs)

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
        "mask_true_pixels": {
            "aoi": int(np.count_nonzero(aoi_mask)),
            "commodity": int(np.count_nonzero(commodity_mask)),
            "jrc_forest_2020": int(np.count_nonzero(baseline_forest)),
            "post_2020_loss_on_jrc_forest_2020": int(
                np.count_nonzero(post_2020_loss_on_baseline)
            ),
            "baseline_forest_and_commodity": int(np.count_nonzero(baseline_and_commodity)),
            "post_2020_loss_and_commodity": int(np.count_nonzero(loss_and_commodity)),
        },
        "nodata": {
            "commodity": commodity.nodata,
            "jrc_gfc2020": int(np.count_nonzero(aoi_mask & (~baseline_valid))),
            "hansen_lossyear": int(np.count_nonzero(aoi_mask & (~loss_valid))),
        },
        "source_alignment": {
            "commodity": commodity.provenance.get("alignment"),
            "baseline": baseline_info,
            "loss": loss_info,
        },
    }
    write_json(debug_path, debug)
    summary = {
        "analysis_id": "single_commodity_assessment_v1",
        "commodity": _commodity_report_block(
            metadata=metadata,
            coverage_status=commodity.coverage_status,
            evidence_available=commodity.evidence_available,
            evidence_gaps=commodity.evidence_gaps,
        ),
        "semantic_layers": {
            "post_2020_loss_inside_jrc_2020_forest": (
                "A: AOI & JRC 2020 forest & Hansen lossyear 2021..effective_end_year"
            ),
            "observed_commodity_area": "B: AOI & configured commodity class mask",
            "loss_and_commodity_intersection": "C: A & B",
            "potential_conversion_candidate": (
                "D: C, flagged for human review; not causation or legal outcome"
            ),
        },
        "metrics": metrics.to_metric_rows(),
        "status_messages": status_messages,
        "provider_metadata": metadata.to_dict(),
        "provenance": commodity.provenance,
        "evidence_gaps": commodity.evidence_gaps,
        "grid": grid,
        "artifacts": {
            "commodity_mask": commodity_mask_path.name,
            "post_2020_loss_overlap_mask": overlap_mask_path.name,
            "debug": debug_path.name,
        },
    }
    write_json(summary_path, summary)

    return CommodityAssessmentResult(
        config=config,
        metadata=metadata,
        metrics=metrics,
        coverage_status=commodity.coverage_status,
        evidence_available=commodity.evidence_available,
        evidence_gaps=commodity.evidence_gaps,
        status_messages=status_messages,
        summary_path=summary_path,
        commodity_mask_path=commodity_mask_path,
        overlap_mask_path=overlap_mask_path,
        debug_path=debug_path,
        provenance=commodity.provenance,
        nodata=commodity.nodata,
        grid=grid,
    )


def commodity_report_block(result: CommodityAssessmentResult) -> dict[str, Any]:
    return _commodity_report_block(
        metadata=result.metadata,
        coverage_status=result.coverage_status,
        evidence_available=result.evidence_available,
        evidence_gaps=result.evidence_gaps,
    )


def _commodity_report_block(
    *,
    metadata: CommodityProviderMetadata,
    coverage_status: str,
    evidence_available: bool,
    evidence_gaps: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "id": metadata.commodity_id,
        "display_name": metadata.display_name,
        "provider": metadata.provider_id,
        "dataset": metadata.dataset_title,
        "version": metadata.dataset_version,
        "observation_year": metadata.observation_year,
        "class_values": list(metadata.class_values),
        "coverage_status": coverage_status,
        "evidence_available": evidence_available,
        "evidence_gaps": evidence_gaps,
    }


def _unavailable_result(
    *,
    config: CommodityConfig,
    metadata: CommodityProviderMetadata,
    coverage_status: str,
    gaps: list[dict[str, Any]],
    status_messages: list[str],
    provenance: dict[str, Any],
    nodata: dict[str, Any],
) -> CommodityAssessmentResult:
    metrics = CommodityMetrics(
        commodity_area_ha=None,
        commodity_percent_of_aoi=None,
        baseline_forest_and_commodity_overlap_ha=None,
        post_2020_loss_and_commodity_overlap_ha=None,
        post_2020_loss_and_commodity_percent_of_aoi=None,
        post_2020_loss_and_commodity_percent_of_loss=None,
        commodity_observation_year=config.observation_year,
    )
    return CommodityAssessmentResult(
        config=config,
        metadata=metadata,
        metrics=metrics,
        coverage_status=coverage_status,
        evidence_available=False,
        evidence_gaps=gaps,
        status_messages=status_messages,
        summary_path=None,
        commodity_mask_path=None,
        overlap_mask_path=None,
        debug_path=None,
        provenance=provenance,
        nodata=nodata,
        grid=None,
    )


def _area_ha(mask: np.ndarray, pixel_area_ha: float) -> float:
    return _round6(float(np.count_nonzero(mask)) * pixel_area_ha)


def _status_messages(*, loss_area: float, overlap_area: float, config: CommodityConfig) -> list[str]:
    messages: list[str] = []
    if loss_area > 0.0:
        messages.append("Post-2020 forest-loss evidence detected within the JRC 2020 baseline.")
    else:
        messages.append("No post-2020 forest-loss evidence detected within the JRC 2020 baseline.")
    if overlap_area > 0.0:
        messages.append(
            f"Post-2020 forest-loss evidence intersects the configured {config.id} layer."
        )
        messages.append(
            f"Potential forest-to-{config.id} conversion candidate; human review required."
        )
    return messages


def artifact_refs(result: CommodityAssessmentResult, bundle_root: Path) -> dict[str, Any]:
    refs: dict[str, Any] = {}
    for key, path in {
        "summary_ref": result.summary_path,
        "commodity_mask_ref": result.commodity_mask_path,
        "post_2020_loss_overlap_mask_ref": result.overlap_mask_path,
        "debug_ref": result.debug_path,
    }.items():
        if path is None:
            refs[key] = None
            continue
        refs[key] = {
            "relpath": str(path.relative_to(bundle_root)).replace("\\", "/"),
            "sha256": compute_sha256(path),
            "content_type": "application/geo+json"
            if path.suffix == ".geojson"
            else "application/json",
        }
    return refs


def aoi_country_from_geojson(path: Path) -> str | None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    prop_blocks: list[dict[str, Any]] = []
    if isinstance(payload.get("properties"), dict):
        prop_blocks.append(payload["properties"])
    if payload.get("type") == "FeatureCollection":
        for feature in payload.get("features", []):
            props = feature.get("properties")
            if isinstance(props, dict):
                prop_blocks.append(props)
    elif payload.get("type") == "Feature" and isinstance(payload.get("properties"), dict):
        prop_blocks.append(payload["properties"])

    keys = (
        "country",
        "country_name",
        "country_of_production",
        "producer_country",
        "admin_country",
    )
    for props in prop_blocks:
        for key in keys:
            value = props.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None
