from __future__ import annotations

import json
from dataclasses import dataclass, replace
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

from .config import MODE_PROBABILITY_THRESHOLD, CommodityConfig
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
    commodity_baseline_observation_year: int | None = None
    new_commodity_since_baseline_ha: float | None = None
    post_2020_loss_and_new_commodity_overlap_ha: float | None = None
    post_2020_loss_and_new_commodity_percent_of_loss: float | None = None
    fdp_new_commodity_since_baseline_ha: float | None = None
    mapbiomas_new_commodity_since_baseline_ha: float | None = None
    fdp_only_new_commodity_since_baseline_ha: float | None = None
    mapbiomas_only_new_commodity_since_baseline_ha: float | None = None
    both_source_agreement_new_commodity_since_baseline_ha: float | None = None
    post_2020_loss_and_fdp_new_commodity_overlap_ha: float | None = None
    post_2020_loss_and_mapbiomas_new_commodity_overlap_ha: float | None = None
    post_2020_loss_and_both_source_agreement_new_commodity_overlap_ha: float | None = None
    probability_configured_threshold: float | None = None
    probability_valid_coverage_of_aoi_percent: float | None = None
    probability_admitted_share_of_valid_pixels_percent: float | None = None
    probability_mean: float | None = None
    probability_median: float | None = None

    def to_metric_rows(self) -> dict[str, dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {
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
        if self.commodity_baseline_observation_year is not None:
            rows["commodity_baseline_observation_year"] = {
                "value": self.commodity_baseline_observation_year,
                "unit": "year",
            }
        optional_rows = {
            "new_commodity_since_baseline_ha": (
                self.new_commodity_since_baseline_ha,
                "ha",
                "latest commodity mask minus baseline-year commodity mask",
            ),
            "post_2020_loss_and_new_commodity_overlap_ha": (
                self.post_2020_loss_and_new_commodity_overlap_ha,
                "ha",
                "post-2020 baseline forest loss intersected with new commodity after the baseline year",
            ),
            "post_2020_loss_and_new_commodity_percent_of_loss": (
                self.post_2020_loss_and_new_commodity_percent_of_loss,
                "percent",
                "share of post-2020 baseline forest loss intersecting new commodity after the baseline year",
            ),
            "fdp_new_commodity_since_baseline_ha": (
                self.fdp_new_commodity_since_baseline_ha,
                "ha",
                "FDP latest commodity probability mask minus FDP baseline-year commodity probability mask",
            ),
            "mapbiomas_new_commodity_since_baseline_ha": (
                self.mapbiomas_new_commodity_since_baseline_ha,
                "ha",
                "MapBiomas latest commodity class mask minus MapBiomas baseline-year commodity class mask",
            ),
            "fdp_only_new_commodity_since_baseline_ha": (
                self.fdp_only_new_commodity_since_baseline_ha,
                "ha",
                "new commodity after the baseline year present in FDP and absent from MapBiomas",
            ),
            "mapbiomas_only_new_commodity_since_baseline_ha": (
                self.mapbiomas_only_new_commodity_since_baseline_ha,
                "ha",
                "new commodity after the baseline year present in MapBiomas and absent from FDP",
            ),
            "both_source_agreement_new_commodity_since_baseline_ha": (
                self.both_source_agreement_new_commodity_since_baseline_ha,
                "ha",
                "new commodity after the baseline year where FDP and MapBiomas agree",
            ),
            "post_2020_loss_and_fdp_new_commodity_overlap_ha": (
                self.post_2020_loss_and_fdp_new_commodity_overlap_ha,
                "ha",
                "post-2020 baseline forest loss intersected with FDP new commodity after the baseline year",
            ),
            "post_2020_loss_and_mapbiomas_new_commodity_overlap_ha": (
                self.post_2020_loss_and_mapbiomas_new_commodity_overlap_ha,
                "ha",
                "post-2020 baseline forest loss intersected with MapBiomas new commodity after the baseline year",
            ),
            "post_2020_loss_and_both_source_agreement_new_commodity_overlap_ha": (
                self.post_2020_loss_and_both_source_agreement_new_commodity_overlap_ha,
                "ha",
                "post-2020 baseline forest loss intersected with both-source agreement new commodity",
            ),
        }
        for name, (value, unit, notes) in optional_rows.items():
            if value is not None:
                rows[name] = {"value": value, "unit": unit, "notes": notes}
        if self.probability_configured_threshold is not None:
            rows["commodity_probability_configured_threshold"] = {
                "value": self.probability_configured_threshold,
                "unit": "probability_fraction",
                "notes": "probability_threshold mode only: probability >= threshold defines the candidate mask",
            }
            rows["commodity_probability_valid_coverage_of_aoi_percent"] = {
                "value": self.probability_valid_coverage_of_aoi_percent,
                "unit": "percent",
                "notes": "share of AOI pixels with a valid (non-nodata) probability value",
            }
            rows["commodity_probability_admitted_share_of_valid_pixels_percent"] = {
                "value": self.probability_admitted_share_of_valid_pixels_percent,
                "unit": "percent",
                "notes": (
                    "share of valid AOI pixels admitted at the configured threshold "
                    "(valid pixels are the primary denominator; see the commodity summary JSON "
                    "for the full sensitivity table across thresholds)"
                ),
            }
            rows["commodity_probability_mean"] = {
                "value": self.probability_mean,
                "unit": "probability_fraction",
                "notes": "mean probability over valid AOI pixels",
            }
            rows["commodity_probability_median"] = {
                "value": self.probability_median,
                "unit": "probability_fraction",
                "notes": "median probability over valid AOI pixels",
            }
        return rows


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
    baseline_commodity_mask_path: Path | None
    overlap_mask_path: Path | None
    derived_mask_paths: dict[str, Path]
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

    probability_profile = (
        commodity.provenance.get("probability_profile")
        if isinstance(commodity.provenance, dict)
        else None
    )
    evidence_gaps = list(commodity.evidence_gaps)
    sample_gap = _sample_aoi_evidence_gap(aoi_geojson_path)
    if sample_gap is not None:
        evidence_gaps.append(sample_gap)
    if config.mode == MODE_PROBABILITY_THRESHOLD:
        evidence_gaps.extend(
            _probability_mode_evidence_gaps(config, probability_profile)
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

    source_new_masks: dict[str, np.ndarray] = {}
    source_latest_masks: dict[str, np.ndarray] = {}
    source_baseline_masks: dict[str, np.ndarray] = {}
    source_diagnostics: dict[str, Any] = {}
    derived_masks: dict[str, np.ndarray] = {}

    primary_key = _commodity_source_key(config)
    for source_config in (config, *config.companion_sources):
        key = _commodity_source_key(source_config)
        source_provider = provider_for_config(source_config)
        source_metadata = source_provider.metadata()
        source_latest = (
            commodity
            if source_config is config
            else source_provider.aligned_mask(
                target_crs=target_crs,
                target_transform=target_transform,
                width=width,
                height=height,
                aoi_mask=aoi_mask,
            )
        )
        source_diagnostics[key] = {
            "provider": source_config.provider,
            "dataset_id": source_metadata.dataset_id,
            "observation_year": source_config.observation_year,
            "baseline_observation_year": source_config.baseline_observation_year,
            "latest_available": source_latest.evidence_available,
            "latest_coverage_status": source_latest.coverage_status,
            "evidence_gaps": source_latest.evidence_gaps,
        }
        if not source_latest.evidence_available or source_latest.mask is None:
            continue
        source_latest_masks[key] = source_latest.mask

        baseline_source = source_config.baseline_raster_source
        if not baseline_source or source_config.baseline_observation_year is None:
            source_diagnostics[key]["baseline_available"] = False
            source_diagnostics[key]["baseline_omission_reason"] = (
                "commodity_baseline_raster_not_configured"
            )
            continue
        baseline_config = replace(
            source_config,
            observation_year=source_config.baseline_observation_year,
            local_path=source_config.baseline_local_path,
            asset_id=source_config.baseline_asset_id or source_config.asset_id,
            baseline_observation_year=None,
            baseline_local_path=None,
            baseline_asset_id=None,
            companion_sources=(),
        )
        baseline_provider = provider_for_config(baseline_config)
        baseline_mask = baseline_provider.aligned_mask(
            target_crs=target_crs,
            target_transform=target_transform,
            width=width,
            height=height,
            aoi_mask=aoi_mask,
        )
        source_diagnostics[key]["baseline_available"] = baseline_mask.evidence_available
        source_diagnostics[key]["baseline_coverage_status"] = baseline_mask.coverage_status
        source_diagnostics[key]["baseline_evidence_gaps"] = baseline_mask.evidence_gaps
        if not baseline_mask.evidence_available or baseline_mask.mask is None:
            continue
        source_baseline_masks[key] = baseline_mask.mask
        source_new_masks[key] = source_latest.mask & ~baseline_mask.mask

    if source_new_masks:
        primary_new = source_new_masks.get(primary_key)
        if primary_new is not None:
            derived_masks["new_commodity_since_baseline"] = primary_new
            derived_masks["post_2020_loss_and_new_commodity"] = (
                post_2020_loss_on_baseline & primary_new
            )
        fdp_new = _first_named_source_mask(source_new_masks, ("fdp", "forest_data_partnership"))
        mapbiomas_new = _first_named_source_mask(source_new_masks, ("mapbiomas",))
        if fdp_new is not None:
            derived_masks["fdp_new_commodity_since_baseline"] = fdp_new
            derived_masks["post_2020_loss_and_fdp_new_commodity"] = (
                post_2020_loss_on_baseline & fdp_new
            )
        if mapbiomas_new is not None:
            derived_masks["mapbiomas_new_commodity_since_baseline"] = mapbiomas_new
            derived_masks["post_2020_loss_and_mapbiomas_new_commodity"] = (
                post_2020_loss_on_baseline & mapbiomas_new
            )
        if fdp_new is not None and mapbiomas_new is not None:
            fdp_only = fdp_new & ~mapbiomas_new
            mapbiomas_only = mapbiomas_new & ~fdp_new
            agreement = fdp_new & mapbiomas_new
            derived_masks["fdp_only_new_commodity_since_baseline"] = fdp_only
            derived_masks["mapbiomas_only_new_commodity_since_baseline"] = mapbiomas_only
            derived_masks["both_source_agreement_new_commodity_since_baseline"] = agreement
            derived_masks["post_2020_loss_and_source_specific_new_commodity"] = (
                post_2020_loss_on_baseline & (fdp_only | mapbiomas_only)
            )
            derived_masks["post_2020_loss_and_both_source_agreement_new_commodity"] = (
                post_2020_loss_on_baseline & agreement
            )

    commodity_area = _area_ha(commodity_mask, pixel_area_ha)
    loss_area = _area_ha(post_2020_loss_on_baseline, pixel_area_ha)
    overlap_area = _area_ha(loss_and_commodity, pixel_area_ha)

    probability_metric_fields: dict[str, Any] = {}
    if config.mode == MODE_PROBABILITY_THRESHOLD and probability_profile is not None:
        configured_threshold = probability_profile.get("configured_threshold")
        admitted_share = None
        for entry in probability_profile.get("sensitivity", {}).values():
            if entry.get("threshold") == configured_threshold:
                admitted_share = entry.get("admitted_share_of_valid_pixels_percent")
                break
        stats = probability_profile.get("stats", {})
        probability_metric_fields = {
            "probability_configured_threshold": configured_threshold,
            "probability_valid_coverage_of_aoi_percent": probability_profile.get(
                "valid_coverage_of_aoi_percent"
            ),
            "probability_admitted_share_of_valid_pixels_percent": admitted_share,
            "probability_mean": stats.get("mean"),
            "probability_median": stats.get("median"),
        }

    primary_new_overlap_area = _mask_area_or_none(
        derived_masks.get("post_2020_loss_and_new_commodity"), pixel_area_ha
    )

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
        commodity_baseline_observation_year=config.baseline_observation_year,
        new_commodity_since_baseline_ha=_mask_area_or_none(
            derived_masks.get("new_commodity_since_baseline"), pixel_area_ha
        ),
        post_2020_loss_and_new_commodity_overlap_ha=primary_new_overlap_area,
        post_2020_loss_and_new_commodity_percent_of_loss=_percent(
            primary_new_overlap_area,
            loss_area,
        )
        if primary_new_overlap_area is not None
        else None,
        fdp_new_commodity_since_baseline_ha=_mask_area_or_none(
            derived_masks.get("fdp_new_commodity_since_baseline"), pixel_area_ha
        ),
        mapbiomas_new_commodity_since_baseline_ha=_mask_area_or_none(
            derived_masks.get("mapbiomas_new_commodity_since_baseline"), pixel_area_ha
        ),
        fdp_only_new_commodity_since_baseline_ha=_mask_area_or_none(
            derived_masks.get("fdp_only_new_commodity_since_baseline"), pixel_area_ha
        ),
        mapbiomas_only_new_commodity_since_baseline_ha=_mask_area_or_none(
            derived_masks.get("mapbiomas_only_new_commodity_since_baseline"), pixel_area_ha
        ),
        both_source_agreement_new_commodity_since_baseline_ha=_mask_area_or_none(
            derived_masks.get("both_source_agreement_new_commodity_since_baseline"), pixel_area_ha
        ),
        post_2020_loss_and_fdp_new_commodity_overlap_ha=_mask_area_or_none(
            derived_masks.get("post_2020_loss_and_fdp_new_commodity"), pixel_area_ha
        ),
        post_2020_loss_and_mapbiomas_new_commodity_overlap_ha=_mask_area_or_none(
            derived_masks.get("post_2020_loss_and_mapbiomas_new_commodity"), pixel_area_ha
        ),
        post_2020_loss_and_both_source_agreement_new_commodity_overlap_ha=_mask_area_or_none(
            derived_masks.get("post_2020_loss_and_both_source_agreement_new_commodity"), pixel_area_ha
        ),
        **probability_metric_fields,
    )

    status_messages = _status_messages(loss_area=loss_area, overlap_area=overlap_area, config=config)

    output_dir.mkdir(parents=True, exist_ok=True)
    commodity_mask_path = output_dir / f"{config.id}_commodity_mask.geojson"
    baseline_commodity_mask = source_baseline_masks.get(primary_key)
    baseline_commodity_mask_path = (
        output_dir / f"{config.id}_baseline_commodity_mask.geojson"
        if baseline_commodity_mask is not None
        else None
    )
    overlap_mask_path = output_dir / f"{config.id}_post2020_loss_overlap_mask.geojson"
    debug_path = output_dir / f"{config.id}_commodity_debug.json"
    summary_path = output_dir / f"{config.id}_commodity_summary.json"
    _write_mask_geojson(commodity_mask_path, commodity_mask, target_transform, target_crs)
    if baseline_commodity_mask_path is not None:
        _write_mask_geojson(
            baseline_commodity_mask_path,
            baseline_commodity_mask,
            target_transform,
            target_crs,
        )
    _write_mask_geojson(overlap_mask_path, loss_and_commodity, target_transform, target_crs)
    derived_mask_paths: dict[str, Path] = {}
    for name, mask in sorted(derived_masks.items()):
        path = output_dir / f"{config.id}_{name}.geojson"
        _write_mask_geojson(path, mask, target_transform, target_crs)
        derived_mask_paths[name] = path

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
            **{
                name: int(np.count_nonzero(mask))
                for name, mask in sorted(derived_masks.items())
            },
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
        "source_specific_commodity_masks": source_diagnostics,
    }
    write_json(debug_path, debug)
    summary = {
        "analysis_id": "single_commodity_assessment_v1",
        "commodity": _commodity_report_block(
            metadata=metadata,
            coverage_status=commodity.coverage_status,
            evidence_available=commodity.evidence_available,
            evidence_gaps=evidence_gaps,
        ),
        "semantic_layers": {
            "post_2020_loss_inside_jrc_2020_forest": (
                "A: AOI & JRC 2020 forest & Hansen lossyear 2021..effective_end_year"
            ),
            "observed_commodity_area": (
                "B: AOI & configured commodity mask (discrete class membership, or "
                "probability >= threshold for probability_threshold mode)"
            ),
            "loss_and_commodity_intersection": "C: A & B",
            "potential_conversion_candidate": (
                "D: C, flagged for human review; not causation or legal outcome. A candidate "
                "commodity-probability overlap is not, by itself, confirmed agricultural "
                "conversion evidence."
            ),
            "new_commodity_after_baseline": (
                "E: latest configured commodity mask minus baseline-year commodity mask, when "
                "both years are available from the source"
            ),
            "source_specific_new_commodity": (
                "F: FDP-only, MapBiomas-only, and both-source agreement masks are reported "
                "separately when both sources are configured"
            ),
        },
        "metrics": metrics.to_metric_rows(),
        "status_messages": status_messages,
        "provider_metadata": metadata.to_dict(),
        "provenance": commodity.provenance,
        "probability_profile": probability_profile,
        "evidence_gaps": evidence_gaps,
        "grid": grid,
        "artifacts": {
            "commodity_mask": commodity_mask_path.name,
            **(
                {"baseline_commodity_mask": baseline_commodity_mask_path.name}
                if baseline_commodity_mask_path is not None
                else {}
            ),
            "post_2020_loss_overlap_mask": overlap_mask_path.name,
            "debug": debug_path.name,
            **{name: path.name for name, path in sorted(derived_mask_paths.items())},
        },
    }
    write_json(summary_path, summary)

    return CommodityAssessmentResult(
        config=config,
        metadata=metadata,
        metrics=metrics,
        coverage_status=commodity.coverage_status,
        evidence_available=commodity.evidence_available,
        evidence_gaps=evidence_gaps,
        status_messages=status_messages,
        summary_path=summary_path,
        commodity_mask_path=commodity_mask_path,
        baseline_commodity_mask_path=baseline_commodity_mask_path,
        overlap_mask_path=overlap_mask_path,
        derived_mask_paths=derived_mask_paths,
        debug_path=debug_path,
        provenance=commodity.provenance,
        nodata=commodity.nodata,
        grid=grid,
    )


def commodity_report_block(result: CommodityAssessmentResult) -> dict[str, Any]:
    # NOTE: this block is validated against the strict, additionalProperties=False
    # `aoi_report_v2.schema.json` "commodity" object, so it must only ever contain the
    # fields already listed there. Mode-specific fields (mode, threshold, probability_band,
    # sensitivity_thresholds, probability_profile) are instead carried in the permissive
    # `parameters.commodity` / `extensions.commodity_assessment.provenance` blocks and picked
    # up from there by the canonical (v3) report adapter.
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
        commodity_baseline_observation_year=config.baseline_observation_year,
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
        baseline_commodity_mask_path=None,
        overlap_mask_path=None,
        derived_mask_paths={},
        debug_path=None,
        provenance=provenance,
        nodata=nodata,
        grid=None,
    )


def _area_ha(mask: np.ndarray, pixel_area_ha: float) -> float:
    return _round6(float(np.count_nonzero(mask)) * pixel_area_ha)


def _mask_area_or_none(mask: np.ndarray | None, pixel_area_ha: float) -> float | None:
    return None if mask is None else _area_ha(mask, pixel_area_ha)


def _commodity_source_key(config: CommodityConfig) -> str:
    provider = config.provider.strip().lower().replace("-", "_").replace(" ", "_")
    if "forest" in provider and "partnership" in provider:
        provider = "fdp"
    if "forestdatapartnership" in provider:
        provider = "fdp"
    if "mapbiomas" in provider:
        provider = "mapbiomas"
    return f"{provider or 'commodity'}_{config.observation_year}"


def _first_named_source_mask(
    masks: dict[str, np.ndarray],
    needles: tuple[str, ...],
) -> np.ndarray | None:
    for key, mask in masks.items():
        if any(needle in key for needle in needles):
            return mask
    return None


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
        "baseline_commodity_mask_ref": result.baseline_commodity_mask_path,
        "post_2020_loss_overlap_mask_ref": result.overlap_mask_path,
        **{f"{name}_mask_ref": path for name, path in result.derived_mask_paths.items()},
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


def _geojson_property_blocks(path: Path) -> list[dict[str, Any]]:
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
    return prop_blocks


def aoi_country_from_geojson(path: Path) -> str | None:
    keys = (
        "country",
        "country_name",
        "country_of_production",
        "producer_country",
        "admin_country",
    )
    for props in _geojson_property_blocks(path):
        for key in keys:
            value = props.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _sample_aoi_evidence_gap(aoi_geojson_path: Path) -> dict[str, Any] | None:
    """Flag AOI source metadata that self-describes as a non-verified sample boundary.

    Purely data-driven (reads the AOI's own `note`-style properties): this is a genuine
    scope limitation regardless of which commodity is configured, and is unrelated to any
    legacy commodity name/property the AOI file may also carry.
    """
    try:
        prop_blocks = _geojson_property_blocks(aoi_geojson_path)
    except Exception:
        return None
    for props in prop_blocks:
        note = props.get("note")
        if isinstance(note, str) and "not a verified farm boundary" in note.lower():
            return {
                "code": "sample_aoi_not_verified_farm_boundary",
                "severity": "warning",
                "message": (
                    "The source AOI metadata states this polygon is a sample for testing, "
                    f"not a verified farm boundary: {note.strip()!r}"
                ),
            }
    return None


def _probability_mode_evidence_gaps(
    config: CommodityConfig, probability_profile: dict[str, Any] | None
) -> list[dict[str, Any]]:
    """Standing evidence-limitation gaps for continuous probability-threshold commodity layers.

    These reflect generic properties of probability-model commodity layers (generic
    thresholds, no localized ground-truthing supplied, absence-is-not-established), not a
    claim specific to any one AOI/country/commodity.
    """
    gaps: list[dict[str, Any]] = [
        {
            "code": "commodity_threshold_not_locally_calibrated",
            "severity": "warning",
            "message": (
                f"The configured probability threshold ({config.threshold}) is a generic model "
                "cutoff; it has not been locally calibrated for this AOI or country."
            ),
        },
        {
            "code": "commodity_local_validation_missing",
            "severity": "warning",
            "message": (
                "No locally validated (ground-truthed) accuracy assessment for the configured "
                f"commodity probability model is supplied for {', '.join(config.country_scope)} "
                "by this run; localized accuracy assessment is the consumer's responsibility."
            ),
        },
        {
            "code": "commodity_absence_not_established",
            "severity": "info",
            "message": (
                "Low or missing localized commodity-probability evidence is not evidence that "
                "the commodity is absent: it reflects a missing localizing signal, not a "
                "negative finding."
            ),
        },
    ]
    if probability_profile is not None and config.threshold is not None:
        configured_threshold = round(float(config.threshold), 6)
        share = None
        for entry in probability_profile.get("sensitivity", {}).values():
            if entry.get("threshold") == configured_threshold:
                share = entry.get("admitted_share_of_valid_pixels_percent")
                break
        if share is not None and share >= 95.0:
            gaps.append(
                {
                    "code": "commodity_localization_low_precision",
                    "severity": "warning",
                    "message": (
                        f"The {config.threshold} threshold admitted {share:.1f}% of valid AOI "
                        "pixels (>=95% operational non-discrimination threshold), so this layer "
                        "cannot localize candidate commodity presence within the AOI at this "
                        "threshold; more precise (higher-probability) information is missing."
                    ),
                    "admitted_share_of_valid_pixels_percent": share,
                }
            )
    return gaps
