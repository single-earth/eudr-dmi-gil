"""JRC Tropical Moist Forest (TMF) deforestation/degradation change observer.

TMF is admitted into the canonical evidence model with two source roles, per
`geospatial-evidence-framework`'s dataset registry (`s/data-sources.md`):
`deforestation_change` and `degradation_change`. It is deliberately **not** collapsed into
Hansen's `deforestation_change` metrics, and TMF degradation is deliberately **not** relabeled
as deforestation or illegal logging: the provider's own class semantics are preserved.

Live GEE catalogue inspection (2026-08-15, project `myproject-gq-74696`) confirmed:

- `projects/JRC/TMF/v1_2025/DeforestationYear` (ImageCollection, regional tiles, mosaic band
  `constant`) — pixel value is the **calendar year** of first-detected deforestation (e.g.
  `2023`), `0` where the TMF product has a defined value but no deforestation was recorded.
- `projects/JRC/TMF/v1_2025/DegradationYear` (ImageCollection, same tiling/encoding) — calendar
  year of first-detected degradation.
- `projects/JRC/TMF/v1_2025/Duration` (mosaic band `constant`) and
  `projects/JRC/TMF/v1_2025/Intensity` (mosaic band `sum`) — provider-native disturbance
  duration/intensity context layers. Values are **not** re-scaled or reinterpreted here; they
  are exposed as provider-native quality context only.
- `DisturbanceObs`, `ValidObs`, `DisturbanceLength`, `DisturbanceIntensity`, `StartMonitoring`
  assets named in early drafts of this work do **not** exist under `v1_2025` (confirmed via a
  live `ee.Image.load` probe); they are recorded as an evidence gap rather than silently
  fabricated as zero/absent metrics.

TMF's own analysis domain ("the TMF domain") is treated as wherever the DeforestationYear /
DegradationYear rasters carry a defined (non-nodata) value. This is distinct from, and reported
alongside, the JRC GFC2020 forest baseline denominator — per the framework's instruction not to
force every source through an identical mask merely to make numbers comparable.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import rasterio

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
from eudr_dmi_gil.reports.bundle import compute_sha256
from eudr_dmi_gil.reports.determinism import write_json

TMF_DATASET_VERSION_DEFAULT = "v1_2025"
TMF_SOURCE_URL = "https://forobs.jrc.ec.europa.eu/TMF"
TMF_DEFORESTATION_ASSET_ID = "projects/JRC/TMF/v1_2025/DeforestationYear"
TMF_DEGRADATION_ASSET_ID = "projects/JRC/TMF/v1_2025/DegradationYear"
TMF_DURATION_ASSET_ID = "projects/JRC/TMF/v1_2025/Duration"
TMF_INTENSITY_ASSET_ID = "projects/JRC/TMF/v1_2025/Intensity"
TMF_NODATA_VALUE = -32768


@dataclass(frozen=True)
class TmfQualityMetadata:
    """Metadata for an optional TMF quality/context raster (Duration or Intensity)."""

    asset_identifier: str
    band: str
    local_path: str | None
    checksum: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TmfChangeMetrics:
    aoi_area_ha: float
    start_year: int
    requested_end_year: int
    effective_end_year: int
    tmf_domain_area_ha: float
    gfc2020_forest_baseline_ha: float
    deforestation_on_tmf_domain_ha: float
    deforestation_on_gfc2020_baseline_ha: float
    deforestation_percent_of_tmf_domain: float
    deforestation_percent_of_gfc2020_baseline: float
    degradation_on_tmf_domain_ha: float
    degradation_on_gfc2020_baseline_ha: float
    degradation_percent_of_tmf_domain: float
    degradation_percent_of_gfc2020_baseline: float
    quality_disturbance_duration_mean_raw: float | None
    quality_disturbance_intensity_mean_raw: float | None

    def to_metric_rows(self) -> dict[str, dict[str, Any]]:
        y0, y1 = self.start_year, self.effective_end_year
        rows: dict[str, dict[str, Any]] = {
            "aoi_area_ha": {"value": self.aoi_area_ha, "unit": "ha"},
            "tmf_domain_area_ha": {
                "value": self.tmf_domain_area_ha,
                "unit": "ha",
                "notes": (
                    "Pixels where JRC TMF DeforestationYear/DegradationYear carry a defined "
                    "value; TMF's own analysis domain, not the JRC GFC2020 forest baseline."
                ),
            },
            "gfc2020_forest_baseline_ha": {
                "value": self.gfc2020_forest_baseline_ha,
                "unit": "ha",
            },
            f"tmf_deforestation_{y0}_{y1}_ha": {
                "value": self.deforestation_on_gfc2020_baseline_ha,
                "unit": "ha",
                "denominator": "gfc2020_forest_baseline",
                "notes": "TMF DeforestationYear inside the JRC GFC2020 forest baseline.",
            },
            f"tmf_deforestation_{y0}_{y1}_on_tmf_domain_ha": {
                "value": self.deforestation_on_tmf_domain_ha,
                "unit": "ha",
                "denominator": "tmf_domain",
                "notes": "TMF DeforestationYear evaluated against TMF's own analysis domain.",
            },
            f"tmf_degradation_{y0}_{y1}_ha": {
                "value": self.degradation_on_gfc2020_baseline_ha,
                "unit": "ha",
                "denominator": "gfc2020_forest_baseline",
                "notes": (
                    "TMF DegradationYear inside the JRC GFC2020 forest baseline. Degradation is "
                    "a distinct evidence stream: not deforestation, not illegal logging, not "
                    "shipment causality."
                ),
            },
            f"tmf_degradation_{y0}_{y1}_on_tmf_domain_ha": {
                "value": self.degradation_on_tmf_domain_ha,
                "unit": "ha",
                "denominator": "tmf_domain",
                "notes": "TMF DegradationYear evaluated against TMF's own analysis domain.",
            },
            "tmf_deforestation_percent_of_tmf_domain": {
                "value": self.deforestation_percent_of_tmf_domain,
                "unit": "percent",
            },
            "tmf_deforestation_percent_of_gfc2020_baseline": {
                "value": self.deforestation_percent_of_gfc2020_baseline,
                "unit": "percent",
            },
            "tmf_degradation_percent_of_tmf_domain": {
                "value": self.degradation_percent_of_tmf_domain,
                "unit": "percent",
            },
            "tmf_degradation_percent_of_gfc2020_baseline": {
                "value": self.degradation_percent_of_gfc2020_baseline,
                "unit": "percent",
            },
            "evidence_start_year": {"value": self.start_year, "unit": "year"},
            "requested_end_year": {"value": self.requested_end_year, "unit": "year"},
            "effective_end_year": {"value": self.effective_end_year, "unit": "year"},
        }
        if self.quality_disturbance_duration_mean_raw is not None:
            rows["tmf_disturbance_duration_mean_raw"] = {
                "value": self.quality_disturbance_duration_mean_raw,
                "unit": "provider_native_raw",
                "notes": (
                    "Mean of the TMF Duration band over pixels flagged as deforestation or "
                    "degradation; provider-native scale, not converted to years or re-scaled."
                ),
            }
        if self.quality_disturbance_intensity_mean_raw is not None:
            rows["tmf_disturbance_intensity_mean_raw"] = {
                "value": self.quality_disturbance_intensity_mean_raw,
                "unit": "provider_native_raw",
                "notes": (
                    "Mean of the TMF Intensity band over pixels flagged as deforestation or "
                    "degradation; provider-native scale, not converted or re-scaled."
                ),
            }
        return rows


@dataclass(frozen=True)
class TmfChangeAnalysisResult:
    metrics: TmfChangeMetrics
    deforestation_metadata: LossDatasetMetadata
    degradation_metadata: LossDatasetMetadata
    duration_metadata: TmfQualityMetadata | None
    intensity_metadata: TmfQualityMetadata | None
    summary_path: Path
    deforestation_mask_path: Path
    degradation_mask_path: Path
    debug_path: Path
    evidence_gaps: list[dict[str, Any]]
    grid: dict[str, Any]


def build_tmf_layer_metadata(
    *,
    raster_path: Path,
    role: str,
    asset_identifier: str,
    dataset_version: str,
    processed_at_utc: str,
) -> LossDatasetMetadata:
    native_crs: str | None = None
    if raster_path.is_file():
        with rasterio.open(raster_path) as ds:
            native_crs = ds.crs.to_string() if ds.crs is not None else None
    checksum = compute_sha256(raster_path) if raster_path.is_file() else None
    return LossDatasetMetadata(
        provider_id="local_jrc_tmf",
        dataset_title=f"JRC Tropical Moist Forest {role.replace('_', ' ')}",
        dataset_id=f"jrc_tmf_{role}",
        asset_identifier=asset_identifier,
        dataset_version=dataset_version,
        source_url=TMF_SOURCE_URL,
        band="constant",
        start_year=1990,
        latest_available_year=2025,
        native_crs=native_crs,
        retrieved_or_processed_at_utc=processed_at_utc,
        checksum=checksum,
        source_fingerprint=checksum,
        local_path=raster_path.as_posix(),
    )


def build_tmf_quality_metadata(
    *,
    raster_path: Path | None,
    asset_identifier: str,
    band: str,
) -> TmfQualityMetadata | None:
    if raster_path is None:
        return None
    checksum = compute_sha256(raster_path) if raster_path.is_file() else None
    return TmfQualityMetadata(
        asset_identifier=asset_identifier,
        band=band,
        local_path=raster_path.as_posix(),
        checksum=checksum,
    )


def compute_tmf_change(
    *,
    aoi_geojson_path: Path,
    tmf_deforestation_raster_path: Path,
    tmf_degradation_raster_path: Path,
    jrc_gfc2020_raster_path: Path,
    output_dir: Path,
    deforestation_metadata: LossDatasetMetadata,
    degradation_metadata: LossDatasetMetadata,
    requested_end_year: int,
    start_year: int = 2021,
    tmf_duration_raster_path: Path | None = None,
    tmf_intensity_raster_path: Path | None = None,
    duration_metadata: TmfQualityMetadata | None = None,
    intensity_metadata: TmfQualityMetadata | None = None,
    target_crs: str = "EPSG:6933",
    target_resolution_m: float = 30.0,
    all_touched: bool = True,
    forest_value: int = 1,
) -> TmfChangeAnalysisResult:
    if requested_end_year < start_year:
        raise ValueError("requested_end_year must be >= start_year")

    effective_end_year = requested_end_year

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

    baseline_values, baseline_valid, baseline_info = _reproject_categorical(
        jrc_gfc2020_raster_path,
        target_crs=target_crs,
        target_transform=target_transform,
        width=width,
        height=height,
    )
    defo_values, defo_valid, defo_info = _reproject_categorical(
        tmf_deforestation_raster_path,
        target_crs=target_crs,
        target_transform=target_transform,
        width=width,
        height=height,
    )
    deg_values, deg_valid, deg_info = _reproject_categorical(
        tmf_degradation_raster_path,
        target_crs=target_crs,
        target_transform=target_transform,
        width=width,
        height=height,
    )

    pixel_area_ha = abs(float(target_transform.a) * float(target_transform.e)) / 10_000.0
    aoi_area_ha = _round6(float(np.count_nonzero(zone_mask)) * pixel_area_ha)

    baseline_forest = zone_mask & baseline_valid & (baseline_values == forest_value)
    tmf_domain = zone_mask & defo_valid & deg_valid

    defo_range = zone_mask & defo_valid & (defo_values >= start_year) & (defo_values <= effective_end_year)
    deg_range = zone_mask & deg_valid & (deg_values >= start_year) & (deg_values <= effective_end_year)

    defo_on_domain = tmf_domain & defo_range
    defo_on_baseline = baseline_forest & defo_range
    deg_on_domain = tmf_domain & deg_range
    deg_on_baseline = baseline_forest & deg_range

    tmf_domain_area = _round6(float(np.count_nonzero(tmf_domain)) * pixel_area_ha)
    gfc2020_forest_area = _round6(float(np.count_nonzero(baseline_forest)) * pixel_area_ha)
    defo_on_domain_area = _round6(float(np.count_nonzero(defo_on_domain)) * pixel_area_ha)
    defo_on_baseline_area = _round6(float(np.count_nonzero(defo_on_baseline)) * pixel_area_ha)
    deg_on_domain_area = _round6(float(np.count_nonzero(deg_on_domain)) * pixel_area_ha)
    deg_on_baseline_area = _round6(float(np.count_nonzero(deg_on_baseline)) * pixel_area_ha)

    duration_mean_raw: float | None = None
    intensity_mean_raw: float | None = None
    disturbance_pixels = defo_range | deg_range
    if tmf_duration_raster_path is not None and np.any(disturbance_pixels):
        dur_values, dur_valid, _ = _reproject_categorical(
            tmf_duration_raster_path,
            target_crs=target_crs,
            target_transform=target_transform,
            width=width,
            height=height,
        )
        sel = disturbance_pixels & dur_valid
        if np.any(sel):
            duration_mean_raw = round(float(np.mean(dur_values[sel])), 3)
    if tmf_intensity_raster_path is not None and np.any(disturbance_pixels):
        int_values, int_valid, _ = _reproject_categorical(
            tmf_intensity_raster_path,
            target_crs=target_crs,
            target_transform=target_transform,
            width=width,
            height=height,
        )
        sel = disturbance_pixels & int_valid
        if np.any(sel):
            intensity_mean_raw = round(float(np.mean(int_values[sel])), 3)

    metrics = TmfChangeMetrics(
        aoi_area_ha=aoi_area_ha,
        start_year=start_year,
        requested_end_year=requested_end_year,
        effective_end_year=effective_end_year,
        tmf_domain_area_ha=tmf_domain_area,
        gfc2020_forest_baseline_ha=gfc2020_forest_area,
        deforestation_on_tmf_domain_ha=defo_on_domain_area,
        deforestation_on_gfc2020_baseline_ha=defo_on_baseline_area,
        deforestation_percent_of_tmf_domain=_percent(defo_on_domain_area, tmf_domain_area),
        deforestation_percent_of_gfc2020_baseline=_percent(defo_on_baseline_area, gfc2020_forest_area),
        degradation_on_tmf_domain_ha=deg_on_domain_area,
        degradation_on_gfc2020_baseline_ha=deg_on_baseline_area,
        degradation_percent_of_tmf_domain=_percent(deg_on_domain_area, tmf_domain_area),
        degradation_percent_of_gfc2020_baseline=_percent(deg_on_baseline_area, gfc2020_forest_area),
        quality_disturbance_duration_mean_raw=duration_mean_raw,
        quality_disturbance_intensity_mean_raw=intensity_mean_raw,
    )

    evidence_gaps = _tmf_evidence_gaps(
        aoi_pixels=int(np.count_nonzero(zone_mask)),
        tmf_domain_pixels=int(np.count_nonzero(tmf_domain)),
        baseline_nodata_pixels=int(np.count_nonzero(zone_mask & (~baseline_valid))),
        defo_nodata_pixels=int(np.count_nonzero(zone_mask & (~defo_valid))),
        deg_nodata_pixels=int(np.count_nonzero(zone_mask & (~deg_valid))),
        quality_supplied=tmf_duration_raster_path is not None or tmf_intensity_raster_path is not None,
        defo_info=defo_info,
        deg_info=deg_info,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{start_year}_{effective_end_year}"
    deforestation_mask_path = output_dir / f"tmf_deforestation_{suffix}_mask.geojson"
    degradation_mask_path = output_dir / f"tmf_degradation_{suffix}_mask.geojson"
    debug_path = output_dir / "tmf_change_debug.json"
    summary_path = output_dir / f"tmf_change_{suffix}_summary.json"

    _write_mask_geojson(deforestation_mask_path, defo_on_domain, target_transform, target_crs)
    _write_mask_geojson(degradation_mask_path, deg_on_domain, target_transform, target_crs)

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
            "aoi": int(np.count_nonzero(zone_mask)),
            "tmf_domain": int(np.count_nonzero(tmf_domain)),
            "gfc2020_forest_baseline": int(np.count_nonzero(baseline_forest)),
            "deforestation_on_tmf_domain": int(np.count_nonzero(defo_on_domain)),
            "deforestation_on_gfc2020_baseline": int(np.count_nonzero(defo_on_baseline)),
            "degradation_on_tmf_domain": int(np.count_nonzero(deg_on_domain)),
            "degradation_on_gfc2020_baseline": int(np.count_nonzero(deg_on_baseline)),
        },
        "source_alignment": {
            "deforestation": defo_info,
            "degradation": deg_info,
        },
    }
    write_json(debug_path, debug)

    summary = {
        "analysis_id": "jrc_tmf_change_v1",
        "formula": (
            "deforestation: AOI AND tmf_defined AND defo_year in [start,end]; "
            "degradation: AOI AND tmf_defined AND deg_year in [start,end]; "
            "each reported against both the tmf_domain and gfc2020_forest_baseline denominators"
        ),
        "metrics": metrics.to_metric_rows(),
        "deforestation_dataset": deforestation_metadata.to_dict(),
        "degradation_dataset": degradation_metadata.to_dict(),
        "duration_dataset": duration_metadata.to_dict() if duration_metadata else None,
        "intensity_dataset": intensity_metadata.to_dict() if intensity_metadata else None,
        "evidence_gaps": evidence_gaps,
        "grid": grid,
        "artifacts": {
            "deforestation_mask": deforestation_mask_path.name,
            "degradation_mask": degradation_mask_path.name,
            "debug": debug_path.name,
        },
    }
    write_json(summary_path, summary)

    return TmfChangeAnalysisResult(
        metrics=metrics,
        deforestation_metadata=deforestation_metadata,
        degradation_metadata=degradation_metadata,
        duration_metadata=duration_metadata,
        intensity_metadata=intensity_metadata,
        summary_path=summary_path,
        deforestation_mask_path=deforestation_mask_path,
        degradation_mask_path=degradation_mask_path,
        debug_path=debug_path,
        evidence_gaps=evidence_gaps,
        grid=grid,
    )


def _tmf_evidence_gaps(
    *,
    aoi_pixels: int,
    tmf_domain_pixels: int,
    baseline_nodata_pixels: int,
    defo_nodata_pixels: int,
    deg_nodata_pixels: int,
    quality_supplied: bool,
    defo_info: dict[str, Any],
    deg_info: dict[str, Any],
) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    if aoi_pixels > 0 and tmf_domain_pixels == 0:
        gaps.append(
            {
                "code": "tmf_no_domain_coverage",
                "severity": "warning",
                "message": (
                    "No AOI pixel falls inside the JRC TMF DeforestationYear/DegradationYear "
                    "analysis domain. TMF is geographically inapplicable here (outside the "
                    "tropical moist forest biome) or the supplied tiles do not cover the AOI. "
                    "This is an evidence gap, not a zero-valued deforestation/degradation layer."
                ),
            }
        )
    if defo_nodata_pixels:
        gaps.append(
            {
                "code": "tmf_deforestation_nodata_inside_aoi",
                "severity": "info",
                "message": "TMF DeforestationYear nodata pixels inside AOI were excluded.",
                "pixels": defo_nodata_pixels,
                "aoi_pixels": aoi_pixels,
            }
        )
    if deg_nodata_pixels:
        gaps.append(
            {
                "code": "tmf_degradation_nodata_inside_aoi",
                "severity": "info",
                "message": "TMF DegradationYear nodata pixels inside AOI were excluded.",
                "pixels": deg_nodata_pixels,
                "aoi_pixels": aoi_pixels,
            }
        )
    if baseline_nodata_pixels:
        gaps.append(
            {
                "code": "jrc_gfc2020_nodata_inside_aoi",
                "severity": "info",
                "message": "JRC GFC2020 nodata pixels inside AOI were excluded from the baseline denominator.",
                "pixels": baseline_nodata_pixels,
                "aoi_pixels": aoi_pixels,
            }
        )
    if not quality_supplied:
        gaps.append(
            {
                "code": "tmf_quality_context_not_supplied",
                "severity": "info",
                "message": (
                    "TMF Duration/Intensity quality-context rasters were not supplied for this "
                    "run; disturbance duration/intensity context is unavailable."
                ),
            }
        )
    gaps.append(
        {
            "code": "tmf_valid_disruption_observation_counts_unavailable",
            "severity": "info",
            "message": (
                "Per-pixel valid-observation-count and disruption-observation-count assets "
                "(e.g. ValidObs, DisturbanceObs) were probed live against "
                "projects/JRC/TMF/v1_2025 and do not exist under that asset path; sparse-vs"
                "-strong TMF observation-history context beyond Duration/Intensity is an "
                "evidence gap, not a fabricated metric."
            ),
        }
    )
    if defo_info.get("grid_alignment_transformation_applied") or deg_info.get(
        "grid_alignment_transformation_applied"
    ):
        gaps.append(
            {
                "code": "tmf_grid_alignment_transformation_applied",
                "severity": "info",
                "message": (
                    "TMF categorical rasters were aligned to the deterministic target grid "
                    "with nearest-neighbour resampling."
                ),
            }
        )
    return gaps
