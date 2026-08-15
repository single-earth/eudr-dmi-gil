from __future__ import annotations

from pathlib import Path
from typing import Any


def unavailable_metrics() -> dict[str, None]:
    return {
        "tmf_deforestation_2021_resolved_end_year_inside_gfc2020_ha": None,
        "tmf_degradation_2021_resolved_end_year_inside_gfc2020_ha": None,
    }


def compute_metrics_or_unavailable(
    *,
    aoi_geojson_path: Path,
    gfc2020_raster_path: Path,
    tmf_deforestation_raster_path: Path | None,
    tmf_degradation_raster_path: Path | None,
    output_dir: Path,
    start_year: int,
    end_year: int,
    processed_at_utc: str,
    dataset_version: str = "v1_2025",
) -> dict[str, Any]:
    """Real TMF metrics when both rasters are supplied; `unavailable_metrics()` otherwise.

    Delegates to the canonical `eudr_dmi_gil.analysis.tmf_change` implementation so this
    standalone local-raster evidence path and the canonical `eudr_dmi_gil.reports.cli` report
    pipeline never define TMF deforestation/degradation semantics twice.
    """
    if tmf_deforestation_raster_path is None or tmf_degradation_raster_path is None:
        return unavailable_metrics()

    from eudr_dmi_gil.analysis.tmf_change import build_tmf_layer_metadata, compute_tmf_change

    deforestation_metadata = build_tmf_layer_metadata(
        raster_path=tmf_deforestation_raster_path,
        role="deforestation_year",
        asset_identifier="projects/JRC/TMF/v1_2025/DeforestationYear",
        dataset_version=dataset_version,
        processed_at_utc=processed_at_utc,
    )
    degradation_metadata = build_tmf_layer_metadata(
        raster_path=tmf_degradation_raster_path,
        role="degradation_year",
        asset_identifier="projects/JRC/TMF/v1_2025/DegradationYear",
        dataset_version=dataset_version,
        processed_at_utc=processed_at_utc,
    )
    result = compute_tmf_change(
        aoi_geojson_path=aoi_geojson_path,
        tmf_deforestation_raster_path=tmf_deforestation_raster_path,
        tmf_degradation_raster_path=tmf_degradation_raster_path,
        jrc_gfc2020_raster_path=gfc2020_raster_path,
        output_dir=output_dir,
        deforestation_metadata=deforestation_metadata,
        degradation_metadata=degradation_metadata,
        requested_end_year=end_year,
        start_year=start_year,
    )
    return {
        "tmf_deforestation_2021_resolved_end_year_inside_gfc2020_ha": (
            result.metrics.deforestation_on_gfc2020_baseline_ha
        ),
        "tmf_degradation_2021_resolved_end_year_inside_gfc2020_ha": (
            result.metrics.degradation_on_gfc2020_baseline_ha
        ),
    }

