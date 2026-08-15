from __future__ import annotations

from pathlib import Path
from typing import Any


def unavailable_metrics() -> dict[str, None]:
    return {
        "radd_confirmed_alert_2021_resolved_end_year_inside_gfc2020_ha": None,
        "radd_low_confidence_alert_2021_resolved_end_year_inside_gfc2020_ha": None,
    }


def compute_metrics_or_unavailable(
    *,
    aoi_geojson_path: Path,
    radd_alert_raster_path: Path | None,
    radd_date_raster_path: Path | None,
    output_dir: Path,
    acquired_at_utc: str | None,
    geography: str = "",
    date_window_start: str | None = None,
    date_window_end: str | None = None,
) -> dict[str, Any]:
    """Real RADD metrics when both rasters + an acquisition timestamp are supplied.

    Falls back to `unavailable_metrics()` (an explicit evidence gap) otherwise. Delegates to
    the canonical `eudr_dmi_gil.analysis.radd_alerts` implementation so this standalone
    local-raster evidence path and the canonical `eudr_dmi_gil.reports.cli` report pipeline
    never define RADD alert semantics twice.
    """
    if radd_alert_raster_path is None or radd_date_raster_path is None or not acquired_at_utc:
        return unavailable_metrics()

    import rasterio

    from eudr_dmi_gil.analysis.radd_alerts import build_radd_dataset_metadata, compute_radd_alerts

    with rasterio.open(radd_alert_raster_path) as dataset:
        bounds = tuple(dataset.bounds)

    dataset_metadata = build_radd_dataset_metadata(
        alert_raster_path=radd_alert_raster_path,
        date_raster_path=radd_date_raster_path,
        geography=geography,
        acquired_at_utc=acquired_at_utc,
        collection_version_or_tile_ids=(),
        aoi_export_bounds_wgs84=bounds,
        date_window_start=date_window_start,
        date_window_end=date_window_end,
    )
    result = compute_radd_alerts(
        aoi_geojson_path=aoi_geojson_path,
        radd_alert_raster_path=radd_alert_raster_path,
        radd_date_raster_path=radd_date_raster_path,
        output_dir=output_dir,
        dataset_metadata=dataset_metadata,
        date_window_start=date_window_start,
        date_window_end=date_window_end,
    )
    return {
        "radd_confirmed_alert_2021_resolved_end_year_inside_gfc2020_ha": (
            result.metrics.radd_confirmed_alert_area_ha
        ),
        "radd_low_confidence_alert_2021_resolved_end_year_inside_gfc2020_ha": (
            result.metrics.radd_low_confidence_alert_area_ha
        ),
    }

