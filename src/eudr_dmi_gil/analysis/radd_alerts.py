"""RADD (Sentinel-1 SAR) near-real-time disturbance alert observer.

RADD is admitted into the canonical evidence model with source role `alert`, per
`geospatial-evidence-framework`'s dataset registry. It is deliberately **not** turned into a
generic annual "deforestation hectares" product: alerts keep their native date and
low/confirmed confidence semantics.

Live GEE catalogue inspection (2026-08-15, project `myproject-gq-74696`) confirmed:

- Collection: `projects/radar-wur/raddalert/v1`. This collection is **not band-homogeneous**:
  most images are dated alert tiles with bands `['Alert', 'Date']` (e.g.
  `projects/radar-wur/raddalert/v1/africa_20240103`), but the same collection also ships
  single-band (`constant`) forest-domain/baseline images such as
  `africa_radd_primaryhumidtropicalforest_baseline2018_v20201206`. Those baseline images are
  RADD's own forest-domain mask, not alert data, and must be excluded before mosaicking alert
  tiles. Their presence in the same collection is itself evidence that RADD's forest-domain
  construction is derived from external humid-tropical-forest baseline data, not an
  independently-collected ground truth — see the independence caveat below.
- `Alert` band values (from the collection's own `description` property):
  `2` = unconfirmed / low-confidence alert, `3` = confirmed / high-confidence alert. `0`/nodata
  is no alert.
- `Date` band encoding: `YYDOY` (2-digit year + 3-digit day-of-year, e.g. `24001` = 2024-01-01).
- Distinct `geography` values observed: `africa`, `asia`, `ca` (Central America), `sa` (South
  America), plus an internal `beta` geography — RADD's spatial coverage is geography-dependent,
  not global.

Independence caveat (method-level, not a per-run judgement): RADD's detection sensor is
Sentinel-1 SAR, which adds real sensor diversity relative to the Landsat-based Hansen/TMF
observers. This is reported as **cross-sensor corroboration** or an **additional sensor
observer**, never as "independent ground truth" — RADD's own forest-domain baseline (see above)
is constructed from historical humid-tropical-forest baseline data, which is a source of shared
dependency with the other observers, not full statistical independence.

This module consumes a frozen local export (already filtered to alert tiles, already mosaicked
to the AOI) rather than querying the live, mutable collection at report-render time. Acquisition
(collection filtering, mosaicking, clipping, metadata freeze) is a separate concern.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from rasterio.features import shapes as rio_shapes

from eudr_dmi_gil.analysis.jrc_post2020_loss import (
    _load_aoi_geometry,
    _rasterize_aoi,
    _reproject_categorical,
    _round6,
    _target_grid,
    _write_mask_geojson,
)
from eudr_dmi_gil.reports.bundle import compute_sha256
from eudr_dmi_gil.reports.determinism import write_json

RADD_COLLECTION_ID = "projects/radar-wur/raddalert/v1"
RADD_SOURCE_URL = "https://wur-radd.users.earthengine.app/view/raddalert"
RADD_LOW_CONFIDENCE_VALUE = 2
RADD_CONFIRMED_VALUE = 3


@dataclass(frozen=True)
class RaddDatasetMetadata:
    """Frozen provenance for one RADD acquisition/export ("freeze every concrete run")."""

    collection_id: str
    geography: str
    acquired_at_utc: str
    collection_version_or_tile_ids: tuple[str, ...]
    aoi_export_bounds_wgs84: tuple[float, float, float, float]
    date_window_start: str | None
    date_window_end: str | None
    confidence_rule: str
    alert_raster_local_path: str | None
    alert_raster_sha256: str | None
    date_raster_local_path: str | None
    date_raster_sha256: str | None
    coverage_warning: str | None
    source_url: str = RADD_SOURCE_URL
    independence_note: str = (
        "Cross-sensor corroboration (Sentinel-1 SAR) relative to Landsat-based Hansen/TMF; "
        "RADD's own forest-domain baseline depends on historical humid-tropical-forest data "
        "and is not independent ground truth."
    )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["collection_version_or_tile_ids"] = list(self.collection_version_or_tile_ids)
        payload["aoi_export_bounds_wgs84"] = list(self.aoi_export_bounds_wgs84)
        return payload


@dataclass(frozen=True)
class RaddAlertMetrics:
    aoi_area_ha: float
    radd_domain_area_ha: float
    radd_low_confidence_alert_area_ha: float
    radd_confirmed_alert_area_ha: float
    radd_low_confidence_alert_cluster_count: int
    radd_confirmed_alert_cluster_count: int
    radd_first_alert_date: str | None
    radd_latest_alert_date: str | None
    date_window_start: str | None
    date_window_end: str | None

    def to_metric_rows(self) -> dict[str, dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {
            "aoi_area_ha": {"value": self.aoi_area_ha, "unit": "ha"},
            "radd_domain_area_ha": {
                "value": self.radd_domain_area_ha,
                "unit": "ha",
                "notes": "Pixels where the frozen RADD Alert/Date export carries a defined value.",
            },
            "radd_low_confidence_alert_area_ha": {
                "value": self.radd_low_confidence_alert_area_ha,
                "unit": "ha",
                "notes": "RADD Alert=2 (unconfirmed/low-confidence), within the date window if set.",
            },
            "radd_confirmed_alert_area_ha": {
                "value": self.radd_confirmed_alert_area_ha,
                "unit": "ha",
                "notes": "RADD Alert=3 (confirmed/high-confidence), within the date window if set.",
            },
            "radd_low_confidence_alert_cluster_count": {
                "value": self.radd_low_confidence_alert_cluster_count,
                "unit": "count",
                "notes": "Connected-component alert clusters, not individual shipment/plot claims.",
            },
            "radd_confirmed_alert_cluster_count": {
                "value": self.radd_confirmed_alert_cluster_count,
                "unit": "count",
            },
        }
        if self.radd_first_alert_date is not None:
            rows["radd_first_alert_date"] = {"value": self.radd_first_alert_date, "unit": "date"}
        if self.radd_latest_alert_date is not None:
            rows["radd_latest_alert_date"] = {"value": self.radd_latest_alert_date, "unit": "date"}
        return rows


@dataclass(frozen=True)
class RaddAlertAnalysisResult:
    metrics: RaddAlertMetrics
    dataset_metadata: RaddDatasetMetadata
    summary_path: Path
    confirmed_mask_path: Path
    low_confidence_mask_path: Path
    debug_path: Path
    evidence_gaps: list[dict[str, Any]]
    grid: dict[str, Any]


def _decode_yydoy(value: int) -> date | None:
    if value <= 0:
        return None
    year = 2000 + (value // 1000)
    doy = value % 1000
    if doy <= 0:
        return None
    try:
        return date(year, 1, 1) + timedelta(days=doy - 1)
    except ValueError:
        return None


def build_radd_dataset_metadata(
    *,
    alert_raster_path: Path,
    date_raster_path: Path,
    geography: str,
    acquired_at_utc: str,
    collection_version_or_tile_ids: tuple[str, ...],
    aoi_export_bounds_wgs84: tuple[float, float, float, float],
    date_window_start: str | None = None,
    date_window_end: str | None = None,
    confidence_rule: str = "alert_value_2_low_confidence__3_confirmed",
    coverage_warning: str | None = None,
) -> RaddDatasetMetadata:
    return RaddDatasetMetadata(
        collection_id=RADD_COLLECTION_ID,
        geography=geography,
        acquired_at_utc=acquired_at_utc,
        collection_version_or_tile_ids=collection_version_or_tile_ids,
        aoi_export_bounds_wgs84=aoi_export_bounds_wgs84,
        date_window_start=date_window_start,
        date_window_end=date_window_end,
        confidence_rule=confidence_rule,
        alert_raster_local_path=alert_raster_path.as_posix(),
        alert_raster_sha256=compute_sha256(alert_raster_path) if alert_raster_path.is_file() else None,
        date_raster_local_path=date_raster_path.as_posix(),
        date_raster_sha256=compute_sha256(date_raster_path) if date_raster_path.is_file() else None,
        coverage_warning=coverage_warning,
    )


def compute_radd_alerts(
    *,
    aoi_geojson_path: Path,
    radd_alert_raster_path: Path,
    radd_date_raster_path: Path,
    output_dir: Path,
    dataset_metadata: RaddDatasetMetadata,
    date_window_start: str | None = None,
    date_window_end: str | None = None,
    target_crs: str = "EPSG:6933",
    target_resolution_m: float = 10.0,
    all_touched: bool = True,
) -> RaddAlertAnalysisResult:
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

    alert_values, alert_valid, alert_info = _reproject_categorical(
        radd_alert_raster_path,
        target_crs=target_crs,
        target_transform=target_transform,
        width=width,
        height=height,
    )
    date_values, date_valid, date_info = _reproject_categorical(
        radd_date_raster_path,
        target_crs=target_crs,
        target_transform=target_transform,
        width=width,
        height=height,
    )

    pixel_area_ha = abs(float(target_transform.a) * float(target_transform.e)) / 10_000.0
    aoi_area_ha = _round6(float(np.count_nonzero(zone_mask)) * pixel_area_ha)

    radd_domain = zone_mask & alert_valid & date_valid

    window_start = _parse_iso_date(date_window_start)
    window_end = _parse_iso_date(date_window_end)

    decoded_dates = np.vectorize(lambda v: _decode_yydoy(int(v)) if v else None, otypes=[object])(
        date_values
    )
    in_window = np.ones_like(radd_domain, dtype=bool)
    if window_start is not None or window_end is not None:
        def _within(d: Any) -> bool:
            if d is None:
                return False
            if window_start is not None and d < window_start:
                return False
            if window_end is not None and d > window_end:
                return False
            return True

        in_window = np.vectorize(_within, otypes=[bool])(decoded_dates)

    low_conf_mask = radd_domain & (alert_values == RADD_LOW_CONFIDENCE_VALUE) & in_window
    confirmed_mask = radd_domain & (alert_values == RADD_CONFIRMED_VALUE) & in_window

    radd_domain_area = _round6(float(np.count_nonzero(radd_domain)) * pixel_area_ha)
    low_conf_area = _round6(float(np.count_nonzero(low_conf_mask)) * pixel_area_ha)
    confirmed_area = _round6(float(np.count_nonzero(confirmed_mask)) * pixel_area_ha)

    alert_pixel_dates = [
        d
        for d, is_alert in zip(
            decoded_dates.ravel(), (low_conf_mask | confirmed_mask).ravel(), strict=True
        )
        if is_alert and d is not None
    ]
    first_alert_date = min(alert_pixel_dates).isoformat() if alert_pixel_dates else None
    latest_alert_date = max(alert_pixel_dates).isoformat() if alert_pixel_dates else None

    output_dir.mkdir(parents=True, exist_ok=True)
    confirmed_mask_path = output_dir / "radd_confirmed_alert_mask.geojson"
    low_confidence_mask_path = output_dir / "radd_low_confidence_alert_mask.geojson"
    debug_path = output_dir / "radd_alerts_debug.json"
    summary_path = output_dir / "radd_alerts_summary.json"

    _write_mask_geojson(confirmed_mask_path, confirmed_mask, target_transform, target_crs)
    _write_mask_geojson(low_confidence_mask_path, low_conf_mask, target_transform, target_crs)

    confirmed_cluster_count = _count_clusters(confirmed_mask, target_transform)
    low_conf_cluster_count = _count_clusters(low_conf_mask, target_transform)

    metrics = RaddAlertMetrics(
        aoi_area_ha=aoi_area_ha,
        radd_domain_area_ha=radd_domain_area,
        radd_low_confidence_alert_area_ha=low_conf_area,
        radd_confirmed_alert_area_ha=confirmed_area,
        radd_low_confidence_alert_cluster_count=low_conf_cluster_count,
        radd_confirmed_alert_cluster_count=confirmed_cluster_count,
        radd_first_alert_date=first_alert_date,
        radd_latest_alert_date=latest_alert_date,
        date_window_start=date_window_start,
        date_window_end=date_window_end,
    )

    evidence_gaps = _radd_evidence_gaps(
        aoi_pixels=int(np.count_nonzero(zone_mask)),
        radd_domain_pixels=int(np.count_nonzero(radd_domain)),
        alert_nodata_pixels=int(np.count_nonzero(zone_mask & (~alert_valid))),
        date_nodata_pixels=int(np.count_nonzero(zone_mask & (~date_valid))),
        dataset_metadata=dataset_metadata,
        alert_info=alert_info,
        date_info=date_info,
    )

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
            "radd_domain": int(np.count_nonzero(radd_domain)),
            "low_confidence_alert": int(np.count_nonzero(low_conf_mask)),
            "confirmed_alert": int(np.count_nonzero(confirmed_mask)),
        },
        "source_alignment": {"alert": alert_info, "date": date_info},
        "confidence_rule": dataset_metadata.confidence_rule,
    }
    write_json(debug_path, debug)

    summary = {
        "analysis_id": "radd_sentinel1_alerts_v1",
        "formula": (
            "AOI AND radd_domain AND alert_value in {2:low_confidence, 3:confirmed} "
            "AND decoded(date) in [date_window_start, date_window_end]"
        ),
        "metrics": metrics.to_metric_rows(),
        "dataset": dataset_metadata.to_dict(),
        "evidence_gaps": evidence_gaps,
        "grid": grid,
        "artifacts": {
            "confirmed_mask": confirmed_mask_path.name,
            "low_confidence_mask": low_confidence_mask_path.name,
            "debug": debug_path.name,
        },
    }
    write_json(summary_path, summary)

    return RaddAlertAnalysisResult(
        metrics=metrics,
        dataset_metadata=dataset_metadata,
        summary_path=summary_path,
        confirmed_mask_path=confirmed_mask_path,
        low_confidence_mask_path=low_confidence_mask_path,
        debug_path=debug_path,
        evidence_gaps=evidence_gaps,
        grid=grid,
    )


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def _count_clusters(mask: np.ndarray, transform: Any) -> int:
    if not np.any(mask):
        return 0
    count = 0
    for _geom, value in rio_shapes(mask.astype(np.uint8), mask=mask, transform=transform):
        if value == 1:
            count += 1
    return count


def _radd_evidence_gaps(
    *,
    aoi_pixels: int,
    radd_domain_pixels: int,
    alert_nodata_pixels: int,
    date_nodata_pixels: int,
    dataset_metadata: RaddDatasetMetadata,
    alert_info: dict[str, Any],
    date_info: dict[str, Any],
) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    if aoi_pixels > 0 and radd_domain_pixels == 0:
        gaps.append(
            {
                "code": "radd_no_domain_coverage",
                "severity": "warning",
                "message": (
                    "No AOI pixel falls inside the frozen RADD alert/date export. RADD "
                    "coverage is geography-dependent (Africa/Asia/Central America/South "
                    "America humid tropics) and may be unavailable here; this is an evidence "
                    "gap, not a zero-valued alert layer."
                ),
            }
        )
    if alert_nodata_pixels:
        gaps.append(
            {
                "code": "radd_alert_nodata_inside_aoi",
                "severity": "info",
                "message": "RADD Alert nodata pixels inside AOI were excluded.",
                "pixels": alert_nodata_pixels,
                "aoi_pixels": aoi_pixels,
            }
        )
    if date_nodata_pixels:
        gaps.append(
            {
                "code": "radd_date_nodata_inside_aoi",
                "severity": "info",
                "message": "RADD Date nodata pixels inside AOI were excluded.",
                "pixels": date_nodata_pixels,
                "aoi_pixels": aoi_pixels,
            }
        )
    if dataset_metadata.coverage_warning:
        gaps.append(
            {
                "code": "radd_coverage_warning",
                "severity": "warning",
                "message": dataset_metadata.coverage_warning,
            }
        )
    gaps.append(
        {
            "code": "radd_mutable_source_frozen_at_acquisition",
            "severity": "info",
            "message": (
                f"RADD is a near-real-time, mutable source. This evidence was frozen at "
                f"{dataset_metadata.acquired_at_utc} from tiles "
                f"{list(dataset_metadata.collection_version_or_tile_ids)}; re-querying the "
                "live collection later may return different alerts for the same AOI/period."
            ),
        }
    )
    if alert_info.get("grid_alignment_transformation_applied") or date_info.get(
        "grid_alignment_transformation_applied"
    ):
        gaps.append(
            {
                "code": "radd_grid_alignment_transformation_applied",
                "severity": "info",
                "message": (
                    "RADD categorical rasters were aligned to the deterministic target grid "
                    "with nearest-neighbour resampling."
                ),
            }
        )
    return gaps
