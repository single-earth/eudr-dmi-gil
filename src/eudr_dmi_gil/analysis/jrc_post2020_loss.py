from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.features import shapes
from rasterio.transform import array_bounds, from_origin
from rasterio.warp import reproject, transform_bounds, transform_geom
from shapely.geometry import mapping, shape
from shapely.ops import unary_union

from eudr_dmi_gil.providers.baseline import BaselineProviderMetadata
from eudr_dmi_gil.reports.bundle import compute_sha256
from eudr_dmi_gil.reports.determinism import write_json


@dataclass(frozen=True)
class LossDatasetMetadata:
    provider_id: str
    dataset_title: str
    dataset_id: str
    asset_identifier: str
    dataset_version: str
    source_url: str
    band: str
    start_year: int
    latest_available_year: int
    native_crs: str | None
    retrieved_or_processed_at_utc: str
    checksum: str | None
    source_fingerprint: str | None
    local_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class JrcPost2020Metrics:
    aoi_area_ha: float
    forest_baseline_2020_ha: float
    forest_baseline_2020_percent_of_aoi: float
    forest_loss_post_2020_on_baseline_ha: float
    forest_loss_post_2020_percent_of_aoi: float
    forest_loss_post_2020_percent_of_baseline: float
    baseline_forest_without_detected_loss_ha: float
    evidence_start_year: int
    requested_end_year: int
    effective_end_year: int

    def to_metric_rows(self) -> dict[str, dict[str, Any]]:
        return {
            "aoi_area_ha": {"value": self.aoi_area_ha, "unit": "ha"},
            "forest_baseline_2020_ha": {"value": self.forest_baseline_2020_ha, "unit": "ha"},
            "forest_baseline_2020_percent_of_aoi": {
                "value": self.forest_baseline_2020_percent_of_aoi,
                "unit": "percent",
            },
            "forest_loss_post_2020_on_baseline_ha": {
                "value": self.forest_loss_post_2020_on_baseline_ha,
                "unit": "ha",
                "notes": "AOI & JRC GFC2020 forest value & Hansen lossyear range",
            },
            "forest_loss_post_2020_percent_of_aoi": {
                "value": self.forest_loss_post_2020_percent_of_aoi,
                "unit": "percent",
            },
            "forest_loss_post_2020_percent_of_baseline": {
                "value": self.forest_loss_post_2020_percent_of_baseline,
                "unit": "percent",
            },
            "baseline_forest_without_detected_loss_ha": {
                "value": self.baseline_forest_without_detected_loss_ha,
                "unit": "ha",
            },
            "evidence_start_year": {"value": self.evidence_start_year, "unit": "year"},
            "requested_end_year": {"value": self.requested_end_year, "unit": "year"},
            "effective_end_year": {"value": self.effective_end_year, "unit": "year"},
        }


@dataclass(frozen=True)
class JrcPost2020AnalysisResult:
    metrics: JrcPost2020Metrics
    baseline_metadata: BaselineProviderMetadata
    loss_metadata: LossDatasetMetadata
    summary_path: Path
    baseline_mask_path: Path
    loss_mask_path: Path
    debug_path: Path
    evidence_gaps: list[dict[str, Any]]
    grid: dict[str, Any]


def compute_jrc_post2020_loss(
    *,
    aoi_geojson_path: Path,
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
) -> JrcPost2020AnalysisResult:
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
    aoi_area_ha = _round6(float(np.count_nonzero(zone_mask)) * pixel_area_ha)

    baseline_nodata = zone_mask & (~baseline_valid)
    loss_nodata = zone_mask & (~loss_valid)
    baseline_forest = zone_mask & baseline_valid & (baseline_values == forest_value)
    loss_range = (
        zone_mask
        & loss_valid
        & (loss_values >= 21)
        & (loss_values <= end_code)
    )
    loss_on_baseline = baseline_forest & loss_range

    baseline_area = _round6(float(np.count_nonzero(baseline_forest)) * pixel_area_ha)
    loss_area = _round6(float(np.count_nonzero(loss_on_baseline)) * pixel_area_ha)
    baseline_without_loss = _round6(max(baseline_area - loss_area, 0.0))

    metrics = JrcPost2020Metrics(
        aoi_area_ha=aoi_area_ha,
        forest_baseline_2020_ha=baseline_area,
        forest_baseline_2020_percent_of_aoi=_percent(baseline_area, aoi_area_ha),
        forest_loss_post_2020_on_baseline_ha=loss_area,
        forest_loss_post_2020_percent_of_aoi=_percent(loss_area, aoi_area_ha),
        forest_loss_post_2020_percent_of_baseline=_percent(loss_area, baseline_area),
        baseline_forest_without_detected_loss_ha=baseline_without_loss,
        evidence_start_year=2021,
        requested_end_year=requested_end_year,
        effective_end_year=effective_end_year,
    )

    evidence_gaps = _evidence_gaps(
        requested_end_year=requested_end_year,
        effective_end_year=effective_end_year,
        baseline_nodata_pixels=int(np.count_nonzero(baseline_nodata)),
        loss_nodata_pixels=int(np.count_nonzero(loss_nodata)),
        aoi_pixels=int(np.count_nonzero(zone_mask)),
        baseline_info=baseline_info,
        loss_info=loss_info,
        baseline_metadata=baseline_metadata,
        loss_metadata=loss_metadata,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"2021_{effective_end_year}"
    baseline_mask_path = output_dir / "jrc_forest_2020_mask.geojson"
    loss_mask_path = output_dir / f"forest_loss_{suffix}_on_jrc_forest_2020_mask.geojson"
    debug_path = output_dir / "jrc_post2020_loss_debug.json"
    summary_path = output_dir / f"jrc_post2020_loss_{suffix}_summary.json"

    _write_mask_geojson(
        baseline_mask_path,
        baseline_forest,
        target_transform,
        target_crs,
    )
    _write_mask_geojson(
        loss_mask_path,
        loss_on_baseline,
        target_transform,
        target_crs,
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
            "jrc_forest_2020": int(np.count_nonzero(baseline_forest)),
            "loss_on_jrc_forest_2020": int(np.count_nonzero(loss_on_baseline)),
        },
        "nodata_pixels_inside_aoi": {
            "jrc_gfc2020": int(np.count_nonzero(baseline_nodata)),
            "hansen_lossyear": int(np.count_nonzero(loss_nodata)),
        },
        "source_alignment": {
            "baseline": baseline_info,
            "loss": loss_info,
        },
    }
    write_json(debug_path, debug)

    summary = {
        "analysis_id": "post_2020_loss_on_jrc_gfc2020_baseline_v1",
        "formula": (
            "AOI AND jrc_forest_2020 AND hansen_lossyear >= 21 "
            "AND hansen_lossyear <= effective_end_year - 2000"
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

    return JrcPost2020AnalysisResult(
        metrics=metrics,
        baseline_metadata=baseline_metadata,
        loss_metadata=loss_metadata,
        summary_path=summary_path,
        baseline_mask_path=baseline_mask_path,
        loss_mask_path=loss_mask_path,
        debug_path=debug_path,
        evidence_gaps=evidence_gaps,
        grid=grid,
    )


def build_hansen_lossyear_metadata(
    *,
    raster_path: Path,
    dataset_version: str,
    latest_available_year: int,
    processed_at_utc: str,
    source_url: str,
    asset_identifier: str,
) -> LossDatasetMetadata:
    native_crs: str | None = None
    if raster_path.is_file():
        with rasterio.open(raster_path) as ds:
            native_crs = ds.crs.to_string() if ds.crs is not None else None
    checksum = compute_sha256(raster_path) if raster_path.is_file() else None
    return LossDatasetMetadata(
        provider_id="local_hansen_lossyear",
        dataset_title="Hansen Global Forest Change lossyear",
        dataset_id="hansen_gfc",
        asset_identifier=asset_identifier,
        dataset_version=dataset_version,
        source_url=source_url,
        band="lossyear",
        start_year=2001,
        latest_available_year=latest_available_year,
        native_crs=native_crs,
        retrieved_or_processed_at_utc=processed_at_utc,
        checksum=checksum,
        source_fingerprint=checksum,
        local_path=raster_path.as_posix(),
    )


def _load_aoi_geometry(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("type") == "FeatureCollection":
        geoms = [shape(f["geometry"]) for f in payload.get("features", []) if f.get("geometry")]
        if not geoms:
            raise ValueError("AOI GeoJSON FeatureCollection has no features")
        return mapping(unary_union(geoms))
    if payload.get("type") == "Feature":
        return payload["geometry"]
    if payload.get("type"):
        return payload
    raise ValueError("Unsupported AOI GeoJSON")


def _target_grid(
    geom_wgs84: dict[str, Any],
    *,
    target_crs: str,
    resolution: float,
) -> tuple[rasterio.Affine, int, int, tuple[float, float, float, float]]:
    minx, miny, maxx, maxy = shape(geom_wgs84).bounds
    left, bottom, right, top = transform_bounds(
        "EPSG:4326",
        target_crs,
        minx,
        miny,
        maxx,
        maxy,
        densify_pts=21,
    )
    left = math.floor(left / resolution) * resolution
    bottom = math.floor(bottom / resolution) * resolution
    right = math.ceil(right / resolution) * resolution
    top = math.ceil(top / resolution) * resolution
    width = max(1, int(round((right - left) / resolution)))
    height = max(1, int(round((top - bottom) / resolution)))
    return from_origin(left, top, resolution, resolution), width, height, (left, bottom, right, top)


def _rasterize_aoi(
    geom_wgs84: dict[str, Any],
    *,
    target_crs: str,
    out_shape: tuple[int, int],
    transform: rasterio.Affine,
    all_touched: bool,
) -> np.ndarray:
    from rasterio.features import rasterize

    geom_target = transform_geom("EPSG:4326", target_crs, geom_wgs84)
    burned = rasterize(
        [(geom_target, 1)],
        out_shape=out_shape,
        transform=transform,
        fill=0,
        dtype=np.uint8,
        all_touched=all_touched,
    )
    return burned.astype(bool)


def _reproject_categorical(
    raster_path: Path,
    *,
    target_crs: str,
    target_transform: rasterio.Affine,
    width: int,
    height: int,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    nodata_value = -32768
    destination = np.full((height, width), nodata_value, dtype=np.int16)
    valid_destination = np.zeros((height, width), dtype=np.uint8)
    with rasterio.open(raster_path) as ds:
        band = ds.read(1, masked=True)
        valid = (~np.ma.getmaskarray(band)).astype(np.uint8)
        values = np.ma.filled(band.astype(np.int16), nodata_value).astype(np.int16)
        reproject(
            source=values,
            destination=destination,
            src_transform=ds.transform,
            src_crs=ds.crs,
            src_nodata=nodata_value,
            dst_transform=target_transform,
            dst_crs=target_crs,
            dst_nodata=nodata_value,
            resampling=Resampling.nearest,
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
            "resampling": "nearest",
            "grid_alignment_transformation_applied": transform_applied,
        }
    return destination, valid_destination.astype(bool), info


def _write_mask_geojson(
    path: Path,
    mask: np.ndarray,
    transform: rasterio.Affine,
    crs: str,
) -> None:
    features: list[dict[str, Any]] = []
    for geom, value in shapes(mask.astype(np.uint8), mask=mask, transform=transform):
        if value != 1:
            continue
        geom_wgs84 = _round_coords(transform_geom(crs, "EPSG:4326", geom))
        features.append({"type": "Feature", "properties": {}, "geometry": geom_wgs84})
    ordered = sorted(features, key=lambda f: json.dumps(f["geometry"], sort_keys=True))
    write_json(path, {"type": "FeatureCollection", "features": ordered})


def _round_coords(value: Any, precision: int = 7) -> Any:
    if isinstance(value, float):
        return round(value, precision)
    if isinstance(value, list):
        return [_round_coords(item, precision) for item in value]
    if isinstance(value, tuple):
        return tuple(_round_coords(item, precision) for item in value)
    if isinstance(value, dict):
        return {key: _round_coords(item, precision) for key, item in value.items()}
    return value


def _percent(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return 0.0
    return _round6(numerator / denominator * 100.0)


def _round6(value: float) -> float:
    return round(float(value), 6)


def _evidence_gaps(
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
                "code": "jrc_nodata_inside_aoi",
                "severity": "warning",
                "message": "JRC GFC2020 nodata pixels inside AOI were excluded from forest baseline metrics.",
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
