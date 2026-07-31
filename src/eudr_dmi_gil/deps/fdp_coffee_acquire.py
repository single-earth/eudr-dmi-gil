"""Deterministic acquisition of Forest Data Partnership (FDP) coffee-probability rasters.

This module separates *source acquisition* (from a Google Earth Engine `ee.ImageCollection`)
from *analysis execution* (which always runs against a pinned local raster, matching this
counterpart's existing deterministic-pinned-raster convention used for Hansen tiles). The
acquisition step:

  1. filters the configured `ee.ImageCollection` to a half-open `[start, end)` date interval
     and to the AOI;
  2. resolves multiple candidate images deterministically (sorted by `system:index`, never
     relying on unspecified collection iteration order);
  3. mosaics (in that explicit order) and clips to the AOI;
  4. exports the continuous probability band as float32, preserving nodata;
  5. validates the exported values are finite and within `[0, 1]` (refusing to silently clamp
     out-of-range values);
  6. writes acquisition metadata (asset id, exact source image ids, date interval, band,
     reduction, projection, scale, access timestamp, output size, SHA-256).

The Earth Engine collection lookup/export is behind the `FdpCollectionAdapter` protocol so the
deterministic-ordering logic (`select_deterministic_images`) and the acquisition orchestration
(`acquire_fdp_coffee_probability`) can be unit-tested with a fake adapter, with no live GEE
credentials required. `EarthEngineFdpAdapter` is the real, opt-in-only implementation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import numpy as np

from eudr_dmi_gil.reports.determinism import sha256_file, write_json

DEFAULT_SCALE_M = 10.0
SOURCE_ACQUISITION_ENGINE = "gee"
EXECUTION_ENGINE = "local-pinned-raster"


@dataclass(frozen=True)
class FdpImageDescriptor:
    image_id: str
    system_index: str
    system_time_start_ms: int | None
    bands: tuple[str, ...]
    crs: str | None
    crs_transform: tuple[float, ...] | None
    dimensions: tuple[int, int] | None


class FdpCollectionAdapter(Protocol):
    """Abstraction over the Earth Engine collection lookup + export, for deterministic testing."""

    def list_images(
        self,
        *,
        asset_id: str,
        start_date: str,
        end_date: str,
        aoi_geojson: dict[str, Any],
    ) -> list[FdpImageDescriptor]:
        raise NotImplementedError

    def export_probability_geotiff(
        self,
        *,
        asset_id: str,
        image_ids_ordered: list[str],
        band: str,
        aoi_geojson: dict[str, Any],
        scale_m: float,
        out_path: Path,
    ) -> dict[str, Any]:
        raise NotImplementedError


def select_deterministic_images(
    images: list[FdpImageDescriptor],
) -> list[FdpImageDescriptor]:
    """Resolve which image(s) to use, deterministically.

    A single image is used as-is. Multiple images are returned sorted by `system:index` (a
    stable, recorded property) so mosaicking order is explicit and reproducible rather than
    relying on the collection's unspecified iteration order. Raises if the filter returned zero
    images: acquisition must fail rather than silently substituting no data.
    """
    if not images:
        raise ValueError("FDP collection filter returned zero images for this AOI/date window")
    return sorted(images, key=lambda img: img.system_index)


def _validate_probability_raster(path: Path) -> None:
    import rasterio

    with rasterio.open(path) as ds:
        band = ds.read(1, masked=True)
    values = np.ma.compressed(band)
    if values.size == 0:
        raise ValueError(f"Exported probability raster {path} has no valid pixels")
    if not np.isfinite(values).all():
        raise ValueError(f"Exported probability raster {path} contains non-finite values")
    out_of_range = values[(values < 0.0) | (values > 1.0)]
    if out_of_range.size:
        raise ValueError(
            f"Exported probability raster {path} contains {out_of_range.size} value(s) "
            f"outside [0, 1] (min={float(values.min())!r}, max={float(values.max())!r}); "
            "refusing to silently clamp material out-of-range probability values."
        )


def _access_timestamp(override: str | None) -> str:
    if override:
        return override
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def acquire_fdp_coffee_probability(
    *,
    adapter: FdpCollectionAdapter,
    asset_id: str,
    observation_year: int,
    band: str,
    aoi_geojson_path: Path,
    out_raster_path: Path,
    out_metadata_path: Path,
    scale_m: float = DEFAULT_SCALE_M,
    access_timestamp_utc: str | None = None,
) -> dict[str, Any]:
    """Acquire, export, validate, and pin one AOI-year's FDP coffee-probability raster."""
    aoi_geojson = json.loads(aoi_geojson_path.read_text(encoding="utf-8"))
    start_date = f"{observation_year}-01-01"
    end_date = f"{observation_year + 1}-01-01"

    images = adapter.list_images(
        asset_id=asset_id,
        start_date=start_date,
        end_date=end_date,
        aoi_geojson=aoi_geojson,
    )
    ordered = select_deterministic_images(images)
    ordered_ids = [img.image_id for img in ordered]

    out_raster_path.parent.mkdir(parents=True, exist_ok=True)
    export_detail = adapter.export_probability_geotiff(
        asset_id=asset_id,
        image_ids_ordered=ordered_ids,
        band=band,
        aoi_geojson=aoi_geojson,
        scale_m=scale_m,
        out_path=out_raster_path,
    )

    _validate_probability_raster(out_raster_path)

    metadata = {
        "asset_id": asset_id,
        "source_acquisition_engine": SOURCE_ACQUISITION_ENGINE,
        "execution_engine": EXECUTION_ENGINE,
        "observation_year": observation_year,
        "date_interval": {"start": start_date, "end": end_date, "kind": "half_open"},
        "band": band,
        "collection_size_in_window": len(images),
        "source_image_ids": sorted(img.image_id for img in images),
        "selection_order": ordered_ids,
        "reduction": (
            "single_image" if len(ordered) == 1 else "mosaic_ordered_by_system_index"
        ),
        "scale_m": scale_m,
        "projection": export_detail.get("projection"),
        "output_dimensions": export_detail.get("output_dimensions"),
        "access_timestamp_utc": _access_timestamp(access_timestamp_utc),
        "output_path": out_raster_path.as_posix(),
        "output_size_bytes": out_raster_path.stat().st_size,
        "output_sha256": sha256_file(out_raster_path),
        "aoi_geojson_path": aoi_geojson_path.as_posix(),
        "aoi_geojson_sha256": sha256_file(aoi_geojson_path),
    }
    write_json(out_metadata_path, metadata)
    return metadata


def _aoi_geometry_for_ee(aoi_geojson: dict[str, Any]) -> Any:
    import ee

    if aoi_geojson.get("type") == "FeatureCollection":
        features = aoi_geojson.get("features", [])
        if len(features) != 1:
            raise ValueError(
                "FDP acquisition expects exactly one AOI feature; "
                f"got {len(features)}"
            )
        geometry = features[0]["geometry"]
    elif aoi_geojson.get("type") == "Feature":
        geometry = aoi_geojson["geometry"]
    else:
        geometry = aoi_geojson
    return ee.Geometry(geometry)


class EarthEngineFdpAdapter:
    """Real Earth Engine-backed `FdpCollectionAdapter`. Requires network + GEE auth."""

    def __init__(
        self,
        *,
        project: str,
        service_account: str | None = None,
        key_file: str | None = None,
    ) -> None:
        self.project = project
        self.service_account = service_account
        self.key_file = key_file
        self._initialized = False

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        import ee

        if self.service_account:
            if not self.key_file:
                raise ValueError("service_account given but no key_file")
            credentials = ee.ServiceAccountCredentials(self.service_account, key_file=self.key_file)
            ee.Initialize(credentials, project=self.project)
        else:
            try:
                ee.Initialize(project=self.project)
            except Exception:
                ee.Authenticate()
                ee.Initialize(project=self.project)
        self._initialized = True

    def list_images(
        self,
        *,
        asset_id: str,
        start_date: str,
        end_date: str,
        aoi_geojson: dict[str, Any],
    ) -> list[FdpImageDescriptor]:
        self._ensure_initialized()
        import ee

        geometry = _aoi_geometry_for_ee(aoi_geojson)
        collection = (
            ee.ImageCollection(asset_id)
            .filterDate(start_date, end_date)
            .filterBounds(geometry)
        )
        info = collection.getInfo()
        descriptors: list[FdpImageDescriptor] = []
        for feature in info.get("features", []):
            props = feature.get("properties", {}) or {}
            bands = feature.get("bands", []) or []
            band0 = bands[0] if bands else {}
            crs_transform = band0.get("crs_transform")
            dimensions = band0.get("dimensions")
            descriptors.append(
                FdpImageDescriptor(
                    image_id=str(feature["id"]),
                    system_index=str(props.get("system:index", feature["id"])),
                    system_time_start_ms=props.get("system:time_start"),
                    bands=tuple(b.get("id", "") for b in bands),
                    crs=band0.get("crs"),
                    crs_transform=tuple(crs_transform) if crs_transform else None,
                    dimensions=tuple(dimensions) if dimensions else None,
                )
            )
        return descriptors

    def export_probability_geotiff(
        self,
        *,
        asset_id: str,
        image_ids_ordered: list[str],
        band: str,
        aoi_geojson: dict[str, Any],
        scale_m: float,
        out_path: Path,
    ) -> dict[str, Any]:
        self._ensure_initialized()
        import urllib.request

        import ee

        geometry = _aoi_geometry_for_ee(aoi_geojson)
        images = [ee.Image(image_id).select([band]) for image_id in image_ids_ordered]
        if len(images) == 1:
            probability_image = images[0]
        else:
            probability_image = ee.ImageCollection(images).mosaic()
        clipped = probability_image.clip(geometry).toFloat()

        projection_info = images[0].select([band]).projection().getInfo()

        url = clipped.getDownloadURL(
            {
                "region": geometry,
                "scale": scale_m,
                "crs": "EPSG:4326",
                "format": "GEO_TIFF",
            }
        )
        with urllib.request.urlopen(url) as response:  # noqa: S310
            data = response.read()
        out_path.write_bytes(data)

        import rasterio

        with rasterio.open(out_path) as ds:
            output_dimensions = [ds.width, ds.height]

        return {
            "projection": projection_info,
            "output_dimensions": output_dimensions,
        }
