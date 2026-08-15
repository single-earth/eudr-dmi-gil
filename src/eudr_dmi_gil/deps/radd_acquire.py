"""Deterministic acquisition of RADD (Sentinel-1 SAR) alert rasters from Earth Engine.

Mirrors the acquisition/execution split used elsewhere in this package (see
`eudr_dmi_gil.deps.fdp_coffee_acquire`, `eudr_dmi_gil.deps.tmf_acquire`): acquisition is a
separate, explicitly opt-in step from analysis (`eudr_dmi_gil.analysis.radd_alerts`), which
always runs against a pinned local raster. `RaddCollectionAdapter` keeps orchestration
unit-testable with a fake adapter and no live GEE credentials; `EarthEngineRaddAdapter` is the
real, opt-in-only implementation.

`projects/radar-wur/raddalert/v1` is **not band-homogeneous** (live-verified 2026-08-15,
project `myproject-gq-74696`): it mixes dated alert tiles (bands `Alert`, `Date`) with
single-band (`constant`) forest-domain/baseline images (e.g.
`africa_radd_primaryhumidtropicalforest_baseline2018_v20201206`). This module filters tiles by
their actual reported band set (`{"Alert", "Date"}`), the same way
`eudr_dmi_gil.deps.fdp_coffee_acquire.select_deterministic_images` filters/orders FDP images —
never by asset-id substring matching alone, which would be brittle against future naming.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from eudr_dmi_gil.analysis.radd_alerts import RADD_COLLECTION_ID
from eudr_dmi_gil.reports.determinism import sha256_file, write_json

DEFAULT_SCALE_M = 10.0
ALERT_BAND_SET = frozenset({"Alert", "Date"})


@dataclass(frozen=True)
class RaddTileDescriptor:
    image_id: str
    system_index: str
    geography: str | None
    version_date: str | None
    bands: tuple[str, ...]


def select_alert_tiles(tiles: list[RaddTileDescriptor]) -> list[RaddTileDescriptor]:
    """Keep only dated alert tiles (Alert+Date bands); drop domain/baseline images.

    Ordered deterministically by `system_index`, never by unspecified collection order.
    """
    alert_tiles = [t for t in tiles if set(t.bands) == ALERT_BAND_SET]
    return sorted(alert_tiles, key=lambda t: t.system_index)


class RaddCollectionAdapter(Protocol):
    def list_tiles(
        self,
        *,
        geography: str | None,
        start_date: str | None,
        end_date: str | None,
        aoi_geojson: dict[str, Any],
    ) -> list[RaddTileDescriptor]:
        ...

    def export_alert_geotiff(
        self,
        *,
        tile_ids_ordered: list[str],
        aoi_geojson: dict[str, Any],
        scale_m: float,
        alert_out_path: Path,
        date_out_path: Path,
    ) -> dict[str, Any]:
        ...


def _aoi_geometry_for_ee(aoi_geojson: dict[str, Any]) -> Any:
    import ee

    if aoi_geojson.get("type") == "FeatureCollection":
        features = aoi_geojson.get("features", [])
        if len(features) != 1:
            raise ValueError(f"RADD acquisition expects exactly one AOI feature; got {len(features)}")
        geometry = features[0]["geometry"]
    elif aoi_geojson.get("type") == "Feature":
        geometry = aoi_geojson["geometry"]
    else:
        geometry = aoi_geojson
    return ee.Geometry(geometry)


class EarthEngineRaddAdapter:
    """Real Earth Engine-backed `RaddCollectionAdapter`. Requires network + GEE auth."""

    def __init__(self, *, project: str) -> None:
        self.project = project
        self._initialized = False

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        import ee

        try:
            ee.Initialize(project=self.project)
        except Exception:
            ee.Authenticate()
            ee.Initialize(project=self.project)
        self._initialized = True

    def list_tiles(
        self,
        *,
        geography: str | None,
        start_date: str | None,
        end_date: str | None,
        aoi_geojson: dict[str, Any],
    ) -> list[RaddTileDescriptor]:
        self._ensure_initialized()
        import ee

        geometry = _aoi_geometry_for_ee(aoi_geojson)
        collection = ee.ImageCollection(RADD_COLLECTION_ID).filterBounds(geometry)
        if geography:
            collection = collection.filter(ee.Filter.eq("geography", geography))
        if start_date:
            collection = collection.filterDate(start_date, end_date or "2100-01-01")
        info = collection.getInfo()
        descriptors = []
        for feature in info.get("features", []):
            props = feature.get("properties", {}) or {}
            bands = feature.get("bands", []) or []
            descriptors.append(
                RaddTileDescriptor(
                    image_id=str(feature["id"]),
                    system_index=str(props.get("system:index", feature["id"])),
                    geography=props.get("geography"),
                    version_date=props.get("version_date"),
                    bands=tuple(b.get("id", "") for b in bands),
                )
            )
        return descriptors

    def export_alert_geotiff(
        self,
        *,
        tile_ids_ordered: list[str],
        aoi_geojson: dict[str, Any],
        scale_m: float,
        alert_out_path: Path,
        date_out_path: Path,
    ) -> dict[str, Any]:
        self._ensure_initialized()
        import urllib.request

        import ee

        geometry = _aoi_geometry_for_ee(aoi_geojson)
        images = [ee.Image(image_id) for image_id in tile_ids_ordered]
        mosaic = images[0] if len(images) == 1 else ee.ImageCollection(images).mosaic()
        clipped = mosaic.clip(geometry)

        dims: dict[str, list[int]] = {}
        for band, out_path in (("Alert", alert_out_path), ("Date", date_out_path)):
            band_image = clipped.select([band]).toInt32()
            url = band_image.getDownloadURL(
                {"region": geometry, "scale": scale_m, "crs": "EPSG:4326", "format": "GEO_TIFF"}
            )
            with urllib.request.urlopen(url) as response:  # noqa: S310
                data = response.read()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(data)

            import rasterio

            with rasterio.open(out_path) as ds:
                dims[band] = [ds.width, ds.height]

        return {"output_dimensions": dims}


def acquire_radd_alerts(
    *,
    adapter: RaddCollectionAdapter,
    aoi_geojson_path: Path,
    out_dir: Path,
    geography: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    scale_m: float = DEFAULT_SCALE_M,
    access_timestamp_utc: str | None = None,
) -> dict[str, Any]:
    """Acquire, filter-to-alert-tiles, export, and pin one AOI's RADD alert evidence."""
    aoi_geojson = json.loads(aoi_geojson_path.read_text(encoding="utf-8"))

    all_tiles = adapter.list_tiles(
        geography=geography, start_date=start_date, end_date=end_date, aoi_geojson=aoi_geojson
    )
    alert_tiles = select_alert_tiles(all_tiles)
    dropped_non_alert = [t.image_id for t in all_tiles if t not in alert_tiles]

    out_dir.mkdir(parents=True, exist_ok=True)
    alert_out_path = out_dir / "radd_alert.tif"
    date_out_path = out_dir / "radd_date.tif"

    coverage_warning: str | None = None
    if not alert_tiles:
        coverage_warning = (
            "No RADD alert tiles (Alert+Date bands) intersect this AOI for the requested "
            f"geography={geography!r} / date window; RADD coverage is geography-dependent."
        )
        metadata = {
            "collection_id": RADD_COLLECTION_ID,
            "geography": geography,
            "status": "no_alert_tile_coverage",
            "tile_ids": [],
            "dropped_non_alert_tile_ids": dropped_non_alert,
            "date_window_start": start_date,
            "date_window_end": end_date,
            "access_timestamp_utc": access_timestamp_utc,
            "coverage_warning": coverage_warning,
            "aoi_geojson_path": aoi_geojson_path.as_posix(),
            "aoi_geojson_sha256": sha256_file(aoi_geojson_path),
        }
        write_json(out_dir / "radd_acquisition_metadata.json", metadata)
        return metadata

    tile_ids = [t.image_id for t in alert_tiles]
    export_detail = adapter.export_alert_geotiff(
        tile_ids_ordered=tile_ids,
        aoi_geojson=aoi_geojson,
        scale_m=scale_m,
        alert_out_path=alert_out_path,
        date_out_path=date_out_path,
    )

    metadata = {
        "collection_id": RADD_COLLECTION_ID,
        "geography": geography,
        "status": "exported",
        "tile_ids": tile_ids,
        "dropped_non_alert_tile_ids": dropped_non_alert,
        "date_window_start": start_date,
        "date_window_end": end_date,
        "scale_m": scale_m,
        "output_dimensions": export_detail.get("output_dimensions"),
        "alert_raster_path": alert_out_path.as_posix(),
        "alert_raster_sha256": sha256_file(alert_out_path),
        "date_raster_path": date_out_path.as_posix(),
        "date_raster_sha256": sha256_file(date_out_path),
        "access_timestamp_utc": access_timestamp_utc,
        "coverage_warning": coverage_warning,
        "aoi_geojson_path": aoi_geojson_path.as_posix(),
        "aoi_geojson_sha256": sha256_file(aoi_geojson_path),
    }
    write_json(out_dir / "radd_acquisition_metadata.json", metadata)
    return metadata
