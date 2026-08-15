"""Deterministic acquisition of JRC Tropical Moist Forest (TMF) rasters from Earth Engine.

Mirrors the acquisition/execution split already used for FDP coffee probability
(`eudr_dmi_gil.deps.fdp_coffee_acquire`): acquisition (mosaic regional TMF tiles, clip to AOI,
export GeoTIFF, freeze metadata) is a separate, explicitly opt-in step from analysis
(`eudr_dmi_gil.analysis.tmf_change`), which always runs against a pinned local raster. The
`TmfCollectionAdapter` protocol keeps the orchestration logic unit-testable with a fake adapter
and no live GEE credentials; `EarthEngineTmfAdapter` is the real, opt-in-only implementation.

Live-verified (2026-08-15, project `myproject-gq-74696`) TMF v1_2025 layer asset ids/bands are
documented in `eudr_dmi_gil.analysis.tmf_change`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from eudr_dmi_gil.analysis.tmf_change import (
    TMF_DATASET_VERSION_DEFAULT,
    TMF_DEGRADATION_ASSET_ID,
    TMF_DEFORESTATION_ASSET_ID,
    TMF_DURATION_ASSET_ID,
    TMF_INTENSITY_ASSET_ID,
)
from eudr_dmi_gil.reports.determinism import sha256_file, write_json

DEFAULT_SCALE_M = 30.0

TMF_LAYERS: dict[str, tuple[str, str]] = {
    "deforestation": (TMF_DEFORESTATION_ASSET_ID, "constant"),
    "degradation": (TMF_DEGRADATION_ASSET_ID, "constant"),
    "duration": (TMF_DURATION_ASSET_ID, "constant"),
    "intensity": (TMF_INTENSITY_ASSET_ID, "sum"),
}


@dataclass(frozen=True)
class TmfTileDescriptor:
    image_id: str
    system_index: str
    bands: tuple[str, ...]


class TmfCollectionAdapter(Protocol):
    def list_tiles(self, *, asset_id: str, aoi_geojson: dict[str, Any]) -> list[TmfTileDescriptor]:
        ...

    def export_layer_geotiff(
        self,
        *,
        asset_id: str,
        band: str,
        tile_ids_ordered: list[str],
        aoi_geojson: dict[str, Any],
        scale_m: float,
        out_path: Path,
    ) -> dict[str, Any]:
        ...


def _aoi_geometry_for_ee(aoi_geojson: dict[str, Any]) -> Any:
    import ee

    if aoi_geojson.get("type") == "FeatureCollection":
        features = aoi_geojson.get("features", [])
        if len(features) != 1:
            raise ValueError(f"TMF acquisition expects exactly one AOI feature; got {len(features)}")
        geometry = features[0]["geometry"]
    elif aoi_geojson.get("type") == "Feature":
        geometry = aoi_geojson["geometry"]
    else:
        geometry = aoi_geojson
    return ee.Geometry(geometry)


class EarthEngineTmfAdapter:
    """Real Earth Engine-backed `TmfCollectionAdapter`. Requires network + GEE auth."""

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
        self, *, asset_id: str, aoi_geojson: dict[str, Any]
    ) -> list[TmfTileDescriptor]:
        self._ensure_initialized()
        import ee

        geometry = _aoi_geometry_for_ee(aoi_geojson)
        collection = ee.ImageCollection(asset_id).filterBounds(geometry)
        info = collection.getInfo()
        descriptors = []
        for feature in info.get("features", []):
            props = feature.get("properties", {}) or {}
            bands = feature.get("bands", []) or []
            descriptors.append(
                TmfTileDescriptor(
                    image_id=str(feature["id"]),
                    system_index=str(props.get("system:index", feature["id"])),
                    bands=tuple(b.get("id", "") for b in bands),
                )
            )
        return sorted(descriptors, key=lambda d: d.system_index)

    def export_layer_geotiff(
        self,
        *,
        asset_id: str,
        band: str,
        tile_ids_ordered: list[str],
        aoi_geojson: dict[str, Any],
        scale_m: float,
        out_path: Path,
    ) -> dict[str, Any]:
        self._ensure_initialized()
        import urllib.request

        import ee

        geometry = _aoi_geometry_for_ee(aoi_geojson)
        images = [ee.Image(image_id).select([band]) for image_id in tile_ids_ordered]
        mosaic = images[0] if len(images) == 1 else ee.ImageCollection(images).mosaic()
        clipped = mosaic.clip(geometry).toInt32()

        url = clipped.getDownloadURL(
            {"region": geometry, "scale": scale_m, "crs": "EPSG:4326", "format": "GEO_TIFF"}
        )
        with urllib.request.urlopen(url) as response:  # noqa: S310
            data = response.read()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(data)

        import rasterio

        with rasterio.open(out_path) as ds:
            output_dimensions = [ds.width, ds.height]

        return {"output_dimensions": output_dimensions}


def acquire_tmf_change(
    *,
    adapter: TmfCollectionAdapter,
    aoi_geojson_path: Path,
    out_dir: Path,
    scale_m: float = DEFAULT_SCALE_M,
    include_quality: bool = True,
    dataset_version: str = TMF_DATASET_VERSION_DEFAULT,
    access_timestamp_utc: str | None = None,
) -> dict[str, Any]:
    """Acquire, export, and pin TMF DeforestationYear/DegradationYear (+ optional quality)."""
    import json

    aoi_geojson = json.loads(aoi_geojson_path.read_text(encoding="utf-8"))

    layer_names = ["deforestation", "degradation"]
    if include_quality:
        layer_names += ["duration", "intensity"]

    out_dir.mkdir(parents=True, exist_ok=True)
    layer_metadata: dict[str, Any] = {}
    for layer_name in layer_names:
        asset_id, band = TMF_LAYERS[layer_name]
        tiles = adapter.list_tiles(asset_id=asset_id, aoi_geojson=aoi_geojson)
        if not tiles:
            layer_metadata[layer_name] = {
                "asset_id": asset_id,
                "band": band,
                "status": "no_tile_coverage",
                "tile_ids": [],
            }
            continue
        tile_ids = [t.image_id for t in tiles]
        out_path = out_dir / f"tmf_{layer_name}.tif"
        export_detail = adapter.export_layer_geotiff(
            asset_id=asset_id,
            band=band,
            tile_ids_ordered=tile_ids,
            aoi_geojson=aoi_geojson,
            scale_m=scale_m,
            out_path=out_path,
        )
        layer_metadata[layer_name] = {
            "asset_id": asset_id,
            "band": band,
            "status": "exported",
            "tile_ids": tile_ids,
            "scale_m": scale_m,
            "output_dimensions": export_detail.get("output_dimensions"),
            "output_path": out_path.as_posix(),
            "output_sha256": sha256_file(out_path),
        }

    metadata = {
        "dataset_version": dataset_version,
        "layers": layer_metadata,
        "access_timestamp_utc": access_timestamp_utc,
        "aoi_geojson_path": aoi_geojson_path.as_posix(),
        "aoi_geojson_sha256": sha256_file(aoi_geojson_path),
    }
    write_json(out_dir / "tmf_acquisition_metadata.json", metadata)
    return metadata
