from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Iterable


def _iter_coords(obj: object) -> Iterable[tuple[float, float]]:
    if isinstance(obj, (list, tuple)):
        if len(obj) >= 2 and isinstance(obj[0], (int, float)) and isinstance(obj[1], (int, float)):
            yield float(obj[0]), float(obj[1])
        else:
            for item in obj:
                yield from _iter_coords(item)
    elif isinstance(obj, dict):
        if "coordinates" in obj:
            yield from _iter_coords(obj.get("coordinates"))
        elif obj.get("type") == "FeatureCollection":
            for feat in obj.get("features", []):
                if isinstance(feat, dict):
                    yield from _iter_coords(feat.get("geometry", {}))
        elif obj.get("type") == "Feature":
            yield from _iter_coords(obj.get("geometry", {}))
        elif obj.get("type") == "GeometryCollection":
            for geom in obj.get("geometries", []):
                if isinstance(geom, dict):
                    yield from _iter_coords(geom)


def load_aoi_bbox(aoi_geojson_path: Path) -> tuple[float, float, float, float]:
    data = json.loads(aoi_geojson_path.read_text(encoding="utf-8"))
    coords = list(_iter_coords(data))
    if not coords:
        raise ValueError("AOI GeoJSON contains no coordinates")
    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    return min(xs), min(ys), max(xs), max(ys)


def _band_start(value: float, band_size: int = 10) -> int:
    return int(math.floor(value / band_size) * band_size)


def _lat_band_start(value: float, band_size: int = 10) -> int:
    return int(math.ceil(value / band_size) * band_size)


def _band_range(min_value: float, max_value: float, band_size: int = 10) -> list[int]:
    min_band = _band_start(min_value, band_size)
    max_band = _band_start(max_value - 1e-9, band_size)
    return list(range(min_band, max_band + band_size, band_size))


def _lat_band_range(min_value: float, max_value: float, band_size: int = 10) -> list[int]:
    min_band = _lat_band_start(min_value + 1e-9, band_size)
    max_band = _lat_band_start(max_value - 1e-9, band_size)
    return list(range(min_band, max_band + band_size, band_size))


def _format_lat_band(lat: int) -> str:
    prefix = "N" if lat >= 0 else "S"
    return f"{prefix}{abs(lat):02d}"


def _format_lon_band(lon: int) -> str:
    prefix = "E" if lon >= 0 else "W"
    return f"{prefix}{abs(lon):03d}"


def hansen_tile_ids_for_bbox(bbox: tuple[float, float, float, float]) -> list[str]:
    minx, miny, maxx, maxy = bbox
    lat_bands = _lat_band_range(miny, maxy)
    lon_bands = _band_range(minx, maxx)

    tile_ids = [
        f"{_format_lat_band(lat)}_{_format_lon_band(lon)}" for lat in lat_bands for lon in lon_bands
    ]
    return sorted(tile_ids)
