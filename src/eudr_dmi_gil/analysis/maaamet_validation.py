from __future__ import annotations

import csv
import json
import os
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import logging

from pyproj import Geod
from shapely.geometry import shape, mapping
from shapely.ops import unary_union

from eudr_dmi_gil.reports.determinism import write_json

LOGGER = logging.getLogger(__name__)
WFS_TIMEOUT_SECONDS = int(os.environ.get("EUDR_DMI_MAAAMET_WFS_TIMEOUT", "60"))


@dataclass(frozen=True)
class ParcelRecord:
    parcel_id: str
    forest_area_ha: float | None
    reference_source: str
    reference_method: str


@dataclass(frozen=True)
class ParcelFeature:
    parcel_id: str
    forest_area_ha: float | None
    reference_source: str
    reference_method: str
    properties: dict[str, Any]
    geometry: dict[str, Any] | None
    pindala_m2: float | None
    geodesic_area_ha: float | None
    maaamet_land_area_ha: float | None
    maaamet_forest_area_ha: float | None
    hansen_land_area_ha: float | None
    hansen_forest_area_ha: float | None
    hansen_forest_loss_ha: float | None
    fields_considered: list[str]
    forest_area_key_used: str | None


@dataclass(frozen=True)
class MaaAmetCrosscheckResult:
    outcome: str
    tolerance_percent: float
    reason: str | None
    fields_used: list[str]
    reference_source: str
    reference_method: str
    reference_value_ha: float | None
    computed_forest_area_ha: float | None
    diff_pct: float | None
    csv_path: Path
    summary_path: Path
    top10_geojson_path: Path | None = None
    top10_csv_path: Path | None = None
    fields_inventory_path: Path | None = None
    parcel_ids: list[str] | None = None


@dataclass(frozen=True)
class MaaAmetTop10Result:
    parcels: list[ParcelFeature]
    parcels_all: list[ParcelRecord]
    parcel_ids: list[str]
    union_geom: dict[str, Any] | None
    fields_used: list[str]
    fields_inventory: dict[str, Any]
    geojson_path: Path
    csv_path: Path
    inventory_path: Path


class MaaAmetProvider:
    def fetch_parcels(self, *, aoi_geojson_path: Path) -> list[ParcelRecord]:
        raise NotImplementedError

    def fetch_parcel_features(self, *, aoi_geojson_path: Path) -> list[ParcelFeature]:
        return []


class LocalFileMaaAmetProvider(MaaAmetProvider):
    def __init__(self, path: Path) -> None:
        self._path = path

    def fetch_parcels(self, *, aoi_geojson_path: Path) -> list[ParcelRecord]:
        if not self._path.exists():
            return []
        if self._path.suffix.lower() == ".json":
            data = json.loads(self._path.read_text(encoding="utf-8"))
            return _parcels_from_json(data)
        if self._path.suffix.lower() == ".csv":
            rows = []
            with self._path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    forest_area_value = row.get("forest_area_ha")
                    forest_area_ha = (
                        float(forest_area_value)
                        if forest_area_value not in (None, "")
                        else None
                    )
                    rows.append(
                        ParcelRecord(
                            parcel_id=str(row.get("parcel_id")),
                            forest_area_ha=forest_area_ha,
                            reference_source="attribute:forest_area_ha"
                            if forest_area_ha is not None
                            else "missing",
                            reference_method="reported" if forest_area_ha is not None else "missing",
                        )
                    )
            return rows
        return []

    def fetch_parcel_features(self, *, aoi_geojson_path: Path) -> list[ParcelFeature]:
        if not self._path.exists() or self._path.suffix.lower() != ".json":
            return []
        data = json.loads(self._path.read_text(encoding="utf-8"))
        aoi_geom = _load_aoi_shape(aoi_geojson_path)
        return _analyze_parcels_from_geojson(data, aoi_geom)


class WfsMaaAmetProvider(MaaAmetProvider):
    def __init__(self, url: str, layer: str) -> None:
        self._url = url
        self._layer = layer

    def fetch_parcels(self, *, aoi_geojson_path: Path) -> list[ParcelRecord]:
        features = self.fetch_parcel_features(aoi_geojson_path=aoi_geojson_path)
        return [
            ParcelRecord(
                parcel_id=f.parcel_id,
                forest_area_ha=f.forest_area_ha,
                reference_source=f.reference_source,
                reference_method=f.reference_method,
            )
            for f in features
        ]

    def fetch_parcel_features(self, *, aoi_geojson_path: Path) -> list[ParcelFeature]:
        aoi_geom = _load_aoi_shape(aoi_geojson_path)
        minx, miny, maxx, maxy = aoi_geom.bounds
        params = {
            "service": "WFS",
            "request": "GetFeature",
            "version": "2.0.0",
            "typeName": self._layer,
            "outputFormat": "application/json",
            "srsName": "EPSG:4326",
            "bbox": f"{minx},{miny},{maxx},{maxy},EPSG:4326",
        }
        url = f"{self._url}?{urllib.parse.urlencode(params)}"
        LOGGER.info("Maa-amet WFS request: %s", url)
        print(f"Maa-amet WFS request: {url}", flush=True)
        try:
            response = urllib.request.urlopen(  # noqa: S310
                url, timeout=WFS_TIMEOUT_SECONDS
            )
        except TypeError:
            response = urllib.request.urlopen(url)  # noqa: S310
        with response as resp:
            payload = resp.read().decode("utf-8")
        print("Maa-amet WFS response received.", flush=True)
        data = json.loads(payload)
        return _analyze_parcels_from_geojson(data, aoi_geom)


def _geodesic_area_ha(geom: dict[str, Any]) -> float:
    geod = Geod(ellps="WGS84")
    area_m2, _ = geod.geometry_area_perimeter(shape(geom))
    return abs(float(area_m2)) / 10_000.0


def _load_aoi_shape(aoi_geojson_path: Path):
    data = json.loads(aoi_geojson_path.read_text(encoding="utf-8"))
    if data.get("type") == "FeatureCollection":
        geometries = [shape(feat["geometry"]) for feat in data.get("features", [])]
        if not geometries:
            raise ValueError("AOI GeoJSON FeatureCollection has no features")
        return unary_union(geometries)
    if data.get("type") == "Feature":
        return shape(data["geometry"])
    if "type" in data:
        return shape(data)
    raise ValueError("Unsupported AOI GeoJSON")


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _forest_related_keys(props: dict[str, Any]) -> list[str]:
    base = {"mets", "mets_ha", "pindala", "haritav", "rohumaa", "metsatyyp", "siht1"}
    keys = set()
    for key in props.keys():
        key_lower = str(key).lower()
        if key_lower in base:
            keys.add(str(key))
            continue
        if "mets" in key_lower or "forest" in key_lower:
            keys.add(str(key))
    return sorted(keys)


_HECTARE_FIELD_TOKEN_RE = re.compile(r"(?:^|[_\-])ha(?:$|[_\-])")


def _is_forest_hectare_field(key: str) -> bool:
    key_lower = key.lower()
    if "mets" not in key_lower and "forest" not in key_lower:
        return False
    if key_lower.endswith("ha"):
        return True
    if _HECTARE_FIELD_TOKEN_RE.search(key_lower):
        return True
    return False


def _normalize_forest_area_ha(
    *,
    area_value: float,
    area_key: str,
    maaamet_land_area_ha: float | None,
) -> tuple[float, str]:
    key_lower = area_key.lower()
    if "m2" in key_lower or key_lower.endswith("_m"):
        return area_value / 10_000.0, "reported_m2"

    if maaamet_land_area_ha is not None and area_value > maaamet_land_area_ha:
        converted = area_value / 10_000.0
        if converted <= maaamet_land_area_ha:
            return converted, "reported_m2_inferred"

    return area_value, "reported_ha"


def _analyze_parcel_feature(
    feature: dict[str, Any],
    *,
    aoi_geom,
    index: int,
) -> ParcelFeature | None:
    geom = feature.get("geometry")
    if not geom:
        return None
    parcel_geom = shape(geom)
    if not aoi_geom.covers(parcel_geom):
        return None
    props = dict(feature.get("properties") or {})
    parcel_id = str(
        props.get("parcel_id")
        or props.get("katastritunnus")
        or props.get("tunnus")
        or props.get("id")
        or f"parcel-{index}"
    )

    fields_considered = _forest_related_keys(props)
    geodesic_area_ha = _geodesic_area_ha(geom) if geom else None
    pindala_m2 = _to_float(props.get("pindala"))
    maaamet_land_area_ha: float | None = None
    if pindala_m2 is not None:
        maaamet_land_area_ha = pindala_m2 / 10_000.0
    elif geodesic_area_ha is not None:
        maaamet_land_area_ha = geodesic_area_ha

    mets_value = _to_float(props.get("mets"))
    mets_ha_value = _to_float(props.get("mets_ha"))
    forest_area_value = _to_float(props.get("forest_area_ha"))

    forest_area_ha: float | None = None
    forest_area_key_used: str | None = None
    reference_source = "missing"
    reference_method = "missing"

    if mets_value is not None:
        forest_area_ha, reference_method = _normalize_forest_area_ha(
            area_value=mets_value,
            area_key="mets_m2",
            maaamet_land_area_ha=maaamet_land_area_ha,
        )
        forest_area_key_used = "mets"
        reference_source = "attribute:mets"
    elif mets_ha_value is not None:
        forest_area_ha, reference_method = _normalize_forest_area_ha(
            area_value=mets_ha_value,
            area_key="mets_ha",
            maaamet_land_area_ha=maaamet_land_area_ha,
        )
        forest_area_key_used = "mets_ha"
        reference_source = "attribute:mets_ha"
    elif forest_area_value is not None:
        forest_area_ha, reference_method = _normalize_forest_area_ha(
            area_value=forest_area_value,
            area_key="forest_area_ha",
            maaamet_land_area_ha=maaamet_land_area_ha,
        )
        forest_area_key_used = "forest_area_ha"
        reference_source = "attribute:forest_area_ha"
    else:
        for key in fields_considered:
            if not _is_forest_hectare_field(key):
                continue
            candidate = _to_float(props.get(key))
            if candidate is None:
                continue
            forest_area_ha, reference_method = _normalize_forest_area_ha(
                area_value=candidate,
                area_key=str(key),
                maaamet_land_area_ha=maaamet_land_area_ha,
            )
            forest_area_key_used = str(key)
            reference_source = f"attribute:{key}"
            break

    if forest_area_ha is None:
        if geodesic_area_ha is not None:
            forest_area_ha = geodesic_area_ha
            forest_area_key_used = None
            reference_source = "geometry"
            reference_method = "geodesic_wgs84_pyproj"

    if forest_area_ha is not None and maaamet_land_area_ha is not None and forest_area_ha > maaamet_land_area_ha:
        forest_area_ha = maaamet_land_area_ha
        reference_method = "capped_to_land_area"

    return ParcelFeature(
        parcel_id=parcel_id,
        forest_area_ha=None if forest_area_ha is None else round(forest_area_ha, 6),
        reference_source=reference_source,
        reference_method=reference_method,
        properties=props,
        geometry=geom,
        pindala_m2=None if pindala_m2 is None else round(pindala_m2, 6),
        geodesic_area_ha=None if geodesic_area_ha is None else round(geodesic_area_ha, 6),
        maaamet_land_area_ha=None if maaamet_land_area_ha is None else round(maaamet_land_area_ha, 6),
        maaamet_forest_area_ha=None if forest_area_ha is None else round(forest_area_ha, 6),
        hansen_land_area_ha=None,
        hansen_forest_area_ha=None,
        hansen_forest_loss_ha=None,
        fields_considered=fields_considered,
        forest_area_key_used=forest_area_key_used,
    )


def _analyze_parcels_from_geojson(data: Any, aoi_geom) -> list[ParcelFeature]:
    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        return []
    parcels: list[ParcelFeature] = []
    for idx, feat in enumerate(data.get("features", []), start=1):
        if not isinstance(feat, dict):
            continue
        parcel = _analyze_parcel_feature(feat, aoi_geom=aoi_geom, index=idx)
        if parcel is not None:
            parcels.append(parcel)
    return parcels


def _select_top10(
    parcels: list[ParcelFeature],
    *,
    min_forest_ha: float = 0.0,
    prefer_hansen: bool = False,
) -> list[ParcelFeature]:
    def _forest_area(parcel: ParcelFeature) -> float:
        if prefer_hansen and parcel.hansen_forest_area_ha is not None:
            return parcel.hansen_forest_area_ha
        return parcel.forest_area_ha or 0.0

    def sort_key(parcel: ParcelFeature) -> tuple[float, float, str]:
        forest_area = _forest_area(parcel)
        pindala = parcel.pindala_m2 or 0.0
        geo_area = (parcel.geodesic_area_ha or 0.0) * 10_000.0
        tie = pindala if pindala > 0 else geo_area
        return (-forest_area, -tie, parcel.parcel_id)

    eligible = [p for p in parcels if _forest_area(p) >= min_forest_ha]
    return sorted(eligible, key=sort_key)[:10]


def _write_top10_geojson(path: Path, parcels: list[ParcelFeature], keys: list[str]) -> None:
    features: list[dict[str, Any]] = []
    for parcel in parcels:
        props = {
            "parcel_id": parcel.parcel_id,
            "forest_area_ha": parcel.forest_area_ha,
            "maaamet_land_area_ha": parcel.maaamet_land_area_ha,
            "maaamet_forest_area_ha": parcel.maaamet_forest_area_ha,
            "hansen_land_area_ha": parcel.hansen_land_area_ha,
            "hansen_forest_area_ha": parcel.hansen_forest_area_ha,
            "hansen_forest_loss_ha": parcel.hansen_forest_loss_ha,
        }
        for key in keys:
            if key in parcel.properties:
                props[key] = parcel.properties.get(key)
        features.append(
            {
                "type": "Feature",
                "properties": props,
                "geometry": parcel.geometry,
            }
        )
    write_json(path, {"type": "FeatureCollection", "features": features})


def _write_top10_csv(path: Path, parcels: list[ParcelFeature], keys: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "parcel_id",
        "forest_area_ha",
        "pindala_m2",
        "geodesic_area_ha",
        "maaamet_land_area_ha",
        "maaamet_forest_area_ha",
        "hansen_land_area_ha",
        "hansen_forest_area_ha",
        "hansen_forest_loss_ha",
    ] + keys
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for parcel in parcels:
            row = {
                "parcel_id": parcel.parcel_id,
                "forest_area_ha": parcel.forest_area_ha,
                "pindala_m2": parcel.pindala_m2,
                "geodesic_area_ha": parcel.geodesic_area_ha,
                "maaamet_land_area_ha": parcel.maaamet_land_area_ha,
                "maaamet_forest_area_ha": parcel.maaamet_forest_area_ha,
                "hansen_land_area_ha": parcel.hansen_land_area_ha,
                "hansen_forest_area_ha": parcel.hansen_forest_area_ha,
                "hansen_forest_loss_ha": parcel.hansen_forest_loss_ha,
            }
            for key in keys:
                row[key] = parcel.properties.get(key)
            writer.writerow(row)


def _build_fields_inventory(parcels: list[ParcelFeature]) -> dict[str, Any]:
    key_counts: dict[str, int] = {}
    used_counts: dict[str, int] = {}
    for parcel in parcels:
        for key in parcel.fields_considered:
            key_counts[key] = key_counts.get(key, 0) + 1
        if parcel.forest_area_key_used:
            used_counts[parcel.forest_area_key_used] = used_counts.get(parcel.forest_area_key_used, 0) + 1
        elif parcel.reference_source == "geometry":
            used_counts["geometry"] = used_counts.get("geometry", 0) + 1
    return {
        "keys_seen": {k: key_counts[k] for k in sorted(key_counts)},
        "forest_area_key_used": {k: used_counts[k] for k in sorted(used_counts)},
        "candidate_keys": sorted(key_counts),
    }


def _parcels_from_json(data: Any) -> list[ParcelRecord]:
    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        parcels: list[ParcelRecord] = []
        for idx, feat in enumerate(data.get("features", []), start=1):
            props = feat.get("properties") or {}
            parcel_id = str(props.get("parcel_id") or f"parcel-{idx}")
            forest_area_value = props.get("forest_area_ha")
            if forest_area_value not in (None, ""):
                forest_area_ha = float(forest_area_value)
                reference_source = "attribute:forest_area_ha"
                reference_method = "reported"
            else:
                geom = feat.get("geometry")
                if geom:
                    forest_area_ha = _geodesic_area_ha(geom)
                    reference_source = "geometry"
                    reference_method = "geodesic_wgs84_pyproj"
                else:
                    forest_area_ha = None
                    reference_source = "missing"
                    reference_method = "missing"
            parcels.append(
                ParcelRecord(
                    parcel_id=parcel_id,
                    forest_area_ha=None if forest_area_ha is None else round(forest_area_ha, 6),
                    reference_source=reference_source,
                    reference_method=reference_method,
                )
            )
        return parcels

    if isinstance(data, list):
        parcels = []
        for idx, item in enumerate(data, start=1):
            if not isinstance(item, dict):
                continue
            parcel_id = str(item.get("parcel_id") or f"parcel-{idx}")
            forest_area_value = item.get("forest_area_ha")
            if forest_area_value not in (None, ""):
                forest_area_ha = float(forest_area_value)
                reference_source = "attribute:forest_area_ha"
                reference_method = "reported"
            else:
                geom = item.get("geometry")
                if geom:
                    forest_area_ha = _geodesic_area_ha(geom)
                    reference_source = "geometry"
                    reference_method = "geodesic_wgs84_pyproj"
                else:
                    forest_area_ha = None
                    reference_source = "missing"
                    reference_method = "missing"
            parcels.append(
                ParcelRecord(
                    parcel_id=parcel_id,
                    forest_area_ha=None if forest_area_ha is None else round(forest_area_ha, 6),
                    reference_source=reference_source,
                    reference_method=reference_method,
                )
            )
        return parcels

    return []


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows_list = list(rows)
    headers = [
        "parcel_id",
        "reference_area_ha",
        "reference_source",
        "reference_method",
        "computed_forest_area_ha",
        "diff_pct",
        "tolerance_percent",
        "outcome",
        "reason",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows_list:
            writer.writerow({h: row.get(h) for h in headers})


def _build_fields_used(parcels: list[ParcelFeature]) -> list[str]:
    keys = set()
    used = set()
    for parcel in parcels:
        keys.update(parcel.fields_considered)
        if parcel.forest_area_key_used:
            used.add(parcel.forest_area_key_used)
        elif parcel.reference_source == "geometry":
            used.add("geometry")
    fields_used = sorted(keys)
    if used:
        fields_used.append(f"forest_area_key_used:{','.join(sorted(used))}")
    return fields_used


def run_maaamet_top10(
    *,
    aoi_geojson_path: Path,
    output_dir: Path,
    provider: MaaAmetProvider | None = None,
    parcels_override: list[ParcelFeature] | None = None,
    min_forest_ha: float = 0.0,
    prefer_hansen: bool = False,
) -> MaaAmetTop10Result | None:
    output_dir.mkdir(parents=True, exist_ok=True)

    if parcels_override is None:
        if provider is None:
            env_path = os.environ.get("EUDR_DMI_MAAAMET_LOCAL_PATH")
            if env_path:
                provider = LocalFileMaaAmetProvider(Path(env_path))
            else:
                wfs_url = os.environ.get("MAAAMET_WFS_URL", "")
                wfs_layer = os.environ.get("MAAAMET_WFS_LAYER") or "kataster:ky_kehtiv"
                if wfs_url:
                    provider = WfsMaaAmetProvider(wfs_url, wfs_layer)

        if provider is None:
            return None

    parcels = parcels_override or provider.fetch_parcel_features(aoi_geojson_path=aoi_geojson_path)
    if not parcels:
        empty_geojson_path = output_dir / "maaamet_top10_parcels.geojson"
        empty_csv_path = output_dir / "maaamet_top10_parcels.csv"
        empty_inventory_path = output_dir / "maaamet_fields_inventory.json"
        write_json(empty_geojson_path, {"type": "FeatureCollection", "features": []})
        _write_top10_csv(empty_csv_path, [], [])
        write_json(empty_inventory_path, {"keys_seen": {}, "forest_area_key_used": {}, "candidate_keys": []})
        return MaaAmetTop10Result(
            parcels=[],
            parcels_all=[],
            parcel_ids=[],
            union_geom=None,
            fields_used=[],
            fields_inventory={"keys_seen": {}, "forest_area_key_used": {}, "candidate_keys": []},
            geojson_path=empty_geojson_path,
            csv_path=empty_csv_path,
            inventory_path=empty_inventory_path,
        )
    parcels_all = [
        ParcelRecord(
            parcel_id=p.parcel_id,
            forest_area_ha=p.forest_area_ha,
            reference_source=p.reference_source,
            reference_method=p.reference_method,
        )
        for p in parcels
    ]

    top10 = _select_top10(parcels, min_forest_ha=min_forest_ha, prefer_hansen=prefer_hansen)
    inventory = _build_fields_inventory(parcels)
    keys = inventory.get("candidate_keys", [])
    geojson_path = output_dir / "maaamet_top10_parcels.geojson"
    csv_path = output_dir / "maaamet_top10_parcels.csv"
    inventory_path = output_dir / "maaamet_fields_inventory.json"

    _write_top10_geojson(geojson_path, top10, keys)
    _write_top10_csv(csv_path, top10, keys)
    write_json(inventory_path, inventory)

    union_geom = None
    if top10:
        union_geom = mapping(unary_union([shape(p.geometry) for p in top10 if p.geometry]))

    return MaaAmetTop10Result(
        parcels=top10,
        parcels_all=parcels_all,
        parcel_ids=[p.parcel_id for p in top10],
        union_geom=union_geom,
        fields_used=_build_fields_used(parcels),
        fields_inventory=inventory,
        geojson_path=geojson_path,
        csv_path=csv_path,
        inventory_path=inventory_path,
    )


def run_maaamet_crosscheck(
    *,
    aoi_geojson_path: Path,
    output_dir: Path,
    computed_forest_area_ha: float | None,
    tolerance_percent: float = 5.0,
    provider: MaaAmetProvider | None = None,
    parcels_override: list[ParcelRecord] | None = None,
    fields_used_override: list[str] | None = None,
    top10_result: MaaAmetTop10Result | None = None,
) -> MaaAmetCrosscheckResult:
    output_dir.mkdir(parents=True, exist_ok=True)

    if provider is None:
        env_path = os.environ.get("EUDR_DMI_MAAAMET_LOCAL_PATH")
        provider = LocalFileMaaAmetProvider(Path(env_path)) if env_path else None
        if provider is None:
            wfs_url = os.environ.get("MAAAMET_WFS_URL", "")
            wfs_layer = os.environ.get("MAAAMET_WFS_LAYER") or "kataster:ky_kehtiv"
            if wfs_url:
                provider = WfsMaaAmetProvider(wfs_url, wfs_layer)

    parcels: list[ParcelRecord] = []
    if parcels_override is not None:
        parcels = parcels_override
    elif provider is not None:
        parcels = provider.fetch_parcels(aoi_geojson_path=aoi_geojson_path)

    rows = []
    for parcel in sorted(parcels, key=lambda p: p.parcel_id):
        if computed_forest_area_ha is None or not parcel.forest_area_ha or parcel.forest_area_ha <= 0:
            diff_pct = None
        else:
            diff_pct = (
                (computed_forest_area_ha - parcel.forest_area_ha)
                / parcel.forest_area_ha
                * 100.0
            )
        rows.append(
            {
                "parcel_id": parcel.parcel_id,
                "reference_area_ha": parcel.forest_area_ha,
                "reference_source": parcel.reference_source,
                "reference_method": parcel.reference_method,
                "computed_forest_area_ha": None
                if computed_forest_area_ha is None
                else round(computed_forest_area_ha, 6),
                "diff_pct": None if diff_pct is None else round(diff_pct, 6),
                "tolerance_percent": tolerance_percent,
                "outcome": "not_comparable" if diff_pct is None else "comparable",
                "reason": "missing_reference_forest_area"
                if parcel.forest_area_ha is None
                else ("missing_computed_forest_area" if computed_forest_area_ha is None else None),
            }
        )

    csv_path = output_dir / "maaamet_forest_area_crosscheck.csv"
    _write_csv(csv_path, rows)

    total_maaamet = sum(p.forest_area_ha for p in parcels if p.forest_area_ha)
    if computed_forest_area_ha is None or total_maaamet <= 0:
        outcome = "not_comparable"
        diff_pct_total = None
        if computed_forest_area_ha is None:
            reason = "missing_computed_forest_area"
        elif total_maaamet <= 0:
            reason = "missing_reference_forest_area"
        else:
            reason = "not_comparable"
    else:
        diff_pct_total = (
            (computed_forest_area_ha - total_maaamet) / total_maaamet * 100.0
        )
        outcome = "pass" if abs(diff_pct_total) <= tolerance_percent else "fail"
        reason = None

    reference_sources = sorted({p.reference_source for p in parcels if p.reference_source})
    reference_methods = sorted({p.reference_method for p in parcels if p.reference_method})
    reference_source = (
        reference_sources[0]
        if len(reference_sources) == 1
        else "mixed" if reference_sources else "missing"
    )
    reference_method = (
        reference_methods[0]
        if len(reference_methods) == 1
        else "mixed" if reference_methods else "missing"
    )

    summary = {
        "outcome": outcome,
        "reason": reason,
        "reference": {
            "source": reference_source,
            "method": reference_method,
            "value_ha": None if total_maaamet <= 0 else round(total_maaamet, 6),
        },
        "computed": {
            "forest_area_ha": None
            if computed_forest_area_ha is None
            else round(computed_forest_area_ha, 6)
        },
        "fields_used": fields_used_override or ["forest_area_ha"],
        "comparison": {
            "tolerance_percent": tolerance_percent,
            "diff_pct": None if diff_pct_total is None else round(diff_pct_total, 6),
            "status": "not_comparable" if outcome == "not_comparable" else "comparable",
        },
        "csv_relpath": csv_path.name,
    }

    summary_path = output_dir / "maaamet_forest_area_crosscheck.json"
    write_json(summary_path, summary)

    return MaaAmetCrosscheckResult(
        outcome=outcome,
        tolerance_percent=tolerance_percent,
        reason=reason,
        fields_used=fields_used_override or ["forest_area_ha"],
        reference_source=reference_source,
        reference_method=reference_method,
        reference_value_ha=None if total_maaamet <= 0 else round(total_maaamet, 6),
        computed_forest_area_ha=None
        if computed_forest_area_ha is None
        else round(computed_forest_area_ha, 6),
        diff_pct=None if diff_pct_total is None else round(diff_pct_total, 6),
        csv_path=csv_path,
        summary_path=summary_path,
        top10_geojson_path=top10_result.geojson_path if top10_result else None,
        top10_csv_path=top10_result.csv_path if top10_result else None,
        fields_inventory_path=top10_result.inventory_path if top10_result else None,
        parcel_ids=top10_result.parcel_ids if top10_result else None,
    )
