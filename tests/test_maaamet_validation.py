from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from eudr_dmi_gil.analysis.maaamet_validation import (
    MaaAmetProvider,
    ParcelRecord,
    run_maaamet_crosscheck,
    run_maaamet_top10,
    LocalFileMaaAmetProvider,
    WfsMaaAmetProvider,
)


class MockProvider(MaaAmetProvider):
    def __init__(self, parcels: list[ParcelRecord]) -> None:
        self._parcels = parcels

    def fetch_parcels(self, *, aoi_geojson_path: Path) -> list[ParcelRecord]:
        return list(self._parcels)


def _write_geojson(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def _write_simple_aoi(path: Path) -> None:
    aoi = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [0.0, 0.0],
                            [1.0, 0.0],
                            [1.0, 1.0],
                            [0.0, 1.0],
                            [0.0, 0.0],
                        ]
                    ],
                },
            }
        ],
    }
    _write_geojson(path, aoi)


def _parcel_feature(parcel_id: str, *, mets: float | None, pindala: float | None) -> dict[str, Any]:
    props: dict[str, Any] = {
        "parcel_id": parcel_id,
        "pindala": pindala,
        "haritav": 1,
        "rohumaa": 2,
        "metsatyyp": "A",
        "siht1": "X",
    }
    if mets is not None:
        props["mets"] = mets
    return {
        "type": "Feature",
        "properties": props,
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [0.1, 0.1],
                    [0.2, 0.1],
                    [0.2, 0.2],
                    [0.1, 0.2],
                    [0.1, 0.1],
                ]
            ],
        },
    }


def test_crosscheck_not_comparable(tmp_path: Path) -> None:
    result = run_maaamet_crosscheck(
        aoi_geojson_path=tmp_path / "aoi.geojson",
        output_dir=tmp_path / "out",
        computed_forest_area_ha=None,
        provider=MockProvider(
            [
                ParcelRecord(
                    parcel_id="p1",
                    forest_area_ha=10.0,
                    reference_source="attribute:forest_area_ha",
                    reference_method="reported",
                )
            ]
        ),
    )
    assert result.outcome == "not_comparable"
    assert result.csv_path.is_file()
    assert result.summary_path.is_file()


def test_crosscheck_consistent(tmp_path: Path) -> None:
    result = run_maaamet_crosscheck(
        aoi_geojson_path=tmp_path / "aoi.geojson",
        output_dir=tmp_path / "out",
        computed_forest_area_ha=10.0,
        tolerance_percent=5.0,
        provider=MockProvider(
            [
                ParcelRecord(
                    parcel_id="p1",
                    forest_area_ha=10.0,
                    reference_source="attribute:forest_area_ha",
                    reference_method="reported",
                )
            ]
        ),
    )
    assert result.outcome == "pass"


def test_crosscheck_divergent(tmp_path: Path) -> None:
    result = run_maaamet_crosscheck(
        aoi_geojson_path=tmp_path / "aoi.geojson",
        output_dir=tmp_path / "out",
        computed_forest_area_ha=20.0,
        tolerance_percent=5.0,
        provider=MockProvider(
            [
                ParcelRecord(
                    parcel_id="p1",
                    forest_area_ha=10.0,
                    reference_source="attribute:forest_area_ha",
                    reference_method="reported",
                )
            ]
        ),
    )
    assert result.outcome == "fail"


def test_maaamet_field_extraction_uses_mets(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_simple_aoi(aoi_path)
    parcels = {
        "type": "FeatureCollection",
        "features": [
            _parcel_feature("p1", mets=20000, pindala=50000),
        ],
    }
    parcels_path = tmp_path / "parcels.json"
    _write_geojson(parcels_path, parcels)

    provider = LocalFileMaaAmetProvider(parcels_path)
    result = run_maaamet_top10(
        aoi_geojson_path=aoi_path,
        output_dir=tmp_path / "out",
        provider=provider,
    )
    assert result is not None
    assert result.parcels
    parcel = result.parcels[0]
    assert parcel.forest_area_ha == pytest.approx(2.0)
    fields_used = result.fields_used
    assert "mets" in fields_used
    assert "pindala" in fields_used
    assert "haritav" in fields_used
    assert "rohumaa" in fields_used
    assert "metsatyyp" in fields_used
    assert "siht1" in fields_used


def test_maaamet_top10_selection_wfs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_simple_aoi(aoi_path)

    features = []
    for idx in range(12):
        mets = (idx + 1) * 1000
        features.append(_parcel_feature(f"p{idx+1}", mets=mets, pindala=100000 + idx))
    payload = json.dumps({"type": "FeatureCollection", "features": features}).encode("utf-8")

    class _Resp:
        def __init__(self, data: bytes) -> None:
            self._data = data

        def read(self) -> bytes:
            return self._data

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    monkeypatch.setattr(
        "eudr_dmi_gil.analysis.maaamet_validation.urllib.request.urlopen",
        lambda url: _Resp(payload),
    )

    provider = WfsMaaAmetProvider("https://gsavalik.envir.ee/geoserver/wfs", "kataster:ky_kehtiv")
    result = run_maaamet_top10(
        aoi_geojson_path=aoi_path,
        output_dir=tmp_path / "out",
        provider=provider,
    )
    assert result is not None
    top_ids = [p.parcel_id for p in result.parcels]
    expected = [f"p{idx}" for idx in range(12, 2, -1)]
    assert top_ids == expected


def test_maaamet_does_not_treat_haritav_as_forest_ha(tmp_path: Path) -> None:
    aoi_path = tmp_path / "aoi.geojson"
    _write_simple_aoi(aoi_path)

    feature = _parcel_feature("p1", mets=None, pindala=886903)
    feature["properties"]["haritav"] = 842831
    feature["properties"]["rohumaa"] = 19636

    parcels_path = tmp_path / "parcels.json"
    _write_geojson(
        parcels_path,
        {
            "type": "FeatureCollection",
            "features": [feature],
        },
    )

    provider = LocalFileMaaAmetProvider(parcels_path)
    result = run_maaamet_top10(
        aoi_geojson_path=aoi_path,
        output_dir=tmp_path / "out",
        provider=provider,
    )
    assert result is not None
    assert len(result.parcels) == 1
    parcel = result.parcels[0]
    assert parcel.maaamet_land_area_ha == pytest.approx(88.6903)
    assert parcel.maaamet_forest_area_ha is not None
    assert parcel.maaamet_forest_area_ha <= parcel.maaamet_land_area_ha
    assert parcel.maaamet_forest_area_ha != pytest.approx(842831.0)
