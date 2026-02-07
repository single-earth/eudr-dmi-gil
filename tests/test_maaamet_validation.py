from __future__ import annotations

from pathlib import Path

from eudr_dmi_gil.analysis.maaamet_validation import (
    MaaAmetProvider,
    ParcelRecord,
    run_maaamet_crosscheck,
)


class MockProvider(MaaAmetProvider):
    def __init__(self, parcels: list[ParcelRecord]) -> None:
        self._parcels = parcels

    def fetch_parcels(self, *, aoi_geojson_path: Path) -> list[ParcelRecord]:
        return list(self._parcels)


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
