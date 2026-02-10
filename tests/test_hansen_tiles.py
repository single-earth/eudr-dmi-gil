from __future__ import annotations

from pathlib import Path

from eudr_dmi_gil.deps.hansen_acquire import infer_hansen_latest_year
from eudr_dmi_gil.deps.hansen_tiles import hansen_tile_ids_for_bbox, load_aoi_bbox


def test_hansen_tile_ids_for_estonia_fixture() -> None:
    aoi_path = Path(__file__).resolve().parents[1] / "aoi_json_examples" / "estonia_testland1.geojson"
    bbox = load_aoi_bbox(aoi_path)
    tile_ids = hansen_tile_ids_for_bbox(bbox)

    assert tile_ids == ["N60_E020"]


def test_hansen_tile_ids_for_multipolygon_fixture() -> None:
    aoi_path = Path(__file__).resolve().parent / "fixtures" / "aoi_multipolygon.geojson"
    bbox = load_aoi_bbox(aoi_path)
    tile_ids = hansen_tile_ids_for_bbox(bbox)

    assert tile_ids == ["N60_E020", "N60_E030", "N70_E020", "N70_E030"]


def test_infer_hansen_latest_year_from_dataset_version(tmp_path: Path) -> None:
    external_root = tmp_path / "external"
    (external_root / "hansen" / "hansen_gfc_2026_v1_12").mkdir(parents=True)

    year = infer_hansen_latest_year(
        dataset_version="2024-v1.12",
        tile_dir=None,
        external_root=external_root,
    )
    assert year == 2024


def test_infer_hansen_latest_year_from_external_root(tmp_path: Path) -> None:
    hansen_root = tmp_path / "hansen"
    (hansen_root / "hansen_gfc_2021_v1_10").mkdir(parents=True)
    (hansen_root / "hansen_gfc_2023_v1_12").mkdir(parents=True)

    year = infer_hansen_latest_year(
        dataset_version=None,
        tile_dir=None,
        external_root=tmp_path,
    )
    assert year == 2023
