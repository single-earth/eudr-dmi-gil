from __future__ import annotations

from pathlib import Path

from eudr_dmi_gil.deps.hansen_tiles import hansen_tile_ids_for_bbox, load_aoi_bbox


def test_hansen_tile_ids_for_estonia_fixture() -> None:
    aoi_path = Path(__file__).resolve().parents[1] / "aoi_json_examples" / "estonia_testland1.geojson"
    bbox = load_aoi_bbox(aoi_path)
    tile_ids = hansen_tile_ids_for_bbox(bbox)

    assert tile_ids == ["N50_E020"]
