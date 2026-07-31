from __future__ import annotations

import json
from pathlib import Path

from eudr_dmi_gil.geo.aoi_alias import geometries_are_identical

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE_AOI = REPO_ROOT / "aoi_json_examples" / "cocoa_west_africa_ghana.geojson"
ALIAS_AOI = REPO_ROOT / "aoi_json_examples" / "ghana_west_africa_shared_aoi.geojson"


def test_alias_files_exist() -> None:
    assert SOURCE_AOI.is_file()
    assert ALIAS_AOI.is_file()


def test_alias_geometry_is_byte_identical_to_source() -> None:
    assert geometries_are_identical(SOURCE_AOI, ALIAS_AOI) is True


def test_alias_has_explicit_commodity_neutral_aoi_id() -> None:
    payload = json.loads(ALIAS_AOI.read_text(encoding="utf-8"))
    props = payload["features"][0]["properties"]
    assert props["aoi_id"] == "ghana_west_africa_shared_aoi"
    assert props.get("commodity") is None


def test_alias_records_source_geometry_provenance() -> None:
    payload = json.loads(ALIAS_AOI.read_text(encoding="utf-8"))
    props = payload["features"][0]["properties"]
    source_geometry = props["source_geometry"]
    assert source_geometry["path"] == "aoi_json_examples/cocoa_west_africa_ghana.geojson"
    assert len(source_geometry["sha256"]) == 64
    assert source_geometry["reused_verbatim"] is True


def test_alias_preserves_sample_boundary_caveat() -> None:
    payload = json.loads(ALIAS_AOI.read_text(encoding="utf-8"))
    props = payload["features"][0]["properties"]
    assert "not a verified farm boundary" in props["note"].lower()


def test_source_file_is_unmodified_cocoa_context() -> None:
    payload = json.loads(SOURCE_AOI.read_text(encoding="utf-8"))
    props = payload["features"][0]["properties"]
    assert props.get("commodity") == "cocoa"


def test_non_identical_geometry_is_detected(tmp_path: Path) -> None:
    """Sanity check that the equivalence helper actually rejects a modified geometry."""
    mutated = json.loads(SOURCE_AOI.read_text(encoding="utf-8"))
    coords = mutated["features"][0]["geometry"]["coordinates"][0]
    coords[0][0] += 0.001  # nudge one vertex
    mutated_path = tmp_path / "mutated.geojson"
    mutated_path.write_text(json.dumps(mutated), encoding="utf-8")
    assert geometries_are_identical(SOURCE_AOI, mutated_path) is False
