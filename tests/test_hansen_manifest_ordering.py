from __future__ import annotations

import json
from pathlib import Path

from eudr_dmi_gil.deps.hansen_acquire import HansenLayerEntry, write_tiles_manifest


def test_tiles_manifest_ordering(tmp_path: Path) -> None:
    entries = [
        HansenLayerEntry(
            tile_id="N40_E010",
            layer="lossyear",
            local_path="/tmp/tiles/N40_E010/lossyear.tif",
            sha256="b",
            size_bytes=2,
            source_url="http://example/lossyear",
            status="present",
        ),
        HansenLayerEntry(
            tile_id="N40_E010",
            layer="treecover2000",
            local_path="/tmp/tiles/N40_E010/treecover2000.tif",
            sha256="a",
            size_bytes=1,
            source_url="http://example/treecover",
            status="present",
        ),
        HansenLayerEntry(
            tile_id="N50_E020",
            layer="lossyear",
            local_path="/tmp/tiles/N50_E020/lossyear.tif",
            sha256="c",
            size_bytes=3,
            source_url="http://example/lossyear2",
            status="present",
        ),
    ]

    manifest_path = tmp_path / "tiles_manifest.json"
    write_tiles_manifest(
        manifest_path,
        entries=entries,
        dataset_version="2024-v1.12",
        tile_source="external",
        aoi_id="aoi-123",
        run_id="run-123",
        tile_ids=["N50_E020", "N40_E010"],
        derived_relpaths={"summary": "summary.json"},
    )

    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    ordered = [(e["tile_id"], e["layer"], e["local_path"]) for e in data["entries"]]
    assert ordered == sorted(ordered)
    assert data["tile_ids"] == ["N40_E010", "N50_E020"]
