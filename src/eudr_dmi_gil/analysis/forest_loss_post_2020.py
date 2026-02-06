from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from eudr_dmi_gil.reports.determinism import write_json
from eudr_dmi_gil.tasks.forest_loss_post_2020 import (
    ForestLossResult,
    HansenConfig,
    compute_forest_loss_post_2020,
)


@dataclass(frozen=True)
class ForestLossComputedOutputs:
    area_ha: float
    pixel_size_m: int
    mask_geojson_relpath: str
    tiles_manifest_relpath: str
    summary_relpath: str


@dataclass(frozen=True)
class ForestLossAnalysisResult:
    computed: ForestLossComputedOutputs
    tiles_manifest_path: Path
    summary_path: Path
    loss_mask_path: Path
    current_mask_path: Path
    raw: ForestLossResult


def _write_tiles_manifest(path: Path, provenance: list[dict[str, Any]]) -> None:
    write_json(
        path,
        {
            "tiles": sorted(
                provenance,
                key=lambda item: (
                    str(item.get("layer", "")),
                    str(item.get("path", "")),
                ),
            )
        },
    )


def run_forest_loss_post_2020(
    *,
    aoi_geojson_path: Path,
    output_dir: Path,
    config: HansenConfig,
) -> ForestLossAnalysisResult:
    output_dir.mkdir(parents=True, exist_ok=True)

    raw = compute_forest_loss_post_2020(
        aoi_geojson_path=aoi_geojson_path,
        output_dir=output_dir,
        config=config,
    )

    tiles_manifest_path = output_dir / "forest_loss_post_2020_tiles.json"
    _write_tiles_manifest(
        tiles_manifest_path,
        [
            {"layer": p.layer, "path": p.relpath, "sha256": p.sha256}
            for p in raw.tile_provenance
        ],
    )

    computed = ForestLossComputedOutputs(
        area_ha=raw.forest_loss_post_2020_ha,
        pixel_size_m=30,
        mask_geojson_relpath=raw.mask_forest_loss_post_2020_path.name,
        tiles_manifest_relpath=tiles_manifest_path.name,
        summary_relpath=raw.summary_path.name,
    )

    return ForestLossAnalysisResult(
        computed=computed,
        tiles_manifest_path=tiles_manifest_path,
        summary_path=raw.summary_path,
        loss_mask_path=raw.mask_forest_loss_post_2020_path,
        current_mask_path=raw.mask_forest_current_path,
        raw=raw,
    )
