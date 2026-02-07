from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from eudr_dmi_gil.deps.hansen_acquire import build_entries_from_provenance, write_tiles_manifest
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


def run_forest_loss_post_2020(
    *,
    aoi_geojson_path: Path,
    output_dir: Path,
    config: HansenConfig,
    aoi_id: str,
    run_id: str,
) -> ForestLossAnalysisResult:
    output_dir.mkdir(parents=True, exist_ok=True)

    raw = compute_forest_loss_post_2020(
        aoi_geojson_path=aoi_geojson_path,
        output_dir=output_dir,
        config=config,
    )

    tiles_manifest_path = output_dir / "forest_loss_post_2020_tiles.json"
    entries = config.tile_entries or build_entries_from_provenance(
        raw.tile_provenance,
        tile_dir=config.tile_dir,
        url_template=config.url_template,
    )
    tile_ids = config.tile_ids or sorted({e.tile_id for e in entries if e.tile_id})
    write_tiles_manifest(
        tiles_manifest_path,
        entries=entries,
        dataset_version=config.dataset_version,
        tile_source=config.tile_source,
        aoi_id=aoi_id,
        run_id=run_id,
        tile_ids=tile_ids,
        derived_relpaths={
            "summary": raw.summary_path.name,
            "loss_mask": raw.mask_forest_loss_post_2020_path.name,
            "current_mask": raw.mask_forest_current_path.name,
        },
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
