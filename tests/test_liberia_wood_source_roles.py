from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from eudr.evidence.source_roles import evidence_gaps, load_source_layers, p0_layers


REPO_ROOT = Path(__file__).resolve().parents[1]
FRAMEWORK_REGISTRY = (
    REPO_ROOT.parent
    / "geospatial-evidence-framework"
    / "inputs"
    / "liberia-eudr-wood-data-source-registry.json"
)
AOI = REPO_ROOT / "aoi_json_examples" / "liberia_fmc_area_k_contract_boundary.geojson"


def test_liberia_registry_parses_source_roles() -> None:
    layers = load_source_layers(FRAMEWORK_REGISTRY)
    ids = {layer.dataset_id for layer in layers}
    assert "JRC-TMF-V2025" in ids
    assert "LBR-FOREST-ATLAS-ANNUAL-COUPE-38" in ids
    assert "LBR-FDA-AOP-HARVEST-BLOCKS-PUBLIC" in ids
    assert any("degradation_change" in layer.roles for layer in p0_layers(layers))
    assert any(gap["dataset_id"] == "LBR-LLA-CADASTRE-PUBLIC" for gap in evidence_gaps(layers))


def test_liberia_screening_runner_emits_required_artifacts(tmp_path: Path) -> None:
    out_dir = tmp_path / "screening"
    handoff = tmp_path / "handoff.json"
    proc = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "run_liberia_wood_screening.py"),
            "--aoi",
            str(AOI),
            "--registry",
            str(FRAMEWORK_REGISTRY),
            "--out-dir",
            str(out_dir),
            "--handoff",
            str(handoff),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    for name in [
        "source_layer_inventory.json",
        "source_agreement_matrix.csv",
        "evidence_gaps.json",
        "production_geometry_status.json",
        "boundary_comparison.json",
        "area_k_layer_map.html",
        "manifest.json",
    ]:
        assert (out_dir / name).is_file()
    payload = json.loads(handoff.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "okf-gsp-evidence-handoff-v1"
    assert payload["commodity"] == "wood"
    assert payload["production_geometry_status"]["production_geometry_role"] == "concession"
    assert payload["production_geometry_status"]["production_plot_status"] == "unresolved"
