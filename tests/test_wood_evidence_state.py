from __future__ import annotations

import json
from pathlib import Path

import pytest

from eudr_dmi_gil.analysis.wood_evidence_state import ObserverInput, build_wood_evidence_state


def _write_mask(path: Path, polygons: list[list[tuple[float, float]]]) -> Path:
    features = [
        {
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "Polygon", "coordinates": [ring]},
        }
        for ring in polygons
    ]
    path.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}), encoding="utf-8"
    )
    return path


SQUARE_A = [(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]
SQUARE_B_OVERLAPPING = [(0.005, 0.0), (0.015, 0.0), (0.015, 0.01), (0.005, 0.01), (0.005, 0.0)]
SQUARE_C_DISJOINT = [(1.0, 1.0), (1.01, 1.0), (1.01, 1.01), (1.0, 1.01), (1.0, 1.0)]


def test_hansen_tmf_disagreement_preserved_not_averaged(tmp_path: Path) -> None:
    hansen_mask = _write_mask(tmp_path / "hansen.geojson", [SQUARE_A])
    tmf_mask = _write_mask(tmp_path / "tmf.geojson", [SQUARE_C_DISJOINT])

    result = build_wood_evidence_state(
        observers=[
            ObserverInput(
                observer_id="hansen",
                role="deforestation_change",
                available=True,
                area_ha=50.0,
                mask_geojson_path=hansen_mask,
            ),
            ObserverInput(
                observer_id="jrc_tmf_deforestation",
                role="deforestation_change",
                available=True,
                area_ha=45.0,
                mask_geojson_path=tmf_mask,
            ),
        ],
        min_report_area_ha=0.01,
    )

    wood = result["wood_evidence_state"]
    assert wood["deforestation"]["area_by_source_ha"] == {
        "hansen": 50.0,
        "jrc_tmf_deforestation": 45.0,
    }
    assert wood["evidence_conflicts"], "disjoint high-area observers must be flagged as conflict"
    agreement = wood["cross_source_agreement"]["hansen_vs_jrc_tmf_deforestation"]
    assert agreement["intersection_ha"] == 0.0
    # Preserved separately, never averaged into one number.
    assert wood["deforestation"]["area_by_source_ha"]["hansen"] == 50.0
    assert wood["deforestation"]["area_by_source_ha"]["jrc_tmf_deforestation"] == 45.0


def test_overlap_agreement_computed_without_erasing_source_metrics(tmp_path: Path) -> None:
    hansen_mask = _write_mask(tmp_path / "hansen.geojson", [SQUARE_A])
    tmf_mask = _write_mask(tmp_path / "tmf.geojson", [SQUARE_B_OVERLAPPING])

    result = build_wood_evidence_state(
        observers=[
            ObserverInput(
                observer_id="hansen",
                role="deforestation_change",
                available=True,
                area_ha=10.0,
                mask_geojson_path=hansen_mask,
            ),
            ObserverInput(
                observer_id="jrc_tmf_deforestation",
                role="deforestation_change",
                available=True,
                area_ha=10.0,
                mask_geojson_path=tmf_mask,
            ),
        ],
        min_report_area_ha=0.01,
    )
    agreement = result["wood_evidence_state"]["cross_source_agreement"][
        "hansen_vs_jrc_tmf_deforestation"
    ]
    assert agreement["intersection_ha"] > 0.0
    assert agreement["union_ha"] > agreement["intersection_ha"]
    assert 0.0 < agreement["agreement_ratio"] < 1.0
    # Source-specific areas are untouched by the agreement computation.
    assert result["wood_evidence_state"]["deforestation"]["area_by_source_ha"]["hansen"] == 10.0


def test_screening_union_is_explicitly_labeled_and_not_a_verdict(tmp_path: Path) -> None:
    hansen_mask = _write_mask(tmp_path / "hansen.geojson", [SQUARE_A])
    radd_mask = _write_mask(tmp_path / "radd.geojson", [SQUARE_B_OVERLAPPING])

    result = build_wood_evidence_state(
        observers=[
            ObserverInput(
                observer_id="hansen",
                role="deforestation_change",
                available=True,
                area_ha=10.0,
                mask_geojson_path=hansen_mask,
            ),
            ObserverInput(
                observer_id="radd",
                role="alert",
                available=True,
                area_ha=8.0,
                mask_geojson_path=radd_mask,
            ),
        ],
        min_report_area_ha=0.01,
    )
    union = result["wood_evidence_state"]["screening_union"]
    assert union["label"] == "screening_union_deforestation_or_alert"
    assert union["not_a_verdict"] is True
    assert set(union["included_observers"]) == {"hansen", "radd"}
    assert union["union_area_ha"] > 0.0


def test_no_synthetic_confidence_probability_emitted(tmp_path: Path) -> None:
    hansen_mask = _write_mask(tmp_path / "hansen.geojson", [SQUARE_A])

    result = build_wood_evidence_state(
        observers=[
            ObserverInput(
                observer_id="hansen",
                role="deforestation_change",
                available=True,
                area_ha=10.0,
                mask_geojson_path=hansen_mask,
            ),
        ],
        min_report_area_ha=0.01,
    )
    payload_text = json.dumps(result)
    for forbidden in ("probability", "confidence_score", "likelihood"):
        assert forbidden not in payload_text


def test_wood_evidence_state_serialization_round_trips(tmp_path: Path) -> None:
    hansen_mask = _write_mask(tmp_path / "hansen.geojson", [SQUARE_A])

    result = build_wood_evidence_state(
        observers=[
            ObserverInput(
                observer_id="hansen",
                role="deforestation_change",
                available=True,
                area_ha=10.0,
                mask_geojson_path=hansen_mask,
                date_window={"start": "2021-01-01", "end": "2025-12-31"},
            ),
        ],
        min_report_area_ha=0.01,
        production_geometry_role="concession",
        production_plot_status="unresolved",
    )
    serialized = json.dumps(result)
    reloaded = json.loads(serialized)
    assert reloaded["wood_evidence_state"]["production_geometry"]["role"] == "concession"
    assert reloaded["change_observers"]["hansen"]["role"] == "deforestation_change"


def test_unavailable_observer_is_gap_not_zero(tmp_path: Path) -> None:
    result = build_wood_evidence_state(
        observers=[
            ObserverInput(
                observer_id="radd",
                role="alert",
                available=False,
                evidence_gaps=[{"code": "radd_no_domain_coverage", "message": "no coverage"}],
            ),
        ],
        min_report_area_ha=0.01,
    )
    wood = result["wood_evidence_state"]
    # No deforestation_change observers at all -> underdetermined, not "not_detected".
    assert wood["deforestation"]["state"] == "underdetermined"
    assert wood["evidence_gaps"]
    assert wood["evidence_gaps"][0]["observer_id"] == "radd"


def test_manual_review_required_when_conflict_present(tmp_path: Path) -> None:
    hansen_mask = _write_mask(tmp_path / "hansen.geojson", [SQUARE_A])
    tmf_mask = _write_mask(tmp_path / "tmf.geojson", [SQUARE_C_DISJOINT])

    result = build_wood_evidence_state(
        observers=[
            ObserverInput(
                observer_id="hansen",
                role="deforestation_change",
                available=True,
                area_ha=50.0,
                mask_geojson_path=hansen_mask,
            ),
            ObserverInput(
                observer_id="jrc_tmf_deforestation",
                role="deforestation_change",
                available=True,
                area_ha=45.0,
                mask_geojson_path=tmf_mask,
            ),
        ],
        min_report_area_ha=0.01,
    )
    assert result["wood_evidence_state"]["manual_review_required"] is True
