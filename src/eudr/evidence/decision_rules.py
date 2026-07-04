from __future__ import annotations


def build_evidence_state(
    *,
    union_post2020_disturbance_candidate_ha: float,
    min_report_area_ha: float,
    agricultural_conversion_evidence_present: bool = False,
    legal_production_evidence_present: bool = False,
    evidence_conflict: bool = False,
) -> dict[str, object]:
    detected = union_post2020_disturbance_candidate_ha >= min_report_area_ha
    possible_eudr = detected and agricultural_conversion_evidence_present
    agricultural_missing = detected and not agricultural_conversion_evidence_present
    human_review = agricultural_missing or evidence_conflict
    if evidence_conflict:
        interpretation = "evidence_conflict_human_review_required"
    elif possible_eudr:
        interpretation = "possible_eudr_relevant_deforestation"
    else:
        interpretation = "insufficient_evidence"

    return {
        "post2020_disturbance_detected": detected,
        "possible_eudr_relevant_deforestation": possible_eudr,
        "agricultural_conversion_evidence_present": agricultural_conversion_evidence_present,
        "agricultural_conversion_evidence_missing": agricultural_missing,
        "legal_production_evidence_present": legal_production_evidence_present,
        "evidence_conflict": evidence_conflict,
        "eudr_interpretation_state": interpretation,
        "human_review_required": human_review,
    }

