"""Cross-source wood evidence combination: `change_observers` + `wood_evidence_state`.

Implements the wood evidence-state extension documented in `geospatial-evidence-framework`
(`bundles/eudr-gee/s/modeling.md`, "Wood Path"). This module never averages conflicting
observers into a single "truth" value, never multiplies published dataset accuracies into a
synthetic confidence probability, and only emits a union as an explicitly labeled **screening
union** with documented inclusion rules — never as an unqualified "deforestation" number.

Each observer keeps its own area, date window, and quality/confidence semantics
(`area_by_source_ha` is preserved per source, not collapsed). Pairwise agreement/disagreement is
computed only where two observers both have spatial mask geometry available (an observer that is
an evidence gap is never silently treated as agreeing at zero).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyproj import Geod
from shapely.geometry import shape
from shapely.ops import unary_union

_GEOD = Geod(ellps="WGS84")


def _load_mask_union(path: Path | None) -> Any:
    if path is None or not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    geoms = [shape(f["geometry"]) for f in payload.get("features", []) if f.get("geometry")]
    if not geoms:
        return None
    return unary_union(geoms)


def _geodesic_area_ha(geom: Any) -> float:
    if geom is None or geom.is_empty:
        return 0.0
    area_m2, _ = _GEOD.geometry_area_perimeter(geom)
    return round(abs(float(area_m2)) / 10_000.0, 6)


def _pairwise_agreement(
    label_a: str,
    geom_a: Any,
    label_b: str,
    geom_b: Any,
) -> dict[str, Any] | None:
    if geom_a is None or geom_b is None:
        return None
    intersection = geom_a.intersection(geom_b)
    union = geom_a.union(geom_b)
    intersection_ha = _geodesic_area_ha(intersection)
    union_ha = _geodesic_area_ha(union)
    a_only_ha = round(_geodesic_area_ha(geom_a) - intersection_ha, 6)
    b_only_ha = round(_geodesic_area_ha(geom_b) - intersection_ha, 6)
    agreement_ratio = round(intersection_ha / union_ha, 6) if union_ha > 0 else None
    return {
        "observers": [label_a, label_b],
        "intersection_ha": intersection_ha,
        "union_ha": union_ha,
        f"{label_a}_only_ha": a_only_ha,
        f"{label_b}_only_ha": b_only_ha,
        "agreement_ratio": agreement_ratio,
    }


class ObserverInput:
    """One source observer's contribution to the cross-source evidence state."""

    def __init__(
        self,
        *,
        observer_id: str,
        role: str,
        available: bool,
        area_ha: float | None = None,
        mask_geojson_path: Path | None = None,
        date_window: dict[str, Any] | None = None,
        confidence: str | None = None,
        dataset_version: str | None = None,
        evidence_gaps: list[dict[str, Any]] | None = None,
        notes: str | None = None,
    ) -> None:
        self.observer_id = observer_id
        self.role = role
        self.available = available
        self.area_ha = area_ha
        self.mask_geojson_path = mask_geojson_path
        self.date_window = date_window or {}
        self.confidence = confidence
        self.dataset_version = dataset_version
        self.evidence_gaps = evidence_gaps or []
        self.notes = notes

    def to_observer_record(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "available": self.available,
            "area_ha": self.area_ha,
            "date_window": self.date_window,
            "confidence": self.confidence,
            "dataset_version": self.dataset_version,
            "evidence_gaps": self.evidence_gaps,
            "notes": self.notes,
        }


CONFLICT_AGREEMENT_RATIO_THRESHOLD = 0.3
CONFLICT_MIN_BOTH_SIDE_AREA_HA = 0.01


def build_wood_evidence_state(
    *,
    observers: list[ObserverInput],
    min_report_area_ha: float,
    production_geometry_role: str = "unknown",
    production_plot_status: str = "unresolved",
    harvest_or_source_linkage_state: str = "not_evaluated",
    legal_provenance_context_state: str = "not_evaluated",
    screening_union_roles: tuple[str, ...] = ("deforestation_change", "alert"),
) -> dict[str, Any]:
    by_id = {o.observer_id: o for o in observers}

    change_observers: dict[str, Any] = {
        o.observer_id: o.to_observer_record() for o in observers
    }

    def _role_state(role: str) -> dict[str, Any]:
        role_observers = [o for o in observers if o.role == role]
        area_by_source = {
            o.observer_id: o.area_ha for o in role_observers if o.available
        }
        available_present = [o for o in role_observers if o.available]
        detected = any((o.area_ha or 0.0) >= min_report_area_ha for o in available_present)
        if not available_present:
            state = "underdetermined"
        elif detected:
            state = "detected"
        else:
            state = "not_detected"
        return {
            "state": state,
            "observers": [o.observer_id for o in role_observers],
            "area_by_source_ha": area_by_source,
        }

    deforestation_state = _role_state("deforestation_change")
    degradation_state = _role_state("degradation_change")

    # Pairwise spatial agreement, restricted to observer pairs that both expose a mask.
    agreement: dict[str, Any] = {}
    conflicts: list[dict[str, Any]] = []
    mask_geoms = {
        o.observer_id: _load_mask_union(o.mask_geojson_path)
        for o in observers
        if o.mask_geojson_path is not None
    }
    ids = sorted(mask_geoms.keys())
    for i, id_a in enumerate(ids):
        for id_b in ids[i + 1 :]:
            geom_a, geom_b = mask_geoms[id_a], mask_geoms[id_b]
            result = _pairwise_agreement(id_a, geom_a, id_b, geom_b)
            if result is None:
                continue
            pair_key = f"{id_a}_vs_{id_b}"
            agreement[pair_key] = result
            area_a = by_id[id_a].area_ha or 0.0
            area_b = by_id[id_b].area_ha or 0.0
            ratio = result["agreement_ratio"]
            if (
                ratio is not None
                and ratio < CONFLICT_AGREEMENT_RATIO_THRESHOLD
                and area_a >= CONFLICT_MIN_BOTH_SIDE_AREA_HA
                and area_b >= CONFLICT_MIN_BOTH_SIDE_AREA_HA
            ):
                conflicts.append(
                    {
                        "observers": [id_a, id_b],
                        "kind": "spatial_disagreement",
                        "agreement_ratio": ratio,
                        "message": (
                            f"{id_a} and {id_b} each report non-trivial area but overlap only "
                            f"{ratio:.2f} of their combined footprint; disagreement is preserved, "
                            "not averaged or resolved automatically."
                        ),
                    }
                )

    # Screening union: explicitly labeled, documented inclusion rule, never a "truth" value.
    included_ids = [o.observer_id for o in observers if o.role in screening_union_roles and o.mask_geojson_path is not None]
    union_geom = None
    for observer_id in included_ids:
        geom = mask_geoms.get(observer_id)
        if geom is None:
            continue
        union_geom = geom if union_geom is None else union_geom.union(geom)
    screening_union = {
        "label": "screening_union_deforestation_or_alert",
        "inclusion_rule": (
            f"Union of observer masks with role in {list(screening_union_roles)} that had a "
            "spatial export available for this run: " + ", ".join(included_ids) if included_ids else "none"
        ),
        "included_observers": included_ids,
        "union_area_ha": _geodesic_area_ha(union_geom),
        "not_a_verdict": True,
    }

    evidence_gaps = [
        {"observer_id": o.observer_id, **gap}
        for o in observers
        for gap in o.evidence_gaps
    ]

    manual_review_required = bool(conflicts) or (
        deforestation_state["state"] == "detected" and production_plot_status == "unresolved"
    )

    wood_evidence_state = {
        "deforestation": deforestation_state,
        "degradation": degradation_state,
        "production_geometry": {
            "role": production_geometry_role,
            "production_plot_status": production_plot_status,
        },
        "harvest_or_source_linkage": {"state": harvest_or_source_linkage_state},
        "legal_provenance_context": {"state": legal_provenance_context_state},
        "evidence_conflicts": conflicts,
        "evidence_gaps": evidence_gaps,
        "manual_review_required": manual_review_required,
        "cross_source_agreement": agreement,
        "screening_union": screening_union,
    }

    return {
        "change_observers": change_observers,
        "wood_evidence_state": wood_evidence_state,
    }
