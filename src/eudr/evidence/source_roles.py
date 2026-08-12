from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal


SourceRole = Literal[
    "baseline",
    "forest_type",
    "deforestation_change",
    "degradation_change",
    "alert",
    "land_use",
    "allocation",
    "tenure_cadastre",
    "harvest_attribution",
    "legality_context",
    "confirmation",
    "explanatory_context",
]

AcquisitionMode = Literal[
    "gee",
    "http_download",
    "api",
    "local_pinned",
    "vector_service",
    "document",
    "manual_gap",
]

LayerType = Literal["raster", "vector", "alerts", "document", "service", "gap"]


@dataclass(frozen=True)
class SourceLayer:
    dataset_id: str
    title: str
    provider: str
    roles: tuple[SourceRole, ...]
    source_url: str
    service_url: str
    version: str
    availability_state: str
    authoritative_level: str
    acquisition_mode: AcquisitionMode
    layer_type: LayerType
    required: bool
    implementation_priority: str
    analysis_semantics: str
    visualization_semantics: str
    licence: str
    access_utc: str
    evidence_gap: str

    @classmethod
    def from_registry_record(cls, record: dict[str, Any]) -> "SourceLayer":
        roles = tuple(str(role) for role in record.get("eudr_evidence_roles", []))
        if not roles:
            raise ValueError(f"{record.get('dataset_id')}: missing eudr_evidence_roles")
        priority = str(record.get("implementation_priority", "P3"))
        state = str(record.get("access_test_result", "METADATA_ONLY")).split(":", 1)[0]
        mode, layer_type = _infer_mode_and_type(record, state)
        return cls(
            dataset_id=str(record["dataset_id"]),
            title=str(record["title"]),
            provider=str(record["provider_custodian"]),
            roles=roles,  # type: ignore[arg-type]
            source_url=str(record.get("source_url", "")),
            service_url=str(record.get("direct_download_or_service_url", "")),
            version=str(record.get("current_version_observation_year_publication_date", "")),
            availability_state=state,
            authoritative_level=str(record.get("authoritative_level", "")),
            acquisition_mode=mode,
            layer_type=layer_type,
            required=priority == "P0",
            implementation_priority=priority,
            analysis_semantics=str(record.get("known_semantics", "")),
            visualization_semantics=str(record.get("eudr_admissible_claim", "")),
            licence=str(record.get("licence_or_terms", "")),
            access_utc=str(record.get("access_utc", "")),
            evidence_gap=str(record.get("evidence_gaps_access_gaps", "")),
        )

    def to_inventory_record(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "title": self.title,
            "provider": self.provider,
            "roles": list(self.roles),
            "source_url": self.source_url,
            "service_url": self.service_url,
            "version": self.version,
            "availability_state": self.availability_state,
            "authoritative_level": self.authoritative_level,
            "acquisition_mode": self.acquisition_mode,
            "layer_type": self.layer_type,
            "required": self.required,
            "implementation_priority": self.implementation_priority,
            "analysis_semantics": self.analysis_semantics,
            "visualization_semantics": self.visualization_semantics,
            "licence": self.licence,
            "access_utc": self.access_utc,
            "evidence_gap": self.evidence_gap,
        }


def _infer_mode_and_type(
    record: dict[str, Any], state: str
) -> tuple[AcquisitionMode, LayerType]:
    url = str(record.get("direct_download_or_service_url", ""))
    fmt = str(record.get("file_or_service_format", "")).lower()
    roles = set(record.get("eudr_evidence_roles", []))

    if "evidence_gap" in str(record.get("candidate_tier", "")).lower():
        return "manual_gap", "gap"
    if state in {"ACCESS_BLOCKED", "METADATA_ONLY", "VERIFIED_VIEW_ONLY"}:
        if "pdf" in fmt or "document" in fmt or "legality_context" in roles:
            return "document", "document"
        return "manual_gap", "gap"
    if url.startswith("ee."):
        return "gee", "raster"
    if "ImageCollection" in url or "Image(" in url or url.startswith("projects/"):
        return "gee", "raster"
    if "arcgis" in fmt or "Feature Layer" in fmt or "MapServer" in url:
        return "vector_service", "vector"
    if "api" in url.lower():
        return "api", "service"
    if "radd" in record.get("dataset_id", "").lower() or "alert" in roles:
        return "api", "alerts"
    if "pdf" in fmt:
        return "http_download", "document"
    return "http_download", "service"


def load_source_layers(registry_path: Path) -> list[SourceLayer]:
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError(f"Registry has no records list: {registry_path}")
    layers = [SourceLayer.from_registry_record(record) for record in records]
    return sorted(layers, key=lambda layer: (layer.implementation_priority, layer.dataset_id))


def p0_layers(layers: list[SourceLayer]) -> list[SourceLayer]:
    return [layer for layer in layers if layer.implementation_priority == "P0"]


def evidence_gaps(layers: list[SourceLayer]) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    for layer in layers:
        if layer.acquisition_mode == "manual_gap" or layer.availability_state in {
            "ACCESS_BLOCKED",
            "METADATA_ONLY",
            "VERIFIED_VIEW_ONLY",
        }:
            gaps.append(
                {
                    "dataset_id": layer.dataset_id,
                    "title": layer.title,
                    "roles": list(layer.roles),
                    "availability_state": layer.availability_state,
                    "required": layer.required,
                    "gap": layer.evidence_gap,
                }
            )
    return sorted(gaps, key=lambda gap: (not gap["required"], gap["dataset_id"]))

