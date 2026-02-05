import pytest
from jsonschema.exceptions import ValidationError

from eudr_dmi_gil.reports.validate import validate_aoi_report_v1


def _golden_aoi_report_v1() -> dict:
    # Use a fixed timestamp for deterministic testing.
    return {
        "report_version": "aoi_report_v1",
        "generated_at_utc": "2026-01-31T00:00:00+00:00",
        "bundle_id": "demo-bundle-001",
        "report_metadata": {
            "report_type": "example",
            "regulatory_context": {
                "regulation": "EUDR",
                "in_scope_articles": ["article-3"],
                "out_of_scope_articles": ["article-9"],
            },
            "assessment_capability": "inspectable_only",
        },
        "aoi_id": "aoi-123",
        "aoi_geometry_ref": {
            "kind": "uri",
            "value": "urn:eudr:aoi-geometry:aoi-123",
        },
        "inputs": {
            "sources": [
                {
                    "source_id": "hansen_gfc_definitions",
                    "version": "GFC-2024-v1.12",
                    "uri": "https://storage.googleapis.com/earthenginepartners-hansen/GFC-2024-v1.12/download.html",
                },
                {
                    "source_id": "maa-amet/forest/v1",
                    "uri": "https://gsavalik.envir.ee/geoserver/wfs",
                },
            ]
        },
        "metrics": {
            "area": {"value": 12.34, "unit": "ha"},
            "forest_cover_fraction": {"value": 0.56, "unit": "fraction"},
        },
        "evidence_artifacts": [
            {
                "relpath": "reports/aoi_summary_v1/aoi-123.json",
                "sha256": "0" * 64,
                "size_bytes": 123,
                "content_type": "application/json",
            }
        ],
        "evidence_registry": {
            "evidence_classes": [
                {
                    "class_id": "aoi_geometry",
                    "mandatory": True,
                    "status": "present",
                }
            ]
        },
        "policy_mapping_refs": [
            "policy-spine:eudr/article-3",
            "policy-spine:eudr/article-9",
        ],
    }


def test_schema_validates_golden_sample() -> None:
    validate_aoi_report_v1(_golden_aoi_report_v1())


def test_schema_validates_example_report_metadata() -> None:
    validate_aoi_report_v1(_golden_aoi_report_v1())


def test_schema_rejects_missing_required_field() -> None:
    bad = _golden_aoi_report_v1()
    bad.pop("aoi_geometry_ref")
    with pytest.raises(ValidationError):
        validate_aoi_report_v1(bad)


def test_schema_rejects_missing_report_metadata_block() -> None:
    bad = _golden_aoi_report_v1()
    bad.pop("report_metadata")
    with pytest.raises(ValidationError):
        validate_aoi_report_v1(bad)


def test_schema_rejects_invalid_report_type_enum() -> None:
    bad = _golden_aoi_report_v1()
    bad["report_metadata"]["report_type"] = "internal"
    with pytest.raises(ValidationError):
        validate_aoi_report_v1(bad)


def test_schema_accepts_missing_status_for_inspectable_only() -> None:
    ok = _golden_aoi_report_v1()
    ok["evidence_registry"]["evidence_classes"] = [
        {"class_id": "deforestation_alerts", "mandatory": True, "status": "missing"}
    ]
    validate_aoi_report_v1(ok)


def test_schema_accepts_partial_status() -> None:
    ok = _golden_aoi_report_v1()
    ok["evidence_registry"]["evidence_classes"] = [
        {"class_id": "deforestation_alerts", "mandatory": False, "status": "partial"}
    ]
    validate_aoi_report_v1(ok)


def test_schema_accepts_present_status() -> None:
    ok = _golden_aoi_report_v1()
    ok["evidence_registry"]["evidence_classes"] = [
        {"class_id": "deforestation_alerts", "mandatory": True, "status": "present"}
    ]
    validate_aoi_report_v1(ok)


def test_schema_rejects_missing_mandatory_when_assessable() -> None:
    bad = _golden_aoi_report_v1()
    bad["report_metadata"]["assessment_capability"] = "assessable"
    bad["evidence_registry"]["evidence_classes"] = [
        {"class_id": "deforestation_alerts", "mandatory": True, "status": "missing"}
    ]
    with pytest.raises(ValidationError):
        validate_aoi_report_v1(bad)
