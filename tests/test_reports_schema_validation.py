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
        "acceptance_criteria": [
            {
                "criteria_id": "aoi_geometry_present",
                "description": "AOI geometry is present and referenced in inputs.",
                "evidence_classes": ["aoi_geometry"],
                "decision_type": "presence",
            }
        ],
        "results": [
            {
                "result_id": "result-001",
                "criteria_ids": ["aoi_geometry_present"],
                "status": "pass",
            }
        ],
        "assumptions": [],
        "computed": {},
        "computed_outputs": {},
        "validation": {},
        "methodology": {},
        "regulatory_traceability": [
            {
                "regulation": "EUDR",
                "article_ref": "article-3",
                "evidence_class": "aoi_geometry",
                "acceptance_criteria": "aoi_geometry_present",
                "result_ref": "result-001",
            }
        ],
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


def test_schema_rejects_missing_acceptance_criteria() -> None:
    bad = _golden_aoi_report_v1()
    bad.pop("acceptance_criteria")
    with pytest.raises(ValidationError):
        validate_aoi_report_v1(bad)


def test_schema_accepts_empty_assumptions() -> None:
    ok = _golden_aoi_report_v1()
    ok["assumptions"] = []
    validate_aoi_report_v1(ok)


def test_schema_accepts_example_assumption() -> None:
    ok = _golden_aoi_report_v1()
    ok["assumptions"] = [
        {
            "assumption_id": "assumption-1",
            "description": "Example assumption used for inspection.",
            "testable": False,
            "affects_results": ["result-001"],
        }
    ]
    ok["results"][0]["assumption_refs"] = ["assumption-1"]
    ok["results"][0]["non_testable_due_to_assumptions"] = True
    validate_aoi_report_v1(ok)


def test_schema_accepts_criteria_without_results() -> None:
    ok = _golden_aoi_report_v1()
    ok["results"] = []
    ok["regulatory_traceability"] = []
    validate_aoi_report_v1(ok)


def test_schema_rejects_result_without_criteria_refs() -> None:
    bad = _golden_aoi_report_v1()
    bad["results"] = [{"result_id": "result-005", "criteria_ids": []}]
    bad["regulatory_traceability"] = [
        {
            "regulation": "EUDR",
            "article_ref": "article-3",
            "evidence_class": "aoi_geometry",
            "acceptance_criteria": "aoi_geometry_present",
            "result_ref": "result-005",
        }
    ]
    with pytest.raises(ValidationError):
        validate_aoi_report_v1(bad)


def test_schema_rejects_assumption_with_missing_result_ref() -> None:
    bad = _golden_aoi_report_v1()
    bad["assumptions"] = [
        {
            "assumption_id": "assumption-1",
            "description": "Example assumption.",
            "testable": True,
            "affects_results": ["missing-result"],
        }
    ]
    with pytest.raises(ValidationError):
        validate_aoi_report_v1(bad)


def test_schema_rejects_result_with_unknown_assumption_ref() -> None:
    bad = _golden_aoi_report_v1()
    bad["assumptions"] = []
    bad["results"][0]["assumption_refs"] = ["missing-assumption"]
    with pytest.raises(ValidationError):
        validate_aoi_report_v1(bad)


def test_schema_rejects_missing_non_testable_flag() -> None:
    bad = _golden_aoi_report_v1()
    bad["assumptions"] = [
        {
            "assumption_id": "assumption-1",
            "description": "Example assumption.",
            "testable": False,
            "affects_results": ["result-001"],
        }
    ]
    bad["results"][0]["assumption_refs"] = ["assumption-1"]
    bad["results"][0].pop("non_testable_due_to_assumptions", None)
    with pytest.raises(ValidationError):
        validate_aoi_report_v1(bad)


def test_schema_accepts_result_with_criteria_refs() -> None:
    ok = _golden_aoi_report_v1()
    ok["results"] = [{"result_id": "result-002", "criteria_ids": ["aoi_geometry_present"]}]
    ok["regulatory_traceability"] = [
        {
            "regulation": "EUDR",
            "article_ref": "article-3",
            "evidence_class": "aoi_geometry",
            "acceptance_criteria": "aoi_geometry_present",
            "result_ref": "result-002",
        }
    ]
    validate_aoi_report_v1(ok)


def test_schema_rejects_orphaned_results_without_traceability() -> None:
    bad = _golden_aoi_report_v1()
    bad["results"] = [{"result_id": "result-003", "criteria_ids": ["aoi_geometry_present"]}]
    bad["regulatory_traceability"] = []
    with pytest.raises(ValidationError):
        validate_aoi_report_v1(bad)


def test_schema_rejects_traceability_unknown_references() -> None:
    bad = _golden_aoi_report_v1()
    bad["results"] = [{"result_id": "result-004", "criteria_ids": ["aoi_geometry_present"]}]
    bad["regulatory_traceability"] = [
        {
            "regulation": "EUDR",
            "article_ref": "article-3",
            "evidence_class": "unknown_class",
            "acceptance_criteria": "aoi_geometry_present",
            "result_ref": "result-004",
        }
    ]
    with pytest.raises(ValidationError):
        validate_aoi_report_v1(bad)


def test_schema_accepts_missing_status_for_inspectable_only() -> None:
    ok = _golden_aoi_report_v1()
    ok["evidence_registry"]["evidence_classes"] = [
        {"class_id": "aoi_geometry", "mandatory": True, "status": "present"},
        {"class_id": "deforestation_alerts", "mandatory": True, "status": "missing"}
    ]
    validate_aoi_report_v1(ok)


def test_schema_accepts_partial_status() -> None:
    ok = _golden_aoi_report_v1()
    ok["evidence_registry"]["evidence_classes"] = [
        {"class_id": "aoi_geometry", "mandatory": True, "status": "present"},
        {"class_id": "deforestation_alerts", "mandatory": False, "status": "partial"}
    ]
    validate_aoi_report_v1(ok)


def test_schema_accepts_present_status() -> None:
    ok = _golden_aoi_report_v1()
    ok["evidence_registry"]["evidence_classes"] = [
        {"class_id": "aoi_geometry", "mandatory": True, "status": "present"},
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
