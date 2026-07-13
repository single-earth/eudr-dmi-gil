from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError


SCHEMA_PATH = Path("schemas/dds/dds_draft_v1.schema.json")


def _load_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _validator() -> Draft202012Validator:
    schema = _load_schema()
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _artifact(relpath: str, role: str | None = None) -> dict:
    item = {
        "relpath": relpath,
        "sha256": "a" * 64,
        "size_bytes": 123,
        "content_type": "application/json",
    }
    if role is not None:
        item["role"] = role
    return item


def _golden_dds_draft() -> dict:
    source_ref = {
        "source_kind": "portal_intake",
        "source_id": "intake-001",
        "json_pointer": "/operator/name",
    }
    evidence_ref = {
        "source_kind": "aoi_report",
        "source_id": "aoi-report-001",
        "json_pointer": "/results_summary/aoi_area/area_ha",
        "relpath": "reports/aoi_report_v2/aoi-001.json",
        "sha256": "a" * 64,
    }
    return {
        "schema_version": "dds_draft_v1",
        "created_at_utc": "2026-07-13T09:00:00+00:00",
        "draft_id": "dds-draft-001",
        "internal_reference_number": "INT-2026-0001",
        "mapper": {
            "name": "dds-draft-mapper",
            "version": "0.1.0",
            "git_commit": "abcdef1",
            "configuration_hash": "b" * 64,
        },
        "portal_intake": {
            "intake_id": "intake-001",
            "case_id": "case-001",
            "schema_version": "portal_intake_v1",
            "submitted_at_utc": "2026-07-13T08:30:00+00:00",
            "operator_id": "operator-001",
            "source_ref": _artifact("portal/intake.json", "portal_intake"),
            "field_refs": [source_ref],
        },
        "evidence_bundle": {
            "bundle_id": "bundle-001",
            "bundle_date": "2026-07-13",
            "bundle_manifest_ref": _artifact("manifest.json", "bundle_manifest"),
            "aoi_report_refs": [_artifact("reports/aoi_report_v2/aoi-001.json", "aoi_report")],
            "derived_output_refs": [_artifact("reports/aoi_report_v2/aoi-001/metrics.csv", "metrics")],
        },
        "field_values": [
            {
                "field_id": "operator.name",
                "display_name": "Operator name",
                "dds_payload_path": "/operator/name",
                "value": "Example Operator",
                "value_type": "string",
                "unit": None,
                "source_kind": "portal_intake",
                "source_refs": [source_ref],
                "manual_entry": None,
                "derived_value": None,
                "required_for_payload": True,
                "mapping_status": "mapped",
            },
            {
                "field_id": "plot.area_ha",
                "display_name": "Plot area",
                "dds_payload_path": "/plots/0/areaHa",
                "value": 12.34,
                "value_type": "number",
                "unit": "ha",
                "source_kind": "derived_value",
                "source_refs": [evidence_ref],
                "manual_entry": None,
                "derived_value": {
                    "method_id": "aoi_area_from_report",
                    "method_version": "v1",
                    "input_refs": [evidence_ref],
                },
                "required_for_payload": True,
                "mapping_status": "mapped",
            },
            {
                "field_id": "producer.country_code",
                "display_name": "Producer country",
                "dds_payload_path": "/producers/0/countryCode",
                "value": "EE",
                "value_type": "code",
                "unit": None,
                "source_kind": "manual_entry",
                "source_refs": [
                    {
                        "source_kind": "manual_entry",
                        "source_id": "manual-producer-country",
                        "field_id": "producer.country_code",
                    }
                ],
                "manual_entry": {
                    "entered_by": "analyst@example.invalid",
                    "entered_at_utc": "2026-07-13T08:45:00+00:00",
                    "review_note": "Entered from supplier questionnaire.",
                },
                "derived_value": None,
                "required_for_payload": True,
                "mapping_status": "mapped",
            },
        ],
        "pre_submission_gates": [
            {
                "gate_id": "mandatory_fields_present",
                "status": "blocked",
                "blocking": True,
                "reason_codes": ["MISSING_MANDATORY_FIELD"],
                "source_refs": [],
            }
        ],
        "validation_result": {
            "status": "blocked_by_validation",
            "schema_version": "dds_draft_v1",
            "mapper_version": "0.1.0",
            "validated_at_utc": "2026-07-13T09:01:00+00:00",
            "errors": [
                {
                    "code": "MISSING_COMMODITY_CODE",
                    "severity": "error",
                    "category": "missing_field",
                    "message": "Commodity code is required before operator review.",
                    "blocking": True,
                    "field_id": "commodity.code",
                    "json_pointer": "/field_values/commodity.code",
                    "requirement_ref": "dds-client:commodity.code",
                    "remediation_code": "PROVIDE_COMMODITY_CODE",
                    "source_refs": [],
                    "details": {"expected_source": "portal_intake"},
                }
            ],
            "warnings": [],
            "missing_mandatory_fields": ["commodity.code"],
            "ambiguous_mappings": [],
            "blocked_gates": ["mandatory_fields_present"],
            "payload_candidate_sha256": None,
        },
        "evidence_hashes": {
            "hash_algorithm": "sha256",
            "bundle_manifest_sha256": "a" * 64,
            "dds_draft_payload_sha256": None,
            "artifacts": [
                _artifact("manifest.json", "bundle_manifest"),
                _artifact("reports/aoi_report_v2/aoi-001.json", "aoi_report"),
            ],
        },
        "payload_candidate": None,
        "review_state": "mitigation_required",
        "limitations": {
            "not_eudr_compliance_determination": True,
            "no_unattended_submission": True,
            "requires_operator_review": True,
            "notes": ["Draft evidence contract only."],
        },
    }


def test_dds_draft_schema_is_valid_json_schema() -> None:
    Draft202012Validator.check_schema(_load_schema())


def test_dds_draft_schema_validates_golden_sample() -> None:
    _validator().validate(_golden_dds_draft())


def test_dds_draft_schema_requires_machine_readable_validation_failures() -> None:
    bad = _golden_dds_draft()
    bad["validation_result"]["errors"][0].pop("code")

    with pytest.raises(ValidationError):
        _validator().validate(bad)


def test_dds_draft_schema_rejects_compliance_assertion_field() -> None:
    bad = _golden_dds_draft()
    bad["compliance_status"] = "compliant"

    with pytest.raises(ValidationError):
        _validator().validate(bad)


def test_dds_draft_schema_requires_non_compliance_determination_marker() -> None:
    bad = _golden_dds_draft()
    bad["limitations"]["not_eudr_compliance_determination"] = False

    with pytest.raises(ValidationError):
        _validator().validate(bad)
