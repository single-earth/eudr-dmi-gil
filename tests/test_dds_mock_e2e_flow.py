from __future__ import annotations

import json
from pathlib import Path

import pytest

from eudr_dmi_gil.dds.draft import write_dds_draft
from eudr_dmi_gil.dds.workflow import (
    DDSWorkflowError,
    MockDDSService,
    blocking_validation_reasons,
    select_evidence_bundle,
    submit_approved_draft,
    write_operator_approval,
)
from eudr_dmi_gil.reports.bundle import write_manifest
from eudr_dmi_gil.reports.determinism import write_json


CREATED_AT = "2026-07-13T09:15:00+00:00"


def _write_bundle(evidence_root: Path) -> Path:
    bundle_dir = evidence_root / "2026-07-13" / "bundle-ready"
    report_path = bundle_dir / "reports" / "aoi_report_v2" / "aoi-001.json"
    aoi_path = bundle_dir / "inputs" / "aoi.geojson"

    write_json(
        aoi_path,
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"plot_id": "plot-001"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [25.0, 58.0],
                                [25.01, 58.0],
                                [25.01, 58.01],
                                [25.0, 58.01],
                                [25.0, 58.0],
                            ]
                        ],
                    },
                }
            ],
        },
    )
    write_json(
        report_path,
        {
            "report_version": "aoi_report_v2",
            "generated_at_utc": "2026-07-13T09:00:00+00:00",
            "bundle_id": "bundle-ready",
            "aoi_id": "aoi-001",
            "results_summary": {
                "aoi_area": {"area_ha": 12.34, "method": "geodesic_wgs84_pyproj"},
                "deforestation_free_post_2020": {
                    "forest_loss_post_2020_ha": 0.0,
                    "percent_of_aoi": 0.0,
                    "threshold_ha": 0.0,
                    "status": "pass",
                },
            },
            "datasets": [
                {
                    "dataset_id": "hansen_gfc_2025_v1_13",
                    "version": "2025-v1.13",
                    "retrieved_at_utc": "2026-07-13T09:00:00+00:00",
                    "license": "Hansen GFC (public)",
                    "source_url": "https://example.invalid/hansen",
                }
            ],
            "parameters": {"implementation": {"forest_loss_post_2020": "v1"}},
            "evidence_registry": {
                "evidence_classes": [
                    {"class_id": "aoi_geometry", "mandatory": True, "status": "present"},
                    {
                        "class_id": "forest_loss_post_2020",
                        "mandatory": True,
                        "status": "present",
                    },
                ]
            },
            "validation": {"forest_area_crosscheck": {"outcome": "pass"}},
        },
    )
    write_manifest(bundle_dir, [report_path, aoi_path])
    return bundle_dir


def _write_portal_intake(path: Path, *, include_commodity_code: bool) -> None:
    commodity = {"description": "Cocoa beans"}
    if include_commodity_code:
        commodity["code"] = "cocoa"

    write_json(
        path,
        {
            "schema_version": "portal_intake_v1",
            "intake_id": path.stem,
            "case_id": f"case-{path.stem}",
            "internal_reference_number": f"INT-{path.stem}",
            "submitted_at_utc": "2026-07-13T08:30:00+00:00",
            "evidence_bundle": {
                "bundle_date": "2026-07-13",
                "bundle_id": "bundle-ready",
            },
            "operator": {
                "id": "operator-001",
                "name": "Example Operator",
                "identifier": "EORI-001",
                "country_code": "EE",
            },
            "commodity": commodity,
            "product": {"hs_code": "180100", "description": "Cocoa beans"},
            "quantity": {"value": 1000, "unit": "kg"},
            "production": {"country_code": "GH"},
            "supplier": {"name": "Example Supplier"},
            "consignment": {"id": "consignment-001"},
            "legality_documents": [{"document_id": "permit-001", "sha256": "a" * 64}],
            "country_risk_assessment": {"status": "reviewed", "source": "fixture"},
        },
    )


def test_end_to_end_mock_dds_flow_blocks_until_approval_and_clear_validation(
    tmp_path: Path,
) -> None:
    evidence_root = tmp_path / "evidence"
    bundle_dir = _write_bundle(evidence_root)

    blocked_intake_path = tmp_path / "portal_intake_missing_commodity.json"
    _write_portal_intake(blocked_intake_path, include_commodity_code=False)
    blocked_selection = select_evidence_bundle(
        evidence_root=evidence_root,
        portal_intake_path=blocked_intake_path,
    )
    assert blocked_selection == bundle_dir
    blocked_draft_path, _, blocked_draft = write_dds_draft(
        bundle_dir=blocked_selection,
        portal_intake_path=blocked_intake_path,
        out_dir=tmp_path / "blocked_dds",
        created_at_utc=CREATED_AT,
    )

    assert "commodity.code" in blocked_draft["validation_result"]["missing_mandatory_fields"]
    assert "mandatory_fields_present" in blocked_draft["validation_result"]["blocked_gates"]
    assert any(
        reason.startswith("blocking_error:MISSING_COMMODITY_CODE")
        for reason in blocking_validation_reasons(blocked_draft)
    )
    with pytest.raises(DDSWorkflowError, match="not ready for operator approval"):
        write_operator_approval(
            draft_path=blocked_draft_path,
            approver_user_id="operator-user-001",
            approver_display_name="Ada Operator",
            authority_confirmed=True,
            approved_at_utc="2026-07-13T09:25:00+00:00",
            out_dir=tmp_path / "blocked_dds",
        )

    ready_intake_path = tmp_path / "portal_intake_ready.json"
    _write_portal_intake(ready_intake_path, include_commodity_code=True)
    selected_bundle = select_evidence_bundle(
        evidence_root=evidence_root,
        portal_intake_path=ready_intake_path,
    )
    assert selected_bundle == bundle_dir

    draft_path, validation_path, draft = write_dds_draft(
        bundle_dir=selected_bundle,
        portal_intake_path=ready_intake_path,
        created_at_utc=CREATED_AT,
    )
    assert json.loads(validation_path.read_text(encoding="utf-8")) == draft["validation_result"]
    assert draft["review_state"] == "ready_for_operator_review"
    assert blocking_validation_reasons(draft) == []

    dds_service = MockDDSService()
    with pytest.raises(DDSWorkflowError, match="approval record is required"):
        submit_approved_draft(
            draft_path=draft_path,
            approval_path=None,
            dds_service=dds_service,
            submitted_at_utc="2026-07-13T09:30:00+00:00",
        )
    assert dds_service.submissions == []
    assert not (bundle_dir / "dds" / "mock_dds_receipt.json").exists()

    approval_path, approval = write_operator_approval(
        draft_path=draft_path,
        approver_user_id="operator-user-001",
        approver_display_name="Ada Operator",
        authority_confirmed=True,
        approved_at_utc="2026-07-13T09:35:00+00:00",
    )
    assert approval_path.exists()
    assert approval["mandatory_validation_errors_at_approval"] == []
    assert approval["blocked_gates_at_approval"] == []

    receipt_path, receipt = submit_approved_draft(
        draft_path=draft_path,
        approval_path=approval_path,
        dds_service=dds_service,
        submitted_at_utc="2026-07-13T09:40:00+00:00",
    )

    assert receipt_path.exists()
    assert len(dds_service.submissions) == 1
    assert receipt["schema_version"] == "mock_dds_receipt_v1"
    assert receipt["environment"] == "local-mock"
    assert receipt["status"] == "accepted"
    assert receipt["dds_reference_number"].startswith("MOCK-DDS-")
    assert receipt["approval_record_ref"]["relpath"] == "operator_approval.json"
    assert receipt["dds_draft_ref"]["relpath"] == "dds_draft.json"
