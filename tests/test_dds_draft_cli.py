from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from eudr_dmi_gil.reports.bundle import write_manifest
from eudr_dmi_gil.reports.determinism import write_json


def _run_cli(args: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parents[1]
    src_path = str(repo_root / "src")
    env = dict(env)
    env["PYTHONPATH"] = src_path + (":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    return subprocess.run(
        [sys.executable, "-m", "eudr_dmi_gil.dds.cli", *args],
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )


def test_dds_draft_cli_writes_draft_and_validation_with_gaps(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "evidence" / "2026-07-13" / "bundle-001"
    report_dir = bundle_dir / "reports" / "aoi_report_v2"
    report_path = report_dir / "aoi-001.json"
    aoi_path = bundle_dir / "inputs" / "aoi.geojson"

    report = {
        "report_version": "aoi_report_v2",
        "generated_at_utc": "2026-07-13T09:00:00+00:00",
        "bundle_id": "bundle-001",
        "aoi_id": "aoi-001",
        "results_summary": {
            "aoi_area": {"area_ha": 12.34, "method": "geodesic_wgs84_pyproj"},
            "deforestation_free_post_2020": {
                "forest_loss_post_2020_ha": 0.25,
                "percent_of_aoi": 2.03,
                "threshold_ha": 0.0,
                "status": "fail",
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
        "parameters": {"implementation": {"forest_loss_post_2020": "v1", "git_commit": "abcdef1"}},
        "evidence_registry": {
            "evidence_classes": [
                {"class_id": "aoi_geometry", "mandatory": True, "status": "present"},
                {"class_id": "forest_loss_post_2020", "mandatory": True, "status": "present"},
            ]
        },
        "validation": {
            "forest_area_crosscheck": {
                "outcome": "not_comparable",
                "reason": "missing_reference_forest_area",
            }
        },
    }
    write_json(report_path, report)
    write_json(
        aoi_path,
        {
            "type": "FeatureCollection",
            "features": [],
        },
    )
    write_manifest(bundle_dir, [report_path, aoi_path])

    intake_path = tmp_path / "portal_intake.json"
    write_json(
        intake_path,
        {
            "schema_version": "portal_intake_v1",
            "intake_id": "intake-001",
            "case_id": "case-001",
            "submitted_at_utc": "2026-07-13T08:30:00+00:00",
            "operator": {"id": "operator-001", "name": "Example Operator"},
            "product": {"hs_code": "180100", "description": "Cocoa beans"},
            "quantity": {"value": 1000, "unit": "kg"},
            "production": {"country_code": "GH"},
        },
    )

    proc = _run_cli(
        [
            "--bundle-dir",
            str(bundle_dir),
            "--portal-intake",
            str(intake_path),
            "--created-at-utc",
            "2026-07-13T09:15:00+00:00",
        ],
        env=os.environ.copy(),
    )

    assert proc.returncode == 0, proc.stderr
    draft_path = bundle_dir / "dds" / "dds_draft.json"
    validation_path = bundle_dir / "dds" / "validation_result.json"
    risk_register_path = bundle_dir / "risk" / "risk_register.json"
    evidence_gaps_path = bundle_dir / "risk" / "evidence_gaps.json"
    mitigation_actions_path = bundle_dir / "risk" / "mitigation_actions.json"
    assert draft_path.exists()
    assert validation_path.exists()
    assert risk_register_path.exists()
    assert evidence_gaps_path.exists()
    assert mitigation_actions_path.exists()

    draft = json.loads(draft_path.read_text(encoding="utf-8"))
    validation_result = json.loads(validation_path.read_text(encoding="utf-8"))
    risk_register = json.loads(risk_register_path.read_text(encoding="utf-8"))
    evidence_gaps = json.loads(evidence_gaps_path.read_text(encoding="utf-8"))
    mitigation_actions = json.loads(mitigation_actions_path.read_text(encoding="utf-8"))

    schema = json.loads(Path("schemas/dds/dds_draft_v1.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(draft)

    assert validation_result == draft["validation_result"]
    assert draft["limitations"]["no_unattended_submission"] is True
    assert "TRACES" in draft["limitations"]["notes"][1]
    assert draft["validation_result"]["status"] == "blocked_by_validation"
    assert "commodity.code" in draft["validation_result"]["missing_mandatory_fields"]
    assert "mandatory_fields_present" in draft["validation_result"]["blocked_gates"]
    assert "post_2020_forest_loss_review" in draft["validation_result"]["blocked_gates"]
    assert "risk_mitigation_actions_resolved" in draft["validation_result"]["blocked_gates"]

    assert risk_register["schema_version"] == "risk_register_v1"
    assert risk_register["workflow_state"] == "disturbance_detected"
    assert risk_register["automated_legal_conclusion"] is None
    assert {
        "geospatial",
        "traceability",
        "legality_documentary",
        "source_country",
        "data_quality",
    } == {item["category"] for item in risk_register["risk_items"]}
    assert "disturbance_detected" in risk_register["allowed_workflow_states"]
    assert "manual_review_required" in risk_register["allowed_workflow_states"]

    assert evidence_gaps["schema_version"] == "evidence_gaps_v1"
    assert any(gap["blocking"] for gap in evidence_gaps["gaps"])
    assert mitigation_actions["schema_version"] == "mitigation_actions_v1"
    assert mitigation_actions["summary"]["dds_approval_blocked"] is True
    assert any(
        action["mandatory"] and action["status"] == "open"
        for action in mitigation_actions["actions"]
    )

    risk_text = "\n".join(
        [
            risk_register_path.read_text(encoding="utf-8"),
            evidence_gaps_path.read_text(encoding="utf-8"),
            mitigation_actions_path.read_text(encoding="utf-8"),
        ]
    )
    assert "deforestation-free" not in risk_text.lower()

    field_ids = {item["field_id"] for item in draft["field_values"]}
    assert "evidence.dataset_versions" in field_ids
    assert "evidence.code_versions" in field_ids
    assert {
        "risk/risk_register.json",
        "risk/evidence_gaps.json",
        "risk/mitigation_actions.json",
    } <= {item["relpath"] for item in draft["evidence_bundle"]["derived_output_refs"]}

    operator_field = next(
        item for item in draft["field_values"] if item["field_id"] == "operator.name"
    )
    assert (
        operator_field["source_refs"][0]["sha256"]
        == draft["portal_intake"]["source_ref"]["sha256"]
    )

    report_ref = draft["evidence_bundle"]["aoi_report_refs"][0]
    area_field = next(item for item in draft["field_values"] if item["field_id"] == "plot.area_ha")
    assert area_field["source_refs"][0]["sha256"] == report_ref["sha256"]
