from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from eudr_dmi_gil.reports.bundle import compute_sha256
from eudr_dmi_gil.reports.determinism import canonical_json_bytes, sha256_bytes, write_json


APPROVAL_SCHEMA_VERSION = "dds_operator_approval_v1"
RECEIPT_SCHEMA_VERSION = "mock_dds_receipt_v1"
DEFAULT_APPROVAL_TEXT = (
    "I confirm that I am authorised by the operator and approve submission of this "
    "Due Diligence Statement."
)


class DDSWorkflowError(RuntimeError):
    """Raised when the local DDS workflow gate blocks progression."""


def _resolve_timestamp(explicit: str | None = None) -> str:
    if explicit:
        dt = datetime.fromisoformat(explicit.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            raise ValueError("timestamp must include a timezone")
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_json(path: str | Path) -> dict[str, Any]:
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return obj


def _artifact_ref(path: Path, *, base_dir: Path | None = None, role: str | None = None) -> dict[str, Any]:
    path = path.resolve()
    relpath = path.as_posix()
    if base_dir is not None:
        try:
            relpath = path.relative_to(base_dir.resolve()).as_posix()
        except ValueError:
            relpath = path.as_posix()
    ref: dict[str, Any] = {
        "relpath": relpath,
        "sha256": compute_sha256(path),
        "size_bytes": path.stat().st_size,
        "content_type": "application/json",
    }
    if role is not None:
        ref["role"] = role
    return ref


def select_evidence_bundle(
    *,
    evidence_root: str | Path,
    portal_intake_path: str | Path,
) -> Path:
    """Resolve the AOI/evidence bundle selected by a normalized portal intake."""

    intake = _load_json(portal_intake_path)
    selected = intake.get("evidence_bundle") or intake.get("selected_evidence_bundle")
    if not isinstance(selected, dict):
        raise DDSWorkflowError("Portal intake does not select an evidence bundle.")

    bundle_id = str(selected.get("bundle_id") or "").strip()
    bundle_date = str(selected.get("bundle_date") or selected.get("date") or "").strip()
    if not bundle_id or not bundle_date:
        raise DDSWorkflowError("Portal intake evidence bundle selection is incomplete.")

    root = Path(evidence_root).resolve()
    bundle_dir = (root / bundle_date / bundle_id).resolve()
    try:
        bundle_dir.relative_to(root)
    except ValueError as exc:
        raise DDSWorkflowError("Portal intake evidence bundle selection escapes evidence root.") from exc

    if not (bundle_dir / "manifest.json").is_file():
        raise DDSWorkflowError(f"Selected evidence bundle manifest is missing: {bundle_dir}")
    return bundle_dir


def blocking_validation_reasons(draft: dict[str, Any]) -> list[str]:
    validation = draft.get("validation_result") or {}
    reasons: list[str] = []

    if validation.get("status") == "blocked_by_validation":
        reasons.append("validation_result.status=blocked_by_validation")
    for field_id in validation.get("missing_mandatory_fields") or []:
        reasons.append(f"missing_mandatory_field:{field_id}")
    for gate_id in validation.get("blocked_gates") or []:
        reasons.append(f"blocked_gate:{gate_id}")
    for error in validation.get("errors") or []:
        if isinstance(error, dict) and error.get("blocking") is True:
            reasons.append(f"blocking_error:{error.get('code')}")

    if draft.get("payload_candidate") is None:
        reasons.append("payload_candidate_missing")
    if not validation.get("payload_candidate_sha256"):
        reasons.append("payload_candidate_hash_missing")
    return sorted(set(reasons))


def assert_draft_ready_for_approval(draft: dict[str, Any]) -> None:
    reasons = blocking_validation_reasons(draft)
    if reasons:
        raise DDSWorkflowError(
            "DDS draft is not ready for operator approval: " + ", ".join(reasons)
        )


def write_operator_approval(
    *,
    draft_path: str | Path,
    approver_user_id: str,
    approver_display_name: str,
    authority_confirmed: bool,
    approval_text: str = DEFAULT_APPROVAL_TEXT,
    approved_at_utc: str | None = None,
    out_dir: str | Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    draft_file = Path(draft_path)
    draft = _load_json(draft_file)
    assert_draft_ready_for_approval(draft)
    if not authority_confirmed:
        raise DDSWorkflowError("Operator authority confirmation is required.")
    if approval_text != DEFAULT_APPROVAL_TEXT:
        raise DDSWorkflowError("Approval text does not match the required confirmation.")

    validation = draft["validation_result"]
    bundle_hash = draft["evidence_hashes"]["bundle_manifest_sha256"]
    approval = {
        "schema_version": APPROVAL_SCHEMA_VERSION,
        "approved_at_utc": _resolve_timestamp(approved_at_utc),
        "draft_id": draft["draft_id"],
        "draft_schema_version": draft["schema_version"],
        "internal_reference_number": draft["internal_reference_number"],
        "approver": {
            "user_id": approver_user_id,
            "display_name": approver_display_name,
            "authority_confirmed": True,
        },
        "approval_text": approval_text,
        "dds_draft_ref": _artifact_ref(draft_file, role="dds_draft"),
        "payload_candidate_sha256": validation["payload_candidate_sha256"],
        "evidence_bundle_sha256": bundle_hash,
        "validation_status_at_approval": validation["status"],
        "blocked_gates_at_approval": list(validation.get("blocked_gates") or []),
        "mandatory_validation_errors_at_approval": [
            error.get("code")
            for error in validation.get("errors") or []
            if isinstance(error, dict) and error.get("blocking") is True
        ],
        "risk_mitigation_gate_status": next(
            (
                gate.get("status")
                for gate in draft.get("pre_submission_gates") or []
                if isinstance(gate, dict) and gate.get("gate_id") == "risk_mitigation_actions_resolved"
            ),
            None,
        ),
    }
    approval_dir = Path(out_dir) if out_dir else draft_file.parent
    approval_path = approval_dir / "operator_approval.json"
    write_json(approval_path, approval)
    return approval_path, approval


@dataclass
class MockDDSService:
    adapter_name: str = "mock-local-dds"
    adapter_version: str = "0.1.0"
    submissions: list[dict[str, Any]] = field(default_factory=list)

    def submit(
        self,
        *,
        payload: dict[str, Any],
        approval: dict[str, Any],
        idempotency_key: str,
        submitted_at_utc: str | None = None,
    ) -> dict[str, Any]:
        payload_sha = sha256_bytes(canonical_json_bytes(payload))
        approval_sha = sha256_bytes(canonical_json_bytes(approval))
        receipt_seed = sha256_bytes(
            canonical_json_bytes(
                {
                    "approval_sha256": approval_sha,
                    "idempotency_key": idempotency_key,
                    "payload_sha256": payload_sha,
                }
            )
        )
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "adapter": {"name": self.adapter_name, "version": self.adapter_version},
            "environment": "local-mock",
            "submitted_at_utc": _resolve_timestamp(submitted_at_utc),
            "status": "accepted",
            "dds_reference_number": f"MOCK-DDS-{receipt_seed[:12].upper()}",
            "verification_number": f"MOCK-VERIFY-{receipt_seed[12:24].upper()}",
            "idempotency_key": idempotency_key,
            "payload_sha256": payload_sha,
            "approval_record_sha256": approval_sha,
            "request_sha256": sha256_bytes(
                canonical_json_bytes(
                    {
                        "payload": payload,
                        "approval_record_sha256": approval_sha,
                        "idempotency_key": idempotency_key,
                    }
                )
            ),
        }
        self.submissions.append(receipt)
        return receipt


def submit_approved_draft(
    *,
    draft_path: str | Path,
    approval_path: str | Path | None,
    dds_service: MockDDSService,
    submitted_at_utc: str | None = None,
    out_dir: str | Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    if approval_path is None:
        raise DDSWorkflowError("Operator approval record is required before DDS submission.")

    draft_file = Path(draft_path)
    approval_file = Path(approval_path)
    receipt_dir = Path(out_dir) if out_dir else draft_file.parent
    draft = _load_json(draft_file)
    approval = _load_json(approval_file)
    assert_draft_ready_for_approval(draft)

    approval_ref = _artifact_ref(approval_file, base_dir=receipt_dir, role="operator_approval")
    if approval["draft_id"] != draft["draft_id"]:
        raise DDSWorkflowError("Operator approval record does not match the DDS draft.")
    if approval["payload_candidate_sha256"] != draft["validation_result"]["payload_candidate_sha256"]:
        raise DDSWorkflowError("Operator approval record does not match the approved payload hash.")
    if approval.get("mandatory_validation_errors_at_approval"):
        raise DDSWorkflowError("Operator approval record contains mandatory validation errors.")

    payload = draft["payload_candidate"]
    operator_id = (
        draft.get("portal_intake", {}).get("operator_id")
        or payload.get("operator", {}).get("identifier")
        or payload.get("operator", {}).get("name")
        or "operator-unknown"
    )
    consignment_id = payload.get("consignment", {}).get("id") or draft["internal_reference_number"]
    idempotency_key = sha256_bytes(
        f"{operator_id}|{consignment_id}|{approval['payload_candidate_sha256']}".encode("utf-8")
    )
    receipt = dds_service.submit(
        payload=payload,
        approval=approval,
        idempotency_key=idempotency_key,
        submitted_at_utc=submitted_at_utc,
    )
    receipt["approval_record_ref"] = approval_ref
    receipt["dds_draft_ref"] = _artifact_ref(draft_file, base_dir=receipt_dir, role="dds_draft")

    receipt_path = receipt_dir / "mock_dds_receipt.json"
    write_json(receipt_path, receipt)
    return receipt_path, receipt
