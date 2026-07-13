from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from eudr_dmi_gil.reports.bundle import compute_sha256
from eudr_dmi_gil.reports.determinism import canonical_json_bytes, sha256_bytes, write_json


SCHEMA_VERSION = "dds_draft_v1"
MAPPER_NAME = "dds-draft-mapper"
MAPPER_VERSION = "0.1.0"
RISK_REGISTER_SCHEMA_VERSION = "risk_register_v1"
EVIDENCE_GAPS_SCHEMA_VERSION = "evidence_gaps_v1"
MITIGATION_ACTIONS_SCHEMA_VERSION = "mitigation_actions_v1"

WORKFLOW_STATES = {
    "no_disturbance_detected",
    "disturbance_detected",
    "insufficient_observations",
    "provider_disagreement",
    "manual_review_required",
}


@dataclass(frozen=True)
class FieldMapping:
    field_id: str
    display_name: str
    dds_payload_path: str | None
    candidates: tuple[str, ...]
    required_for_payload: bool
    value_type: str | None = None
    unit: str | None = None


PORTAL_FIELD_MAPPINGS: tuple[FieldMapping, ...] = (
    FieldMapping(
        "operator.name",
        "Operator name",
        "/operator/name",
        ("/operator/name", "/operator/legal_name", "/operator/legalName", "/operatorName"),
        True,
    ),
    FieldMapping(
        "operator.identifier",
        "Operator identifier",
        "/operator/identifier",
        (
            "/operator/identifier",
            "/operator/eori",
            "/operator/registration_number",
            "/operator/registrationNumber",
        ),
        False,
    ),
    FieldMapping(
        "operator.country_code",
        "Operator country",
        "/operator/countryCode",
        ("/operator/country_code", "/operator/countryCode"),
        False,
        value_type="code",
    ),
    FieldMapping(
        "commodity.code",
        "Commodity code",
        "/commodity/code",
        (
            "/commodity/code",
            "/commodityCode",
            "/commodity_code",
            "/product/commodity/code",
            "/product/commodityCode",
        ),
        True,
        value_type="code",
    ),
    FieldMapping(
        "commodity.description",
        "Commodity description",
        "/commodity/description",
        ("/commodity/description", "/commodity/name", "/product/commodity/description"),
        False,
    ),
    FieldMapping(
        "product.hs_code",
        "Product HS/CN code",
        "/product/hsCode",
        (
            "/product/hs_code",
            "/product/hsCode",
            "/product/cn_code",
            "/product/cnCode",
            "/hs_code",
            "/cn_code",
        ),
        True,
        value_type="code",
    ),
    FieldMapping(
        "product.description",
        "Product description",
        "/product/description",
        ("/product/description", "/product/name", "/goods/description"),
        False,
    ),
    FieldMapping(
        "quantity.value",
        "Quantity",
        "/quantity/value",
        ("/quantity/value", "/product/quantity/value", "/volume/value", "/netMassKg"),
        True,
    ),
    FieldMapping(
        "quantity.unit",
        "Quantity unit",
        "/quantity/unit",
        ("/quantity/unit", "/product/quantity/unit", "/volume/unit", "/netMassUnit"),
        True,
        value_type="code",
    ),
    FieldMapping(
        "production.country_code",
        "Production country",
        "/production/countryCode",
        (
            "/production/country_code",
            "/production/countryCode",
            "/country_code",
            "/countryCode",
            "/producers/0/country_code",
            "/producers/0/countryCode",
        ),
        True,
        value_type="code",
    ),
    FieldMapping(
        "supplier.name",
        "Supplier name",
        "/supplier/name",
        ("/supplier/name", "/suppliers/0/name"),
        False,
    ),
    FieldMapping(
        "consignment.id",
        "Consignment id",
        "/consignment/id",
        ("/consignment/id", "/consignment_id", "/consignmentId"),
        False,
    ),
    FieldMapping(
        "plots",
        "Production plots",
        "/plots",
        ("/plots", "/aoi/plots", "/geolocations"),
        False,
        value_type="array",
    ),
)


def _resolve_created_at_utc(explicit: str | None = None) -> tuple[str, datetime]:
    value = explicit or os.environ.get("EUDR_DMI_GENERATED_AT_UTC", "").strip()
    if value:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("created_at_utc must be an ISO-8601 timestamp") from exc
        if dt.tzinfo is None:
            raise ValueError("created_at_utc must include a timezone")
        dt = dt.astimezone(timezone.utc).replace(microsecond=0)
        return dt.isoformat(), dt
    dt = datetime.now(timezone.utc).replace(microsecond=0)
    return dt.isoformat(), dt


def _git_commit() -> str | None:
    override = os.environ.get("EUDR_DMI_GIT_COMMIT")
    if override is not None:
        value = override.strip()
        return value if _looks_like_git_sha(value) else None
    try:
        value = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return None
    return value if _looks_like_git_sha(value) else None


def _looks_like_git_sha(value: str) -> bool:
    return 7 <= len(value) <= 40 and all(c in "0123456789abcdefABCDEF" for c in value)


def _json_pointer_escape(part: str) -> str:
    return part.replace("~", "~0").replace("/", "~1")


def _json_pointer_unescape(part: str) -> str:
    return part.replace("~1", "/").replace("~0", "~")


def _read_pointer(obj: Any, pointer: str) -> tuple[bool, Any]:
    if pointer == "":
        return True, obj
    if not pointer.startswith("/"):
        raise ValueError(f"JSON pointer must start with '/': {pointer}")
    current = obj
    for raw_part in pointer.split("/")[1:]:
        part = _json_pointer_unescape(raw_part)
        if isinstance(current, dict):
            if part not in current:
                return False, None
            current = current[part]
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return False, None
        else:
            return False, None
    return True, current


def _first_present(obj: Any, pointers: tuple[str, ...]) -> tuple[Any, str | None]:
    for pointer in pointers:
        found, value = _read_pointer(obj, pointer)
        if found and value not in ("", [], {}):
            return value, pointer
    return None, None


def _value_type(value: Any, override: str | None = None) -> str:
    if override is not None:
        return override
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "string"


def _artifact_ref(
    path: Path,
    *,
    base_dir: Path | None = None,
    role: str | None = None,
    source_system: str | None = None,
    content_type: str | None = None,
) -> dict[str, Any]:
    path = path.resolve()
    if base_dir is not None:
        try:
            relpath = path.relative_to(base_dir.resolve()).as_posix()
        except ValueError:
            relpath = path.as_posix()
    else:
        relpath = path.as_posix()
    ref: dict[str, Any] = {
        "relpath": relpath,
        "sha256": compute_sha256(path),
        "size_bytes": path.stat().st_size,
        "content_type": content_type or _content_type(path),
    }
    if role is not None:
        ref["role"] = role
    if source_system is not None:
        ref["source_system"] = source_system
    return ref


def _json_artifact_ref_from_obj(
    *,
    relpath: str,
    obj: object,
    role: str,
    content_type: str = "application/json",
) -> dict[str, Any]:
    data = canonical_json_bytes(obj) + b"\n"
    return {
        "relpath": relpath,
        "sha256": sha256_bytes(data),
        "size_bytes": len(data),
        "content_type": content_type,
        "role": role,
    }


def _content_type(path: Path) -> str | None:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return "application/json"
    if suffix == ".geojson":
        return "application/geo+json"
    if suffix == ".csv":
        return "text/csv"
    if suffix == ".html":
        return "text/html"
    if suffix == ".wkt":
        return "text/plain"
    return None


def _source_ref(
    *,
    source_kind: str,
    source_id: str,
    json_pointer: str | None = None,
    relpath: str | None = None,
    sha256: str | None = None,
    field_id: str | None = None,
) -> dict[str, Any]:
    ref: dict[str, Any] = {
        "source_kind": source_kind,
        "source_id": source_id,
    }
    if json_pointer is not None:
        ref["json_pointer"] = json_pointer
    if relpath is not None:
        ref["relpath"] = relpath
    if sha256 is not None:
        ref["sha256"] = sha256
    if field_id is not None:
        ref["field_id"] = field_id
    return ref


def _field_value(
    *,
    field_id: str,
    display_name: str,
    dds_payload_path: str | None,
    value: Any,
    value_type: str,
    unit: str | None,
    source_kind: str,
    source_refs: list[dict[str, Any]],
    required_for_payload: bool,
    mapping_status: str = "mapped",
    manual_entry: dict[str, Any] | None = None,
    derived_value: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "field_id": field_id,
        "display_name": display_name,
        "dds_payload_path": dds_payload_path,
        "value": value,
        "value_type": value_type,
        "unit": unit,
        "source_kind": source_kind,
        "source_refs": source_refs,
        "manual_entry": manual_entry,
        "derived_value": derived_value,
        "required_for_payload": required_for_payload,
        "mapping_status": mapping_status,
    }


def _validation_failure(
    *,
    code: str,
    severity: str,
    category: str,
    message: str,
    blocking: bool,
    field_id: str | None = None,
    json_pointer: str | None = None,
    requirement_ref: str | None = None,
    remediation_code: str | None = None,
    source_refs: list[dict[str, Any]] | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "category": category,
        "message": message,
        "blocking": blocking,
        "field_id": field_id,
        "json_pointer": json_pointer,
        "requirement_ref": requirement_ref,
        "remediation_code": remediation_code,
        "source_refs": source_refs or [],
        "details": details or {},
    }


def _load_json(path: Path) -> dict[str, Any]:
    data = path.read_text(encoding="utf-8")
    obj = json.loads(data)
    if not isinstance(obj, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return obj


def _manifest_artifact_map(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    artifacts = manifest.get("artifacts") or []
    if not isinstance(artifacts, list):
        return {}
    return {
        str(item.get("relpath")): item
        for item in artifacts
        if isinstance(item, dict) and item.get("relpath")
    }


def _find_aoi_report(bundle_dir: Path, manifest: dict[str, Any]) -> Path:
    manifest_artifacts = _manifest_artifact_map(manifest)
    candidates = [
        bundle_dir / relpath
        for relpath, item in manifest_artifacts.items()
        if relpath.startswith("reports/aoi_report_")
        and relpath.endswith(".json")
        and (item.get("content_type") in (None, "application/json"))
    ]
    candidates.extend(sorted(bundle_dir.glob("reports/aoi_report_*/*.json")))
    unique_candidates = []
    seen: set[Path] = set()
    for path in candidates:
        if path not in seen and path.is_file():
            unique_candidates.append(path)
            seen.add(path)
    if not unique_candidates:
        raise FileNotFoundError(f"No AOI report JSON found under {bundle_dir}")
    return sorted(unique_candidates)[0]


def _report_ref(bundle_dir: Path, report_path: Path) -> dict[str, Any]:
    return _artifact_ref(report_path, base_dir=bundle_dir, role="aoi_report")


def _report_source_ref(
    *,
    report: dict[str, Any],
    report_ref: dict[str, Any],
    json_pointer: str,
    field_id: str | None = None,
) -> dict[str, Any]:
    source_id = str(report.get("aoi_id") or report_ref["relpath"])
    return _source_ref(
        source_kind="aoi_report",
        source_id=source_id,
        json_pointer=json_pointer,
        relpath=report_ref["relpath"],
        sha256=report_ref["sha256"],
        field_id=field_id,
    )


def _system_source(field_id: str) -> dict[str, Any]:
    return _source_ref(
        source_kind="system_generated",
        source_id=MAPPER_NAME,
        field_id=field_id,
    )


def _configuration_hash() -> str:
    config = [
        {
            "field_id": item.field_id,
            "dds_payload_path": item.dds_payload_path,
            "candidates": list(item.candidates),
            "required_for_payload": item.required_for_payload,
            "value_type": item.value_type,
            "unit": item.unit,
        }
        for item in PORTAL_FIELD_MAPPINGS
    ]
    return sha256_bytes(canonical_json_bytes(config))


def _pointer_set(payload: dict[str, Any], pointer: str, value: Any) -> None:
    parts = [_json_pointer_unescape(part) for part in pointer.split("/")[1:]]
    current: Any = payload
    for idx, part in enumerate(parts):
        is_last = idx == len(parts) - 1
        if is_last:
            if isinstance(current, list):
                current[int(part)] = value
            else:
                current[part] = value
            return
        next_part = parts[idx + 1]
        wants_list = next_part.isdigit()
        if isinstance(current, list):
            index = int(part)
            while len(current) <= index:
                current.append([] if wants_list else {})
            if current[index] in (None, ""):
                current[index] = [] if wants_list else {}
            current = current[index]
        else:
            if part not in current or current[part] in (None, ""):
                current[part] = [] if wants_list else {}
            current = current[part]


def _payload_candidate(field_values: list[dict[str, Any]]) -> dict[str, Any] | None:
    required_missing = [
        item
        for item in field_values
        if item.get("required_for_payload") and item.get("mapping_status") != "mapped"
    ]
    if required_missing:
        return None
    payload: dict[str, Any] = {}
    for item in field_values:
        path = item.get("dds_payload_path")
        if not path or item.get("mapping_status") != "mapped":
            continue
        _pointer_set(payload, path, item.get("value"))
    return payload


def _extract_bundle_date(bundle_dir: Path) -> str | None:
    parent_name = bundle_dir.parent.name
    try:
        datetime.strptime(parent_name, "%Y-%m-%d")
    except ValueError:
        return None
    return parent_name


def _collect_manifest_artifact_refs(
    *,
    bundle_dir: Path,
    manifest: dict[str, Any],
    manifest_ref: dict[str, Any],
    report_ref: dict[str, Any],
    intake_ref: dict[str, Any],
) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = [manifest_ref, report_ref, intake_ref]
    seen = {(item["relpath"], item["sha256"]) for item in refs}
    for item in _manifest_artifact_map(manifest).values():
        if not isinstance(item, dict):
            continue
        relpath = str(item.get("relpath") or "")
        sha256 = item.get("sha256")
        if not relpath or not sha256:
            continue
        ref = {
            "relpath": relpath,
            "sha256": sha256,
            "size_bytes": item.get("size_bytes", 0),
            "content_type": item.get("content_type"),
        }
        if (ref["relpath"], ref["sha256"]) not in seen:
            refs.append(ref)
            seen.add((ref["relpath"], ref["sha256"]))
    return refs


def _evidence_errors_and_warnings(
    *,
    bundle_dir: Path,
    manifest: dict[str, Any],
    report: dict[str, Any],
    manifest_ref: dict[str, Any],
    report_ref: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    gates: list[dict[str, Any]] = []

    manifest_source = _source_ref(
        source_kind="evidence_bundle",
        source_id=str(report.get("bundle_id") or bundle_dir.name),
        relpath=manifest_ref["relpath"],
        sha256=manifest_ref["sha256"],
    )
    report_source = _source_ref(
        source_kind="aoi_report",
        source_id=str(report.get("aoi_id") or report_ref["relpath"]),
        relpath=report_ref["relpath"],
        sha256=report_ref["sha256"],
    )

    missing_or_mismatched: list[str] = []
    for relpath, item in sorted(_manifest_artifact_map(manifest).items()):
        artifact_path = bundle_dir / relpath
        if not artifact_path.is_file():
            missing_or_mismatched.append(relpath)
            errors.append(
                _validation_failure(
                    code="EVIDENCE_ARTIFACT_MISSING",
                    severity="error",
                    category="evidence_missing",
                    message=f"Evidence artifact listed in manifest is missing: {relpath}",
                    blocking=True,
                    requirement_ref="evidence_manifest:artifacts",
                    source_refs=[manifest_source],
                    details={"relpath": relpath},
                )
            )
            continue
        expected = item.get("sha256")
        actual = compute_sha256(artifact_path)
        if expected and expected != actual:
            missing_or_mismatched.append(relpath)
            errors.append(
                _validation_failure(
                    code="EVIDENCE_ARTIFACT_HASH_MISMATCH",
                    severity="error",
                    category="hash_mismatch",
                    message=f"Evidence artifact hash mismatch: {relpath}",
                    blocking=True,
                    requirement_ref="evidence_manifest:artifacts.sha256",
                    source_refs=[manifest_source],
                    details={
                        "relpath": relpath,
                        "expected_sha256": expected,
                        "actual_sha256": actual,
                    },
                )
            )

    gates.append(
        {
            "gate_id": "evidence_hashes_verified",
            "status": "blocked" if missing_or_mismatched else "pass",
            "blocking": bool(missing_or_mismatched),
            "reason_codes": ["EVIDENCE_HASH_OR_FILE_GAP"] if missing_or_mismatched else [],
            "source_refs": [manifest_source],
        }
    )

    missing_classes: list[str] = []
    for item in (report.get("evidence_registry") or {}).get("evidence_classes") or []:
        if not isinstance(item, dict):
            continue
        if item.get("mandatory") is True and item.get("status") != "present":
            class_id = str(item.get("class_id") or "unknown")
            missing_classes.append(class_id)
            errors.append(
                _validation_failure(
                    code="MANDATORY_EVIDENCE_CLASS_MISSING",
                    severity="error",
                    category="evidence_missing",
                    message=f"Mandatory evidence class is not present: {class_id}",
                    blocking=True,
                    requirement_ref="aoi_report:evidence_registry",
                    source_refs=[report_source],
                    details={"evidence_class": class_id, "status": item.get("status")},
                )
            )

    gates.append(
        {
            "gate_id": "mandatory_evidence_present",
            "status": "blocked" if missing_classes else "pass",
            "blocking": bool(missing_classes),
            "reason_codes": ["MANDATORY_EVIDENCE_CLASS_MISSING"] if missing_classes else [],
            "source_refs": [report_source],
        }
    )

    forest_loss = (report.get("results_summary") or {}).get("deforestation_free_post_2020") or {}
    forest_loss_ha = forest_loss.get("forest_loss_post_2020_ha")
    forest_status = forest_loss.get("status")
    needs_review = forest_status == "fail" or (
        isinstance(forest_loss_ha, (int, float)) and forest_loss_ha > 0
    )
    if needs_review:
        errors.append(
            _validation_failure(
                code="POST_2020_FOREST_LOSS_REVIEW_REQUIRED",
                severity="error",
                category="gate_blocked",
                message=(
                    "Post-2020 forest-loss evidence requires analyst mitigation review "
                    "before operator review."
                ),
                blocking=True,
                requirement_ref="aoi_report:results_summary.deforestation_free_post_2020",
                source_refs=[
                    _report_source_ref(
                        report=report,
                        report_ref=report_ref,
                        json_pointer="/results_summary/deforestation_free_post_2020",
                    )
                ],
                details={
                    "forest_loss_post_2020_ha": forest_loss_ha,
                    "report_status": forest_status,
                },
            )
        )
    gates.append(
        {
            "gate_id": "post_2020_forest_loss_review",
            "status": "blocked" if needs_review else ("pass" if forest_loss else "not_evaluated"),
            "blocking": bool(needs_review),
            "reason_codes": ["POST_2020_FOREST_LOSS_REVIEW_REQUIRED"] if needs_review else [],
            "source_refs": [report_source],
        }
    )

    crosscheck = ((report.get("validation") or {}).get("forest_area_crosscheck") or {})
    if crosscheck.get("outcome") in {"not_comparable", "missing_reference", "missing"}:
        warnings.append(
            _validation_failure(
                code="FOREST_AREA_CROSSCHECK_NOT_COMPARABLE",
                severity="warning",
                category="evidence_missing",
                message="Independent forest-area crosscheck is not comparable for this bundle.",
                blocking=False,
                requirement_ref="aoi_report:validation.forest_area_crosscheck",
                source_refs=[
                    _report_source_ref(
                        report=report,
                        report_ref=report_ref,
                        json_pointer="/validation/forest_area_crosscheck",
                    )
                ],
                details={"outcome": crosscheck.get("outcome"), "reason": crosscheck.get("reason")},
            )
        )

    return errors, warnings, gates


def _has_any_intake_value(intake: dict[str, Any], pointers: tuple[str, ...]) -> bool:
    value, pointer = _first_present(intake, pointers)
    return pointer is not None and value not in (None, "", [], {})


def _intake_source_ref(
    *,
    intake_id: str,
    intake_ref: dict[str, Any],
    json_pointer: str | None = None,
    field_id: str | None = None,
) -> dict[str, Any]:
    return _source_ref(
        source_kind="portal_intake",
        source_id=intake_id,
        json_pointer=json_pointer,
        relpath=intake_ref["relpath"],
        sha256=intake_ref["sha256"],
        field_id=field_id,
    )


def _risk_level_for_state(workflow_state: str) -> str:
    if workflow_state == "no_disturbance_detected":
        return "low"
    if workflow_state == "disturbance_detected":
        return "high"
    if workflow_state in {"insufficient_observations", "provider_disagreement"}:
        return "unknown"
    return "review_required"


def _overall_workflow_state(states: list[str]) -> str:
    for state in (
        "provider_disagreement",
        "disturbance_detected",
        "insufficient_observations",
        "manual_review_required",
    ):
        if state in states:
            return state
    return "no_disturbance_detected"


def _new_gap(
    *,
    gap_id: str,
    category: str,
    workflow_state: str,
    severity: str,
    blocking: bool,
    reason_code: str,
    title: str,
    description: str,
    source_refs: list[dict[str, Any]],
    risk_ids: list[str],
) -> dict[str, Any]:
    return {
        "gap_id": gap_id,
        "category": category,
        "workflow_state": workflow_state,
        "severity": severity,
        "blocking": blocking,
        "reason_code": reason_code,
        "title": title,
        "description": description,
        "source_refs": source_refs,
        "risk_ids": risk_ids,
        "mitigation_action_ids": [],
    }


def _new_action(
    *,
    action_id: str,
    category: str,
    workflow_state: str,
    title: str,
    description: str,
    mandatory: bool,
    risk_ids: list[str],
    evidence_gap_ids: list[str],
    source_refs: list[dict[str, Any]],
    resolution_requirements: list[str],
    owner_role: str = "analyst",
) -> dict[str, Any]:
    return {
        "action_id": action_id,
        "category": category,
        "workflow_state": workflow_state,
        "title": title,
        "description": description,
        "mandatory": mandatory,
        "status": "open" if mandatory else "recommended",
        "blocking": mandatory,
        "owner_role": owner_role,
        "risk_ids": risk_ids,
        "evidence_gap_ids": evidence_gap_ids,
        "source_refs": source_refs,
        "resolution_requirements": resolution_requirements,
    }


def _link_action_to_gaps(
    gaps: list[dict[str, Any]],
    *,
    action_id: str,
    evidence_gap_ids: list[str],
) -> None:
    wanted = set(evidence_gap_ids)
    for gap in gaps:
        if gap.get("gap_id") in wanted:
            gap.setdefault("mitigation_action_ids", []).append(action_id)


def _build_risk_package(
    *,
    bundle_dir: Path,
    report: dict[str, Any],
    report_ref: dict[str, Any],
    intake: dict[str, Any],
    intake_id: str,
    intake_ref: dict[str, Any],
    created_at_utc: str,
    missing_mandatory_fields: list[str],
    evidence_errors: list[dict[str, Any]],
    evidence_warnings: list[dict[str, Any]],
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    bundle_id = str(report.get("bundle_id") or bundle_dir.name)
    aoi_id = str(report.get("aoi_id") or report_ref["relpath"])
    report_source = _source_ref(
        source_kind="aoi_report",
        source_id=aoi_id,
        relpath=report_ref["relpath"],
        sha256=report_ref["sha256"],
    )
    intake_source = _intake_source_ref(intake_id=intake_id, intake_ref=intake_ref)

    risk_items: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []

    forest_loss = (report.get("results_summary") or {}).get("deforestation_free_post_2020") or {}
    forest_loss_ha = forest_loss.get("forest_loss_post_2020_ha")
    forest_status = forest_loss.get("status")
    crosscheck = ((report.get("validation") or {}).get("forest_area_crosscheck") or {})
    crosscheck_outcome = crosscheck.get("outcome")
    provider_disagreement = crosscheck_outcome in {
        "disagreement",
        "mismatch",
        "failed",
        "fail",
        "outside_tolerance",
    }
    if provider_disagreement:
        geo_state = "provider_disagreement"
        geo_reason_codes = ["PROVIDER_DISAGREEMENT"]
    elif not isinstance(forest_loss_ha, (int, float)):
        geo_state = "insufficient_observations"
        geo_reason_codes = ["POST_2020_DISTURBANCE_EVIDENCE_MISSING"]
    elif forest_status == "fail" or forest_loss_ha > 0:
        geo_state = "disturbance_detected"
        geo_reason_codes = ["POST_2020_DISTURBANCE_DETECTED"]
    else:
        geo_state = "no_disturbance_detected"
        geo_reason_codes = []

    geo_risk_id = "risk-geospatial-post-2020-disturbance"
    geo_source_refs = [
        _report_source_ref(
            report=report,
            report_ref=report_ref,
            json_pointer="/results_summary/deforestation_free_post_2020",
        )
    ]
    geo_action_ids: list[str] = []
    if geo_state != "no_disturbance_detected":
        gap_id = "gap-geospatial-post-2020-disturbance-review"
        action_id = "action-geospatial-review-and-mitigation"
        gaps.append(
            _new_gap(
                gap_id=gap_id,
                category="geospatial",
                workflow_state=geo_state,
                severity="error",
                blocking=True,
                reason_code=geo_reason_codes[0],
                title="Post-2020 disturbance evidence needs review",
                description=(
                    "The geospatial evidence is not ready for operator approval without "
                    "analyst review and documented mitigation."
                ),
                source_refs=geo_source_refs,
                risk_ids=[geo_risk_id],
            )
        )
        actions.append(
            _new_action(
                action_id=action_id,
                category="geospatial",
                workflow_state=geo_state,
                title="Review disturbance evidence and document mitigation",
                description=(
                    "Inspect the affected area, reconcile imagery or provider evidence, "
                    "and document the operator decision path before DDS approval."
                ),
                mandatory=True,
                risk_ids=[geo_risk_id],
                evidence_gap_ids=[gap_id],
                source_refs=geo_source_refs,
                resolution_requirements=[
                    "Attach analyst review notes for the affected AOI.",
                    "Attach high-resolution or independent evidence where available.",
                    "Remove unsupported plots from the consignment or document accepted mitigation.",
                ],
            )
        )
        _link_action_to_gaps(gaps, action_id=action_id, evidence_gap_ids=[gap_id])
        geo_action_ids.append(action_id)

    risk_items.append(
        {
            "risk_id": geo_risk_id,
            "category": "geospatial",
            "workflow_state": geo_state,
            "risk_level": _risk_level_for_state(geo_state),
            "blocking": geo_state != "no_disturbance_detected",
            "reason_codes": geo_reason_codes,
            "observations": {
                "post_2020_disturbance_candidate_ha": forest_loss_ha,
                "report_status": forest_status,
                "crosscheck_outcome": crosscheck_outcome,
            },
            "source_refs": geo_source_refs,
            "mitigation_action_ids": geo_action_ids,
        }
    )

    traceability_present = _has_any_intake_value(
        intake,
        (
            "/supplier/name",
            "/suppliers/0/name",
            "/consignment/id",
            "/consignment_id",
            "/consignmentId",
            "/traceability/documents",
            "/traceability_records",
        ),
    )
    trace_risk_id = "risk-traceability-chain-of-custody"
    trace_state = "no_disturbance_detected" if traceability_present else "manual_review_required"
    trace_action_ids: list[str] = []
    if not traceability_present:
        gap_id = "gap-traceability-supporting-records"
        action_id = "action-reconcile-supplier-traceability"
        gaps.append(
            _new_gap(
                gap_id=gap_id,
                category="traceability",
                workflow_state=trace_state,
                severity="error",
                blocking=True,
                reason_code="TRACEABILITY_RECORDS_MISSING",
                title="Traceability records are missing",
                description=(
                    "Supplier, consignment, or chain-of-custody records were not found "
                    "in the portal intake."
                ),
                source_refs=[intake_source],
                risk_ids=[trace_risk_id],
            )
        )
        actions.append(
            _new_action(
                action_id=action_id,
                category="traceability",
                workflow_state=trace_state,
                title="Reconcile supplier and consignment traceability",
                description="Attach or enter supplier, consignment, and chain-of-custody records.",
                mandatory=True,
                risk_ids=[trace_risk_id],
                evidence_gap_ids=[gap_id],
                source_refs=[intake_source],
                resolution_requirements=[
                    "Provide supplier or upstream actor identity.",
                    "Provide consignment or chain-of-custody reference.",
                ],
            )
        )
        _link_action_to_gaps(gaps, action_id=action_id, evidence_gap_ids=[gap_id])
        trace_action_ids.append(action_id)

    risk_items.append(
        {
            "risk_id": trace_risk_id,
            "category": "traceability",
            "workflow_state": trace_state,
            "risk_level": _risk_level_for_state(trace_state),
            "blocking": not traceability_present,
            "reason_codes": [] if traceability_present else ["TRACEABILITY_RECORDS_MISSING"],
            "observations": {"supporting_traceability_present": traceability_present},
            "source_refs": [intake_source],
            "mitigation_action_ids": trace_action_ids,
        }
    )

    legality_present = _has_any_intake_value(
        intake,
        (
            "/legality_documents",
            "/documents/legality",
            "/permits",
            "/harvest_permits",
            "/supporting_documents/legal",
        ),
    )
    legal_risk_id = "risk-legality-documentary-evidence"
    legal_state = "no_disturbance_detected" if legality_present else "manual_review_required"
    legal_action_ids: list[str] = []
    if not legality_present:
        gap_id = "gap-legality-documentary-evidence"
        action_id = "action-attach-legality-documentation"
        gaps.append(
            _new_gap(
                gap_id=gap_id,
                category="legality_documentary",
                workflow_state=legal_state,
                severity="error",
                blocking=True,
                reason_code="LEGALITY_DOCUMENTS_MISSING",
                title="Legality documentation is missing",
                description="Required legality or production-rights documentation was not found.",
                source_refs=[intake_source],
                risk_ids=[legal_risk_id],
            )
        )
        actions.append(
            _new_action(
                action_id=action_id,
                category="legality_documentary",
                workflow_state=legal_state,
                title="Attach legality documentation",
                description="Attach harvest permits, production rights, or equivalent legal records.",
                mandatory=True,
                risk_ids=[legal_risk_id],
                evidence_gap_ids=[gap_id],
                source_refs=[intake_source],
                resolution_requirements=[
                    "Provide legality documents relevant to the production country and commodity.",
                    "Record analyst review of documentary sufficiency.",
                ],
            )
        )
        _link_action_to_gaps(gaps, action_id=action_id, evidence_gap_ids=[gap_id])
        legal_action_ids.append(action_id)

    risk_items.append(
        {
            "risk_id": legal_risk_id,
            "category": "legality_documentary",
            "workflow_state": legal_state,
            "risk_level": _risk_level_for_state(legal_state),
            "blocking": not legality_present,
            "reason_codes": [] if legality_present else ["LEGALITY_DOCUMENTS_MISSING"],
            "observations": {"legality_documents_present": legality_present},
            "source_refs": [intake_source],
            "mitigation_action_ids": legal_action_ids,
        }
    )

    country_present = _has_any_intake_value(
        intake,
        (
            "/production/country_code",
            "/production/countryCode",
            "/country_code",
            "/countryCode",
            "/producers/0/country_code",
            "/producers/0/countryCode",
        ),
    )
    country_risk_present = _has_any_intake_value(
        intake,
        (
            "/country_risk_assessment",
            "/source_country_risk",
            "/risk/source_country",
            "/risk/country",
        ),
    )
    country_risk_id = "risk-source-country-review"
    country_state = (
        "no_disturbance_detected"
        if country_present and country_risk_present
        else "manual_review_required"
    )
    country_action_ids: list[str] = []
    if not country_present or not country_risk_present:
        gap_id = "gap-source-country-risk-assessment"
        action_id = "action-complete-source-country-risk-review"
        reason = (
            "PRODUCTION_COUNTRY_MISSING"
            if not country_present
            else "SOURCE_COUNTRY_RISK_ASSESSMENT_MISSING"
        )
        gaps.append(
            _new_gap(
                gap_id=gap_id,
                category="source_country",
                workflow_state=country_state,
                severity="error",
                blocking=True,
                reason_code=reason,
                title="Source and country risk review is incomplete",
                description=(
                    "Production country and country-risk review evidence must be available "
                    "before DDS approval."
                ),
                source_refs=[intake_source],
                risk_ids=[country_risk_id],
            )
        )
        actions.append(
            _new_action(
                action_id=action_id,
                category="source_country",
                workflow_state=country_state,
                title="Complete source and country risk review",
                description="Attach or record the source and country risk assessment.",
                mandatory=True,
                risk_ids=[country_risk_id],
                evidence_gap_ids=[gap_id],
                source_refs=[intake_source],
                resolution_requirements=[
                    "Confirm production country.",
                    "Attach country-risk assessment source or analyst rationale.",
                ],
            )
        )
        _link_action_to_gaps(gaps, action_id=action_id, evidence_gap_ids=[gap_id])
        country_action_ids.append(action_id)

    risk_items.append(
        {
            "risk_id": country_risk_id,
            "category": "source_country",
            "workflow_state": country_state,
            "risk_level": _risk_level_for_state(country_state),
            "blocking": country_state != "no_disturbance_detected",
            "reason_codes": []
            if country_state == "no_disturbance_detected"
            else [
                "PRODUCTION_COUNTRY_MISSING"
                if not country_present
                else "SOURCE_COUNTRY_RISK_ASSESSMENT_MISSING"
            ],
            "observations": {
                "production_country_present": country_present,
                "source_country_risk_assessment_present": country_risk_present,
            },
            "source_refs": [intake_source],
            "mitigation_action_ids": country_action_ids,
        }
    )

    data_risk_id = "risk-data-quality-and-coverage"
    data_reason_codes: list[str] = []
    if evidence_errors:
        data_reason_codes.append("EVIDENCE_VALIDATION_ERRORS")
    if evidence_warnings:
        data_reason_codes.append("EVIDENCE_VALIDATION_WARNINGS")
    if missing_mandatory_fields:
        data_reason_codes.append("DDS_MANDATORY_FIELDS_MISSING")
    data_state = "no_disturbance_detected" if not data_reason_codes else "insufficient_observations"
    data_action_ids: list[str] = []
    if data_reason_codes:
        gap_id = "gap-data-quality-validation"
        action_id = "action-resolve-data-quality-gaps"
        gaps.append(
            _new_gap(
                gap_id=gap_id,
                category="data_quality",
                workflow_state=data_state,
                severity="error" if evidence_errors or missing_mandatory_fields else "warning",
                blocking=bool(evidence_errors or missing_mandatory_fields),
                reason_code=data_reason_codes[0],
                title="Data quality or coverage gaps are present",
                description=(
                    "DDS preparation found evidence validation issues, warnings, or missing "
                    "mandatory draft fields."
                ),
                source_refs=[report_source, intake_source],
                risk_ids=[data_risk_id],
            )
        )
        mandatory = bool(evidence_errors or missing_mandatory_fields)
        actions.append(
            _new_action(
                action_id=action_id,
                category="data_quality",
                workflow_state=data_state,
                title="Resolve data quality and coverage gaps",
                description="Correct missing fields, evidence hashes, or coverage limitations.",
                mandatory=mandatory,
                risk_ids=[data_risk_id],
                evidence_gap_ids=[gap_id],
                source_refs=[report_source, intake_source],
                resolution_requirements=[
                    "Resolve blocking validation errors.",
                    "Document nonblocking evidence limitations for operator review.",
                ],
            )
        )
        _link_action_to_gaps(gaps, action_id=action_id, evidence_gap_ids=[gap_id])
        data_action_ids.append(action_id)

    risk_items.append(
        {
            "risk_id": data_risk_id,
            "category": "data_quality",
            "workflow_state": data_state,
            "risk_level": _risk_level_for_state(data_state),
            "blocking": bool(evidence_errors or missing_mandatory_fields),
            "reason_codes": data_reason_codes,
            "observations": {
                "blocking_evidence_error_count": len(evidence_errors),
                "evidence_warning_count": len(evidence_warnings),
                "missing_mandatory_fields": sorted(missing_mandatory_fields),
            },
            "source_refs": [report_source, intake_source],
            "mitigation_action_ids": data_action_ids,
        }
    )

    overall_state = _overall_workflow_state(
        [str(item["workflow_state"]) for item in risk_items if item.get("workflow_state")]
    )
    mandatory_open_actions = [
        item for item in actions if item.get("mandatory") is True and item.get("status") == "open"
    ]
    risk_register = {
        "schema_version": RISK_REGISTER_SCHEMA_VERSION,
        "created_at_utc": created_at_utc,
        "bundle_id": bundle_id,
        "aoi_id": aoi_id,
        "workflow_state": overall_state,
        "allowed_workflow_states": sorted(WORKFLOW_STATES),
        "automated_legal_conclusion": None,
        "limitations": {
            "not_eudr_compliance_determination": True,
            "requires_operator_review": True,
            "notes": [
                "Risk states are workflow evidence states only.",
                "No automated legal or DDS approval conclusion is made.",
            ],
        },
        "risk_items": risk_items,
        "summary": {
            "risk_count": len(risk_items),
            "blocking_risk_count": sum(1 for item in risk_items if item.get("blocking")),
            "mandatory_open_mitigation_count": len(mandatory_open_actions),
        },
    }
    evidence_gaps = {
        "schema_version": EVIDENCE_GAPS_SCHEMA_VERSION,
        "created_at_utc": created_at_utc,
        "bundle_id": bundle_id,
        "aoi_id": aoi_id,
        "workflow_state": overall_state,
        "gaps": gaps,
        "summary": {
            "gap_count": len(gaps),
            "blocking_gap_count": sum(1 for item in gaps if item.get("blocking")),
        },
    }
    mitigation_actions = {
        "schema_version": MITIGATION_ACTIONS_SCHEMA_VERSION,
        "created_at_utc": created_at_utc,
        "bundle_id": bundle_id,
        "aoi_id": aoi_id,
        "workflow_state": overall_state,
        "actions": actions,
        "summary": {
            "action_count": len(actions),
            "mandatory_open_action_count": len(mandatory_open_actions),
            "dds_approval_blocked": bool(mandatory_open_actions),
        },
    }

    risk_gate = {
        "gate_id": "risk_mitigation_actions_resolved",
        "status": "blocked" if mandatory_open_actions else "pass",
        "blocking": bool(mandatory_open_actions),
        "reason_codes": ["UNRESOLVED_MANDATORY_MITIGATION"]
        if mandatory_open_actions
        else [],
        "source_refs": [report_source, intake_source],
    }

    risk_errors: list[dict[str, Any]] = []
    risk_warnings: list[dict[str, Any]] = []
    if mandatory_open_actions:
        risk_errors.append(
            _validation_failure(
                code="UNRESOLVED_MANDATORY_MITIGATION_ACTIONS",
                severity="error",
                category="gate_blocked",
                message="Mandatory mitigation actions remain open before DDS approval.",
                blocking=True,
                requirement_ref="risk:mitigation_actions",
                remediation_code="RESOLVE_MANDATORY_MITIGATION_ACTIONS",
                source_refs=[report_source, intake_source],
                details={
                    "action_ids": [str(item["action_id"]) for item in mandatory_open_actions],
                    "risk_register_relpath": "risk/risk_register.json",
                    "mitigation_actions_relpath": "risk/mitigation_actions.json",
                },
            )
        )
    elif actions:
        risk_warnings.append(
            _validation_failure(
                code="RECOMMENDED_MITIGATION_ACTIONS_PRESENT",
                severity="warning",
                category="operator_review_required",
                message="Recommended mitigation actions should be reviewed by the operator.",
                blocking=False,
                requirement_ref="risk:mitigation_actions",
                source_refs=[report_source, intake_source],
                details={"action_ids": [str(item["action_id"]) for item in actions]},
            )
        )

    return (
        risk_register,
        evidence_gaps,
        mitigation_actions,
        risk_gate,
        risk_errors,
        risk_warnings,
    )


def _add_report_field(
    field_values: list[dict[str, Any]],
    *,
    report: dict[str, Any],
    report_ref: dict[str, Any],
    pointer: str,
    field_id: str,
    display_name: str,
    dds_payload_path: str | None,
    required_for_payload: bool = False,
    value_type: str | None = None,
    unit: str | None = None,
    method_id: str = "aoi_report_mapping",
) -> None:
    found, value = _read_pointer(report, pointer)
    if not found or value in ("", [], {}):
        if required_for_payload:
            field_values.append(
                _field_value(
                    field_id=field_id,
                    display_name=display_name,
                    dds_payload_path=dds_payload_path,
                    value=None,
                    value_type="null",
                    unit=unit,
                    source_kind="system_generated",
                    source_refs=[_system_source(field_id)],
                    required_for_payload=required_for_payload,
                    mapping_status="missing",
                )
            )
        return
    source = _report_source_ref(
        report=report,
        report_ref=report_ref,
        json_pointer=pointer,
        field_id=field_id,
    )
    field_values.append(
        _field_value(
            field_id=field_id,
            display_name=display_name,
            dds_payload_path=dds_payload_path,
            value=value,
            value_type=_value_type(value, value_type),
            unit=unit,
            source_kind="derived_value",
            source_refs=[source],
            required_for_payload=required_for_payload,
            derived_value={
                "method_id": method_id,
                "method_version": "v1",
                "input_refs": [source],
            },
        )
    )


def build_dds_draft(
    *,
    bundle_dir: str | Path,
    portal_intake_path: str | Path,
    aoi_report_path: str | Path | None = None,
    created_at_utc: str | None = None,
    return_risk_package: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]] | tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, dict[str, Any]],
]:
    bundle_path = Path(bundle_dir)
    intake_path = Path(portal_intake_path)
    manifest_path = bundle_path / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Bundle manifest not found: {manifest_path}")
    if not intake_path.is_file():
        raise FileNotFoundError(f"Portal intake JSON not found: {intake_path}")

    manifest = _load_json(manifest_path)
    intake = _load_json(intake_path)
    report_path = (
        Path(aoi_report_path) if aoi_report_path else _find_aoi_report(bundle_path, manifest)
    )
    if not report_path.is_absolute():
        report_path = bundle_path / report_path
    if not report_path.is_file():
        raise FileNotFoundError(f"AOI report JSON not found: {report_path}")
    report = _load_json(report_path)

    created_iso, _ = _resolve_created_at_utc(created_at_utc)
    manifest_ref = _artifact_ref(manifest_path, base_dir=bundle_path, role="bundle_manifest")
    report_ref = _report_ref(bundle_path, report_path)
    intake_ref = _artifact_ref(
        intake_path,
        base_dir=bundle_path,
        role="portal_intake",
        source_system="eudr-client-portal",
    )
    intake_id = str(intake.get("intake_id") or intake.get("id") or intake_path.stem)
    case_id = intake.get("case_id") or intake.get("caseId")
    operator_id = (
        (intake.get("operator") or {}).get("id")
        if isinstance(intake.get("operator"), dict)
        else None
    )
    operator_id = operator_id or intake.get("operator_id") or intake.get("operatorId")
    intake_schema_version = str(intake.get("schema_version") or "portal_intake_unknown")
    submitted_at = intake.get("submitted_at_utc") or intake.get("submittedAtUtc")
    internal_reference = str(
        intake.get("internal_reference_number")
        or intake.get("internalReferenceNumber")
        or case_id
        or intake_id
    )
    draft_hash_input = (internal_reference + report_ref["sha256"]).encode("utf-8")
    draft_id = f"dds-draft-{sha256_bytes(draft_hash_input)[:12]}"

    field_values: list[dict[str, Any]] = []
    intake_relpath = intake_ref["relpath"]
    intake_sha = intake_ref["sha256"]
    for mapping in PORTAL_FIELD_MAPPINGS:
        value, pointer = _first_present(intake, mapping.candidates)
        if pointer is None:
            if mapping.required_for_payload:
                field_values.append(
                    _field_value(
                        field_id=mapping.field_id,
                        display_name=mapping.display_name,
                        dds_payload_path=mapping.dds_payload_path,
                        value=None,
                        value_type="null",
                        unit=mapping.unit,
                        source_kind="system_generated",
                        source_refs=[_system_source(mapping.field_id)],
                        required_for_payload=True,
                        mapping_status="missing",
                    )
                )
            continue
        field_values.append(
            _field_value(
                field_id=mapping.field_id,
                display_name=mapping.display_name,
                dds_payload_path=mapping.dds_payload_path,
                value=value,
                value_type=_value_type(value, mapping.value_type),
                unit=mapping.unit,
                source_kind="portal_intake",
                source_refs=[
                    _source_ref(
                        source_kind="portal_intake",
                        source_id=intake_id,
                        json_pointer=pointer,
                        relpath=intake_relpath,
                        sha256=intake_sha,
                        field_id=mapping.field_id,
                    )
                ],
                required_for_payload=mapping.required_for_payload,
            )
        )

    _add_report_field(
        field_values,
        report=report,
        report_ref=report_ref,
        pointer="/aoi_id",
        field_id="aoi.id",
        display_name="AOI id",
        dds_payload_path="/plots/0/id",
    )
    _add_report_field(
        field_values,
        report=report,
        report_ref=report_ref,
        pointer="/results_summary/aoi_area/area_ha",
        field_id="plot.area_ha",
        display_name="Plot area",
        dds_payload_path="/plots/0/areaHa",
        required_for_payload=True,
        value_type="number",
        unit="ha",
        method_id="aoi_area_from_report",
    )
    _add_report_field(
        field_values,
        report=report,
        report_ref=report_ref,
        pointer="/results_summary/deforestation_free_post_2020/forest_loss_post_2020_ha",
        field_id="evidence.forest_loss_post_2020_ha",
        display_name="Post-2020 forest loss",
        dds_payload_path=None,
        value_type="number",
        unit="ha",
        method_id="forest_loss_post_2020_from_report",
    )
    _add_report_field(
        field_values,
        report=report,
        report_ref=report_ref,
        pointer="/datasets",
        field_id="evidence.dataset_versions",
        display_name="Evidence dataset versions",
        dds_payload_path=None,
        value_type="array",
    )
    _add_report_field(
        field_values,
        report=report,
        report_ref=report_ref,
        pointer="/parameters/implementation",
        field_id="evidence.code_versions",
        display_name="Evidence code versions",
        dds_payload_path=None,
        value_type="object",
    )
    _add_report_field(
        field_values,
        report=report,
        report_ref=report_ref,
        pointer="/report_version",
        field_id="evidence.report_version",
        display_name="AOI report version",
        dds_payload_path=None,
    )

    evidence_errors, evidence_warnings, evidence_gates = _evidence_errors_and_warnings(
        bundle_dir=bundle_path,
        manifest=manifest,
        report=report,
        manifest_ref=manifest_ref,
        report_ref=report_ref,
    )
    errors = list(evidence_errors)
    warnings = list(evidence_warnings)

    missing_mandatory_fields: list[str] = []
    for item in field_values:
        if item.get("required_for_payload") and item.get("mapping_status") != "mapped":
            field_id = str(item["field_id"])
            missing_mandatory_fields.append(field_id)
            errors.append(
                _validation_failure(
                    code=f"MISSING_{field_id.upper().replace('.', '_')}",
                    severity="error",
                    category="missing_field",
                    message=f"{item['display_name']} is required before operator review.",
                    blocking=True,
                    field_id=field_id,
                    json_pointer=f"/field_values/{_json_pointer_escape(field_id)}",
                    requirement_ref=f"dds-client:{field_id}",
                    remediation_code=f"PROVIDE_{field_id.upper().replace('.', '_')}",
                    source_refs=item.get("source_refs") or [],
                    details={"expected_source": "portal_intake_or_evidence_bundle"},
                )
            )

    mandatory_gate = {
        "gate_id": "mandatory_fields_present",
        "status": "blocked" if missing_mandatory_fields else "pass",
        "blocking": bool(missing_mandatory_fields),
        "reason_codes": ["MISSING_MANDATORY_FIELD"] if missing_mandatory_fields else [],
        "source_refs": [],
    }
    operator_gate = {
        "gate_id": "operator_review_required",
        "status": "warning",
        "blocking": False,
        "reason_codes": ["OPERATOR_REVIEW_REQUIRED"],
        "source_refs": [],
    }
    (
        risk_register,
        evidence_gaps,
        mitigation_actions,
        risk_gate,
        risk_errors,
        risk_warnings,
    ) = _build_risk_package(
        bundle_dir=bundle_path,
        report=report,
        report_ref=report_ref,
        intake=intake,
        intake_id=intake_id,
        intake_ref=intake_ref,
        created_at_utc=created_iso,
        missing_mandatory_fields=missing_mandatory_fields,
        evidence_errors=evidence_errors,
        evidence_warnings=evidence_warnings,
    )
    errors.extend(risk_errors)
    warnings.extend(risk_warnings)
    warnings.append(
        _validation_failure(
            code="OPERATOR_REVIEW_REQUIRED",
            severity="warning",
            category="operator_review_required",
            message=(
                "Draft DDS evidence contract requires authorised operator review before any "
                "submission."
            ),
            blocking=False,
            requirement_ref="workflow:operator_review",
            source_refs=[],
            details={"submission_adapter": "not implemented in this repository"},
        )
    )

    pre_submission_gates = [mandatory_gate, *evidence_gates, risk_gate, operator_gate]
    blocked_gates = [gate["gate_id"] for gate in pre_submission_gates if gate.get("blocking")]

    candidate = _payload_candidate(field_values)
    candidate_sha = sha256_bytes(canonical_json_bytes(candidate)) if candidate is not None else None
    status = (
        "blocked_by_validation"
        if errors or blocked_gates
        else ("warnings_present" if warnings else "valid_for_operator_review")
    )
    validation_result = {
        "status": status,
        "schema_version": SCHEMA_VERSION,
        "mapper_version": MAPPER_VERSION,
        "validated_at_utc": created_iso,
        "errors": errors,
        "warnings": warnings,
        "missing_mandatory_fields": sorted(missing_mandatory_fields),
        "ambiguous_mappings": [],
        "blocked_gates": sorted(blocked_gates),
        "payload_candidate_sha256": candidate_sha,
    }

    all_artifacts = _collect_manifest_artifact_refs(
        bundle_dir=bundle_path,
        manifest=manifest,
        manifest_ref=manifest_ref,
        report_ref=report_ref,
        intake_ref=intake_ref,
    )
    risk_artifact_refs = [
        _json_artifact_ref_from_obj(
            relpath="risk/risk_register.json",
            obj=risk_register,
            role="risk_register",
        ),
        _json_artifact_ref_from_obj(
            relpath="risk/evidence_gaps.json",
            obj=evidence_gaps,
            role="evidence_gaps",
        ),
        _json_artifact_ref_from_obj(
            relpath="risk/mitigation_actions.json",
            obj=mitigation_actions,
            role="mitigation_actions",
        ),
    ]
    all_artifacts.extend(risk_artifact_refs)
    draft = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": created_iso,
        "draft_id": draft_id,
        "internal_reference_number": internal_reference,
        "mapper": {
            "name": MAPPER_NAME,
            "version": MAPPER_VERSION,
            "git_commit": _git_commit(),
            "configuration_hash": _configuration_hash(),
        },
        "portal_intake": {
            "intake_id": intake_id,
            **({"case_id": str(case_id)} if case_id else {}),
            "schema_version": intake_schema_version,
            "submitted_at_utc": submitted_at,
            "operator_id": str(operator_id) if operator_id is not None else None,
            "source_ref": intake_ref,
            "field_refs": [
                ref
                for item in field_values
                for ref in item.get("source_refs", [])
                if ref.get("source_kind") == "portal_intake"
            ],
        },
        "evidence_bundle": {
            "bundle_id": str(report.get("bundle_id") or bundle_path.name),
            "bundle_date": _extract_bundle_date(bundle_path),
            "bundle_manifest_ref": manifest_ref,
            "aoi_report_refs": [report_ref],
            "derived_output_refs": risk_artifact_refs,
        },
        "field_values": field_values,
        "pre_submission_gates": pre_submission_gates,
        "validation_result": validation_result,
        "evidence_hashes": {
            "hash_algorithm": "sha256",
            "bundle_manifest_sha256": manifest_ref["sha256"],
            "dds_draft_payload_sha256": candidate_sha,
            "artifacts": all_artifacts,
        },
        "payload_candidate": candidate,
        "review_state": (
            "mitigation_required"
            if status == "blocked_by_validation"
            else "ready_for_operator_review"
        ),
        "limitations": {
            "not_eudr_compliance_determination": True,
            "no_unattended_submission": True,
            "requires_operator_review": True,
            "notes": [
                "Draft evidence contract only.",
                "This repository does not submit to TRACES or EUDR DDS services.",
            ],
        },
    }

    risk_package = {
        "risk_register": risk_register,
        "evidence_gaps": evidence_gaps,
        "mitigation_actions": mitigation_actions,
    }
    if return_risk_package:
        return draft, validation_result, risk_package
    return draft, validation_result


def validate_dds_draft(draft: dict[str, Any], *, schema_path: str | Path | None = None) -> None:
    from jsonschema import Draft202012Validator, FormatChecker

    schema_file = Path(schema_path) if schema_path else Path("schemas/dds/dds_draft_v1.schema.json")
    schema = _load_json(schema_file)
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(draft)


def write_dds_draft(
    *,
    bundle_dir: str | Path,
    portal_intake_path: str | Path,
    aoi_report_path: str | Path | None = None,
    out_dir: str | Path | None = None,
    created_at_utc: str | None = None,
    schema_path: str | Path | None = None,
) -> tuple[Path, Path, dict[str, Any]]:
    draft, validation_result, risk_package = build_dds_draft(
        bundle_dir=bundle_dir,
        portal_intake_path=portal_intake_path,
        aoi_report_path=aoi_report_path,
        created_at_utc=created_at_utc,
        return_risk_package=True,
    )
    validate_dds_draft(draft, schema_path=schema_path)
    risk_dir = Path(bundle_dir) / "risk"
    write_json(risk_dir / "risk_register.json", risk_package["risk_register"])
    write_json(risk_dir / "evidence_gaps.json", risk_package["evidence_gaps"])
    write_json(risk_dir / "mitigation_actions.json", risk_package["mitigation_actions"])
    output_dir = Path(out_dir) if out_dir else Path(bundle_dir) / "dds"
    draft_path = output_dir / "dds_draft.json"
    validation_path = output_dir / "validation_result.json"
    write_json(draft_path, draft)
    write_json(validation_path, validation_result)
    return draft_path, validation_path, draft
