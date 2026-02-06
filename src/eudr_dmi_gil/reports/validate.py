from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import jsonschema
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError


def _find_repo_root(start: Path) -> Path:
    current = start
    for _ in range(10):
        if (current / "pyproject.toml").exists() and (current / "schemas").exists():
            return current
        if current.parent == current:
            break
        current = current.parent
    raise RuntimeError("Could not locate repo root (pyproject.toml + schemas/) from: " + str(start))


def _default_schema_path() -> Path:
    repo_root = _find_repo_root(Path(__file__).resolve())
    return repo_root / "schemas" / "reports" / "aoi_report_v1.schema.json"


def load_schema(schema_path: str | Path | None = None) -> dict[str, Any]:
    path = Path(schema_path) if schema_path is not None else _default_schema_path()
    return json.loads(path.read_text(encoding="utf-8"))


def validate_aoi_report_v1(
    report: Mapping[str, Any],
    *,
    schema_path: str | Path | None = None,
) -> None:
    """Validate an AOI report JSON object against the AOI report v1 schema.

    Raises:
      jsonschema.exceptions.ValidationError if invalid.
    """

    schema = load_schema(schema_path)
    validator = Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
    validator.validate(dict(report))

    _validate_traceability(dict(report))
    _validate_hansen_methodology(dict(report))


def _validate_traceability(report: Mapping[str, Any]) -> None:
    evidence_classes = {
        item.get("class_id")
        for item in report.get("evidence_registry", {}).get("evidence_classes", [])
        if isinstance(item, Mapping)
    }
    acceptance_criteria = {
        item.get("criteria_id")
        for item in report.get("acceptance_criteria", [])
        if isinstance(item, Mapping)
    }
    results = {
        item.get("result_id")
        for item in report.get("results", [])
        if isinstance(item, Mapping)
    }

    traceability = report.get("regulatory_traceability", [])

    referenced_results: set[str] = set()
    for entry in traceability:
        if not isinstance(entry, Mapping):
            continue
        evidence_class = entry.get("evidence_class")
        criteria_id = entry.get("acceptance_criteria")
        result_ref = entry.get("result_ref")

        if evidence_class and evidence_class not in evidence_classes:
            raise ValidationError(
                f"Traceability references unknown evidence_class: {evidence_class}"
            )
        if criteria_id and criteria_id not in acceptance_criteria:
            raise ValidationError(
                f"Traceability references unknown acceptance_criteria: {criteria_id}"
            )
        if result_ref and result_ref not in results:
            raise ValidationError(
                f"Traceability references unknown result_ref: {result_ref}"
            )
        if isinstance(result_ref, str):
            referenced_results.add(result_ref)

    orphaned_results = sorted(r for r in results if r not in referenced_results)
    if orphaned_results:
        raise ValidationError(f"Orphaned results without traceability: {orphaned_results}")

    _validate_assumptions(report, results)


def _validate_assumptions(report: Mapping[str, Any], results: set[str]) -> None:
    assumptions = report.get("assumptions", [])
    assumption_ids: set[str] = set()
    non_testable_result_ids: set[str] = set()

    for item in assumptions:
        if not isinstance(item, Mapping):
            continue
        assumption_id = item.get("assumption_id")
        if isinstance(assumption_id, str):
            assumption_ids.add(assumption_id)
        affects = item.get("affects_results")
        if isinstance(affects, list):
            for result_ref in affects:
                if not isinstance(result_ref, str):
                    continue
                if result_ref not in results:
                    raise ValidationError(
                        f"Assumption references unknown result_ref: {result_ref}"
                    )
                if item.get("testable") is False:
                    non_testable_result_ids.add(result_ref)

    for entry in report.get("results", []):
        if not isinstance(entry, Mapping):
            continue
        result_id = entry.get("result_id")
        assumption_refs = entry.get("assumption_refs")
        if isinstance(assumption_refs, list):
            for assumption_ref in assumption_refs:
                if not isinstance(assumption_ref, str):
                    continue
                if assumption_ref not in assumption_ids:
                    raise ValidationError(
                        f"Result references unknown assumption_ref: {assumption_ref}"
                    )

        if isinstance(result_id, str) and result_id in non_testable_result_ids:
            if entry.get("non_testable_due_to_assumptions") is not True:
                raise ValidationError(
                    f"Result {result_id} must set non_testable_due_to_assumptions=true"
                )


def _validate_hansen_methodology(report: Mapping[str, Any]) -> None:
    metrics = report.get("metrics", {})
    if not isinstance(metrics, dict):
        return

    result_refs_forest_loss = _results_reference_forest_loss(report)

    if "pixel_forest_loss_post_2020_ha" not in metrics and not result_refs_forest_loss:
        return

    computed = report.get("computed")
    if not isinstance(computed, Mapping):
        raise ValidationError("computed.forest_loss_post_2020 must be present")

    computed_forest = computed.get("forest_loss_post_2020")
    if not isinstance(computed_forest, Mapping):
        raise ValidationError("computed.forest_loss_post_2020 must be present")

    methodology = report.get("methodology")
    if not isinstance(methodology, Mapping):
        raise ValidationError("methodology block is required for post-2020 forest loss metrics")

    forest_method = methodology.get("forest_loss_post_2020")
    if not isinstance(forest_method, Mapping):
        raise ValidationError("methodology.forest_loss_post_2020 must be present")

    if forest_method.get("is_placeholder") is not False:
        raise ValidationError("methodology.forest_loss_post_2020.is_placeholder must be false")

    computed_outputs = report.get("computed_outputs")
    if not isinstance(computed_outputs, Mapping):
        raise ValidationError("computed_outputs.forest_loss_post_2020 must be present")
    if not isinstance(computed_outputs.get("forest_loss_post_2020"), Mapping):
        raise ValidationError("computed_outputs.forest_loss_post_2020 must be present")

    forest_outputs = computed_outputs.get("forest_loss_post_2020", {})
    if not isinstance(forest_outputs, Mapping):
        raise ValidationError("computed_outputs.forest_loss_post_2020 must be present")

    _ensure_evidence_refs(report, forest_outputs)
    _ensure_validation_refs(report)


def _results_reference_forest_loss(report: Mapping[str, Any]) -> bool:
    for entry in report.get("results", []):
        if not isinstance(entry, Mapping):
            continue
        evidence_classes = entry.get("evidence_classes")
        if isinstance(evidence_classes, list):
            if "forest_loss_post_2020" in evidence_classes:
                return True
    return False


def _collect_evidence_relpaths(report: Mapping[str, Any]) -> set[str]:
    relpaths: set[str] = set()
    for item in report.get("evidence_artifacts", []):
        if not isinstance(item, Mapping):
            continue
        relpath = item.get("relpath")
        if isinstance(relpath, str):
            relpaths.add(relpath)
    return relpaths


def _ensure_evidence_refs(report: Mapping[str, Any], forest_outputs: Mapping[str, Any]) -> None:
    relpaths = _collect_evidence_relpaths(report)

    mask_ref = forest_outputs.get("mask_geojson_ref")
    if isinstance(mask_ref, Mapping):
        relpath = mask_ref.get("relpath")
        if isinstance(relpath, str) and relpath not in relpaths:
            raise ValidationError(f"Missing evidence_artifacts relpath: {relpath}")

    tiles_ref = forest_outputs.get("tiles_manifest_ref")
    if isinstance(tiles_ref, Mapping):
        relpath = tiles_ref.get("relpath")
        if isinstance(relpath, str) and relpath not in relpaths:
            raise ValidationError(f"Missing evidence_artifacts relpath: {relpath}")


def _ensure_validation_refs(report: Mapping[str, Any]) -> None:
    relpaths = _collect_evidence_relpaths(report)
    validation = report.get("validation")
    if not isinstance(validation, Mapping):
        return
    crosscheck = validation.get("forest_area_crosscheck")
    if not isinstance(crosscheck, Mapping):
        return
    csv_ref = crosscheck.get("csv_ref")
    if isinstance(csv_ref, Mapping):
        relpath = csv_ref.get("relpath")
        if isinstance(relpath, str) and relpath not in relpaths:
            raise ValidationError(f"Missing evidence_artifacts relpath: {relpath}")


def validate_aoi_report_v1_file(
    json_path: str | Path,
    *,
    schema_path: str | Path | None = None,
) -> dict[str, Any]:
    """Load and validate a report JSON file; returns the parsed JSON."""

    obj = json.loads(Path(json_path).read_text(encoding="utf-8"))
    validate_aoi_report_v1(obj, schema_path=schema_path)
    return obj
