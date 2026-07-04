from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


DatasetRole = Literal["forest_baseline_2020", "change", "confirmation", "attribution"]


@dataclass(frozen=True)
class DatasetDefinition:
    dataset_id: str
    display_name: str
    asset_id: str
    role: DatasetRole
    start_year: int | None
    latest_available_year: int | None
    update_policy: str
    mandatory: bool
    notes: str

    def output_version(self, *, used_through_year: int | None = None) -> dict[str, object]:
        payload: dict[str, object] = {
            "asset_id": self.asset_id,
            "role": self.role,
            "mandatory": self.mandatory,
        }
        if self.role == "forest_baseline_2020":
            payload["baseline_year"] = 2020
        else:
            payload["latest_available_year"] = self.latest_available_year
            payload["used_through_year"] = used_through_year
        if self.notes:
            payload["notes"] = self.notes
        return payload


DATASET_REGISTRY: dict[str, DatasetDefinition] = {
    "gfc2020": DatasetDefinition(
        dataset_id="gfc2020",
        display_name="JRC Global Forest Cover 2020",
        asset_id="JRC/GFC2020/V3",
        role="forest_baseline_2020",
        start_year=2020,
        latest_available_year=2020,
        update_policy="versioned_baseline",
        mandatory=True,
        notes=(
            "Non-mandatory, non-exclusive, non-legally-binding 2020 forest baseline "
            "evidence layer."
        ),
    ),
    "hansen_gfc": DatasetDefinition(
        dataset_id="hansen_gfc",
        display_name="Hansen Global Forest Change",
        asset_id="UMD/hansen/global_forest_change_2025_v1_13",
        role="change",
        start_year=2001,
        latest_available_year=2025,
        update_policy="annual_release",
        mandatory=True,
        notes="Use lossyear values corresponding to 2021 onward for post-cutoff evidence.",
    ),
    "jrc_tmf": DatasetDefinition(
        dataset_id="jrc_tmf",
        display_name="JRC Tropical Moist Forest deforestation/degradation",
        asset_id="projects/JRC/TMF/v1_2025",
        role="change",
        start_year=1990,
        latest_available_year=2025,
        update_policy="annual_release",
        mandatory=False,
        notes="Applicable primarily in tropical moist forest geographies.",
    ),
    "radd": DatasetDefinition(
        dataset_id="radd",
        display_name="RADD Sentinel-1 disturbance alerts",
        asset_id="projects/radar-wur/raddalert/v1",
        role="change",
        start_year=2019,
        latest_available_year=None,
        update_policy="near_real_time_geography_dependent",
        mandatory=False,
        notes="Coverage and temporal availability depend on selected geography.",
    ),
    "sentinel_confirmation": DatasetDefinition(
        dataset_id="sentinel_confirmation",
        display_name="Sentinel-1/2 before-after confirmation indices",
        asset_id="provider_configured_sentinel_confirmation",
        role="confirmation",
        start_year=2015,
        latest_available_year=None,
        update_policy="provider_configured",
        mandatory=False,
        notes="Provider-specific confirmation layer; not enabled by default.",
    ),
}


def get_dataset(dataset_id: str) -> DatasetDefinition:
    try:
        return DATASET_REGISTRY[dataset_id]
    except KeyError as exc:
        raise KeyError(f"Unknown dataset_id: {dataset_id}") from exc


def selected_change_datasets(include_optional: list[str] | None = None) -> list[DatasetDefinition]:
    optional = set(include_optional or [])
    selected = [DATASET_REGISTRY["hansen_gfc"]]
    for dataset_id in sorted(optional):
        dataset = get_dataset(dataset_id)
        if dataset.role != "forest_baseline_2020":
            selected.append(dataset)
    return selected


def latest_available_complete_evidence_year(
    datasets: list[DatasetDefinition],
) -> int:
    years = [
        dataset.latest_available_year
        for dataset in datasets
        if dataset.mandatory and dataset.role != "forest_baseline_2020"
    ]
    known_years = [year for year in years if year is not None]
    if len(known_years) != len(years) or not known_years:
        raise ValueError("Latest available year is unknown for a mandatory selected layer")
    return min(known_years)


def resolve_end_year(
    *,
    requested_end_year: str | int,
    selected_datasets: list[DatasetDefinition],
) -> tuple[int, str, list[dict[str, object]]]:
    warnings: list[dict[str, object]] = []

    if requested_end_year == "auto":
        return (
            latest_available_complete_evidence_year(selected_datasets),
            "auto_latest_available_complete_evidence_year",
            warnings,
        )

    try:
        resolved = int(requested_end_year)
    except (TypeError, ValueError) as exc:
        raise ValueError("--end-year must be 'auto' or a calendar year") from exc

    for dataset in selected_datasets:
        latest = dataset.latest_available_year
        if latest is None:
            if dataset.mandatory:
                raise ValueError(
                    f"Requested end year {resolved} cannot be validated for mandatory "
                    f"layer {dataset.dataset_id}"
                )
            warnings.append(
                {
                    "code": "optional_layer_year_unknown",
                    "dataset_id": dataset.dataset_id,
                    "requested_end_year": resolved,
                    "latest_available_year": None,
                    "message": (
                        f"Optional layer {dataset.dataset_id} has geography/provider-dependent "
                        "temporal coverage."
                    ),
                }
            )
            continue
        if resolved > latest:
            if dataset.mandatory:
                raise ValueError(
                    f"Requested end year {resolved} exceeds latest available year {latest} "
                    f"for mandatory layer {dataset.dataset_id}"
                )
            warnings.append(
                {
                    "code": "optional_layer_unavailable_for_requested_year",
                    "dataset_id": dataset.dataset_id,
                    "requested_end_year": resolved,
                    "latest_available_year": latest,
                    "message": (
                        f"Optional layer {dataset.dataset_id} only supports evidence through "
                        f"{latest}."
                    ),
                }
            )

    return resolved, "user_specified", warnings

