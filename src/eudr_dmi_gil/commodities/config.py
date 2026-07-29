from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CommodityConfig:
    id: str
    display_name: str
    provider: str
    dataset_title: str
    dataset_version: str
    asset_id: str
    class_values: tuple[int, ...]
    class_labels: tuple[str, ...]
    observation_year: int
    country_scope: tuple[str, ...]
    optional: bool = True
    local_path: str | None = None
    source_url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["class_values"] = list(self.class_values)
        out["class_labels"] = list(self.class_labels)
        out["country_scope"] = list(self.country_scope)
        return out

    @property
    def raster_source(self) -> str:
        return self.local_path or self.asset_id


def load_commodity_config(path: str | Path) -> CommodityConfig:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if "commodities" in payload:
        commodities = payload.get("commodities")
        if isinstance(commodities, list) and len(commodities) != 1:
            raise ValueError(
                "Commodity config must contain exactly one commodity; multi-commodity mode is not implemented"
            )
        if isinstance(commodities, list) and commodities:
            payload = {"commodity": commodities[0]}
    raw = payload.get("commodity", payload)
    if not isinstance(raw, dict):
        raise ValueError("Commodity config must be an object")
    return _commodity_config_from_mapping(raw)


def resolve_commodity_config(
    *,
    commodity_id: str | None,
    commodity_config_path: str | Path | None,
) -> CommodityConfig | None:
    config = load_commodity_config(commodity_config_path) if commodity_config_path else None
    if commodity_id:
        commodity_id = commodity_id.strip().lower()
        if not commodity_id:
            raise ValueError("--commodity cannot be empty")
        if config is not None and config.id != commodity_id:
            raise ValueError(
                f"--commodity {commodity_id!r} does not match commodity config id {config.id!r}"
            )
        if config is None:
            config = _builtin_commodity_config(commodity_id)
    return config


def _commodity_config_from_mapping(raw: dict[str, Any]) -> CommodityConfig:
    commodity_id = str(raw.get("id") or raw.get("commodity") or "").strip().lower()
    if not commodity_id:
        raise ValueError("Commodity config requires id")

    class_values = tuple(int(v) for v in raw.get("class_values", []))
    if not class_values:
        raise ValueError("Commodity config requires at least one class value")
    class_labels_raw = tuple(str(v) for v in raw.get("class_labels", []))
    if class_labels_raw and len(class_labels_raw) != len(class_values):
        raise ValueError("Commodity class_labels length must match class_values length")
    class_labels = class_labels_raw or tuple(str(v) for v in class_values)

    country_scope = tuple(str(v).strip() for v in raw.get("country_scope", []) if str(v).strip())
    if not country_scope:
        raise ValueError("Commodity config requires country_scope")

    observation_year = int(raw["observation_year"])
    if observation_year < 1900:
        raise ValueError("Commodity observation_year must be >= 1900")

    return CommodityConfig(
        id=commodity_id,
        display_name=str(raw.get("display_name") or commodity_id.title()),
        provider=str(raw.get("provider") or "").strip(),
        dataset_title=str(raw.get("dataset_title") or raw.get("dataset") or "").strip(),
        dataset_version=str(raw.get("dataset_version") or raw.get("version") or "").strip(),
        asset_id=str(raw.get("asset_id") or raw.get("source") or "").strip(),
        class_values=class_values,
        class_labels=class_labels,
        observation_year=observation_year,
        country_scope=country_scope,
        optional=bool(raw.get("optional", True)),
        local_path=str(raw.get("local_path")).strip() if raw.get("local_path") else None,
        source_url=str(raw.get("source_url")).strip() if raw.get("source_url") else None,
    )


def _builtin_commodity_config(commodity_id: str) -> CommodityConfig:
    if commodity_id != "coffee":
        raise ValueError(f"Unsupported commodity: {commodity_id}")
    dataset_version = os.environ.get("EUDR_DMI_COFFEE_MAPBIOMAS_VERSION", "").strip()
    asset_id = os.environ.get("EUDR_DMI_COFFEE_MAPBIOMAS_ASSET_ID", "").strip()
    local_path = os.environ.get("EUDR_DMI_COFFEE_MAPBIOMAS_LOCAL_PATH", "").strip() or None
    return CommodityConfig(
        id="coffee",
        display_name="Coffee",
        provider="mapbiomas_brazil",
        dataset_title="MapBiomas Brazil Land Cover",
        dataset_version=dataset_version,
        asset_id=asset_id,
        class_values=(46,),
        class_labels=("Coffee plantations",),
        observation_year=2023,
        country_scope=("Brazil",),
        optional=True,
        local_path=local_path,
        source_url="https://brasil.mapbiomas.org/",
    )
