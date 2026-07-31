from __future__ import annotations

import json
from pathlib import Path

import pytest

from eudr_dmi_gil.commodities.config import (
    MODE_DISCRETE_CLASSES,
    MODE_PROBABILITY_THRESHOLD,
    CommodityConfig,
    load_commodity_config,
)


def _write_config(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _base_probability_payload(**overrides) -> dict:
    payload = {
        "commodity": {
            "id": "coffee",
            "display_name": "Coffee",
            "provider": "forestdatapartnership",
            "mode": "probability_threshold",
            "dataset_title": "FDP Coffee Probability model 2025b",
            "dataset_version": "2025b",
            "asset_id": "projects/forestdatapartnership/assets/coffee/model_2025b",
            "observation_year": 2024,
            "probability_band": "probability",
            "threshold": 0.1,
            "sensitivity_thresholds": [0.28, 0.5],
            "country_scope": ["Ghana"],
        }
    }
    payload["commodity"].update(overrides)
    return payload


def test_probability_mode_requires_threshold(tmp_path: Path) -> None:
    payload = _base_probability_payload()
    del payload["commodity"]["threshold"]
    path = _write_config(tmp_path / "config.json", payload)
    with pytest.raises(ValueError, match="threshold"):
        load_commodity_config(path)


def test_probability_mode_rejects_out_of_range_threshold(tmp_path: Path) -> None:
    path = _write_config(tmp_path / "config.json", _base_probability_payload(threshold=1.5))
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        load_commodity_config(path)


def test_probability_mode_rejects_out_of_range_sensitivity_threshold(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path / "config.json", _base_probability_payload(sensitivity_thresholds=[-0.1])
    )
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        load_commodity_config(path)


def test_probability_mode_ignores_missing_class_values(tmp_path: Path) -> None:
    path = _write_config(tmp_path / "config.json", _base_probability_payload())
    cfg = load_commodity_config(path)
    assert cfg.mode == MODE_PROBABILITY_THRESHOLD
    assert cfg.class_values == ()
    assert cfg.threshold == 0.1
    assert cfg.sensitivity_thresholds == (0.28, 0.5)


def test_probability_mode_defaults_band_when_absent(tmp_path: Path) -> None:
    payload = _base_probability_payload()
    del payload["commodity"]["probability_band"]
    path = _write_config(tmp_path / "config.json", payload)
    cfg = load_commodity_config(path)
    assert cfg.probability_band == "probability"


def test_provider_field_is_not_used_for_mode_dispatch(tmp_path: Path) -> None:
    """`provider` records provenance only; `mode` must select raster semantics."""
    from eudr_dmi_gil.commodities.providers import provider_for_config, ProbabilityThresholdCommodityProvider

    path = _write_config(
        tmp_path / "config.json",
        _base_probability_payload(provider="some_other_org"),
    )
    cfg = load_commodity_config(path)
    provider = provider_for_config(cfg)
    assert isinstance(provider, ProbabilityThresholdCommodityProvider)


def test_discrete_classes_mode_unchanged_requires_class_values(tmp_path: Path) -> None:
    payload = {
        "commodity": {
            "id": "coffee",
            "display_name": "Coffee",
            "provider": "mapbiomas_brazil",
            "dataset_title": "MapBiomas Brazil Land Cover",
            "dataset_version": "collection-9-2023",
            "asset_id": "gs://fixture/coffee.tif",
            "observation_year": 2023,
            "country_scope": ["Brazil"],
        }
    }
    path = _write_config(tmp_path / "config.json", payload)
    with pytest.raises(ValueError, match="class value"):
        load_commodity_config(path)


def test_discrete_classes_mode_still_works(tmp_path: Path) -> None:
    payload = {
        "commodity": {
            "id": "coffee",
            "display_name": "Coffee",
            "provider": "mapbiomas_brazil",
            "dataset_title": "MapBiomas Brazil Land Cover",
            "dataset_version": "collection-9-2023",
            "asset_id": "gs://fixture/coffee.tif",
            "class_values": [46],
            "observation_year": 2023,
            "country_scope": ["Brazil"],
        }
    }
    path = _write_config(tmp_path / "config.json", payload)
    cfg = load_commodity_config(path)
    assert cfg.mode == MODE_DISCRETE_CLASSES
    assert cfg.threshold is None
    assert cfg.class_values == (46,)


def test_unsupported_mode_is_rejected(tmp_path: Path) -> None:
    path = _write_config(tmp_path / "config.json", _base_probability_payload(mode="not_a_real_mode"))
    with pytest.raises(ValueError, match="Unsupported commodity mode"):
        load_commodity_config(path)


def test_commodity_config_construction_requires_no_default_threshold() -> None:
    """The provider itself must not supply a hidden threshold default."""
    cfg = CommodityConfig(
        id="coffee",
        display_name="Coffee",
        provider="forestdatapartnership",
        dataset_title="FDP Coffee Probability model 2025b",
        dataset_version="2025b",
        asset_id="projects/forestdatapartnership/assets/coffee/model_2025b",
        observation_year=2024,
        country_scope=("Ghana",),
        mode=MODE_PROBABILITY_THRESHOLD,
    )
    assert cfg.threshold is None
