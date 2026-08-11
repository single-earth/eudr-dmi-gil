"""Tests for the two-situation geemap coffee-screening pipeline.

Verdict/metric-naming logic is pure Python and tested without Earth Engine. The Image-algebra
tests (validity masking, union-not-sum, temporal loss-year capping) need real ``ee.Image``
objects, so they call live Earth Engine with tiny synthetic constant rasters (no real dataset
downloads) and are skipped if Earth Engine credentials are unavailable.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_two_situation_geemap_pipeline.py"

_MODULE_NAME = "run_two_situation_geemap_pipeline"
_spec = importlib.util.spec_from_file_location(_MODULE_NAME, SCRIPT_PATH)
pipeline = importlib.util.module_from_spec(_spec)
sys.modules[_MODULE_NAME] = pipeline  # dataclass() needs the module registered to resolve
# string annotations produced by `from __future__ import annotations` in the target script.
_spec.loader.exec_module(pipeline)


def _make_run_config(**overrides):
    defaults = dict(
        config_path=REPO_ROOT / "data_db" / "two_situation_geemap_pipeline_config.json",
        asset_catalogue_path=REPO_ROOT / "data_db" / "two_situation_geemap_pipeline_assets.csv",
        aoi_geojson_paths=(),
        baseline_year=2020,
        change_start_year=2021,
        change_end_year=2025,
        latest_coffee_year=2024,
        coffee_class=46,
        fdp_threshold=0.25,
        intersection_scale=30,
        season_start_mmdd="07-01",
        season_end_mmdd="10-01",
        s2_max_scene_cloud=20,
        mapbiomas_collection_id=10.0,
        mapbiomas_version="v1",
        asset_ids={},
        esri_world_imagery="",
        esri_attribution="",
        openstreetmap_mapnik="",
        openstreetmap_attribution="",
    )
    defaults.update(overrides)
    return pipeline.PipelineConfig(**defaults)


def _metrics(run_config, *, loss=0.0, agreement=0.0, any_new=0.0, any_latest=0.0):
    attribution_end = pipeline.attribution_end_year(run_config)
    change_key = f"{run_config.change_start_year}_{run_config.change_end_year}"
    attribution_key = f"{run_config.change_start_year}_{attribution_end}"
    return {
        "attribution_end_year": attribution_end,
        f"jrc_forest_loss_{change_key}_ha": loss,
        f"loss_{attribution_key}_and_agreement_new_coffee_ha": agreement,
        f"loss_{attribution_key}_and_any_new_coffee_ha": any_new,
        f"loss_{change_key}_and_any_latest_coffee_ha": any_latest,
    }


@pytest.fixture(autouse=True)
def _run_config():
    pipeline.RUN_CONFIG = _make_run_config()
    yield
    pipeline.RUN_CONFIG = None


# ---- Pure-logic tests: attribution_end_year (Test 7) ----


def test_attribution_end_year_caps_at_latest_coffee_year():
    run_config = _make_run_config(change_end_year=2025, latest_coffee_year=2024)
    assert pipeline.attribution_end_year(run_config) == 2024


def test_attribution_end_year_caps_at_change_end_year_when_coffee_is_newer():
    run_config = _make_run_config(change_end_year=2023, latest_coffee_year=2024)
    assert pipeline.attribution_end_year(run_config) == 2023


# ---- Pure-logic tests: verdict_text() evidence hierarchy ----


def test_verdict_no_loss_case_a():
    text = pipeline.verdict_text(_metrics(pipeline.RUN_CONFIG, loss=0.0))
    assert "no forest-loss evidence" in text


def test_verdict_loss_without_coffee_association_case_b():
    text = pipeline.verdict_text(
        _metrics(pipeline.RUN_CONFIG, loss=1.0, agreement=0, any_new=0, any_latest=0)
    )
    assert "disturbance-without-coffee-attribution" in text


def test_verdict_fdp_only_candidate_case_d_fazenda_sucuri_like():
    # Mirrors the Fazenda Sucuri regression: an FDP-only new-coffee signal intersects loss but
    # MapBiomas does not agree, so BOTH (agreement) is zero while ANY is nonzero.
    text = pipeline.verdict_text(
        _metrics(pipeline.RUN_CONFIG, loss=24.15, agreement=0.0, any_new=0.4244, any_latest=0.8488)
    )
    assert "Additional screening is required" in text
    assert "agreement is absent" in text


def test_verdict_mapbiomas_only_candidate_case_d_symmetric():
    # Source identity shouldn't matter to the ANY-vs-BOTH distinction, only which combination of
    # metrics is nonzero.
    text = pipeline.verdict_text(
        _metrics(pipeline.RUN_CONFIG, loss=10.0, agreement=0.0, any_new=1.2, any_latest=0.0)
    )
    assert "Additional screening is required" in text


def test_verdict_cross_source_agreement_case_c_takes_priority_over_any():
    text = pipeline.verdict_text(
        _metrics(pipeline.RUN_CONFIG, loss=10.0, agreement=2.0, any_new=2.0, any_latest=2.0)
    )
    assert "cross-source candidate-conversion evidence" in text


def test_verdict_latest_only_association_case_e():
    text = pipeline.verdict_text(
        _metrics(pipeline.RUN_CONFIG, loss=5.0, agreement=0.0, any_new=0.0, any_latest=0.25)
    )
    assert "does not establish a post-2020 coffee transition" in text


def test_verdict_never_claims_compliance_determination():
    cases = [
        dict(loss=0, agreement=0, any_new=0, any_latest=0),
        dict(loss=5, agreement=0, any_new=0, any_latest=0),
        dict(loss=5, agreement=2, any_new=2, any_latest=2),
        dict(loss=5, agreement=0, any_new=2, any_latest=0),
        dict(loss=5, agreement=0, any_new=0, any_latest=1),
    ]
    for case in cases:
        text = pipeline.verdict_text(_metrics(pipeline.RUN_CONFIG, **case)).lower()
        assert "compliant" not in text
        assert "confirmed" not in text
        assert "deforestation caused by coffee" not in text


# ---- Earth Engine image-algebra tests ----


@pytest.fixture(scope="module")
def ee_ready():
    import ee

    try:
        ee.Initialize(project="myproject-gq-74696")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"Earth Engine not available: {exc}")
    return ee


def test_fdp_new_coffee_requires_valid_baseline_observation(ee_ready):
    ee = ee_ready
    aoi = ee.Geometry.Rectangle([0.0, 0.0, 0.02, 0.01])
    lon = ee.Image.pixelLonLat().select("longitude")
    western_half = lon.lt(0.01)  # baseline FDP observed only in the western half

    previous_probability = ee.Image.constant(0.1).updateMask(western_half).clip(aoi)
    previous_coffee = previous_probability.gte(0.25).selfMask().rename("prev_coffee")
    current_probability = ee.Image.constant(0.9).clip(aoi)  # observed everywhere, coffee=True
    current_coffee = current_probability.gte(0.25).selfMask().rename("cur_coffee")

    result = pipeline.fdp_new_coffee_with_validity(
        previous_probability, previous_coffee, current_probability, current_coffee, "test_new_coffee"
    )
    result_area = pipeline.area_ha(result, aoi, 10).getInfo()
    western_area = pipeline.area_ha(
        ee.Image.constant(1).updateMask(western_half).clip(aoi), aoi, 10
    ).getInfo()
    full_area = pipeline.area_ha(ee.Image.constant(1).clip(aoi), aoi, 10).getInfo()

    # New-coffee signal restricted to the western half, where baseline was actually observed.
    assert result_area == pytest.approx(western_area, rel=0.1)
    # The eastern (masked-baseline) half must NOT be counted as a new-coffee transition, even
    # though naive unmask(0) logic would flag it (no observation != confirmed non-coffee).
    assert result_area < full_area * 0.6


def test_any_latest_coffee_union_not_summed(ee_ready):
    ee = ee_ready
    aoi = ee.Geometry.Rectangle([0.0, 0.0, 0.02, 0.01])
    lon = ee.Image.pixelLonLat().select("longitude")
    fdp_mask = lon.lt(0.015)  # covers the western 75% of the AOI
    mb_mask = lon.gt(0.005)  # covers the eastern 75% of the AOI; overlaps fdp_mask in the middle

    fdp_latest_coffee = ee.Image.constant(1).updateMask(fdp_mask).selfMask().clip(aoi)
    mb_latest_coffee = ee.Image.constant(1).updateMask(mb_mask).selfMask().clip(aoi)
    any_latest = fdp_latest_coffee.unmask(0).Or(mb_latest_coffee.unmask(0)).selfMask()

    union_area = pipeline.area_ha(any_latest, aoi, 10).getInfo()
    fdp_area = pipeline.area_ha(fdp_latest_coffee, aoi, 10).getInfo()
    mb_area = pipeline.area_ha(mb_latest_coffee, aoi, 10).getInfo()
    full_area = pipeline.area_ha(ee.Image.constant(1).clip(aoi), aoi, 10).getInfo()

    # The two masks together cover the whole AOI, so the union area should equal the full area...
    assert union_area == pytest.approx(full_area, rel=0.1)
    # ...which is strictly less than summing the two overlapping source-specific areas would give.
    assert union_area < (fdp_area + mb_area) * 0.9


def test_loss_after_attribution_end_year_excluded_from_attribution_mask(ee_ready):
    ee = ee_ready
    aoi = ee.Geometry.Rectangle([0.0, 0.0, 0.02, 0.01])
    lon = ee.Image.pixelLonLat().select("longitude")
    # Western half "lost" in 2023 (inside the attribution window); eastern half in 2025 (after
    # attribution_end_year=2024, since latest_coffee_year=2024 caps it below change_end_year=2025).
    lossyear = (
        ee.Image.constant(23)
        .updateMask(lon.lt(0.01))
        .blend(ee.Image.constant(25).updateMask(lon.gte(0.01)))
        .clip(aoi)
    )
    jrc_forest = ee.Image.constant(1).clip(aoi)

    run_config = _make_run_config(change_start_year=2021, change_end_year=2025, latest_coffee_year=2024)
    attribution_end = pipeline.attribution_end_year(run_config)
    assert attribution_end == 2024

    change_start, change_end = run_config.change_start_year, run_config.change_end_year
    full_mask = lossyear.gte(change_start - 2000).And(lossyear.lte(change_end - 2000)).selfMask()
    attribution_mask = (
        lossyear.gte(change_start - 2000).And(lossyear.lte(attribution_end - 2000)).selfMask()
    )

    full_area = pipeline.area_ha(jrc_forest.And(full_mask).selfMask(), aoi, 10).getInfo()
    attribution_area = pipeline.area_ha(jrc_forest.And(attribution_mask).selfMask(), aoi, 10).getInfo()
    whole_aoi_area = pipeline.area_ha(jrc_forest, aoi, 10).getInfo()

    # Full-period loss covers the whole AOI (both the 2023 and 2025 halves).
    assert full_area == pytest.approx(whole_aoi_area, rel=0.1)
    # Attribution-period loss excludes the 2025-only half.
    assert attribution_area < full_area * 0.6
