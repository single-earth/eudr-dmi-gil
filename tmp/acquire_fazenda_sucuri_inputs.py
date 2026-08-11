from __future__ import annotations

import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))

import ee  # noqa: E402

from eudr_dmi_gil.reports.determinism import sha256_file, write_json  # noqa: E402

PROJECT = "myproject-gq-74696"
AOI_ID = "fazenda_sucuri_screening_aoi"
AOI_PATH = REPO_ROOT / "aoi_json_examples" / f"{AOI_ID}.geojson"
OUT_DIR = REPO_ROOT / "out" / f"{AOI_ID}_inputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ASSETS = {
    "jrc_gfc2020": "JRC/GFC2020/V3",
    "hansen_gfc": "UMD/hansen/global_forest_change_2025_v1_13",
    "fdp_coffee": "projects/forestdatapartnership/assets/coffee/model_2025b",
    "mapbiomas_lulc": "projects/mapbiomas-public/assets/brazil/lulc/v1",
    "sentinel2": "COPERNICUS/S2_SR_HARMONIZED",
}

ee.Initialize(project=PROJECT)

aoi_geojson = json.loads(AOI_PATH.read_text(encoding="utf-8"))
geometry = ee.Geometry(aoi_geojson["features"][0]["geometry"])
export_region = geometry.buffer(1500).bounds()
access_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def download(url: str, out_path: Path) -> None:
    with urllib.request.urlopen(url) as response:  # noqa: S310
        out_path.write_bytes(response.read())


def acquire_image(
    *,
    image: ee.Image,
    band_names: list[str],
    scale_m: float,
    out_name: str,
    dataset_id: str,
    dataset_version: str,
    role: str,
) -> dict[str, Any]:
    selected = image.select(band_names).clip(export_region)
    url = selected.getDownloadURL(
        {"region": export_region, "scale": scale_m, "crs": "EPSG:4326", "format": "GEO_TIFF"}
    )
    out_path = OUT_DIR / out_name
    download(url, out_path)
    metadata = {
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "role": role,
        "band_names": band_names,
        "scale_m": scale_m,
        "source_acquisition_engine": "gee",
        "execution_engine": "local-pinned-raster",
        "access_timestamp_utc": access_ts,
        "aoi_geojson_path": str(AOI_PATH.relative_to(REPO_ROOT)),
        "aoi_geojson_sha256": sha256_file(AOI_PATH),
        "output_path": str(out_path.relative_to(REPO_ROOT)),
        "output_size_bytes": out_path.stat().st_size,
        "output_sha256": sha256_file(out_path),
    }
    write_json(OUT_DIR / f"{out_name}.acquisition_metadata.json", metadata)
    print(f"Wrote {out_path} ({metadata['output_size_bytes']} bytes)")
    return metadata


def mask_s2_sr(image: ee.Image) -> ee.Image:
    scl = image.select("SCL")
    keep = (
        scl.neq(0)
        .And(scl.neq(1))
        .And(scl.neq(3))
        .And(scl.neq(8))
        .And(scl.neq(9))
        .And(scl.neq(10))
        .And(scl.neq(11))
    )
    return image.updateMask(keep).divide(10000).copyProperties(
        image, ["system:time_start", "CLOUDY_PIXEL_PERCENTAGE"]
    )


def s2_season(year: int) -> ee.ImageCollection:
    return (
        ee.ImageCollection(ASSETS["sentinel2"])
        .filterBounds(geometry)
        .filterDate(f"{year}-07-01", f"{year}-10-01")
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
    )


def s2_best_and_diagnostics(year: int) -> tuple[ee.Image, dict[str, Any]]:
    col = s2_season(year)
    masked = col.map(mask_s2_sr)
    sorted_first = col.sort("CLOUDY_PIXEL_PERCENTAGE").first()
    best = ee.Image(mask_s2_sr(ee.Image(sorted_first))).clip(export_region)
    valid_obs = masked.select("B4").count().rename("valid_obs").clip(geometry)
    diagnostics = ee.Dictionary(
        {
            "scene_count": col.size(),
            "least_cloudy_scene_date": ee.Image(sorted_first).date().format("YYYY-MM-dd"),
            "least_cloudy_scene_cloud_pct": ee.Image(sorted_first).get("CLOUDY_PIXEL_PERCENTAGE"),
            "mean_valid_obs_per_pixel": valid_obs.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=geometry,
                scale=20,
                maxPixels=1e13,
                tileScale=4,
            ).get("valid_obs"),
            "min_valid_obs_per_pixel": valid_obs.reduceRegion(
                reducer=ee.Reducer.min(),
                geometry=geometry,
                scale=20,
                maxPixels=1e13,
                tileScale=4,
            ).get("valid_obs"),
        }
    ).getInfo()
    visual = best.visualize(bands=["B4", "B3", "B2"], min=0.0, max=0.3)
    return visual, diagnostics


def fdp_probability(year: int) -> ee.Image:
    subset = (
        ee.ImageCollection(ASSETS["fdp_coffee"])
        .filterBounds(geometry)
        .filterDate(ee.Date.fromYMD(year, 1, 1), ee.Date.fromYMD(year + 1, 1, 1))
    )
    return ee.Image(subset.mosaic()).select("probability")


def mapbiomas_classification(year: int) -> ee.Image:
    subset = (
        ee.ImageCollection(ASSETS["mapbiomas_lulc"])
        .filterBounds(geometry)
        .filter(ee.Filter.eq("collection_id", 10.0))
        .filter(ee.Filter.eq("version", "v1"))
        .filter(ee.Filter.eq("year", year))
    )
    return ee.Image(subset.mosaic()).select("classification")


metadata = {
    "jrc": acquire_image(
        image=ee.Image(ASSETS["jrc_gfc2020"]),
        band_names=["Map"],
        scale_m=10,
        out_name="jrc_gfc2020_v3.tif",
        dataset_id=ASSETS["jrc_gfc2020"],
        dataset_version="V3",
        role="forest_baseline",
    ),
    "hansen_lossyear": acquire_image(
        image=ee.Image(ASSETS["hansen_gfc"]),
        band_names=["lossyear"],
        scale_m=30,
        out_name="hansen_lossyear_2025_v1_13.tif",
        dataset_id=ASSETS["hansen_gfc"],
        dataset_version="2025-v1.13",
        role="forest_loss",
    ),
    "hansen_treecover2000": acquire_image(
        image=ee.Image(ASSETS["hansen_gfc"]),
        band_names=["treecover2000"],
        scale_m=30,
        out_name="hansen_treecover2000_2025_v1_13.tif",
        dataset_id=ASSETS["hansen_gfc"],
        dataset_version="2025-v1.13",
        role="hansen_canopy_baseline",
    ),
    "fdp_2020": acquire_image(
        image=fdp_probability(2020),
        band_names=["probability"],
        scale_m=10,
        out_name="fdp_coffee_probability_2020.tif",
        dataset_id=ASSETS["fdp_coffee"],
        dataset_version="2025b",
        role="commodity_probability_baseline",
    ),
    "fdp_2024": acquire_image(
        image=fdp_probability(2024),
        band_names=["probability"],
        scale_m=10,
        out_name="fdp_coffee_probability_2024.tif",
        dataset_id=ASSETS["fdp_coffee"],
        dataset_version="2025b",
        role="commodity_probability_latest",
    ),
    "mapbiomas_2020": acquire_image(
        image=mapbiomas_classification(2020),
        band_names=["classification"],
        scale_m=30,
        out_name="mapbiomas_lulc_2020.tif",
        dataset_id=ASSETS["mapbiomas_lulc"],
        dataset_version="collection10-v1",
        role="commodity_classification_baseline",
    ),
    "mapbiomas_2024": acquire_image(
        image=mapbiomas_classification(2024),
        band_names=["classification"],
        scale_m=30,
        out_name="mapbiomas_lulc_2024.tif",
        dataset_id=ASSETS["mapbiomas_lulc"],
        dataset_version="collection10-v1",
        role="commodity_classification_latest",
    ),
}

s2_years: dict[str, Any] = {}
for year, out_name in [
    (2020, "sentinel2_baseline_2020.tif"),
    (2025, "sentinel2_recent_2025.tif"),
]:
    image, diagnostics = s2_best_and_diagnostics(year)
    s2_years[str(year)] = diagnostics
    metadata[f"sentinel2_{year}"] = acquire_image(
        image=image,
        band_names=["vis-red", "vis-green", "vis-blue"],
        scale_m=10,
        out_name=out_name,
        dataset_id=ASSETS["sentinel2"],
        dataset_version="sentinel-2-l2a",
        role=f"sentinel2_visual_context_{year}",
    )

write_json(OUT_DIR / "sentinel2_scene_diagnostics.json", {"years": s2_years})

commodity_config = {
    "commodities": [
        {
            "id": "coffee",
            "display_name": "Coffee",
            "provider": "forestdatapartnership",
            "dataset_title": "FDP Coffee Probability model 2025b",
            "dataset_version": "2025b",
            "asset_id": ASSETS["fdp_coffee"],
            "local_path": str(OUT_DIR / "fdp_coffee_probability_2024.tif"),
            "baseline_asset_id": ASSETS["fdp_coffee"],
            "baseline_local_path": str(OUT_DIR / "fdp_coffee_probability_2020.tif"),
            "baseline_observation_year": 2020,
            "observation_year": 2024,
            "country_scope": ["Brazil"],
            "mode": "probability_threshold",
            "probability_band": "probability",
            "threshold": 0.25,
            "sensitivity_thresholds": [0.1, 0.25, 0.5],
            "source_url": "https://dataforgood.facebook.com/dfg/tools/forest-data-partnership",
        },
        {
            "id": "coffee",
            "display_name": "Coffee",
            "provider": "mapbiomas_brazil",
            "dataset_title": "MapBiomas Brazil Land Cover",
            "dataset_version": "collection10-v1",
            "asset_id": ASSETS["mapbiomas_lulc"],
            "local_path": str(OUT_DIR / "mapbiomas_lulc_2024.tif"),
            "baseline_asset_id": ASSETS["mapbiomas_lulc"],
            "baseline_local_path": str(OUT_DIR / "mapbiomas_lulc_2020.tif"),
            "baseline_observation_year": 2020,
            "observation_year": 2024,
            "country_scope": ["Brazil"],
            "mode": "discrete_classes",
            "class_values": [46],
            "class_labels": ["Coffee plantations"],
            "source_url": "https://brasil.mapbiomas.org/",
        },
    ]
}
write_json(OUT_DIR / "coffee_config_fazenda_sucuri_two_source.json", commodity_config)
write_json(OUT_DIR / "acquisition_manifest.json", metadata)
print(json.dumps({"out_dir": str(OUT_DIR), "metadata": metadata, "s2": s2_years}, indent=2))
