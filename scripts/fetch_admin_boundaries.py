#!/usr/bin/env python3
"""Fetch real administrative (state/province) boundaries clipped to a padded AOI bbox.

Used to draw non-fabricated regional context - state/province boundaries and their real
place-name labels - on the Regional Overview evidence page, instead of only a bare satellite
image. Boundaries and names are read from a real, publicly published country-boundaries
dataset (see ``--source-url`` in the output metadata); nothing here is invented, and an AOI
whose country is not in ``COUNTRY_SOURCES`` produces no output rather than a guessed one.

The clip bbox uses the same AOI-bounds-padded-by-factor formula that
``eudr_dmi_gil.reports.report_model._write_regional_overview_png`` uses to crop the wide
regional satellite raster for display, so the fetched boundaries line up with what is actually
shown on that page.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import requests
from shapely.geometry import box, mapping, shape
from shapely.ops import unary_union

# Each entry is a real, publicly published boundaries dataset for the named country.
COUNTRY_SOURCES = {
    "brazil": {
        "url": (
            "https://raw.githubusercontent.com/codeforgermany/click_that_hood/main/"
            "public/data/brazil-states.geojson"
        ),
        "name_property": "name",
        "dataset_title": "Brazil state (UF) boundaries, published via click_that_hood",
    },
    "ghana": {
        "url": (
            "https://github.com/wmgeolab/geoBoundaries/raw/9469f09/releaseData/gbOpen/"
            "GHA/ADM1/geoBoundaries-GHA-ADM1_simplified.geojson"
        ),
        "name_property": "shapeName",
        "dataset_title": (
            "Ghana region (ADM1) boundaries, published via geoBoundaries "
            "(CC BY-SA, OpenStreetMap-derived, build gbOpen/GHA/ADM1 9469f09)"
        ),
    },
}


def _load_aoi_geometry(aoi_geojson_path: Path) -> Any:
    payload = json.loads(aoi_geojson_path.read_text(encoding="utf-8"))
    if payload.get("type") == "FeatureCollection":
        geoms = [shape(f["geometry"]) for f in payload["features"]]
        return unary_union(geoms)
    if payload.get("type") == "Feature":
        return shape(payload["geometry"])
    return shape(payload)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--aoi-geojson", required=True, type=Path)
    p.add_argument("--country", required=True, choices=sorted(COUNTRY_SOURCES))
    p.add_argument("--output-geojson", required=True, type=Path)
    p.add_argument(
        "--pad-factor",
        type=float,
        default=3.0,
        help=(
            "AOI-bounds padding multiple, matched to the Regional Overview page's own render "
            "pad factor (default: 3.0, its 'a regional_raster_ref is configured' pad factor)."
        ),
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    source = COUNTRY_SOURCES[args.country]

    aoi_geom = _load_aoi_geometry(args.aoi_geojson)
    minx, miny, maxx, maxy = aoi_geom.bounds
    pad_x = (maxx - minx) * args.pad_factor or 0.01
    pad_y = (maxy - miny) * args.pad_factor or 0.01
    clip_box = box(minx - pad_x, miny - pad_y, maxx + pad_x, maxy + pad_y)

    resp = requests.get(source["url"], timeout=60)
    resp.raise_for_status()
    payload = resp.json()

    kept: list[dict[str, Any]] = []
    for feature in payload.get("features", []):
        geom = shape(feature["geometry"])
        if not geom.is_valid:
            geom = geom.buffer(0)
        if not geom.intersects(clip_box):
            continue
        clipped = geom.intersection(clip_box)
        if clipped.is_empty:
            continue
        name = (feature.get("properties") or {}).get(source["name_property"])
        # Label at the *clipped* portion's representative point (not the whole state's, which
        # can sit far outside a tightly padded regional view), so the name lands inside the
        # visible render bbox instead of being silently dropped as out-of-frame.
        label_point = list(clipped.representative_point().coords[0])
        kept.append(
            {
                "type": "Feature",
                "properties": {"name": name},
                "geometry": mapping(clipped),
                "label_point": label_point,
            }
        )

    out = {
        "type": "FeatureCollection",
        "metadata": {
            "source_url": source["url"],
            "dataset_title": source["dataset_title"],
            "aoi_geojson": str(args.aoi_geojson),
            "country": args.country,
            "clip_bbox_wgs84": list(clip_box.bounds),
            "pad_factor": args.pad_factor,
        },
        "features": kept,
    }
    args.output_geojson.parent.mkdir(parents=True, exist_ok=True)
    args.output_geojson.write_text(
        json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({"output": str(args.output_geojson), "feature_count": len(kept)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
