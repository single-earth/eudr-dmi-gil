#!/usr/bin/env python3
"""Parameterized deterministic acquisition of an FDP coffee-probability raster.

Pulls a single AOI-year probability GeoTIFF from a Forest Data Partnership
`ee.ImageCollection` (e.g. `projects/forestdatapartnership/assets/coffee/model_2025b`) and
pins it locally, plus writes acquisition metadata (asset id, exact source image ids, date
window, band, reduction, projection, scale, access timestamp, hashes). No AOI/asset/year/
threshold value is hardcoded: everything comes from CLI args.

Example:

    python scripts/acquire_fdp_coffee_probability.py \\
      --aoi-geojson aoi_json_examples/ghana_west_africa_shared_aoi.geojson \\
      --asset-id projects/forestdatapartnership/assets/coffee/model_2025b \\
      --observation-year 2024 \\
      --band probability \\
      --scale-m 10 \\
      --project myproject-gq-74696 \\
      --out-raster out/fdp_coffee_ghana/probability.tif \\
      --out-metadata out/fdp_coffee_ghana/acquisition_metadata.json
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from eudr_dmi_gil.deps.fdp_coffee_acquire import (  # noqa: E402
    DEFAULT_SCALE_M,
    EarthEngineFdpAdapter,
    acquire_fdp_coffee_probability,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--aoi-geojson", required=True, type=Path, help="Path to AOI GeoJSON")
    p.add_argument(
        "--asset-id",
        required=True,
        help="Earth Engine ImageCollection asset id (e.g. projects/forestdatapartnership/assets/coffee/model_2025b)",
    )
    p.add_argument("--observation-year", required=True, type=int, help="Year filter (half-open [year, year+1))")
    p.add_argument("--band", required=True, help="Probability band name to select")
    p.add_argument("--scale-m", type=float, default=DEFAULT_SCALE_M, help=f"Export scale in meters (default: {DEFAULT_SCALE_M})")
    p.add_argument("--project", required=True, help="Google Cloud project id registered for Earth Engine")
    p.add_argument("--service-account", help="Optional service-account email for CI/deterministic runs")
    p.add_argument("--key-file", help="Service-account key file (required if --service-account is set)")
    p.add_argument("--out-raster", required=True, type=Path, help="Output GeoTIFF path")
    p.add_argument("--out-metadata", required=True, type=Path, help="Output acquisition metadata JSON path")
    p.add_argument(
        "--access-timestamp-utc",
        help="Override the recorded access timestamp (ISO-8601 UTC); defaults to now",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.aoi_geojson.is_file():
        raise FileNotFoundError(f"AOI GeoJSON not found: {args.aoi_geojson}")

    adapter = EarthEngineFdpAdapter(
        project=args.project,
        service_account=args.service_account,
        key_file=args.key_file,
    )
    metadata = acquire_fdp_coffee_probability(
        adapter=adapter,
        asset_id=args.asset_id,
        observation_year=args.observation_year,
        band=args.band,
        aoi_geojson_path=args.aoi_geojson,
        out_raster_path=args.out_raster,
        out_metadata_path=args.out_metadata,
        scale_m=args.scale_m,
        access_timestamp_utc=args.access_timestamp_utc,
    )
    print(f"Wrote {args.out_raster} ({metadata['output_size_bytes']} bytes, sha256={metadata['output_sha256']})")
    print(f"Wrote {args.out_metadata}")
    print(f"source_image_ids: {metadata['source_image_ids']}")
    print(f"selection_order: {metadata['selection_order']}")
    print(f"reduction: {metadata['reduction']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
