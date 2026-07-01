# Dev notes

## BBox → Hansen tiles
- Tile resolution remains bbox-driven via `load_aoi_bbox()` + `hansen_tile_ids_for_bbox()` in src/eudr_dmi_gil/deps/hansen_tiles.py.
- AOI coordinate iteration now also supports GeometryCollection geometries so all polygon rings contribute to min/max bounds.
- Tests: tests/test_hansen_tiles.py covers the Estonia fixture plus a MultiPolygon fixture at tests/fixtures/aoi_multipolygon.geojson.

## Most recent Hansen year
- New helper: `infer_hansen_latest_year()` in src/eudr_dmi_gil/deps/hansen_acquire.py.
- It prefers the explicit dataset version (e.g., 2025-v1.13) or year embedded in the tile_dir path, and only scans the external Hansen root if neither is available.
- Unit tests in tests/test_hansen_tiles.py cover dataset version priority and external-root fallback.

## Map integration (HTML bundle)
- Map config generation and HTML wiring live in src/eudr_dmi_gil/reports/cli.py.
- A Leaflet-based map block is injected into the report HTML when Hansen + GeoJSON inputs are present.
- The map consumes a bundle-local map_config.json (reports/aoi_report_v2/<aoi_id>/map/map_config.json) that points to:
  - forest_2000_tree_cover_mask.geojson
  - forest_end_year_tree_cover_mask.geojson
  - forest_loss_post_2020_mask.geojson
  - AOI boundary GeoJSON
  - Maa-amet top-10 parcel GeoJSON

## Schema updates
- schemas/reports/aoi_report_v2.schema.json now allows an optional top-level `map_assets` block.
- The report JSON is populated with map asset pointers whenever map config is generated.

## Parcel table enrichment
- Maa-amet top-10 output now includes Hansen/Maa-amet land & forest area columns.
- Top-10 selection enforces forest >= 3 ha and prefers Hansen-derived forest area when available.
- CSV/GeoJSON parcel outputs include `hansen_*` and `maaamet_*` fields.
