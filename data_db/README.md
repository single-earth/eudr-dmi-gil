# data_db

This directory holds the DuckDB geodata catalogue consumed by MCP servers and AOI
report pipelines.

## Canonical seed inputs (tracked in git)

These two CSVs are the canonical public seed inputs for building the local
catalogue:

- [data_db/dataset_catalogue_auto.csv](dataset_catalogue_auto.csv)
- [data_db/dataset_families_summary.csv](dataset_families_summary.csv)

The geemap two-situation inspection workflow also keeps its public, reviewable
runtime configuration here:

- [data_db/two_situation_geemap_pipeline_config.json](two_situation_geemap_pipeline_config.json)
- [data_db/two_situation_geemap_pipeline_assets.csv](two_situation_geemap_pipeline_assets.csv)

Stakeholders may propose expansions/updates to these CSVs via the DAO feedback
process (treat them as the public, reviewable “source of truth”).

## Derived artifacts (generated locally, not committed)

These are generated locally and must not be committed:

- `data_db/dataset_catalogue_with_families.csv`
- `data_db/geodata_catalogue.duckdb`

- Default catalogue path: `data_db/geodata_catalogue.duckdb`
- Bootstrap script: `python scripts/bootstrap_data_db.py`

Notes:
- The `.duckdb` files and derived `*with_families*.csv` are ignored by git.
- Keep everything repo-relative (no hard-coded absolute server paths).
