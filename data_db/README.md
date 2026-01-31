# data_db

This directory holds the DuckDB geodata catalogue consumed by MCP servers and AOI
report pipelines.

- Default catalogue path: `data_db/geodata_catalogue.duckdb`
- Bootstrap script: `python scripts/bootstrap_data_db.py`

Notes:
- The `.duckdb` files are ignored by git for now.
- Keep everything repo-relative (no hard-coded absolute server paths).
