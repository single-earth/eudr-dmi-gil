# AOI JSON examples

This folder contains **public example AOI geometries** used for deterministic test runs and documentation.

## Purpose

- Provide inspection-friendly example AOIs for report generation.
- Enable reproducible, deterministic smoke tests.
- Keep public example inputs separate from private client data.

## Licensing / attribution

The example AOIs are provided as public, non-sensitive demonstration inputs. If a specific dataset or source attribution is required for a future example, document it here.

## How to run the example test

From the repo root:

```sh
scripts/test_run_estonia_testland1.sh
```

This script generates a deterministic AOI bundle from the Estonia example GeoJSON and exports the DT-ready bundle for inspection.
