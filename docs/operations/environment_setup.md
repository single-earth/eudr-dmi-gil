# Environment setup (Python)

## Role in the ecosystem

This repository is the authoritative implementation for environment and runtime setup. The Digital Twin repository is the public, non-authoritative portal for inspection and governance.

## Python version

- Required for the current packaging contract: Python 3.11

## Create the virtual environment

From repo root:

```sh
python3 -m venv .venv
```

Activate:

```sh
source .venv/bin/activate
```

Confirm:

```sh
python --version
which python
```

## Install dependencies

Upgrade packaging tools:

```sh
python -m pip install --upgrade pip
```

Install the repo as an editable package + dev tooling (CI uses this):

```sh
pip install -e ".[dev]"
```

Install geospatial method dependencies (optional; may require system libraries):

```sh
pip install -r requirements-methods.txt
```

This includes the Python MinIO client (`minio`) used by AOI/report upload pipelines.

## Repo-local data plane

This repo stores externally downloaded inputs and intermediate files under a **repo-local data
plane** that must never be committed to Git or published to the Digital Twin.

Environment variable:
- `EUDR_DMI_DATA_ROOT` (optional): overrides the default data root.

Default layout under `./data/`:
- `data/external/` — downloaded upstream datasets (Hansen, Maa-amet, etc).
- `data/cache/` — HTTP caches / temporary downloads.
- `data/derived/` — intermediate products not meant for evidence bundles.

Rules:
- Do not commit anything under `data/`.
- Do not publish `data/` to the Digital Twin.
- Only publish the site bundle outputs (e.g. `out/site_bundle/...`).

MinIO operator setup (local server + required env vars):
- `docs/operations/minio_setup.md`

## Quick sanity checks

Run tests (no external services):

```sh
pytest -q
```

## Common failure modes

- `python3: command not found`: install Python 3.11 and ensure `python3` is on `PATH`.
- `pip` installs to the wrong interpreter: ensure you activated `.venv` and use `python -m pip ...`.
- SSL / certificate errors on macOS: upgrade pip (`python -m pip install -U pip`) and ensure system certificates are present.
- Build failures for wheels (C extensions): install Xcode Command Line Tools (`xcode-select --install`).
- Linux CI note (GDAL/rasterio): `rasterio` commonly requires GDAL system packages. On Ubuntu runners, you may need:
	- `sudo apt-get update && sudo apt-get install -y gdal-bin libgdal-dev`

Notes:
- This repo can be used without installing `requirements-methods.txt` unless you work on geospatial methods.

Operational changes that affect reproducibility or determinism should reference DAO proposals produced via the DTE.

## See also

- [README.md](../../README.md)
- [docs/governance/roles_and_workflow.md](../governance/roles_and_workflow.md)
- Digital Twin DTE Instructions (Canonical): https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/dte_instructions.md
- Digital Twin Inspection Index: https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/INSPECTION_INDEX.md
- https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin
