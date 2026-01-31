# Environment setup (Python)

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

MinIO operator setup (local server + required env vars):
- `docs/operations/minio_setup.md`

## Quick sanity checks

Run the shim unit test (no external services):

```sh
python -m unittest -q tests.test_methods_maa_amet_crosscheck
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
