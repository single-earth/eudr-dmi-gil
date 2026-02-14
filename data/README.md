# Repo-local data plane

This folder is the repo-local **data plane**.

It is intentionally **not committed to Git** and must not be published into the Digital Twin.
Only derived report bundles (under `EUDR_DMI_EVIDENCE_ROOT`) and the exported site bundle outputs
(e.g. `out/site_bundle/...`) are publishable.

## Layout

- `external/` — externally downloaded upstream datasets (e.g. Hansen tiles).
- `cache/` — HTTP caches / temporary downloads.
- `derived/` — intermediate products that are not meant for the evidence bundle.

## Actual location (this workspace)

Externally downloaded datasets are stored outside the repo in (default):

- `/Users/server/data/eudr-dmi/` (contains `hansen/`)

You can override this path via:

- `EUDR_DMI_DATA_ROOT=/path/to/external/data/root`

Authoritative resolution logic:

- `src/eudr_dmi_gil/io/data_plane.py` (`external_root()`)

## MINIO cache

MINIO is used as an optional object store cache for externally downloaded datasets.
When enabled (via `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`),
the pipeline stores and retrieves tiles and manifests under keys such as:

- `s3://<MINIO_BUCKET>/tiles/<tile_id>/<layer>.tif`
- `s3://<MINIO_BUCKET>/manifests/<aoi_id>/tiles_manifest.json`

This cache is read-through/write-through: missing objects are downloaded from the
upstream source, written to MINIO, and then used locally.

Local MinIO data root (Docker volume mapping in [infra/minio/docker-compose.yml](infra/minio/docker-compose.yml)):

- `/Users/server/projects/eudr-dmi-gil/infra/minio/minio_data/`
- Buckets are stored under: `/Users/server/projects/eudr-dmi-gil/infra/minio/minio_data/<MINIO_BUCKET>/`

## Notes

- Evidence bundles remain under `EUDR_DMI_EVIDENCE_ROOT` only.
- Do not commit anything under `data/`.
