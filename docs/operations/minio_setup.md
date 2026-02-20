# MinIO (Local) Setup

## Role in the ecosystem

This repository is the authoritative implementation for runtime storage and pipelines. The Digital Twin repository is the public, non-authoritative portal for inspection and governance.

This repository uses MinIO as an S3-compatible object store for **runtime artifacts** (e.g., generated reports).

## Local-only data rule

- MinIO bucket contents are **runtime artifacts** and **must not be committed to git**.
- This repo enforces this by ignoring:
  - `infra/minio/minio_data/` (MinIO server data directory)
  - `outputs/` (pipeline outputs)
  - `audit/` (operator/audit outputs)

## Required environment variables (pipelines)

Pipelines that upload to MinIO require these environment variables:

- `MINIO_ENDPOINT` (e.g. `localhost:9000`)
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET` (the bucket to write into)

Example (local dev with default compose credentials):

```sh
export MINIO_ENDPOINT="localhost:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"
export MINIO_BUCKET="eudr-reports"
```

## Option A — Docker Compose (recommended)

This repo ships a local MinIO compose file:

```sh
cd infra/minio
docker compose up -d
```

- MinIO API: `http://localhost:9000`
- MinIO console: `http://localhost:9001`
- Persistent data directory (gitignored): `infra/minio/minio_data/`

To stop:

```sh
docker compose down
```

### Create the bucket

Create the bucket in the web console (`http://localhost:9001`) or using the `mc` client.

If you have `mc` installed:

```sh
mc alias set local http://localhost:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
mc mb --ignore-existing "local/$MINIO_BUCKET"
```

## Option B — Homebrew (standalone MinIO)

Install MinIO:

```sh
brew install minio/stable/minio
```

Run a local server (data stored outside the repo):

```sh
mkdir -p ~/minio_data
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  minio server ~/minio_data --console-address ":9001"
```

Then export the pipeline env vars:

```sh
export MINIO_ENDPOINT="localhost:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"
export MINIO_BUCKET="eudr-reports"
```

## Quick verification

Health endpoint:

```sh
curl -f http://localhost:9000/minio/health/ready
```

Sanity-check that the Task 3 runner can see required env vars:

```sh
python -m task3_eudr_reports.run_eudr_report_to_minio --check-minio-env
```

Note: the full report generator/upload pipeline is intentionally not included in this snapshot; the module currently provides a stable DT path plus a MinIO env-var preflight.

Operational changes that affect reproducibility or determinism should reference DAO proposals produced via the DTE.

## See also

- [README.md](../../README.md)
- [docs/governance/roles_and_workflow.md](../governance/roles_and_workflow.md)
- Digital Twin DTE Instructions (Canonical): https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/dte_instructions.md
- Digital Twin Inspection Index: https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/INSPECTION_INDEX.md
- https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin
