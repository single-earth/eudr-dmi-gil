#!/usr/bin/env python3
"""Report runner shim: generate EUDR report and upload to MinIO.

This module exists to preserve a stable public path referenced by the Digital
Twin Dependencies page (“Used by”).

Implementation is intentionally not included in this repository snapshot.
"""

from __future__ import annotations

import argparse
import os
import textwrap


_REQUIRED_MINIO_ENV = (
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "MINIO_BUCKET",
)


def _env_optional(name: str) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    return value


def _require_minio_env() -> dict[str, str]:
    missing = [name for name in _REQUIRED_MINIO_ENV if not _env_optional(name)]
    if missing:
        raise SystemExit(
            textwrap.dedent(
                """\
                Missing required MinIO environment variables: {missing}

                Required:
                  - MINIO_ENDPOINT (e.g. localhost:9000)
                  - MINIO_ACCESS_KEY
                  - MINIO_SECRET_KEY
                  - MINIO_BUCKET

                Tip (local docker compose default credentials):
                  export MINIO_ENDPOINT=localhost:9000
                  export MINIO_ACCESS_KEY=minioadmin
                  export MINIO_SECRET_KEY=minioadmin
                  export MINIO_BUCKET=eudr-reports
                """
            ).format(missing=", ".join(missing))
        )

    return {name: str(os.environ[name]) for name in _REQUIRED_MINIO_ENV}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compatibility shim. The full report-to-MinIO pipeline is not yet "
            "implemented in this repository snapshot."
        )
    )
    parser.add_argument(
        "--skip-minio",
        action="store_true",
        help="Do not require or use MinIO env vars (offline/local-only run).",
    )
    parser.add_argument(
        "--check-minio-env",
        action="store_true",
        help="Validate required MINIO_* env vars and exit 0 (preflight).",
    )
    parser.add_argument("--run-id", help="Optional run identifier")
    args = parser.parse_args(argv)

    # Operator-facing check: enforce MinIO env var contract unless explicitly skipped.
    # This confirms the runner can read MINIO_* and fails loudly if misconfigured.
    if args.check_minio_env and args.skip_minio:
        raise SystemExit("--check-minio-env and --skip-minio are mutually exclusive")

    if args.check_minio_env:
        _require_minio_env()
        print("OK: MinIO environment variables present")
        return 0

    if not args.skip_minio:
        _require_minio_env()

    raise SystemExit(
        "src/task3_eudr_reports/run_eudr_report_to_minio.py is a compatibility shim. "
        "Add the report generation + MinIO upload implementation here when ready."
    )


if __name__ == "__main__":
    raise SystemExit(main())
