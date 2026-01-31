#!/usr/bin/env python3
"""Report runner shim: generate EUDR report and upload to MinIO.

This module exists to preserve a stable public path referenced by the Digital
Twin Dependencies page (“Used by”).

Implementation is intentionally not included in this repository snapshot.
"""

from __future__ import annotations

import argparse


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compatibility shim. The full report-to-MinIO pipeline is not yet "
            "implemented in this repository snapshot."
        )
    )
    parser.add_argument("--run-id", help="Optional run identifier")
    parser.parse_args(argv)

    raise SystemExit(
        "src/task3_eudr_reports/run_eudr_report_to_minio.py is a compatibility shim. "
        "Add the report generation + MinIO upload implementation here when ready."
    )


if __name__ == "__main__":
    raise SystemExit(main())
