from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .draft import write_dds_draft


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m eudr_dmi_gil.dds.cli",
        description=(
            "Generate local DDS draft evidence artifacts from an AOI evidence bundle "
            "and normalized portal intake JSON. This command does not submit to TRACES/EUDR."
        ),
    )
    parser.add_argument(
        "--bundle-dir",
        required=True,
        help="Path to an AOI evidence bundle directory containing manifest.json.",
    )
    parser.add_argument(
        "--portal-intake",
        required=True,
        help="Path to normalized portal intake JSON.",
    )
    parser.add_argument(
        "--aoi-report",
        help=(
            "Optional AOI report JSON path. If omitted, the first report under "
            "reports/aoi_report_* in the bundle is used."
        ),
    )
    parser.add_argument(
        "--out-dir",
        help="Output directory for DDS artifacts (defaults to <bundle-dir>/dds).",
    )
    parser.add_argument(
        "--created-at-utc",
        help="Optional ISO-8601 timestamp. Defaults to EUDR_DMI_GENERATED_AT_UTC or current UTC.",
    )
    parser.add_argument(
        "--schema",
        default="schemas/dds/dds_draft_v1.schema.json",
        help="DDS draft JSON Schema path used to validate dds_draft.json.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    draft_path, validation_path, draft = write_dds_draft(
        bundle_dir=Path(args.bundle_dir),
        portal_intake_path=Path(args.portal_intake),
        aoi_report_path=Path(args.aoi_report) if args.aoi_report else None,
        out_dir=Path(args.out_dir) if args.out_dir else None,
        created_at_utc=args.created_at_utc,
        schema_path=Path(args.schema),
    )
    print(f"wrote {draft_path}")
    print(f"wrote {validation_path}")
    risk_dir = Path(args.bundle_dir) / "risk"
    print(f"wrote {risk_dir / 'risk_register.json'}")
    print(f"wrote {risk_dir / 'evidence_gaps.json'}")
    print(f"wrote {risk_dir / 'mitigation_actions.json'}")
    print(f"validation_status={draft['validation_result']['status']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
