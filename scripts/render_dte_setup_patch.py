#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from detect_example_bundle_artifact_changes import _render_dte_patch, _write_json


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render DTE setup patch for example bundle.")
    parser.add_argument("--generated-utc", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Declared artifact relative path.")
    parser.add_argument("--out", default="out/dte_update/dte_setup_patch.md")
    args = parser.parse_args(argv)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_render_dte_patch(args.generated_utc, args.artifact), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
