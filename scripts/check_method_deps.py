#!/usr/bin/env python3
"""Check method dependency contract.

This is a lightweight guardrail aligned with the private `eudr_dmi` digest.

It validates that `requirements-methods.txt` exists and contains a minimum set
of geospatial dependencies expected by method primitives:
- rasterio
- shapely
- pyproj
- numpy

This script intentionally does not import these packages (they may require
system libraries like GDAL on Linux).
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


REQUIRED = {"rasterio", "shapely", "pyproj", "numpy"}


_REQ_NAME_RE = re.compile(r"^([A-Za-z0-9_.-]+)")


def parse_requirement_names(text: str) -> set[str]:
    names: set[str] = set()
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        # Strip inline comments.
        if " #" in line:
            line = line.split(" #", 1)[0].strip()
        m = _REQ_NAME_RE.match(line)
        if not m:
            continue
        names.add(m.group(1).lower())
    return names


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate requirements-methods.txt content")
    parser.add_argument(
        "--requirements",
        default="requirements-methods.txt",
        help="Path to requirements-methods.txt (default: requirements-methods.txt)",
    )
    args = parser.parse_args(argv)

    req_path = Path(args.requirements)
    if not req_path.exists():
        print(f"ERROR: missing requirements file: {req_path}", file=sys.stderr)
        return 2

    names = parse_requirement_names(req_path.read_text(encoding="utf-8"))
    missing = sorted(REQUIRED - names)
    if missing:
        print(
            "ERROR: requirements-methods.txt missing required packages: " + ", ".join(missing),
            file=sys.stderr,
        )
        return 3

    print("OK: requirements-methods.txt includes required method deps")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
