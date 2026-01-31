#!/usr/bin/env python3
"""Demo entrypoint for MCP servers.

This file exists to preserve a stable public path referenced by the Digital Twin
Dependencies page (“Used by”).

It intentionally does not contain production logic.
"""

from __future__ import annotations


def main() -> int:
    raise SystemExit(
        "demo_mcp_servers.py is a compatibility shim. "
        "Run specific servers from src/mcp_servers/ as they are implemented."
    )


if __name__ == "__main__":
    raise SystemExit(main())
