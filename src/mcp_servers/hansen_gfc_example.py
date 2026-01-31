"""Hansen GFC example MCP server shim.

This module exists to preserve a stable import/path referenced by the Digital
Twin Dependencies page (“Used by”).

No compliance claims are made here, and no evidence bundles are produced.
"""

from __future__ import annotations

from eudr_dmi.data_db import ENV_GEODATA_CATALOGUE_PATH, get_catalogue_path


def run() -> None:
    catalogue = get_catalogue_path()
    raise RuntimeError(
        "src/mcp_servers/hansen_gfc_example.py is a compatibility shim. "
        "Implement the MCP server here when ready. "
        f"Default catalogue: {catalogue.as_posix()} (override with {ENV_GEODATA_CATALOGUE_PATH})."
    )
