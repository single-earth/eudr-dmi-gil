# Dependency Register (DT contract)

This register exists to document the dependency contract referenced by the
Digital Twin (DT) Dependencies page, including “Used by” paths that must remain
stable even when underlying implementation moves.

## Public path preservation (“Used by”)

The DT Dependencies page references these paths as public stable entrypoints.
They must exist in this repository (as real implementations or compatibility
shims):

- `demo_mcp_servers.py`
- `src/mcp_servers/hansen_gfc_example.py`
- `src/eudr_dmi/methods/maa_amet_crosscheck.py`
- `src/task3_eudr_reports/run_eudr_report_to_minio.py`
- `tests/test_methods_maa_amet_crosscheck.py`

## Shim mapping

In this repository snapshot:

- `demo_mcp_servers.py` is a wrapper entrypoint placeholder.
- `src/mcp_servers/hansen_gfc_example.py` is a placeholder MCP server module.
- `src/eudr_dmi/methods/maa_amet_crosscheck.py` provides a deterministic scaffold
  function and the canonical dependency record for Maa-amet.
- `src/task3_eudr_reports/run_eudr_report_to_minio.py` is a placeholder script.

If/when implementation is adopted from a private repository with different
paths, keep these public paths as thin wrappers that import and call the new
implementation, and update this mapping section with the adopted module path.

## Source registry

Authoritative dependency identifiers and server audit paths are recorded in:

- [docs/dependencies/sources.md](../dependencies/sources.md)
- [docs/dependencies/sources.json](../dependencies/sources.json)
