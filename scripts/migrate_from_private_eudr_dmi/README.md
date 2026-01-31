# Migrate From private `eudr_dmi`

This directory contains an idempotent scaffold for adopting a snapshot from the private `eudr_dmi` repository into this public repository.

Constraints enforced by this tool:

- Do not copy secrets or env files (e.g., `.env*`).
- Do not copy key material (e.g., `keys.yml`).
- Do not copy runtime data plane content (e.g., `/Users/server/data/dmi`).
- Do not copy generated artefacts (`audit/`, `outputs/`).

## Usage

From the repository root:

1) Configure the source location:

- `export PRIVATE_EUDR_DMI_SRC=/absolute/path/to/private/eudr_dmi`

2) Copy a selected, sanitized snapshot (idempotent):

- `bash scripts/migrate_from_private_eudr_dmi/01_copy_selected.sh`

3) Write a deterministic SHA-256 manifest:

- `python3 scripts/migrate_from_private_eudr_dmi/02_write_manifest.py`

Outputs:

- Snapshot directory: `adopted/private_eudr_dmi_snapshot/`
- SHA-256 manifest: `adopted/private_eudr_dmi_snapshot/latest_manifest.sha256`

## Notes

- The migration is designed to be idempotent: re-running it with the same source produces the same destination layout and a stable manifest.
- The tool copies only files required for code and documentation adoption and intentionally excludes runtime data and generated outputs.
