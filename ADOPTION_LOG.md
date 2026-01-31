# Adoption Log

This log records provenance for any material adopted into this repository from external sources (including the private `eudr_dmi` repository).

Principles:

- **Copy + own**: adopted material becomes maintained in this repository under its public governance and review process.
- **Provenance first**: every adoption must be recorded with source reference, time, scope, and any transformations.
- **No secrets / no runtime data**: adoption must not include credentials, keys, `.env*`, runtime data planes, or generated outputs.

## Entries

Use one entry per adoption pull request.

### Template

- Date (UTC):
- PR:
- Source system:
- Source reference (commit/tag/path):
- Scope adopted (high level):
- Exclusions applied (required):
  - `.env*`
  - `keys.yml` (and similar key material)
  - runtime data plane (e.g., `/Users/server/data/dmi`)
  - `audit/`
  - `outputs/`
- Transformations performed (normalization, renames, deletions):
- Review notes (what was checked, by whom):
- Reproducibility notes (how to re-run migration and verify manifest):
