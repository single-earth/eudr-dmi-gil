# Reports

This folder documents the report pipeline architecture for EUDR-DMI-GIL.

## Key conventions

- **Authoritative generation happens here** (this repo).
- **Publication/hosting happens elsewhere** in the Digital Twin portal repository:
  - https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin

## Evidence bundles

Reports are written into **evidence bundles** under the operator-configured audit root.

Grounding (per upstream `eudr_dmi` README conventions):

- Audit root is overrideable by the operator.
- Bundle layout is: `<AUDIT_ROOT>/<YYYY-MM-DD>/<bundle_id>/`
- Bundles may include a **portable site bundle zip** for portal publishing.

## Where to look

- ADR: see the decision record in `docs/architecture/decision_records/` about report pipeline architecture.
- Schemas: see `schemas/reports/` for JSON Schemas describing report outputs.
- Implementation scaffold: `src/eudr_dmi_gil/reports/`.
