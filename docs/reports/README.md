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

## Policy-to-evidence spine references (no interpretation)

AOI reports can include `policy_mapping_refs`: a list of **reference strings** pointing into a separate
"policy-to-evidence spine" (IDs, URIs, or other stable keys).

These refs are intended for **DAO review and traceability** only:

- They enable stakeholders to discuss which policy clauses/controls are relevant to which evidence artifacts.
- They may be placeholders.
- They are **not compliance claims** and must not be interpreted as an automated EUDR determination.

CLI usage:

- Provide refs directly (repeatable): `--policy-mapping-ref "policy-spine:eudr/article-3"`
- Or load newline-separated refs from files (repeatable): `--policy-mapping-ref-file policy_refs.txt`
