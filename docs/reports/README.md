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
- End-to-end runbook: `docs/reports/runbook_generate_aoi_report.md`

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

## Publishing to the Digital Twin repo (human-in-the-loop)

This repository (`eudr-dmi-gil`) is responsible for **authoritative generation** of deterministic evidence bundles
and portable site bundles.

The Digital Twin portal repository is responsible for **publishing/hosting** those artifacts.

Recommended workflow:

1) Generate AOI evidence bundles (JSON/HTML/metrics.csv/manifest.json) under the evidence root.
2) Export a portable site bundle folder + deterministic zip (see `scripts/export_reports_site_bundle.py`).
3) Copy the portable folder into a sibling checkout of the portal repo and review the diff.

Helper script (no auto-push, no credentials assumed):

- `scripts/publish_reports_to_digital_twin.sh`

By default it copies:

- from: `docs/site_bundle_reports/`
- to: `../eudr-dmi-gil-digital-twin/site/aoi_reports/`

The final commit/push happens from the portal repository after human review.
