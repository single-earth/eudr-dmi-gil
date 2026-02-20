# Dependency Sources Registry

_This file is generated from data_db/dependency_sources.csv; do not edit by hand._

## Role in the ecosystem

This repository is the authoritative implementation for dependency provenance. The Digital Twin repository is the public, non-authoritative portal for inspection and governance.

Purpose: record the upstream dependency identifiers, URLs, expected content types, and server audit paths referenced by the Digital Twin Dependencies page.

Implementation references (authoritative):
- [docs/dependencies/README.md](README.md)
- [docs/architecture/dependency_register.md](../architecture/dependency_register.md)

Server audit root (convention): `/Users/server/audit/eudr_dmi`

| id | url | expected content type | server audit path |
|---|---|---|---|
| `hansen_gfc_definitions` | https://storage.googleapis.com/earthenginepartners-hansen/GFC-2024-v1.12/download.html | `text/html` | `/Users/server/audit/eudr_dmi/dependencies/hansen_gfc_definitions` |
| `maa-amet/forest/v1` | https://gsavalik.envir.ee/geoserver/wfs | `application/xml` | `/Users/server/audit/eudr_dmi/dependencies/maa_amet_forest_v1` |

Notes:
- Do not paste upstream content into this repository.
- Store any mirrored/verified artifacts under the corresponding server audit path.

Changes to dependencies or evidence sources may originate from stakeholder DAO proposals reviewed via the DTE.

## See also

- [README.md](../../README.md)
- [docs/governance/roles_and_workflow.md](../governance/roles_and_workflow.md)
- Digital Twin DTE Instructions (Canonical): https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/dte_instructions.md
- Digital Twin Inspection Index: https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/INSPECTION_INDEX.md
- https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin
