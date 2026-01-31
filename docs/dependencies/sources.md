# Dependency Sources Registry

Purpose: record the upstream dependency identifiers, URLs, expected content
types, and server audit paths referenced by the Digital Twin Dependencies page.

Server audit root (convention): `/Users/server/audit/eudr_dmi`

| id | url | expected content type | server audit path |
|---|---|---|---|
| `hansen_gfc_definitions` | https://storage.googleapis.com/earthenginepartners-hansen/GFC-2024-v1.12/download.html | `text/html` | `/Users/server/audit/eudr_dmi/dependencies/hansen_gfc_definitions` |
| `maa-amet/forest/v1` | https://gsavalik.envir.ee/geoserver/wfs | `application/xml` | `/Users/server/audit/eudr_dmi/dependencies/maa_amet_forest_v1` |

Notes:
- Do not paste upstream content into this repository.
- Store any mirrored/verified artifacts under the corresponding server audit path.
