# Runbook: generate a browsable AOI report site (local + Digital Twin publish)

This runbook provides a single end-to-end flow:

1) Generate an AOI evidence bundle (JSON + HTML + metrics.csv + manifest)
2) Export a portable report site bundle (folder + deterministic zip + sha256)
3) Copy into the Digital Twin portal repo working tree (human-in-the-loop)

## Conventions (evidence root + bundle layout)

Grounding (per upstream `eudr_dmi` conventions):

- Evidence/audit root is overrideable by the operator.
- Bundle layout is `<EVIDENCE_ROOT>/<YYYY-MM-DD>/<bundle_id>/` (UTC date).
- Portable site bundles are a folder plus a deterministic zip plus a sha256 file.

This repository is the authoritative implementation for generation.
Publishing happens in the portal repository.

## Prereqs

- Python 3.11
- From repo root: `pip install -e ".[dev]"`

## Step 0: pick an evidence root (recommended)

The generator defaults to `audit/evidence/` but you’ll usually want an absolute path:

```sh
export EUDR_DMI_EVIDENCE_ROOT="$PWD/audit/evidence"
```

## Step 1: generate an AOI bundle

Example using WKT:

```sh
python -m eudr_dmi_gil.reports.cli \
  --aoi-id aoi-123 \
  --aoi-wkt "POINT (0 0)" \
  --out-format both \
  --metric area_ha=12.34:ha:operator:placeholder \
  --metric forest_cover_fraction=0.56:fraction:operator:placeholder \
  --policy-mapping-ref "policy-spine:eudr/article-3" \
  --policy-mapping-ref "placeholder:TODO"
```

This writes a bundle under:

- `$EUDR_DMI_EVIDENCE_ROOT/<YYYY-MM-DD>/<bundle_id>/`

The bundle includes:

- `reports/aoi_report_v1/<aoi_id>.json` (schema-valid)
- `reports/aoi_report_v1/<aoi_id>.html` (inspectable summary, relative links)
- `reports/aoi_report_v1/<aoi_id>/metrics.csv` (deterministic metric rows)
- `inputs/aoi.wkt` or `inputs/aoi.geojson`
- `manifest.json` (sha256 + sizes for artifacts)

## Step 2: export a portable site bundle

Export for a single UTC date:

```sh
python scripts/export_reports_site_bundle.py --date "$(date -u +%F)"
```

Outputs (in `docs/` by default):

- `docs/site_bundle_reports/` (portable folder)
- `docs/site_bundle_reports.zip` (deterministic zip)
- `docs/site_bundle_reports.zip.sha256`

You can already browse locally:

- open `docs/site_bundle_reports/index.html`

## Step 3: publish into the Digital Twin repo working tree (no auto-push)

Assume you have a sibling checkout:

- `../eudr-dmi-gil-digital-twin/`

Copy the portable folder into the portal repo’s `site/aoi_reports/` working tree:

```sh
scripts/publish_reports_to_digital_twin.sh
```

Then, from inside the Digital Twin repo:

```sh
cd ../eudr-dmi-gil-digital-twin
git status
git diff
```

When the changes look correct, commit and push from the portal repository.

## Notes on policy refs (no interpretation)

`policy_mapping_refs` are references only:

- They can be placeholders.
- They are intended for DAO review/traceability.
- They are not compliance claims.
