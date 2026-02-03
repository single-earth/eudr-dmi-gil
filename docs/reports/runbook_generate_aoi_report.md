# Runbook: generate a browsable AOI report site (local + Digital Twin publish)

## Role in the ecosystem

This repository is the authoritative implementation for AOI report generation. The Digital Twin repository is the public, non-authoritative portal for inspection and governance.

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

Client AOI outputs are private by default and must remain in operator-controlled storage. Any export to the Digital Twin is an **example/public export only**.

AOI report structure and acceptance criteria are inspected via the Digital Twin and governed by DTE-driven proposals.

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

### Test run: Estonia example AOI (deterministic smoke)

This repo’s report entrypoint is:

- `python -m eudr_dmi_gil.reports.cli`

Deterministic example command (no timestamps in filenames):

```sh
python -m eudr_dmi_gil.reports.cli \
  --aoi-id estonia_testland1 \
  --aoi-geojson /Users/server/projects/eudr-dmi-gil/aoi_json_examples/estonia_testland1.geojson \
  --bundle-id estonia_testland1_example \
  --out-format both \
  --metric area_ha=12.34:ha:example:deterministic \
  --metric forest_cover_fraction=0.56:fraction:example:deterministic
```

Wrapper script (runs the command, exports DT-staging output, validates JSON, and prints absolute paths):

```sh
scripts/run_example_report.sh
```

This prints:

- input file path
- stable output directory: `out/site_bundle/aoi_reports/` with **runs/example/** only
- absolute paths for `index.html`, `report.html`, `aoi_report.json` (and `summary.json` if present)

Publishing policy:

- Only one AOI-agnostic example is published at `runs/example/`.
- Older examples remain available via git history.

## Operational Runbook (current publish workflow)

**Generation happens in this repo.** The DT portal publishes only the **latest 2 AOI report runs**.

Local output locations:

- Evidence bundles: `audit/evidence/<YYYY-MM-DD>/<bundle_id>/`
- DT-staging AOI reports: `out/site_bundle/aoi_reports/`

DT target path:

- `eudr-dmi-gil-digital-twin/docs/site/aoi_reports/`

Run the deterministic example generation:

```sh
scripts/test_run_estonia_testland1.sh
```

Publishing policy:

- Only one AOI-agnostic example is published at `runs/example/`.
- Older examples are available via git history.

Publish the latest 2 AOI reports to the DT repo:

```sh
scripts/publish_latest_aoi_reports_to_dt.sh \
  --dt-repo /Users/server/projects/eudr-dmi-gil-digital-twin \
  --dt-aoi-dir docs/site/aoi_reports \
  --source-dir out/site_bundle/aoi_reports \
  --keep 2
```

Verify DT links (runs link checker + AOI navigation check):

```sh
scripts/verify_dt_links.sh --dt-repo /Users/server/projects/eudr-dmi-gil-digital-twin
```

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

## See also

- [README.md](../../README.md)
- [docs/governance/roles_and_workflow.md](../governance/roles_and_workflow.md)
- Digital Twin DTE Instructions (Canonical): https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/governance/dte_instructions.md
- Digital Twin Inspection Index: https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/INSPECTION_INDEX.md
- https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin
