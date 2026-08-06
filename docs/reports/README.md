# Reports

## Role in the ecosystem

This repository is the authoritative implementation for report generation. The Digital Twin repository is the public, non-authoritative portal for inspection and governance.

This folder documents the report pipeline architecture for EUDR-DMI-GIL.

AOI report structure and acceptance criteria are inspected via the Digital Twin and governed by DTE-driven proposals.

## Key conventions

- **Authoritative generation happens here** (this repo).
- **Publication/hosting happens elsewhere** in the Digital Twin portal repository:
  - https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin
- **Client AOI outputs are private by default** and remain in operator-controlled storage (evidence root/MinIO). Any export to the Digital Twin is an **example/public export only**.

## Evidence bundles

Reports are written into **evidence bundles** under the operator-configured audit root.

Grounding (per upstream `eudr_dmi` README conventions):

- Audit root is overrideable by the operator.
- Bundle layout is: `<AUDIT_ROOT>/<YYYY-MM-DD>/<bundle_id>/`
- Bundles may include a **portable site bundle zip** for portal publishing.

Local generation outputs:

- Evidence bundles: `audit/evidence/<YYYY-MM-DD>/<bundle_id>/`
- DT-staging AOI reports: `out/site_bundle/aoi_reports/`

Deterministic example runner:

- `scripts/run_example_report.sh`

## AOI report regeneration contract

All AOI report generation must be preceded by:

- `scripts/clean_aoi_reports.sh`

Mandatory regression test:

- `scripts/run_example_report_clean.sh`

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

## Single-commodity evidence

AOI report generation can run with one explicitly configured commodity layer:

```bash
python -m eudr_dmi_gil.reports.cli \
  --aoi-id brazil_coffee_plot_001 \
  --aoi-geojson inputs/aoi.geojson \
  --jrc-gfc2020-raster data/jrc_gfc2020.tif \
  --hansen-lossyear-raster data/hansen_lossyear.tif \
  --end-year 2025 \
  --commodity-config docs/reports/examples/coffee_commodity_config.json
```

Current commodity implementation:

- `coffee` via provider `mapbiomas_brazil`
- configured MapBiomas Brazil class values, initially `[46]`
- Brazil scope only; the provider does not treat the class as a global coffee layer
- missing/unsupported commodity evidence is reported as unavailable/unsupported and metric values remain `null`, not `0`

The report JSON includes `commodity.*`, commodity metrics, provider provenance, coverage status, nodata details, and evidence gaps. Status language is evidence-only: it may flag post-2020 forest-loss evidence, intersection with the configured coffee layer, or a potential forest-to-coffee conversion candidate for human review. It must not state EUDR compliance, illegal deforestation, proven conversion, or causation by coffee.

Example coffee configuration:

- `docs/reports/examples/coffee_commodity_config.json`

Example execution command:

```bash
python -m eudr_dmi_gil.reports.cli \
  --aoi-id coffee_plot_001 \
  --aoi-geojson inputs/aoi.geojson \
  --bundle-id coffee_review_001 \
  --out-format both \
  --jrc-gfc2020-raster data/jrc_gfc2020.tif \
  --hansen-lossyear-raster data/hansen_lossyear.tif \
  --loss-dataset-end-year 2026 \
  --end-year 2026 \
  --commodity coffee \
  --commodity-config docs/reports/examples/coffee_commodity_config.json
```

### Acquiring pinned raster inputs

`--jrc-gfc2020-raster`, `--hansen-lossyear-raster`, and any commodity `local_path` raster must be
real Google Earth Engine downloads clipped to the AOI, never hand-built or synthetic fixtures — a
report's evidence is only as good as its inputs, and a fabricated raster silently produces a
fabricated verdict. Acquire them with a small `tmp/acquire_<aoi>_inputs.py` script following the
existing pattern in `tmp/acquire_brazil_compliant_inputs.py`, `tmp/acquire_brazil_coffee_inputs.py`,
and `tmp/acquire_ghana_baseline_inputs.py`: `ee.Image(asset_id).select([band]).clip(geometry)`,
downloaded via `getDownloadURL(...)`, with an `.acquisition_metadata.json` sidecar recording the
asset id, band, scale, projection, access timestamp, and output sha256. A quick sanity check that
catches fabricated inputs before they reach a report: independently acquired bands at different
native resolutions (e.g. JRC at 10 m vs Hansen/MapBiomas at 30 m) must have different pixel grids
over the same AOI — identical grids across bands acquired at different scales means the file was
not actually downloaded from the asset it claims to be.

## Canonical report schema and artifacts

Schema:

- `schemas/reports/eudr_evidence_report_v3.schema.json`

Canonical output tree:

```text
<bundle>/reports/aoi_report_v2/<aoi_id>/
  report.json
  report.html
  report.pdf
  metrics.csv
  manifest.sha256
  evidence/
    02_jrc_forest_2020.png
    03_forest_loss_2021_<effective_end_year>.png
    04_commodity_layer.png
    05_intersection.png
    legend.png
```

Contract:

- `report.json` is canonical and schema-validated.
- `report.html`, `report.pdf`, and `metrics.csv` are derived from the same canonical model.
- `manifest.sha256` is sorted by relative path and excludes itself.
- Optional evidence uses explicit gaps (`available: false`, `path: null`) instead of fabricated content.
- Paths are relative so bundles work locally and under `docs/site/bundles/`.

Dataset/provider notes:

- JRC Global Forest Cover 2020 (`JRC/GFC2020/V3`) is the configured 2020 forest-baseline evidence layer.
- Hansen `lossyear` is filtered to `2021..effective_end_year` and intersected with the JRC 2020 forest mask.
- Categorical rasters are aligned to an equal-area target grid with nearest-neighbour resampling.
- Nodata inside the AOI is excluded and reported as an evidence gap.
- Coffee currently uses a configurable `mapbiomas_brazil` provider; MapBiomas class values and versions are config fields.
- Unsupported commodity geography is an evidence gap, not a measured zero.

Terminology:

- Use neutral evidence language: "post-2020 forest-loss evidence", "intersection", "review required", and "evidence gap".
- Do not describe commodity intersection as proven causation.
- Do not present Hansen treecover2000/RFM metrics as the JRC 2020 baseline.
- Do not state that the report determines legal outcomes.

## Publishing to the Digital Twin repo (human-in-the-loop)

This repository (`eudr-dmi-gil`) is responsible for **authoritative generation** of deterministic evidence bundles
and portable site bundles.

The Digital Twin portal repository is responsible for **publishing/hosting** those artifacts.

Digital Twin publishing policy:

- The DT portal publishes only the **latest 2 AOI report runs**.
- Older runs remain on the server (authoritative environment) and are not published to the DT portal.

Recommended workflow:

1) Generate AOI evidence bundles (JSON/HTML/metrics.csv/manifest.json) under the evidence root.
2) Export a portable site bundle folder + deterministic zip (see `scripts/export_reports_site_bundle.py`).
3) Copy the portable folder into a sibling checkout of the portal repo and review the diff.

Publishing script (auto-commit + push into DT):

- `scripts/publish_latest_aoi_reports_to_dt.sh`

Default target path in the DT repo:

- `../eudr-dmi-gil-digital-twin/docs/site/aoi_reports/`

## See also

- [README.md](../../README.md)
- [docs/governance/roles_and_workflow.md](../governance/roles_and_workflow.md)
- Digital Twin DTE Instructions (current working fork): https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/dte_instructions.md
- Digital Twin Inspection Index (DT-side router): https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/INSPECTION_INDEX.md
- https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin
