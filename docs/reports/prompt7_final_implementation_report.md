# Prompt 7 final implementation report

## 1. Summary of changes

- Completed integration review for JRC GFC2020 baseline, post-2020 Hansen lossyear masking, single-commodity coffee evidence, canonical report model, HTML/PDF rendering, manifests, and DT coffee publication artifacts.
- Fixed lint defects, dependency-update promotion regression, DT staging export of canonical artifacts, sample-report fallback for canonical evidence PNGs, and evidence-only terminology.
- Regenerated coffee-related DT artifacts for `docs/site/bundles/runs/se_asia` and `docs/site/sample_reports/runs/demo_2026-03-08/demo_plot_02`.

## 2. Files added and modified

Primary added files:

- `src/eudr_dmi_gil/analysis/jrc_post2020_loss.py`
- `src/eudr_dmi_gil/commodities/*`
- `src/eudr_dmi_gil/providers/*`
- `src/eudr_dmi_gil/reports/report_model.py`
- `schemas/reports/eudr_evidence_report_v3.schema.json`
- `tests/test_jrc_post2020_loss.py`
- `tests/test_commodity_assessment.py`
- `tests/test_canonical_report_model.py`
- `tests/test_export_aoi_reports_staging.py`
- `docs/reports/eudr_evidence_report_v3_migration.md`
- `docs/reports/examples/coffee_commodity_config.json`

Primary modified files:

- `src/eudr_dmi_gil/reports/cli.py`
- `src/eudr_dmi_gil/reports/validate.py`
- `scripts/export_aoi_reports_staging.py`
- `scripts/suggest_dependency_updates.py`
- `docs/reports/README.md`
- `README.md`
- `CHANGELOG.md`
- DT: `scripts/generate_sample_report_from_bundle.py`
- DT: `docs/site/bundles/runs/se_asia/**`
- DT: `docs/site/sample_reports/runs/demo_2026-03-08/demo_plot_02/**`

## 3. Commands executed

- `.venv/bin/python -m pytest`
- `.venv/bin/ruff check src tests scripts`
- `.venv/bin/python -m compileall -q src scripts tests`
- `.venv/bin/python -m eudr_dmi_gil.reports.validate <dt se_asia json files>`
- `shasum -a 256 -c reports/aoi_report_v2/coffee_se_asia_vietnam/manifest.sha256`
- `shasum -a 256 -c manifest.sha256`
- `pdfinfo <canonical report.pdf>`
- `pdftotext <canonical report.pdf> -`
- `rsync -a --delete /private/tmp/prompt7_dt_coffee_staging/runs/se_asia/ <dt>/docs/site/bundles/runs/se_asia/`
- `python3 scripts/rebuild_aoi_reports_index.py --site-root docs/site`
- `/Users/server/projects/eudr-dmi-gil/.venv/bin/python scripts/generate_sample_report_from_bundle.py --bundle-dir docs/site/bundles/runs/se_asia --output-dir docs/site/sample_reports/runs/demo_2026-03-08/demo_plot_02 --report-json se_asia_aoi_report.json`

## 4. Test results

- Unit/integration/schema/golden/render tests: `114 passed`.
- Lint: Ruff passed.
- Bytecode compilation: passed.
- Static type checker: no `mypy` or `pyright` executable/config is present in this repo; `compileall` was used as the available syntax/import check.
- Manifest verification: canonical DT coffee manifest and sample-report manifest passed.
- PDF rendering: canonical PDF is A4, 12 pages, deterministic ReportLab metadata; sample PDF is A4, 7 pages.
- Screenshot/manual visual check: regenerated sample `deforestation_map.png` was inspected and is nonblank.

## 5. Example output paths

- `/Users/server/projects/eudr-dmi-gil-digital-twin/docs/site/bundles/runs/se_asia/se_asia_aoi_report.json`
- `/Users/server/projects/eudr-dmi-gil-digital-twin/docs/site/bundles/runs/se_asia/reports/aoi_report_v2/coffee_se_asia_vietnam/report.json`
- `/Users/server/projects/eudr-dmi-gil-digital-twin/docs/site/bundles/runs/se_asia/reports/aoi_report_v2/coffee_se_asia_vietnam/report.html`
- `/Users/server/projects/eudr-dmi-gil-digital-twin/docs/site/bundles/runs/se_asia/reports/aoi_report_v2/coffee_se_asia_vietnam/report.pdf`
- `/Users/server/projects/eudr-dmi-gil-digital-twin/docs/site/sample_reports/runs/demo_2026-03-08/demo_plot_02/report.html`

## 6. Schema changes

- New canonical schema: `eudr_evidence_report_v3`.
- Required top-level fields include `schema_version`, `assessment`, `aoi`, `commodity`, `temporal_scope`, `metrics`, `datasets`, `methods`, `evidence_gaps`, `layers`, `artifacts`, `audit`, and `references`.
- HTML, PDF, and CSV consume the same canonical metrics/model.

## 7. Deprecated fields

- Legacy Hansen treecover2000/RFM fields remain only as audit compatibility fields.
- Consumers should not use `rfm_area_ha` or `pixel_initial_tree_cover_ha` as the JRC 2020 baseline.

## 8. Known limitations

- Local fixture imagery is not satellite imagery; missing satellite/before-after layers are explicit gaps.
- Coffee provider is currently MapBiomas Brazil scoped.
- DT `se_asia` coffee example intentionally reports unsupported commodity geography for Vietnam.
- No configured static type checker is present.

## 9. Evidence gaps

- Unsupported commodity geography for the Vietnam coffee example.
- Commodity layer unavailable for unsupported geography.
- Satellite context and before/after imagery unavailable in the regenerated local bundle.
- JRC/Hansen nodata inside the synthetic fixture AOI is reported and excluded.

## 10. Security or privacy considerations

- AOI examples are public sample artifacts only.
- Client AOIs remain private by default and should not be committed to the DT repo.
- Manifests provide integrity checks but are not access-control mechanisms.
- Generated reports include local fixture paths in provenance; avoid publishing sensitive local paths for client runs.

## 11. Suggested commit sequence

1. JRC baseline and post-2020 loss analysis.
2. Commodity provider/config integration.
3. Canonical report model, schema, HTML/PDF/manifest generation.
4. Regression tests and lint fixes.
5. Documentation and migration notes.
6. Digital Twin coffee bundle/sample regeneration.

## 12. PR title and description

Title: `Add canonical JRC baseline evidence reports and coffee provider integration`

Description:

```markdown
## Summary
- Adds JRC GFC2020 baseline + Hansen lossyear post-2020 loss evidence.
- Adds configurable single-commodity coffee provider support.
- Adds canonical `eudr_evidence_report_v3` JSON-first HTML/PDF/CSV/manifest generation.
- Regenerates the DT coffee example with explicit unsupported-geography evidence gaps.

## Validation
- `114 passed`
- Ruff passed
- Manifest checks passed
- Canonical PDF checked as A4 / 12 pages
```

## 13. Reviewer checklist

- [ ] JRC GFC2020 is the baseline for post-2020 loss metrics.
- [ ] Hansen lossyear is filtered to 2021 through `effective_end_year`.
- [ ] Commodity gaps are distinct from zero overlap.
- [ ] Canonical `report.json` validates and drives HTML/PDF/CSV.
- [ ] `manifest.sha256` verifies all declared canonical artifacts.
- [ ] Reports use evidence-only terminology and do not determine legal outcomes.
- [ ] DT coffee bundle/sample paths open locally with relative links.
