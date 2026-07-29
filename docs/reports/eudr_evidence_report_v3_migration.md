# EUDR Evidence Report v3 migration note

`eudr_evidence_report_v3` makes `report.json` the canonical report model for
`report.html`, `report.pdf`, `metrics.csv`, and `manifest.sha256`.

Renamed fields:

- `report_version` -> `schema_version`
- `generated_at_utc` -> `generated_utc`
- `aoi_id` -> `aoi.name`
- `results_summary.aoi_area.area_ha` -> `metrics.aoi_area_ha`
- JRC baseline fields are canonicalized under
  `metrics.forest_baseline_2020_*` and
  `metrics.forest_loss_post_2020_on_baseline_ha`

Compatibility:

- `canonical_report_from_aoi_report_v2` adapts existing AOI report v2 payloads.
- Legacy Hansen tree-cover metrics such as `rfm_area_ha` and
  `pixel_initial_tree_cover_ha` remain audit fields when present.
- The adapter does not reinterpret old Hansen tree-cover metrics as JRC 2020
  baseline metrics.
- The adapter does not infer commodity identity from AOI names or filenames;
  commodity fields are populated only from explicit commodity report/config data.

Artifact rules:

- Evidence artifact paths in v3 are relative to the canonical report directory.
- Missing optional commodity and before/after evidence is represented with
  `path: null`, `available: false`, and an explicit availability status.
- `manifest.sha256` is sorted by stable relative path and excludes itself.

Deprecated fields retained for audit:

- `forest_metrics.rfm_area_ha`
- `pixel_initial_tree_cover_ha`
- `pixel_current_tree_cover_ha`
- `loss_2021_<end_year>_ha` from the legacy Hansen treecover2000 workflow

Consumers should prefer:

- `metrics.forest_baseline_2020_ha`
- `metrics.forest_loss_post_2020_on_baseline_ha`
- `temporal_scope.effective_end_year`
- `commodity.coverage_status`
- `evidence_gaps[]`
