# Changelog

## 2026-07-25

### Canonical evidence report v3 and coffee integration review

- Added canonical `eudr_evidence_report_v3` output with JSON-first HTML/PDF/CSV generation and stable `manifest.sha256`.
- Added JRC GFC2020 baseline plus Hansen lossyear post-2020 loss intersection with configurable `end_year`.
- Added configurable single-commodity coffee assessment provider with MapBiomas config, unsupported-geography gaps, and null metrics for missing evidence.
- Regenerated the Digital Twin coffee bundle/sample report for `se_asia` / `demo_plot_02`.
- Tightened terminology so reports remain evidence-only and do not state legal outcomes.

## 2026-02-19

### AOI reports link + forest layer alignment

- eudr-dmi-gil — 165556f
  - Updated report map layer wiring so `forest_end_year` points to the current forest mask, preventing visual overlap confusion with post-2020 loss.
- eudr-client-portal — 48b5891
  - Updated Dashboard "DAO Reports" link to open AOI reports index instead of the example run report.
- eudr-dmi-gil-digital-twin — add372d
  - Regenerated 4 AOI report runs (example, latin_america, se_asia, west_africa) with updated map configs and synchronized report artifacts.
