# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Four-Repo Project Overview

This repo is one of four that form the EUDR evidence platform, all under the `georgemadlis` GitHub account:

| Repo | Role |
|------|------|
| **eudr-dmi-gil** ← (this repo) | **Authoritative** — all geospatial analysis, report generation, evidence contracts |
| **eudr-client-portal** | Private web portal — orchestrates this library via Python subprocess |
| **eudr-dmi-gil-digital-twin** | Public Digital Twin — consumes `out/site_bundle/` from this repo |
| **eudr-dmi-gil-digital-twin-ai-mirror** | Read-only archive for AI inspection |

This is the **single source of truth** for all report generation logic. The portal has no analytics code; the Digital Twin has no authoritative code. Both depend on this library.

---

## Commands

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Tests
pytest -q                          # Full test suite
ruff check src/                    # Lint

# Example regression test (mandatory before publish)
bash scripts/run_example_report_clean.sh
# Custom AOI:
AOI_GEOJSON=/path/to/aoi.geojson bash scripts/run_example_report_clean.sh

# Clean derived outputs before regenerating
bash scripts/clean_aoi_reports.sh

# Generate full report bundle (JSON + HTML + PDF)
python scripts/generate_report_v1.py \
  --run-id <id> --plot-id <id> \
  --aoi-geojson <path> --kyc-json <path> --analysis-json <path> \
  --out-dir out/reports

# Publish to Digital Twin (keep 2 most recent runs)
python tools/publish_latest_aoi_reports_to_dt.py \
  --dt-repo /Users/server/projects/eudr-dmi-gil-digital-twin \
  --dt-aoi-dir docs/site/bundles \
  --source-dir out/site_bundle/aoi_reports \
  --keep 2

# Verify DT links after publish
bash scripts/verify_dt_links.sh \
  --dt-repo /Users/server/projects/eudr-dmi-gil-digital-twin
```

---

## Architecture

### Package structure (`src/`)

```
src/
├── eudr_dmi_gil/          # Main authoritative package
│   ├── reports/           # Report generation CLI (entry: python -m eudr_dmi_gil.reports.cli)
│   ├── analysis/          # Geospatial analysis pipelines
│   ├── geo/               # GIS utilities
│   ├── io/                # File I/O (GeoJSON, tiles)
│   ├── tasks/             # Processing tasks
│   └── deps/              # External dependency management
└── eudr_dmi/              # Legacy namespace (do not add new code here)
```

### Key directories

| Path | Purpose |
|------|---------|
| `aoi_json_examples/` | Test AOI inputs — `estonia_testland1.geojson` is the canonical regression input |
| `out/site_bundle/` | **Only publication artifact** — what the Digital Twin consumes |
| `out/reports/` | Generated reports for manual inspection (not published) |
| `audit/evidence/` | Historical, immutable evidence bundles — **never modify** |
| `.tmp/evidence_example/` | Evidence from the example regression run |
| `schemas/reports/` | JSON schemas: `aoi_report_v2`, `bundle_manifest_v1`, `site_bundle_v1` |
| `docs/` | Architecture, governance, operations runbooks |

### Report generation CLI

The portal calls this package as a subprocess:
```
python -m eudr_dmi_gil.reports.cli \
  --aoi-id <id> --aoi-geojson <path> \
  --bundle-id <uuid> --out-format both
```
Output lands in a local temp directory; the portal then ingests it into MinIO.

### Output contract

Each report run produces (inside `out/site_bundle/aoi_reports/runs/<run_id>/`):
- `report.html` — human-readable report
- `aoi_report.json` — machine-readable structured output (source of truth for artifacts)
- `reports/aoi_report_v2/<plot_id>.json` — per-plot detail
- `reports/aoi_report_v2/<plot_id>/` — subfolder with `hansen/`, `maaamet/`, `map/` data

---

## Publish to Digital Twin

The Digital Twin is the public-facing GitHub Pages site at:
`https://georgemadlis.github.io/eudr-dmi-gil-digital-twin/site/`

**Publish flow (human-in-the-loop — not automated):**
1. Run regression test: `bash scripts/run_example_report_clean.sh`
2. Verify outputs in `out/site_bundle/aoi_reports/`
3. Run publish script (requires exactly 2 run directories in `out/site_bundle/aoi_reports/runs/`):
   ```bash
   bash scripts/publish_aoi_reports_to_dt.sh
   # or more flexibly:
   python tools/publish_latest_aoi_reports_to_dt.py \
     --dt-repo /Users/server/projects/eudr-dmi-gil-digital-twin \
     --dt-aoi-dir docs/site/bundles --source-dir out/site_bundle/aoi_reports --keep 2
   ```
4. Verify links: `bash scripts/verify_dt_links.sh --dt-repo /Users/server/projects/eudr-dmi-gil-digital-twin`
5. The script auto-commits and pushes to the DT repo; GitHub Pages redeploys automatically.

**Delete-before-publish invariant:** Always run `bash scripts/clean_aoi_reports.sh` before copying new bundles. Stale artifacts from previous runs must not persist.

**Publication boundaries:**
- `out/site_bundle/**` → **only** artifact that goes to the DT
- `audit/evidence/**` → historical record only; **never** published directly
- Private client AOI reports → stored in MinIO by the portal; **never** committed to DT

---

## Evidence Integrity Rules

- Evidence bundles in `audit/evidence/` are **immutable**. Create new bundles rather than editing.
- Every published report must validate against `schemas/reports/aoi_report_v2.schema.json`.
- `aoi_report.json` is the source of truth for declared artifacts — every artifact it references must exist at the declared relative path.
