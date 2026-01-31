# ADR 0001: Deterministic, Inspectable AOI Report Pipeline

Date: 2026-01-31

## Status

Accepted

## Context

We need an AOI (Area of Interest) report pipeline that is:

- **Deterministic**: reruns with identical inputs produce identical outputs (or explainable, explicitly-versioned differences).
- **Inspectable**: outputs are self-describing, checksumed, and reviewable without proprietary systems.
- **Publishable**: outputs can be lifted into a portal (Digital Twin) as static artifacts.

This repository (`eudr-dmi-gil`) is the **authoritative** source for deterministic generation.
The Digital Twin portal repository is responsible for **publishing** the generated artifacts.

Acceptance requirement: the design must align with the evidence-root + bundle layout and site-bundle conventions described in the upstream `eudr_dmi` README.

## Decision

### Responsibilities

- **Authoritative generation here, portal publishes elsewhere**.
  - Generation: this repo produces evidence bundles and portable site bundles.
  - Publication: the portal repo ingests bundles and publishes static views.

### Evidence root and bundle layout

Grounding (as described in upstream `eudr_dmi` README):

- There is an operator-configurable **audit/evidence root** ("audit root override").
- All outputs are written under:

  `<AUDIT_ROOT>/<YYYY-MM-DD>/<bundle_id>/`

Within a bundle root, outputs are organized into:

- `reports/` (machine-readable report outputs)
- `bundle_manifest.json` (bundle index + checksums)
- `site_bundle.zip` (portable site bundle for publication)

### Report types

Report types are stable, named, and versioned. Initial types:

- `aoi_summary_v1`: deterministic JSON summary for an AOI.
- `aoi_evidence_index_v1`: index of evidence references (hashes, paths, and sources).
- `site_bundle_v1`: portable zip containing an inspectable static site.

Future types can be introduced by adding new versioned names (e.g. `aoi_summary_v2`).

### Determinism requirements

A report run is considered deterministic when:

- **Inputs are declared** in `bundle_manifest.json` (dataset IDs, AOI IDs, tool versions, parameters).
- **Stable serialization** is used (sorted keys, stable whitespace, UTF-8).
- **Stable ordering** is enforced for collections (sorted feature lists, sorted file lists).
- **Time** is handled explicitly:
  - bundle date is `YYYY-MM-DD`
  - timestamps are UTC ISO-8601
  - portable zips use fixed timestamps for entries where feasible
- **Floating point outputs** are rounded consistently and documented per report type.

### Inspectability requirements

- Each bundle contains a `bundle_manifest.json` listing all artifacts with `sha256` and size.
- Artifacts are written to human-navigable, stable paths.
- Schemas live in this repo under `schemas/reports/` and are versioned.

### Publishing model (Digital Twin)

- The portal repo consumes bundles as build inputs.
- The portal repo republishes:
  - selected bundle artifacts (e.g. summary JSON)
  - derived HTML views
  - the portable `site_bundle.zip`

The portal repo must not be treated as the place where authoritative generation happens.

## Consequences

- Report pipelines can be verified locally by re-running and comparing checksums.
- Bundles become portable units for review, governance, and publication.
- Introducing new report types requires schema + manifest updates, preserving old versions.
