# Inspection Index (Authoritative Implementation)

## Authority boundary

This repository is the authoritative implementation for EUDR-DMI-GIL. The Digital Twin repository is the public, non-authoritative portal for inspection and governance.

## Workspace Bootstrap

- Open scripts/eudr-dmi-gil.code-workspace.
- Re-run Prompt 0: Workspace Bootstrap.
- All changes must be tested via scripts/run_example_report_clean.sh.

## Governance and DAO Context

DAO proposals are produced via the DTE workflow and must be grounded in the Digital Twin inspection surface.

- Digital Twin DTE Instructions (Canonical): https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/governance/dte_instructions.md

All implementation changes should be traceable back to a DAO proposal grounded via the DTE workflow.

## Architecture

- [docs/architecture/decision_records/0001-report-pipeline-architecture.md](architecture/decision_records/0001-report-pipeline-architecture.md)
- [docs/architecture/dependency_register.md](architecture/dependency_register.md)

## Dependencies

- [docs/dependencies/README.md](dependencies/README.md)
- [docs/dependencies/sources.md](dependencies/sources.md)

## Reports

- [docs/reports/README.md](reports/README.md)
- [docs/reports/runbook_generate_aoi_report.md](reports/runbook_generate_aoi_report.md)

## Operations

- [docs/operations/environment_setup.md](operations/environment_setup.md)
- [docs/operations/minio_setup.md](operations/minio_setup.md)
- [docs/operations/migration_runbook.md](operations/migration_runbook.md)
