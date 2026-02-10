# Baselines

Baselines are committed for audit and to prevent silent drift in published example artifacts.

## Rules

- Baselines must only be updated intentionally.
- Updating a baseline requires the explicit `--write-baseline` flag in the detector script.
- Any baseline update must be committed alongside the regenerated artifacts or related changes.
