# Dependencies (source registry)

## Role in the ecosystem

This repository is the authoritative implementation for dependency tracking. The Digital Twin repository is the public, non-authoritative portal for inspection and governance.

This folder records upstream data sources referenced by the Digital Twin (DT)
Dependencies page, without embedding upstream content in this repository.

Authoritative dependency sources are seeded from
`data_db/dependency_sources.csv` and exported to `docs/dependencies/sources.*`
via `scripts/export_dependency_sources.py`.

Validate dependency links (operator/local agent):

```sh
python scripts/validate_dependency_links.py --fail-on-broken
python scripts/validate_dependency_links.py --only hansen
```

Dependency source workflow (authoritative):

```sh
python scripts/export_dependency_sources.py
python scripts/validate_dependency_links.py --fail-on-broken
python scripts/suggest_dependency_updates.py
python scripts/suggest_dependency_updates.py --promote-best
python scripts/bootstrap_data_db.py
```

## Compatibility note

Some code paths referenced by the DT are preserved as *compatibility shims* in
this repository snapshot. See [docs/architecture/dependency_register.md](../architecture/dependency_register.md)
for “used by” path preservation.

Changes to dependencies or evidence sources may originate from stakeholder DAO proposals reviewed via the DTE.

## See also

- [README.md](../../README.md)
- [docs/governance/roles_and_workflow.md](../governance/roles_and_workflow.md)
- Digital Twin DTE Instructions (Canonical): https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/governance/dte_instructions.md
- Digital Twin Inspection Index: https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin/blob/main/docs/INSPECTION_INDEX.md
- https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin
