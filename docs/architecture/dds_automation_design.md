# Cross-Repo DDS Automation Design

Date: 2026-07-13

## Status

Draft design for implementation planning.

## Purpose

This document defines how the local EUDR workspace should automate Due
Diligence Statement (DDS) draft creation and operator-approved submission
without turning the system into an automatic compliance-decision engine.

The governing product requirement is the DDS automation note attached to the
planning request. Its central constraint is preserved here:

- the system may automate intake, validation, evidence production, draft DDS
  preparation, submission mechanics, and receipt archival;
- the system must not claim automatic EUDR compliance, certified
  deforestation-free status, guaranteed DDS approval, or unattended legal
  declaration;
- an authorised operator representative remains responsible for approving the
  statement before submission.

## Workspace Roles

The DDS automation flow spans three local repositories.

| Repository | Role | Must do | Must not do |
|---|---|---|---|
| `eudr-client-portal` | Private intake and orchestration portal | Collect operator, consignment, supplier, AOI, approval, and submission metadata; enforce access control; store private artifacts; call official generation/submission adapters | Implement geospatial analytics, fork evidence logic, or make compliance determinations |
| `eudr-dmi-gil` | Authoritative evidence generation and contract repo | Generate deterministic evidence bundles, report artifacts, risk/mitigation registers, DDS draft contracts, validation results, and manifests | Submit to TRACES/EUDR or store operator credentials |
| `eudr-dds-client` | DDS web-service client and conformance harness | Validate/serialize DDS payloads, submit approved statements, retrieve/amend/retract where supported, and write redacted submission evidence | Decide whether a case is compliant or bypass portal approval |

`eudr-dmi-gil-digital-twin` remains public/example inspection infrastructure
only. Private DDS cases and private operator evidence must not be published
there.

## Core Workflow

The system should implement the following controlled flow:

```text
portal intake
  -> evidence generation
  -> risk and mitigation review
  -> DDS draft generation
  -> operator review
  -> operator approval
  -> DDS submission adapter
  -> receipt and provenance archive
```

### 1. Intake

`eudr-client-portal` collects structured data and files:

- operator identity, role, contact details, and legal identifiers;
- authorised representative details where applicable;
- product, commodity, CN/HS code, quantity, unit, and production country;
- suppliers, producers, upstream actors, production dates, and traceability
  notes;
- AOI geometries and original source files;
- previous DDS references and verification numbers where applicable;
- supporting legality, traceability, and risk-mitigation documents.

The immediate portal output is an `intake_manifest.json`-style record stored
with source filenames, hashes, upload timestamps, owner scope, and validation
results.

### 2. Evidence Generation

`eudr-client-portal` invokes the official `eudr-dmi-gil` generation entrypoint
and stores the returned bundle manifest reference.

`eudr-dmi-gil` produces deterministic artifacts such as:

- normalized geometries and geometry validation results;
- baseline forest metrics around the EUDR cutoff;
- post-2020 disturbance metrics;
- provider coverage and uncertainty notes;
- report HTML/PDF/JSON artifacts;
- bundle manifest with file hashes, dataset versions, code version, parameters,
  and timestamps.

Evidence states must be descriptive workflow states, not legal conclusions.
Allowed examples include:

- `no_disturbance_detected`
- `disturbance_detected`
- `insufficient_observations`
- `provider_disagreement`
- `manual_review_required`

`no_disturbance_detected` must not be converted automatically into
`deforestation_free` or any equivalent compliance conclusion.

### 3. Risk And Mitigation

`eudr-dmi-gil` should produce a structured risk package:

```text
risk/
  risk_register.json
  evidence_gaps.json
  mitigation_actions.json
```

The risk register should separate at least:

- geospatial risk;
- traceability risk;
- legality/documentary risk;
- source and country risk;
- data-quality risk.

Mitigation actions block DDS approval when they are mandatory and unresolved.
Examples include corrected polygons, supplier reconciliation, missing harvest
permits, independent documentation, high-resolution image review, or removal
of unsupported plots from the consignment.

### 4. DDS Draft Contract

`eudr-dmi-gil` owns the versioned DDS draft evidence contract because it owns
the authoritative evidence and schema conventions.

Recommended bundle layout extension:

```text
dds/
  dds_draft.json
  validation_result.json
```

`dds_draft.json` should include:

- schema version;
- internal reference number;
- DDS field values;
- per-field source references;
- manual-entry markers;
- derived-value method and code version;
- evidence artifact references and hashes;
- risk/mitigation gate status;
- bundle manifest hash;
- creation timestamp.

`validation_result.json` should include:

- machine-readable errors and warnings;
- missing mandatory DDS fields;
- ambiguous or partial mappings;
- blocked gate reasons;
- payload hash candidate, when serialization is possible;
- schema and mapper version.

The existing DDS mapping shows that several DDS fields are not yet collected
or are only partially represented in the portal. Until those gaps are closed,
draft generation must produce validation errors rather than silently guessing.

### 5. Operator Review And Approval

`eudr-client-portal` owns the review and approval experience.

Recommended state machine:

```text
draft
  -> evidence_processing
  -> analyst_review
  -> mitigation_required
  -> ready_for_operator_review
  -> operator_approved
  -> submitted
  -> accepted_or_recorded
```

No transition from `ready_for_operator_review` to `submitted` is allowed
without an approval record containing:

- approver user ID and display identity;
- authority confirmation;
- timestamp;
- exact DDS draft schema version;
- payload hash;
- evidence bundle hash;
- risk/mitigation status at approval time;
- approval text accepted by the user.

The approval action should be explicit, for example:

```text
I confirm that I am authorised by the operator and approve submission of this
Due Diligence Statement.
```

The portal must block approval when mandatory validation errors or mandatory
mitigation actions remain unresolved.

### 6. Submission

`eudr-client-portal` calls a provider-neutral DDS adapter. The adapter boundary
should expose operations rather than SOAP or payload internals:

```text
validate(statement)
submit(statement, approval_record)
amend(reference, statement, approval_record)
withdraw(reference, approval_record)
retrieve(reference)
```

Initial implementations:

- `mock`: deterministic local adapter for tests and development;
- `eudr-dds-client`: adapter shell invoking the Python DDS client;
- future providers: possible alternative clients if official tooling changes.

`eudr-dds-client` remains responsible for:

- constructing the exact DDS payload model;
- enforcing schema-level payload validation;
- separating acceptance and production environments;
- redacting request credentials in evidence artifacts;
- writing append-only submission evidence;
- exposing retrieval/amend/retract operations where supported.

## Idempotency

DDS submission is consequential and should be protected against duplicate
creation.

Recommended idempotency key:

```text
sha256(operator_id + consignment_id + payload_hash)
```

Before submission, `eudr-client-portal` checks whether the same approved
payload is already:

- submitted;
- in flight;
- recorded with a DDS reference;
- blocked due to a prior uncertain response.

Automatic retries should be limited to operations known to be safe and
idempotent. Uncertain responses require human review before another submit
attempt.

## Provenance Archive

The portal should archive submission provenance with the private case:

- DDS draft hash;
- serialized payload hash;
- evidence bundle hash;
- approval record hash;
- DDS client version or commit;
- adapter name and version;
- environment (`acceptance` or `production`);
- submission timestamp;
- response status;
- DDS reference number;
- verification number where available;
- request and response hashes;
- retry history;
- retrieval evidence for post-submit confirmation.

`eudr-dds-client` writes its own redacted submission evidence under its
`evidence/<YYYY-MM-DD>/<run_id>/` contract. The portal should store or link the
resulting evidence bundle without exposing secrets or unnecessary personal
data.

## Security Controls

Required controls:

- credentials are operator-specific where possible;
- credentials are stored in encrypted secret storage, not GitHub or logs;
- acceptance and production credentials are separate;
- production submission requires explicit environment opt-in;
- request XML and logs redact authentication material;
- private evidence remains in portal-controlled private storage;
- public Digital Twin publication is limited to curated example artifacts;
- access control is enforced by user/company/tenant scope on every case,
  artifact, and submission record.

## Data Contracts

### Portal Intake Contract

`eudr-client-portal` should persist:

- latest relevant KYC/onboarding snapshot;
- AOI record and geometry hash;
- uploaded source document hashes;
- consignment/product structured fields;
- supplier/producer structured fields;
- previous DDS reference pairs;
- review and approval records.

The portal schema needs new structured fields for DDS values currently marked
as gaps or partial mappings in the DDS client field mapping, especially
activity type, identifier type, split address fields, border country, net
weight, supplementary units, species names, geolocation confidentiality, and
associated statement verification numbers.

### Evidence Contract

`eudr-dmi-gil` should extend existing bundle manifests with DDS-preparation
artifacts, while preserving deterministic serialization, stable paths, schema
versioning, and checksumed outputs.

### Submission Contract

`eudr-dds-client` should keep the DDS payload model aligned with vendored WSDL
and XSD snapshots, and should continue to record append-only redacted evidence
for submit, retrieve, amend, and retract operations.

## Failure Handling

Failures should be classified distinctly:

- intake validation failure;
- evidence generation failure;
- unresolved mitigation;
- DDS draft validation failure;
- operator approval missing or revoked;
- authentication failure;
- authorization failure;
- schema-version mismatch;
- duplicate submission attempt;
- temporary server failure;
- uncertain or partial response;
- production environment blocked by configuration.

Only the submission adapter layer should classify DDS web-service responses.
The portal should present actionable status and preserve the full provenance
needed for review.

## Implementation Phases

### Phase 1: Contracts And Gaps

- Add a DDS draft JSON schema in `eudr-dmi-gil`.
- Extend portal onboarding/intake fields for known DDS mapping gaps.
- Add deterministic validation output for missing and partial mappings.
- Add mock DDS adapter interface in the portal.

### Phase 2: Draft Generation

- Generate `dds/dds_draft.json` and `dds/validation_result.json` from portal
  intake plus evidence bundle outputs.
- Add tests with a complete timber fixture and missing-field fixtures.
- Add portal UI for DDS draft inspection.

### Phase 3: Approval Gate

- Add portal DDS case state.
- Persist approval records.
- Block submission without approval and without resolved mandatory validation.
- Add end-to-end mock submission tests.

### Phase 4: DDS Client Integration

- Wire the portal adapter to `eudr-dds-client` in acceptance mode.
- Store submission provenance and redacted DDS client evidence references.
- Add idempotency checks.
- Run acceptance-environment conformance tests only with real credentials and
  record real run IDs.

### Phase 5: Production Readiness

- Confirm production authorisation and delegation model.
- Confirm official API lifecycle, rate limits, and bulk-processing rules.
- Review secret storage and operational controls.
- Require explicit production opt-in and operator approval for every
  production submission.

## Non-Goals

This design does not include:

- autonomous legal conclusions;
- automatic negligible-risk decisions;
- unattended DDS submission;
- black-box LLM risk scoring;
- automated interpretation of all national legislation;
- publication of private DDS cases to the public Digital Twin;
- storing raw credentials in generated bundles;
- treating the TRACES NT document catalogue as an EUDR DDS API specification.

## Open Questions

- Which official production credential and delegation model applies for a
  service provider submitting on behalf of an operator?
- Does the Information System accept evidence attachments, or only statement
  data and references?
- What production rate limits, duplicate-submission rules, and bulk-processing
  limits apply?
- Which DDS lifecycle operations are available and stable in production:
  create, amend, retract, retrieve, status?
- Which country-risk, legality, and traceability evidence standards should be
  configured as mandatory for each commodity and geography?
- What evidence-retention period should the platform enforce per customer and
  jurisdiction?

## Acceptance Criteria For This Design

- Repo boundaries remain explicit and enforceable.
- DDS draft generation is deterministic and schema-versioned.
- Mandatory evidence gaps block approval rather than being guessed.
- Operator approval is required before submission.
- Submission is idempotency-protected.
- Provenance is sufficient to reproduce what was approved and submitted.
- No automated output claims compliance, deforestation-free status, or legal
  approval.
