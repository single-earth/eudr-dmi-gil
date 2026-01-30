# eudr-dmi-gil

## What is EUDR-DMI-GIL?

EUDR-DMI-GIL is an authoritative, open implementation of a geospatial data management and evidence-generation system designed to support inspection and due-diligence workflows related to EUDR.

## Abbreviations

- EUDR = European Union Deforestation Regulation
- DMI = Data Management Infrastructure
- GIL = Geospatial Intelligence Layer
- DAO = Decentralized Autonomous Organization (procedural, non-blockchain governance model in this project)

## Definitions

### DAO (Procedural DAO)

In this project, “DAO” refers to a procedural, evidence-driven governance workflow for proposing, reviewing, and implementing changes. It does not refer to a blockchain-based DAO.

The intent is to make governance artefacts auditable and transparent, with deterministic outputs that can be reproduced and reviewed.

### Digital Twin

The Digital Twin is a public, inspectable, versioned representation of system state, evidence mappings, and generated outputs.

It is not a live “real-time twin” by default. Updates are published through controlled releases.

## Repository Role (Authoritative)

This repository is the authoritative source for:

- code
- evidence contracts and schemas
- deterministic data-processing pipelines
- reproducible report generation

## Current Digital Twin Update Model (Human-in-the-loop)

Current updates follow a controlled feedback loop:

1) Stakeholders submit questions, evidence gaps, or change proposals via portal templates  
2) Developers implement approved changes in this authoritative repository  
3) Deterministic pipelines regenerate evidence artefacts and views  
4) The portal republishes the updated inspectable state  

All changes are expected to be versioned, reproducible, and reviewable.

## Relationship to the portal

Inspectable HTML views and procedural-DAO governance interfaces are published separately in the eudr-dmi-gil-digital-twin repository.

https://github.com/GeorgeMadlis/eudr-dmi-gil-digital-twin

---

## Attribution & Intent

This work has been developed on the author’s personal time and is
intended for use by the Single.Earth Foundation.  
No formal affiliation or endorsement is implied unless explicitly stated.

