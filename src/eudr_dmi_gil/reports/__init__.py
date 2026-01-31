"""Deterministic, inspectable report generation.

This package defines the report pipeline architecture for EUDR-DMI-GIL.
It intentionally focuses on:
- deterministic outputs (byte-for-byte where feasible)
- inspectable bundles (self-describing manifests + checksums)
- portable publication artifacts (site bundle zip)

Authoritative generation happens in this repository; publication happens in the
Digital Twin portal repository.
"""

from .layout import BundleLayout, BundleRef, resolve_audit_root
from .pipeline import ReportPipeline
from .types import ReportType
from .bundle import compute_sha256, resolve_evidence_root, write_manifest

__all__ = [
    "BundleLayout",
    "BundleRef",
    "ReportPipeline",
    "ReportType",
    "compute_sha256",
    "resolve_audit_root",
    "resolve_evidence_root",
    "write_manifest",
]
