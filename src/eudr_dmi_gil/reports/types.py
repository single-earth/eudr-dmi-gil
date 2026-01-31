from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping


class ReportType(str, Enum):
    """Known report types.

    The intent is to keep report types stable and versioned because the Digital
    Twin portal consumes these outputs by convention.
    """

    AOI_SUMMARY_V1 = "aoi_summary_v1"
    AOI_EVIDENCE_INDEX_V1 = "aoi_evidence_index_v1"
    SITE_BUNDLE_V1 = "site_bundle_v1"


@dataclass(frozen=True)
class ReportArtifact:
    """A single file produced by the pipeline."""

    relpath: str
    sha256: str
    size_bytes: int
    content_type: str | None = None
    meta: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ReportBundleManifest:
    """Bundle-level manifest used for inspection + publication."""

    schema: str
    bundle_date: str  # YYYY-MM-DD
    bundle_id: str
    created_utc: str  # ISO-8601 timestamp, UTC
    generator: Mapping[str, Any]
    inputs: Mapping[str, Any]
    artifacts: tuple[ReportArtifact, ...]
