from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

from .determinism import file_size_bytes, sha256_file, utc_now_iso, write_json
from .layout import BundleLayout, BundleRef, resolve_audit_root
from .types import ReportArtifact, ReportBundleManifest, ReportType


class ReportPipeline:
    """Scaffolded report pipeline.

    This is intentionally small: it defines the bundle layout, manifest rules,
    and deterministic writing conventions. Concrete report generators can be
    added incrementally.
    """

    def __init__(
        self,
        *,
        audit_root: str | Path | None = None,
        generator_meta: Mapping[str, Any] | None = None,
    ) -> None:
        self._audit_root = resolve_audit_root(audit_root)
        self._generator_meta = dict(generator_meta or {})

    def bundle_layout(self, *, bundle_date: str, bundle_id: str) -> BundleLayout:
        return BundleLayout(
            audit_root=self._audit_root,
            ref=BundleRef(bundle_date=bundle_date, bundle_id=bundle_id),
        )

    def write_bundle_manifest(
        self,
        *,
        layout: BundleLayout,
        inputs: Mapping[str, Any],
        artifacts: list[ReportArtifact],
    ) -> ReportBundleManifest:
        manifest = ReportBundleManifest(
            schema="schemas/reports/bundle_manifest_v1.schema.json",
            bundle_date=layout.ref.bundle_date,
            bundle_id=layout.ref.bundle_id,
            created_utc=utc_now_iso(),
            generator={
                "repo": "eudr-dmi-gil",
                **self._generator_meta,
            },
            inputs=dict(inputs),
            artifacts=tuple(sorted(artifacts, key=lambda a: a.relpath)),
        )

        # Serialize with stable ordering.
        write_json(layout.manifest_path, _manifest_to_json(manifest))
        return manifest

    def record_artifact(
        self,
        *,
        layout: BundleLayout,
        path: Path,
        content_type: str | None = None,
        meta: Mapping[str, Any] | None = None,
    ) -> ReportArtifact:
        relpath = str(path.relative_to(layout.bundle_root))
        return ReportArtifact(
            relpath=relpath,
            sha256=sha256_file(path),
            size_bytes=file_size_bytes(path),
            content_type=content_type,
            meta=dict(meta or {}) or None,
        )


def _manifest_to_json(manifest: ReportBundleManifest) -> dict[str, Any]:
    d = asdict(manifest)
    d["artifacts"] = [asdict(a) for a in manifest.artifacts]

    # Lightly encode the declared report types as a hint for portal consumers.
    d.setdefault("declared_report_types", [t.value for t in ReportType])
    return d
