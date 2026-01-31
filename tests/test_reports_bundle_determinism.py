from __future__ import annotations

from pathlib import Path

from eudr_dmi_gil.reports.bundle import write_manifest


def test_manifest_bytes_deterministic_same_inputs(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "audit" / "evidence" / "2026-01-31" / "bundle-001"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    a = bundle_dir / "reports" / "a.json"
    b = bundle_dir / "reports" / "b.json"
    a.parent.mkdir(parents=True, exist_ok=True)

    a.write_text("{\"x\": 1}\n", encoding="utf-8")
    b.write_text("{\"y\": 2}\n", encoding="utf-8")

    first = write_manifest(bundle_dir, [a, b])
    second = write_manifest(bundle_dir, [a, b])

    assert first == second


def test_manifest_bytes_stable_ordering(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "audit" / "evidence" / "2026-01-31" / "bundle-002"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    a = bundle_dir / "reports" / "a.json"
    b = bundle_dir / "reports" / "b.json"
    a.parent.mkdir(parents=True, exist_ok=True)

    a.write_text("a\n", encoding="utf-8")
    b.write_text("b\n", encoding="utf-8")

    # Provide artifacts in different order; manifest should be identical.
    m1 = write_manifest(bundle_dir, [a, b])
    m2 = write_manifest(bundle_dir, [b, a])

    assert m1 == m2

    # Also ensure the file on disk matches returned bytes.
    assert (bundle_dir / "manifest.json").read_bytes() == m1
