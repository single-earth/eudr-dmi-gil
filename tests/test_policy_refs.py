from __future__ import annotations

from pathlib import Path

from eudr_dmi_gil.reports.policy_refs import collect_policy_mapping_refs


def test_collect_policy_mapping_refs_strings_and_files(tmp_path: Path) -> None:
    f = tmp_path / "refs.txt"
    f.write_text("# header\nref-a\n\nref-b\n", encoding="utf-8")

    refs = collect_policy_mapping_refs(refs=["ref-c", "  ", "ref-a"], ref_files=[str(f)])

    # Stable, de-duplicated, and includes placeholders.
    assert refs == ["ref-a", "ref-b", "ref-c"]
