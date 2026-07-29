from __future__ import annotations

from pathlib import Path

from eudr_dmi_gil.reports.cli import main as aoi_cli_main

from scripts.export_aoi_reports_staging import export_aoi_reports


def test_staging_export_preserves_canonical_report_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    evidence_root = tmp_path / "evidence"
    monkeypatch.setenv("EUDR_DMI_EVIDENCE_ROOT", str(evidence_root))
    monkeypatch.setenv("EUDR_DMI_GENERATED_AT_UTC", "2026-07-25T00:00:00+00:00")

    assert (
        aoi_cli_main(
            [
                "--aoi-id",
                "aoi-1",
                "--aoi-wkt",
                "POINT (0 0)",
                "--bundle-id",
                "bundle-1",
                "--out-format",
                "both",
                "--metric",
                "a_metric=1:count:src:note",
            ]
        )
        == 0
    )

    output_root = tmp_path / "staging"
    export_aoi_reports(
        evidence_root=evidence_root,
        output_root=output_root,
        staged_run_id="example",
        report_json_filename="aoi_report.json",
    )

    prefix = (
        output_root
        / "runs"
        / "example"
        / "reports"
        / "aoi_report_v2"
        / "aoi-1"
    )
    assert (prefix / "report.json").is_file()
    assert (prefix / "report.html").is_file()
    assert (prefix / "report.pdf").is_file()
    assert (prefix / "metrics.csv").is_file()
    assert (prefix / "manifest.sha256").is_file()
