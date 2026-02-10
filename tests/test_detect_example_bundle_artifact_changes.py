from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.detect_example_bundle_artifact_changes as detector


REPORT_URL = "https://example.test/aoi_reports/runs/example/report.html"


def _make_report_html(generated: str, relpaths: list[str]) -> str:
    links = "\n".join(
        f"<li><a href=\"{rel}\">{rel}</a></li>" for rel in relpaths
    )
    return (
        "<html><body>"
        "<p><b>Generated (UTC)</b>: <code>"
        + generated
        + "</code></p>"
        "<h2>Declared evidence artifacts</h2>"
        "<ul>"
        + links
        + "</ul>"
        "</body></html>"
    )


class FetchStub:
    def __init__(self, html: str, artifacts: dict[str, bytes]) -> None:
        self.html = html.encode("utf-8")
        self.artifacts = artifacts

    def __call__(self, url: str, headers: dict[str, str] | None = None):
        if url == REPORT_URL:
            return 200, self.html, {}
        if url in self.artifacts:
            return 200, self.artifacts[url], {}
        raise RuntimeError(f"Unexpected URL: {url}")


def _run_detector(tmp_path: Path, local_root: Path, fetch: FetchStub) -> int:
    out_dir = tmp_path / "out"
    cache_dir = tmp_path / "cache"
    baseline = tmp_path / "baseline.json"
    instructions = tmp_path / "dte_instructions.txt"
    instructions.write_text("DTE instructions\n", encoding="utf-8")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(detector, "_fetch_url", fetch)

    try:
        return detector.run(
            [
                "--local-run-root",
                str(local_root),
                "--published-report-url",
                REPORT_URL,
                "--cache-dir",
                str(cache_dir),
                "--baseline-manifest",
                str(baseline),
                "--instructions-file",
                str(instructions),
                "--out-dir",
                str(out_dir),
            ]
        )
    finally:
        monkeypatch.undo()


def test_no_changes_exit_zero(tmp_path: Path) -> None:
    relpaths = [
        "reports/aoi_report_v2/estonia_testland1.json",
        "reports/aoi_report_v2/estonia_testland1.html",
    ]
    generated = "2026-02-06T10:57:34+00:00"
    html = _make_report_html(generated, relpaths)

    local_root = tmp_path / "run"
    for rel in relpaths:
        path = local_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"same")

    artifact_urls = {
        f"https://example.test/aoi_reports/runs/example/{rel}": b"same" for rel in relpaths
    }

    status = _run_detector(tmp_path, local_root, FetchStub(html, artifact_urls))
    assert status == 0

    out_dir = tmp_path / "out"
    diff_summary = json.loads((out_dir / "diff_summary.json").read_text(encoding="utf-8"))
    assert diff_summary["changes_detected"] is False
    assert not (out_dir / "dte_setup_patch.md").exists()


def test_hash_change_exit_three(tmp_path: Path) -> None:
    relpaths = ["reports/aoi_report_v2/estonia_testland1.json"]
    html = _make_report_html("2026-02-06T10:57:34+00:00", relpaths)

    local_root = tmp_path / "run"
    path = local_root / relpaths[0]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"local")

    artifact_urls = {
        "https://example.test/aoi_reports/runs/example/reports/aoi_report_v2/estonia_testland1.json": b"published"
    }

    status = _run_detector(tmp_path, local_root, FetchStub(html, artifact_urls))
    assert status == 3

    out_dir = tmp_path / "out"
    diff_summary = json.loads((out_dir / "diff_summary.json").read_text(encoding="utf-8"))
    assert diff_summary["diff"]["hash_changed"]
    assert (out_dir / "dte_setup_patch.md").exists()


def test_missing_local_exit_three(tmp_path: Path) -> None:
    relpaths = ["reports/aoi_report_v2/estonia_testland1.json"]
    html = _make_report_html("2026-02-06T10:57:34+00:00", relpaths)

    local_root = tmp_path / "run"
    local_root.mkdir(parents=True, exist_ok=True)

    artifact_urls = {
        "https://example.test/aoi_reports/runs/example/reports/aoi_report_v2/estonia_testland1.json": b"published"
    }

    status = _run_detector(tmp_path, local_root, FetchStub(html, artifact_urls))
    assert status == 3

    out_dir = tmp_path / "out"
    diff_summary = json.loads((out_dir / "diff_summary.json").read_text(encoding="utf-8"))
    assert diff_summary["diff"]["missing_locally"] == relpaths
