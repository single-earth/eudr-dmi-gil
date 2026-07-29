from __future__ import annotations

import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from eudr_dmi_gil.reports.validate import validate_aoi_report_file


def _run_cli(args: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parents[1]
    src_path = str(repo_root / "src")
    env = dict(env)
    env["PYTHONPATH"] = src_path + (":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    return subprocess.run(
        [sys.executable, "-m", "eudr_dmi_gil.reports.cli", *args],
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )


def test_inspection_html_contains_sections(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    env = {
        "EUDR_DMI_EVIDENCE_ROOT": str(evidence_root),
    }

    aoi_id = "aoi-inspect"
    bundle_id = "bundle-inspect"

    proc = _run_cli(
        [
            "--aoi-id",
            aoi_id,
            "--aoi-wkt",
            "POINT (0 0)",
            "--bundle-id",
            bundle_id,
            "--out-format",
            "both",
            "--metric",
            "dummy=1:count:example",
        ],
        env=env,
    )
    assert proc.returncode == 0, proc.stderr

    bundle_date = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")
    bundle_dir = evidence_root / bundle_date / bundle_id
    report_json = bundle_dir / "reports" / "aoi_report_v2" / f"{aoi_id}.json"
    report_html = bundle_dir / "reports" / "aoi_report_v2" / f"{aoi_id}.html"

    report = validate_aoi_report_file(report_json)
    assert report.get("policy_mapping"), "policy_mapping must be non-empty"

    html = report_html.read_text(encoding="utf-8")
    for section in [
        "Deforestation-free (post-2020)",
        "Data sources & provenance",
        "Traceability to EUDR Articles",
        "Evidence artifact index",
    ]:
        assert section in html

    hrefs = re.findall(r'href="([^"]+)"', html)
    assert hrefs, "expected at least one link"
    for href in hrefs:
        if href.startswith("https://unpkg.com/leaflet@"):  # external Leaflet assets
            continue
        assert not href.startswith("/")
        assert "://" not in href
        resolved = (report_html.parent / href).resolve()
        assert resolved.is_file(), f"Missing linked artifact: {href}"
