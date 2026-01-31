from __future__ import annotations

import os
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from eudr_dmi_gil.reports.validate import validate_aoi_report_v1_file


def _run_cli(args: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "eudr_dmi_gil.reports.cli", *args],
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )


def test_cli_help() -> None:
    proc = _run_cli(["--help"], env=os.environ.copy())
    assert proc.returncode == 0
    assert "Generate a deterministic AOI report bundle" in proc.stdout


def test_cli_golden_run_creates_bundle(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    env = os.environ.copy()
    env["EUDR_DMI_EVIDENCE_ROOT"] = str(evidence_root)

    bundle_id = "bundle-001"
    aoi_id = "aoi-123"

    policy_ref_file = tmp_path / "policy_refs.txt"
    policy_ref_file.write_text(
        "# comment line\npolicy-spine:eudr/article-3\nplaceholder:TODO\n\n",
        encoding="utf-8",
    )

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
            "b_metric=2:count:dummy_source:note b",
            "--metric",
            "a_metric=1:count:dummy_source:note a",
            "--policy-mapping-ref",
            "policy-spine:eudr/article-9",
            "--policy-mapping-ref-file",
            str(policy_ref_file),
        ],
        env=env,
    )

    assert proc.returncode == 0, proc.stderr

    bundle_date = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")
    bundle_dir = evidence_root / bundle_date / bundle_id

    assert bundle_dir.exists()

    report_json = bundle_dir / "reports" / "aoi_report_v1" / f"{aoi_id}.json"
    report_html = bundle_dir / "reports" / "aoi_report_v1" / f"{aoi_id}.html"
    metrics_csv = bundle_dir / "reports" / "aoi_report_v1" / aoi_id / "metrics.csv"
    manifest = bundle_dir / "manifest.json"
    geometry = bundle_dir / "inputs" / "aoi.wkt"

    assert report_json.exists()
    assert report_html.exists()
    assert metrics_csv.exists()
    assert manifest.exists()
    assert geometry.exists()

    # Contract validation.
    validate_aoi_report_v1_file(report_json)

    # metrics.csv header and stable row ordering.
    lines = metrics_csv.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "variable,value,unit,source,notes"
    assert lines[1].startswith("a_metric,")
    assert lines[2].startswith("b_metric,")

    # HTML links should be portable (no absolute paths, no schemes).
    html = report_html.read_text(encoding="utf-8")
    hrefs = re.findall(r'href="([^"]+)"', html)
    assert hrefs, "expected at least one link"
    for href in hrefs:
        assert not href.startswith("/")
        assert "://" not in href
        assert str(tmp_path) not in href

    # manifest.json includes metrics.csv
    manifest_obj = json.loads(manifest.read_text(encoding="utf-8"))
    relpaths = [a["relpath"] for a in manifest_obj.get("artifacts", [])]
    assert f"reports/aoi_report_v1/{aoi_id}/metrics.csv" in relpaths
