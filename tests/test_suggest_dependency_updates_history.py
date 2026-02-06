from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_script_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    def __init__(self, *, status: int, url: str, headers: dict[str, str]) -> None:
        self.status = status
        self._url = url
        self.headers = headers
        self.fp = SimpleNamespace(raw=SimpleNamespace(_sock=SimpleNamespace(settimeout=lambda _t: None)))

    def geturl(self) -> str:
        return self._url

    def read(self, _size: int = -1) -> bytes:
        return b""


def test_suggest_dependency_updates_history(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script_repo = Path(__file__).resolve().parents[1]
    suggest_updates = _load_script_module(
        script_repo / "scripts" / "suggest_dependency_updates.py",
        "suggest_dependency_updates",
    )

    repo_root = tmp_path
    docs = repo_root / "docs" / "dependencies"
    docs.mkdir(parents=True, exist_ok=True)
    sources_path = docs / "sources.json"
    sources_path.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "server_audit_root": "/audit",
                "sources": [
                    {
                        "id": "hansen_gfc_definitions",
                        "url": "https://example.org/GFC-2024-v1.12/download.html",
                        "expected_content_type": "text/html",
                        "server_audit_path": "/audit/hansen",
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    sources_csv = repo_root / "data_db" / "dependency_sources.csv"
    sources_csv.parent.mkdir(parents=True, exist_ok=True)
    sources_csv.write_text(
        "dependency_id,url,expected_content_type,server_audit_path,description,family_or_tag,used_by,update_policy,version_pattern,last_verified_utc\n"
        "hansen_gfc_definitions,https://example.org/GFC-2024-v1.12/download.html,text/html,/audit/hansen,desc,forest,docs/dependencies/hansen_gfc.yaml,probe_yearly,GFC-YYYY-v1.12,\n",
        encoding="utf-8",
    )

    def fake_urlopen(request, timeout=0):  # noqa: ANN001
        url = request.full_url
        if "2025" in url:
            return FakeResponse(status=200, url=url, headers={"Content-Type": "text/html"})
        return FakeResponse(status=404, url=url, headers={"Content-Type": "text/html"})

    monkeypatch.setattr(suggest_updates, "repo_root", lambda: repo_root)
    monkeypatch.setattr(suggest_updates.urllib.request, "urlopen", fake_urlopen)

    history_csv = repo_root / "data_db" / "dependency_link_history.csv"
    history_csv.parent.mkdir(parents=True, exist_ok=True)
    history_csv.write_text(
        "dependency_id,dataset_family,link_role,url,discovered_by,discovery_method,discovered_on_utc,http_status,observed_content_type,ok,score,note\n",
        encoding="utf-8",
    )

    args = [
        "--sources-json",
        "docs/dependencies/sources.json",
        "--out",
        "out/dependency_update_suggestions.json",
        "--history-csv",
        "data_db/dependency_link_history.csv",
        "--no-timestamps",
        "--write-history",
        "--promote-best",
    ]

    assert suggest_updates.main(args) == 0
    assert suggest_updates.main(args) == 0

    rows = list(csv.DictReader(history_csv.open("r", encoding="utf-8")))
    roles = sorted({row["link_role"] for row in rows})
    assert roles == ["candidate", "current"]

    # Ensure no duplicates (same key) after double run
    keys = {
        (
            row["dependency_id"],
            row["url"],
            row["discovered_by"],
            row["discovery_method"],
        )
        for row in rows
    }
    assert len(keys) == len(rows)

    updated = list(csv.DictReader(sources_csv.open("r", encoding="utf-8")))
    assert updated[0]["url"].endswith("2025-v1.12/download.html")

    promoted_rows = [
        row
        for row in rows
        if row["discovery_method"] == "promoted" and row["link_role"] == "current"
    ]
    assert len(promoted_rows) == 1
