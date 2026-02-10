#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import posixpath
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class ArtifactEntry:
    relative_path: str
    url: str | None
    sha256: str | None
    bytes: int | None
    missing: bool = False


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _fetch_url(url: str, headers: dict[str, str] | None = None) -> tuple[int, bytes, dict[str, str]]:
    req = Request(url, headers=headers or {})
    try:
        with urlopen(req, timeout=30) as resp:
            status = getattr(resp, "status", 200)
            data = resp.read()
            resp_headers = {k: v for k, v in resp.headers.items()}
            return status, data, resp_headers
    except HTTPError as exc:
        if exc.code == 304:
            return 304, b"", {k: v for k, v in exc.headers.items()}
        raise


def _cache_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _fetch_with_cache(url: str, cache_dir: Path) -> tuple[Path, dict[str, Any]]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = _cache_key(url)
    data_path = cache_dir / f"{key}.bin"
    meta_path = cache_dir / f"{key}.json"
    headers: dict[str, str] = {}
    meta: dict[str, Any] = {}

    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        etag = meta.get("etag")
        last_modified = meta.get("last_modified")
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified

    status, data, resp_headers = _fetch_url(url, headers=headers)
    if status == 304:
        if not data_path.exists():
            raise RuntimeError(f"Cache miss for 304 response: {url}")
        return data_path, meta

    data_path.write_bytes(data)
    meta = {
        "url": url,
        "etag": resp_headers.get("ETag"),
        "last_modified": resp_headers.get("Last-Modified"),
        "retrieved_utc": _utcnow(),
        "sha256": _sha256_bytes(data),
        "bytes": len(data),
    }
    meta_path.write_text(json.dumps(meta, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return data_path, meta


class DeclaredArtifactsParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_h2 = False
        self._h2_text = ""
        self._declared_section = False
        self._in_list = False
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "h2":
            self._in_h2 = True
            self._h2_text = ""
            return
        if self._declared_section and tag == "ul":
            self._in_list = True
            return
        if self._declared_section and tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)

    def handle_endtag(self, tag: str) -> None:
        if tag == "h2":
            self._in_h2 = False
            header = self._h2_text.lower()
            if "declared evidence artifacts" in header or header.strip() == "artifacts":
                self._declared_section = True
            return
        if tag == "ul" and self._declared_section and self._in_list:
            self._in_list = False
            self._declared_section = False

    def handle_data(self, data: str) -> None:
        if self._in_h2:
            self._h2_text += data


def _parse_generated_utc(html: str) -> str:
    patterns = [
        r"Generated \(UTC\).*?<code>([^<]+)</code>",
        r"Generated \(UTC\).*?<td>([^<]+)</td>",
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
    raise RuntimeError("Generated (UTC) timestamp not found in report.html")


def _parse_declared_artifacts(html: str) -> list[str]:
    parser = DeclaredArtifactsParser()
    parser.feed(html)
    seen: set[str] = set()
    links: list[str] = []
    for link in parser.links:
        clean = link.strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        links.append(clean)
    if not links:
        raise RuntimeError("Declared evidence artifacts not found in report.html")
    return links


def _relative_path_for_url(report_url: str, href: str) -> tuple[str, str]:
    abs_url = urljoin(report_url, href)
    report_path = urlparse(report_url).path
    base_dir = posixpath.dirname(report_path)
    if not base_dir.endswith("/"):
        base_dir += "/"
    target_path = urlparse(abs_url).path
    rel = posixpath.relpath(target_path, base_dir)
    rel = rel.lstrip("./")
    return rel, abs_url


def _artifact_entries_from_urls(report_url: str, hrefs: list[str]) -> list[ArtifactEntry]:
    entries: list[ArtifactEntry] = []
    for href in hrefs:
        relpath, abs_url = _relative_path_for_url(report_url, href)
        entries.append(ArtifactEntry(relative_path=relpath, url=abs_url, sha256=None, bytes=None))
    return entries


def _build_published_manifest(
    *,
    report_url: str,
    cache_dir: Path,
    retrieved_utc: str,
) -> tuple[dict[str, Any], list[ArtifactEntry], str]:
    report_cache_path, _ = _fetch_with_cache(report_url, cache_dir)
    html = report_cache_path.read_text(encoding="utf-8", errors="replace")
    generated_utc = _parse_generated_utc(html)
    hrefs = _parse_declared_artifacts(html)
    entries = _artifact_entries_from_urls(report_url, hrefs)

    artifacts: list[ArtifactEntry] = []
    for entry in entries:
        if entry.url is None:
            continue
        artifact_path, meta = _fetch_with_cache(entry.url, cache_dir)
        artifacts.append(
            ArtifactEntry(
                relative_path=entry.relative_path,
                url=entry.url,
                sha256=meta.get("sha256"),
                bytes=meta.get("bytes"),
            )
        )

    manifest = {
        "bundle": "example",
        "aoi": "estonia_testland1",
        "generated_utc": generated_utc,
        "source": {
            "published_report_url": report_url,
            "retrieved_utc": retrieved_utc,
        },
        "artifacts": [
            {
                "relative_path": a.relative_path,
                "url": a.url,
                "sha256": a.sha256,
                "bytes": a.bytes,
            }
            for a in sorted(artifacts, key=lambda x: x.relative_path)
        ],
    }
    return manifest, artifacts, generated_utc


def _build_local_manifest(
    *,
    local_root: Path,
    declared: list[ArtifactEntry],
    generated_utc: str,
) -> tuple[dict[str, Any], list[ArtifactEntry]]:
    artifacts: list[ArtifactEntry] = []
    for entry in declared:
        rel = entry.relative_path
        local_path = local_root / rel
        if not local_path.is_file():
            artifacts.append(
                ArtifactEntry(
                    relative_path=rel,
                    url=None,
                    sha256=None,
                    bytes=None,
                    missing=True,
                )
            )
            continue
        sha256 = _sha256_file(local_path)
        artifacts.append(
            ArtifactEntry(
                relative_path=rel,
                url=None,
                sha256=sha256,
                bytes=local_path.stat().st_size,
            )
        )

    manifest = {
        "bundle": "example",
        "aoi": "estonia_testland1",
        "generated_utc": generated_utc,
        "source": {
            "local_run_root": str(local_root.as_posix()),
            "retrieved_utc": _utcnow(),
        },
        "artifacts": [
            {
                "relative_path": a.relative_path,
                "sha256": a.sha256,
                "bytes": a.bytes,
                "missing": a.missing,
            }
            for a in sorted(artifacts, key=lambda x: x.relative_path)
        ],
    }
    return manifest, artifacts


def _artifact_map(entries: list[ArtifactEntry]) -> dict[str, ArtifactEntry]:
    return {entry.relative_path: entry for entry in entries}


def _diff_published_local(
    published: list[ArtifactEntry],
    local: list[ArtifactEntry],
) -> dict[str, Any]:
    published_map = _artifact_map(published)
    local_map = _artifact_map(local)

    published_paths = set(published_map)
    local_paths = set(local_map)

    added = sorted(local_paths - published_paths)
    removed = sorted(published_paths - local_paths)

    hash_changed: list[dict[str, str]] = []
    missing_locally: list[str] = []
    for relpath in sorted(published_paths & local_paths):
        pub_entry = published_map[relpath]
        loc_entry = local_map[relpath]
        if loc_entry.missing:
            missing_locally.append(relpath)
            continue
        if pub_entry.sha256 and loc_entry.sha256 and pub_entry.sha256 != loc_entry.sha256:
            hash_changed.append(
                {
                    "relative_path": relpath,
                    "published_sha256": pub_entry.sha256,
                    "local_sha256": loc_entry.sha256,
                }
            )

    return {
        "added_path": added,
        "removed_path": removed,
        "hash_changed": hash_changed,
        "missing_locally": missing_locally,
    }


def _load_baseline(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _manifest_signature(manifest: dict[str, Any]) -> dict[str, str]:
    sig: dict[str, str] = {}
    for entry in manifest.get("artifacts", []):
        relpath = entry.get("relative_path")
        sha256 = entry.get("sha256")
        if relpath and sha256:
            sig[str(relpath)] = str(sha256)
    return sig


def _diff_signatures(a: dict[str, str], b: dict[str, str]) -> dict[str, Any]:
    added = sorted(set(a) - set(b))
    removed = sorted(set(b) - set(a))
    hash_changed = [
        {
            "relative_path": relpath,
            "published_sha256": a[relpath],
            "baseline_sha256": b[relpath],
        }
        for relpath in sorted(set(a) & set(b))
        if a[relpath] != b[relpath]
    ]
    return {
        "added_path": added,
        "removed_path": removed,
        "hash_changed": hash_changed,
    }


def _render_diff_md(summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Example bundle diff summary")
    lines.append("")
    lines.append(f"Bundle: {summary.get('bundle')}")
    lines.append(f"AOI: {summary.get('aoi')}")
    lines.append(f"Generated (UTC): {summary.get('generated_utc')}")
    lines.append(f"Published report: {summary.get('published_report_url')}")
    lines.append("")

    def _section(title: str, items: list[str]) -> None:
        lines.append(f"## {title}")
        if not items:
            lines.append("- (none)")
        else:
            for item in items:
                lines.append(f"- {item}")
        lines.append("")

    diff = summary.get("diff", {})
    _section("Added paths", diff.get("added_path", []))
    _section("Removed paths", diff.get("removed_path", []))

    hash_changed = diff.get("hash_changed", [])
    lines.append("## Hash changed")
    if not hash_changed:
        lines.append("- (none)")
    else:
        for entry in hash_changed:
            rel = entry.get("relative_path")
            pub = entry.get("published_sha256")
            loc = entry.get("local_sha256")
            base = entry.get("baseline_sha256")
            if loc:
                lines.append(f"- {rel}: published={pub} local={loc}")
            elif base:
                lines.append(f"- {rel}: published={pub} baseline={base}")
    lines.append("")

    _section("Missing locally", diff.get("missing_locally", []))
    return "\n".join(lines).rstrip() + "\n"


def _render_dte_patch(generated_utc: str, artifacts: list[str]) -> str:
    lines: list[str] = []
    lines.append("## Bundle Update Intake (auto-generated; paste into DTE setup when example artifacts change)")
    lines.append("")
    lines.append(f"Generated (UTC): {generated_utc}")
    lines.append("")
    lines.append("Declared evidence artifacts (relative paths):")
    for rel in artifacts:
        lines.append(f"- {rel}")
    lines.append("")
    lines.append(
        "Role refresh after bundle update: stable URLs may have mutable content; "
        "every inspection session must cite opened URLs; rerun baseline + diff after regeneration."
    )
    lines.append("")
    return "\n".join(lines)


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Detect published example AOI artifact changes.")
    parser.add_argument(
        "--local-run-root",
        default="out/site_bundle/aoi_reports/runs/example",
    )
    parser.add_argument(
        "--published-report-url",
        default=(
            "https://georgemadlis.github.io/eudr-dmi-gil-digital-twin/site/aoi_reports/"
            "runs/example/report.html"
        ),
    )
    parser.add_argument("--cache-dir", default=".cache/dt_example")
    parser.add_argument("--baseline-manifest", default="docs/baselines/dt_example_manifest.json")
    parser.add_argument("--instructions-file", default="docs/baselines/dte_gpt_instructions.txt")
    parser.add_argument("--out-dir", default="out/dte_update")
    parser.add_argument("--write-baseline", action="store_true")

    args = parser.parse_args(argv)

    local_root = Path(args.local_run_root)
    cache_dir = Path(args.cache_dir)
    baseline_path = Path(args.baseline_manifest)
    instructions_path = Path(args.instructions_file)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not instructions_path.is_file():
        print(f"ERROR: instructions file missing: {instructions_path}", file=sys.stderr)
        return 2
    instructions_sha256 = _sha256_file(instructions_path)

    retrieved_utc = _utcnow()
    try:
        published_manifest, published_entries, generated_utc = _build_published_manifest(
            report_url=str(args.published_report_url),
            cache_dir=cache_dir,
            retrieved_utc=retrieved_utc,
        )
        local_manifest, local_entries = _build_local_manifest(
            local_root=local_root,
            declared=published_entries,
            generated_utc=generated_utc,
        )
    except (RuntimeError, HTTPError, URLError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    _write_json(out_dir / "published_manifest.json", published_manifest)
    _write_json(out_dir / "local_manifest.json", local_manifest)

    diff = _diff_published_local(published_entries, local_entries)

    baseline_manifest = _load_baseline(baseline_path)
    published_sig = _manifest_signature(published_manifest)

    baseline_diff: dict[str, Any] | None = None
    changes_detected = False
    if baseline_manifest is not None:
        baseline_sig = _manifest_signature(baseline_manifest)
        baseline_diff = _diff_signatures(published_sig, baseline_sig)
        changes_detected = any(
            baseline_diff[key] for key in ("added_path", "removed_path", "hash_changed")
        )
    else:
        local_sig = _manifest_signature(local_manifest)
        baseline_diff = _diff_signatures(published_sig, local_sig)
        changes_detected = any(
            baseline_diff[key] for key in ("added_path", "removed_path", "hash_changed")
        ) or any(diff[key] for key in ("added_path", "removed_path", "hash_changed", "missing_locally"))
    summary = {
        "bundle": "example",
        "aoi": "estonia_testland1",
        "generated_utc": generated_utc,
        "published_report_url": str(args.published_report_url),
        "retrieved_utc": retrieved_utc,
        "dte_instructions": {
            "path": str(instructions_path.as_posix()),
            "sha256": instructions_sha256,
        },
        "changes_detected": changes_detected,
        "diff": diff,
        "baseline_diff": baseline_diff,
    }
    _write_json(out_dir / "diff_summary.json", summary)
    (out_dir / "diff_summary.md").write_text(_render_diff_md(summary), encoding="utf-8")

    if changes_detected:
        artifact_relpaths = [a.relative_path for a in sorted(published_entries, key=lambda x: x.relative_path)]
        (out_dir / "dte_setup_patch.md").write_text(
            _render_dte_patch(generated_utc, artifact_relpaths),
            encoding="utf-8",
        )

    if args.write_baseline:
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(baseline_path, local_manifest)

    return 3 if changes_detected else 0


if __name__ == "__main__":
    sys.exit(run())
