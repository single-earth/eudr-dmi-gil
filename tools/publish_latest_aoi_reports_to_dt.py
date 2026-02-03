#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class RunEntry:
    name: str
    src_dir: Path
    dt_dir: Path
    timestamp: datetime


_TIMESTAMP_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(\d{8}T\d{6}Z)"), "%Y%m%dT%H%M%SZ"),
    (re.compile(r"(\d{8}_\d{6})"), "%Y%m%d_%H%M%S"),
    (re.compile(r"(\d{8}-\d{6})"), "%Y%m%d-%H%M%S"),
    (re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)"), "%Y-%m-%dT%H:%M:%SZ"),
    (re.compile(r"(\d{4}-\d{2}-\d{2})"), "%Y-%m-%d"),
    (re.compile(r"(\d{8})"), "%Y%m%d"),
]


def _run(cmd: list[str], *, cwd: Path | None = None) -> str:
    result = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\n{result.stdout}\n{result.stderr}".strip()
        )
    return result.stdout.strip()


def _git_root(path: Path) -> Path:
    return Path(_run(["git", "-C", str(path), "rev-parse", "--show-toplevel"]))


def _ensure_clean_git(repo_root: Path, *, label: str) -> None:
    status = _run(["git", "-C", str(repo_root), "status", "--porcelain"])
    if status:
        raise RuntimeError(f"{label} repo has uncommitted changes. Commit or stash first.")


def _parse_timestamp_from_name(name: str) -> datetime | None:
    for pattern, fmt in _TIMESTAMP_PATTERNS:
        match = pattern.search(name)
        if not match:
            continue
        try:
            return datetime.strptime(match.group(1), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _parse_timestamp_from_manifest(run_dir: Path) -> datetime | None:
    candidates = [
        run_dir / "aoi_report.json",
        run_dir / "summary.json",
        run_dir / "manifest.json",
    ]
    for path in candidates:
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        for key in ("generated_at_utc", "generated_utc", "generated_at", "generated"):
            value = data.get(key)
            if isinstance(value, str) and value:
                value = value.replace("Z", "+00:00")
                try:
                    dt = datetime.fromisoformat(value)
                except ValueError:
                    continue
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
    return None


def _timestamp_for_run(run_dir: Path) -> datetime:
    ts = _parse_timestamp_from_name(run_dir.name)
    if ts is not None:
        return ts

    try:
        mtime = run_dir.stat().st_mtime
        return datetime.fromtimestamp(mtime, tz=timezone.utc)
    except OSError:
        pass

    ts = _parse_timestamp_from_manifest(run_dir)
    if ts is not None:
        return ts

    return datetime.fromtimestamp(0, tz=timezone.utc)


def _iter_runs(source_dir: Path) -> Iterable[Path]:
    runs_root = source_dir / "runs"
    if runs_root.is_dir():
        yield from sorted([p for p in runs_root.iterdir() if p.is_dir()], key=lambda p: p.name)
        return

    for p in sorted(source_dir.iterdir(), key=lambda p: p.name):
        if p.is_dir():
            yield p


def _copy_tree(src: Path, dest: Path) -> None:
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest)


def _clear_dir(path: Path) -> None:
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        return
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def _render_index(entries: list[RunEntry]) -> str:
    if entries:
        items = []
        for entry in entries:
            run_id = entry.name
            report_href = f"runs/{run_id}/report.html"
            summary_href = None
            if (entry.dt_dir / "summary.json").is_file():
                summary_href = f"runs/{run_id}/summary.json"
            elif (entry.dt_dir / "aoi_report.json").is_file():
                summary_href = f"runs/{run_id}/aoi_report.json"

            if summary_href:
                line = (
                    f'<li><a href="{report_href}">{run_id}</a> '
                    f'<span class="muted">(</span><a href="{summary_href}">'
                    f'{Path(summary_href).name}</a><span class="muted">)</span></li>'
                )
            else:
                line = f'<li><a href="{report_href}">{run_id}</a></li>'
            items.append(line)
        items_html = "\n".join(items)
    else:
        items_html = '<li class="muted">(none)</li>'

    return """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>AOI Reports</title>
    <style>
      :root { --fg:#111; --bg:#fff; --muted:#666; --card:#f6f7f9; --link:#0b5fff; }
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
             color: var(--fg); background: var(--bg); margin: 0; }
      header { border-bottom: 1px solid #e7e7e7; background: #fff; position: sticky; top: 0; }
      .wrap { max-width: 980px; margin: 0 auto; padding: 16px 20px; }
      nav a { margin-right: 14px; text-decoration: none; color: var(--link); font-weight: 600; }
      nav a.active { color: var(--fg); }
      main { padding: 18px 20px 40px; }
      h1 { margin: 0 0 6px; font-size: 22px; }
      h2 { margin-top: 24px; font-size: 18px; }
      p { line-height: 1.5; }
      .muted { color: var(--muted); }
      .card { background: var(--card); border: 1px solid #e8eaee; border-radius: 12px; padding: 14px 14px; }
      ul { padding-left: 18px; }
      code { background: #f1f1f1; padding: 1px 4px; border-radius: 6px; }
      .grid { display: grid; grid-template-columns: 1fr; gap: 12px; }
      @media (min-width: 760px) { .grid { grid-template-columns: 1fr 1fr; } }
    </style>
  </head>
  <body>
    <header>
      <div class="wrap">
        <nav>
          <a href="../index.html">Home</a>
          <a href="../articles/index.html">Articles</a>
          <a href="../dependencies/index.html">Dependencies</a>
          <a href="../../regulation/links.html">Regulation</a>
          <a href="../../regulation/policy_to_evidence_spine.md">Spine</a>
          <a href="../aoi_reports/index.html" class="active">AOI Reports</a>
          <a href="../dao_stakeholders/index.html">DAO (Stakeholders)</a>
          <a href="../dao_dev/index.html">DAO (Developers)</a>
        </nav>
      </div>
    </header>
    <main>
      <div class="wrap">
<h1>AOI Reports</h1>
<p class="muted">Portable mode: links point into the bundle under <code>runs/&lt;run_id&gt;/report.html</code>.</p>
<div class="card">
  <h2>Runs (newest first)</h2>
  <ul>
""" + items_html + """
  </ul>
</div>
      </div>
    </main>
    <footer style="border-top:1px solid #e7e7e7; background:#fff;">
      <div style="max-width:980px; margin:0 auto; padding:18px 20px 28px; color:#666; font-size:13px;">
        <a href="../privacy.html" style="color:#0b5fff; text-decoration:none; font-weight:600;">Privacy Policy</a>
      </div>
    </footer>
  </body>
</html>
"""


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not content.endswith("\n"):
        content += "\n"
    path.write_text(content, encoding="utf-8", newline="\n")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish only the latest AOI report runs into the Digital Twin repo, "
            "rebuilding docs/site/aoi_reports/index.html."
        )
    )
    parser.add_argument("--dt-repo", required=True, type=Path, help="Digital Twin repo path")
    parser.add_argument(
        "--dt-aoi-dir",
        required=True,
        type=Path,
        help="AOI reports folder inside the DT repo (e.g. docs/site/aoi_reports)",
    )
    parser.add_argument(
        "--source-dir",
        default="out/site_bundle/aoi_reports",
        type=Path,
        help="Source AOI reports folder in this repo (default: out/site_bundle/aoi_reports)",
    )
    parser.add_argument("--keep", type=int, default=2, help="How many latest runs to keep")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    script_root = Path(__file__).resolve().parent
    repo_root = _git_root(script_root)
    dt_repo_root = _git_root(args.dt_repo)

    _ensure_clean_git(repo_root, label="Authoritative")
    _ensure_clean_git(dt_repo_root, label="Digital Twin")

    source_dir = args.source_dir
    if not source_dir.is_absolute():
        source_dir = (repo_root / source_dir).resolve()
    if not source_dir.is_dir():
        raise RuntimeError(f"Source directory not found: {source_dir}")

    dt_aoi_dir = args.dt_aoi_dir
    if not dt_aoi_dir.is_absolute():
        dt_aoi_dir = (dt_repo_root / dt_aoi_dir).resolve()

    run_dirs = list(_iter_runs(source_dir))
    if not run_dirs:
        raise RuntimeError(f"No run directories found under: {source_dir}")

    runs: list[RunEntry] = []
    for run_dir in run_dirs:
        timestamp = _timestamp_for_run(run_dir)
        dt_run_dir = dt_aoi_dir / "runs" / run_dir.name
        runs.append(RunEntry(name=run_dir.name, src_dir=run_dir, dt_dir=dt_run_dir, timestamp=timestamp))

    runs_sorted = sorted(runs, key=lambda r: (r.timestamp, r.name), reverse=True)
    keep = args.keep
    if keep <= 0:
        raise RuntimeError("--keep must be >= 1")

    selected = runs_sorted[:keep]

    _clear_dir(dt_aoi_dir)

    for item in source_dir.iterdir():
        if item.name in {"runs", "index.html"}:
            continue
        dest = dt_aoi_dir / item.name
        if item.is_dir():
            _copy_tree(item, dest)
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, dest)

    dt_runs_root = dt_aoi_dir / "runs"
    dt_runs_root.mkdir(parents=True, exist_ok=True)
    for entry in selected:
        _copy_tree(entry.src_dir, entry.dt_dir)

    index_html = _render_index(selected)
    _write_text(dt_aoi_dir / "index.html", index_html)

    status_after = _run(["git", "-C", str(dt_repo_root), "status", "--porcelain"])
    if not status_after:
        print("No changes to publish.")
        return 0

    _run(["git", "-C", str(dt_repo_root), "add", "-A"])
    _run(
        [
            "git",
            "-C",
            str(dt_repo_root),
            "commit",
            "-m",
            f"Publish latest {keep} AOI reports (from eudr-dmi-gil)",
        ]
    )
    _run(["git", "-C", str(dt_repo_root), "push", "origin", "main"])

    print("Publish complete.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
