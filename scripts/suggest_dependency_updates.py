#!/usr/bin/env python3
"""Suggest dependency URL updates and record link history.

Deterministic output: stable ordering, optional timestamps.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 20
RETRIES = 1

HISTORY_COLUMNS = [
    "dependency_id",
    "dataset_family",
    "link_role",
    "url",
    "discovered_by",
    "discovery_method",
    "discovered_on_utc",
    "http_status",
    "observed_content_type",
    "ok",
    "score",
    "note",
]

DEPENDENCY_SOURCE_REQUIRED_COLUMNS = [
    "dependency_id",
    "url",
    "expected_content_type",
    "server_audit_path",
    "description",
    "family_or_tag",
    "used_by",
    "update_policy",
    "version_pattern",
    "last_verified_utc",
]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def ensure_repo_relative(path: Path, *, label: str) -> None:
    if path.is_absolute():
        raise ValueError(f"{label} must be repo-relative (no absolute paths): {path}")


def resolve_under_repo(rel: Path) -> Path:
    root = repo_root().resolve()
    resolved = (root / rel).resolve()
    if root not in resolved.parents and resolved != root:
        raise ValueError(f"Path escapes repo root: {rel} -> {resolved}")
    return resolved


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _request_with_retries(request: urllib.request.Request) -> urllib.response.addinfourl:
    last_exc: Exception | None = None
    for attempt in range(RETRIES + 1):
        try:
            response = urllib.request.urlopen(request, timeout=CONNECT_TIMEOUT)  # noqa: S310
            return response
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < RETRIES:
                time.sleep(0.2)
                continue
            raise
    raise RuntimeError("unreachable") from last_exc


def _read_limited(response: urllib.response.addinfourl) -> None:
    try:
        sock = response.fp.raw._sock  # type: ignore[attr-defined]
        sock.settimeout(READ_TIMEOUT)
    except Exception:
        pass
    response.read(1024)


def _try_head(url: str) -> tuple[int, str, dict[str, str]]:
    req = urllib.request.Request(url, method="HEAD")
    response = _request_with_retries(req)
    status = getattr(response, "status", 200)
    final_url = response.geturl()
    headers = {k.lower(): v for k, v in response.headers.items()}
    return status, final_url, headers


def _try_get_range(url: str) -> tuple[int, str, dict[str, str]]:
    req = urllib.request.Request(url, method="GET")
    req.add_header("Range", "bytes=0-1024")
    response = _request_with_retries(req)
    status = getattr(response, "status", 200)
    final_url = response.geturl()
    headers = {k.lower(): v for k, v in response.headers.items()}
    _read_limited(response)
    return status, final_url, headers


def _check_url(url: str) -> tuple[int | None, str, dict[str, str]]:
    try:
        status, final_url, headers = _try_head(url)
        if status in {405, 501}:
            return _try_get_range(url)
        return status, final_url, headers
    except urllib.error.HTTPError as exc:
        if exc.code in {405, 501}:
            return _try_get_range(url)
        return exc.code, exc.geturl(), {k.lower(): v for k, v in exc.headers.items()}
    except Exception:
        return None, url, {}


def _load_sources(sources_path: Path) -> list[dict[str, Any]]:
    data = json.loads(sources_path.read_text(encoding="utf-8"))
    sources = data.get("sources") or []
    normalized = []
    for item in sources:
        if not isinstance(item, dict):
            continue
        dep_id = item.get("id")
        url = item.get("url")
        if not isinstance(dep_id, str) or not isinstance(url, str):
            continue
        normalized.append(
            {
                "dependency_id": dep_id,
                "url": url,
                "expected_content_type": item.get("expected_content_type") or "",
                "server_audit_path": item.get("server_audit_path") or "",
            }
        )
    return sorted(normalized, key=lambda d: d["dependency_id"])


def _load_dependency_sources_csv(
    csv_path: Path,
) -> tuple[list[str], list[dict[str, str]], list[str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing dependency_sources.csv at {csv_path}")

    raw_lines = csv_path.read_text(encoding="utf-8").splitlines()
    comment_lines: list[str] = []
    data_lines: list[str] = []
    for line in raw_lines:
        if line.strip().startswith("#") and not data_lines:
            comment_lines.append(line)
            continue
        data_lines.append(line)

    if not data_lines:
        raise ValueError("dependency_sources.csv is empty")

    reader = csv.DictReader(data_lines)
    header = reader.fieldnames or []
    if not header:
        raise ValueError("dependency_sources.csv missing header")

    columns = list(header)
    for col in DEPENDENCY_SOURCE_REQUIRED_COLUMNS:
        if col not in columns:
            columns.append(col)

    rows = [dict(row) for row in reader]
    return columns, rows, comment_lines


def _write_dependency_sources_csv(
    csv_path: Path,
    *,
    columns: list[str],
    rows: list[dict[str, str]],
    comment_lines: list[str],
) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    rows_sorted = sorted(rows, key=lambda r: r.get("dependency_id", ""))
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        if comment_lines:
            fh.write("\n".join(comment_lines) + "\n")
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows_sorted:
            writer.writerow({col: row.get(col, "") for col in columns})


def _candidate_urls(url: str) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    year_match = re.search(r"(20\d{2})", url)
    if year_match:
        year = int(year_match.group(1))
        bumped = url.replace(str(year), str(year + 1), 1)
        if bumped != url:
            candidates.append((bumped, "year_bump_probe"))

    version_match = re.search(r"v(\d+)(?:\.(\d+))?", url)
    if version_match:
        major = int(version_match.group(1))
        minor = int(version_match.group(2) or 0)
        bumped_minor = url.replace(f"v{major}.{minor}", f"v{major}.{minor + 1}", 1)
        if bumped_minor != url:
            candidates.append((bumped_minor, "version_probe"))

    seen = set()
    uniq: list[tuple[str, str]] = []
    for cand, method in candidates:
        if cand in seen:
            continue
        seen.add(cand)
        uniq.append((cand, method))
    return uniq


def _score(ok: bool, status: int | None, content_type_match: bool) -> float:
    score = 0.0
    if ok:
        score += 1.0
    if content_type_match:
        score += 0.5
    if status is not None and 200 <= status < 300:
        score += 0.2
    return score


def _load_history(csv_path: Path) -> tuple[list[dict[str, str]], set[tuple[str, str, str, str]]]:
    if not csv_path.exists():
        return [], set()
    with csv_path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = [dict(row) for row in reader]
    keys = {
        (
            row.get("dependency_id", ""),
            row.get("url", ""),
            row.get("discovered_by", ""),
            row.get("discovery_method", ""),
        )
        for row in rows
    }
    return rows, keys


def _write_history(csv_path: Path, rows: list[dict[str, str]]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    rows_sorted = sorted(
        rows,
        key=lambda r: (
            r.get("dependency_id", ""),
            r.get("link_role", ""),
            r.get("url", ""),
            r.get("discovery_method", ""),
            r.get("discovered_on_utc", ""),
        ),
    )
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=HISTORY_COLUMNS)
        writer.writeheader()
        for row in rows_sorted:
            writer.writerow({col: row.get(col, "") for col in HISTORY_COLUMNS})


def _as_row(
    *,
    dependency_id: str,
    dataset_family: str,
    link_role: str,
    url: str,
    discovered_by: str,
    discovery_method: str,
    discovered_on_utc: str,
    http_status: int | None,
    observed_content_type: str,
    ok: bool,
    score: float,
    note: str,
) -> dict[str, str]:
    return {
        "dependency_id": dependency_id,
        "dataset_family": dataset_family,
        "link_role": link_role,
        "url": url,
        "discovered_by": discovered_by,
        "discovery_method": discovery_method,
        "discovered_on_utc": discovered_on_utc,
        "http_status": "" if http_status is None else str(http_status),
        "observed_content_type": observed_content_type,
        "ok": "true" if ok else "false",
        "score": str(score),
        "note": note,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Suggest dependency URL updates")
    parser.add_argument(
        "--sources-json",
        default="docs/dependencies/sources.json",
        help="Repo-relative sources.json (default: docs/dependencies/sources.json)",
    )
    parser.add_argument(
        "--out",
        default="out/dependency_update_suggestions.json",
        help="Repo-relative output JSON (default: out/dependency_update_suggestions.json)",
    )
    parser.add_argument(
        "--history-csv",
        default="data_db/dependency_link_history.csv",
        help="Repo-relative CSV for link history (default: data_db/dependency_link_history.csv)",
    )
    parser.add_argument(
        "--write-history",
        dest="write_history",
        action="store_true",
        default=True,
        help="Write dependency link history (default: true)",
    )
    parser.add_argument(
        "--no-write-history",
        dest="write_history",
        action="store_false",
        help="Disable history CSV write",
    )
    parser.add_argument("--no-timestamps", action="store_true", help="Omit discovered_on_utc")
    parser.add_argument(
        "--promote-best",
        action="store_true",
        help="Mark best candidate with higher score if different from current",
    )
    args = parser.parse_args(argv)

    try:
        sources_rel = Path(args.sources_json)
        out_rel = Path(args.out)
        history_rel = Path(args.history_csv)
        ensure_repo_relative(sources_rel, label="--sources-json")
        ensure_repo_relative(out_rel, label="--out")
        ensure_repo_relative(history_rel, label="--history-csv")
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    sources_path = resolve_under_repo(sources_rel)
    if not sources_path.exists():
        print(f"ERROR: sources JSON not found: {sources_path}", file=sys.stderr)
        return 2

    sources = _load_sources(sources_path)
    suggestions: list[dict[str, Any]] = []

    if args.write_history:
        history_path = resolve_under_repo(history_rel)
        history_rows, history_keys = _load_history(history_path)
    else:
        history_path = None
        history_rows, history_keys = [], set()

    promote_updates: dict[str, str] = {}
    promote_rows: list[dict[str, str]] = []
    sources_csv_path = resolve_under_repo(Path("data_db/dependency_sources.csv"))
    if args.promote_best:
        src_columns, src_rows, src_comments = _load_dependency_sources_csv(sources_csv_path)
        src_index = {row.get("dependency_id", ""): row for row in src_rows}

    for src in sources:
        dep_id = src["dependency_id"]
        current_url = src["url"]
        expected = src.get("expected_content_type", "")
        dataset_family = dep_id.split("/")[0] if dep_id else ""
        discovered_on = "" if args.no_timestamps else _now_utc_iso()

        status, final_url, headers = _check_url(current_url)
        observed_content_type = headers.get("content-type", "")
        content_type_match = bool(expected and observed_content_type.startswith(expected))
        ok = status is not None and 200 <= status < 400
        current_score = _score(ok, status, content_type_match)

        current_entry = {
            "url": current_url,
            "final_url": final_url,
            "http_status": status,
            "ok": ok,
            "observed_content_type": observed_content_type,
            "content_type_match": content_type_match,
            "score": current_score,
            "note": "current_url",
        }

        candidates: list[dict[str, Any]] = []
        best_candidate_url = None
        best_candidate_score = current_score
        best_candidate_entry: dict[str, Any] | None = None

        for candidate_url, method in _candidate_urls(current_url):
            c_status, c_final_url, c_headers = _check_url(candidate_url)
            c_observed = c_headers.get("content-type", "")
            c_match = bool(expected and c_observed.startswith(expected))
            c_ok = c_status is not None and 200 <= c_status < 400
            c_score = _score(c_ok, c_status, c_match)
            entry = {
                "url": candidate_url,
                "final_url": c_final_url,
                "http_status": c_status,
                "ok": c_ok,
                "observed_content_type": c_observed,
                "content_type_match": c_match,
                "score": c_score,
                "note": method,
            }
            candidates.append(entry)
            if c_score > best_candidate_score:
                best_candidate_score = c_score
                best_candidate_url = candidate_url
                best_candidate_entry = entry

            if args.write_history:
                row = _as_row(
                    dependency_id=dep_id,
                    dataset_family=dataset_family,
                    link_role="candidate",
                    url=candidate_url,
                    discovered_by="suggest_dependency_updates.py",
                    discovery_method=method,
                    discovered_on_utc=discovered_on,
                    http_status=c_status,
                    observed_content_type=c_observed,
                    ok=c_ok,
                    score=c_score,
                    note="candidate_probe",
                )
                key = (dep_id, candidate_url, row["discovered_by"], row["discovery_method"])
                if key not in history_keys:
                    history_keys.add(key)
                    history_rows.append(row)

        if args.write_history:
            row = _as_row(
                dependency_id=dep_id,
                dataset_family=dataset_family,
                link_role="current",
                url=current_url,
                discovered_by="suggest_dependency_updates.py",
                discovery_method="current_url",
                discovered_on_utc=discovered_on,
                http_status=status,
                observed_content_type=observed_content_type,
                ok=ok,
                score=current_score,
                note="current_url",
            )
            key = (dep_id, current_url, row["discovered_by"], row["discovery_method"])
            if key not in history_keys:
                history_keys.add(key)
                history_rows.append(row)

        if args.promote_best and best_candidate_url and best_candidate_url != current_url:
            if best_candidate_entry:
                best_candidate_entry = dict(best_candidate_entry)
                best_candidate_entry["score"] = best_candidate_entry.get("score", 0) + 1.0
            if args.write_history:
                row = _as_row(
                    dependency_id=dep_id,
                    dataset_family=dataset_family,
                    link_role="candidate",
                    url=best_candidate_url,
                    discovered_by="suggest_dependency_updates.py",
                    discovery_method="best_candidate",
                    discovered_on_utc=discovered_on,
                    http_status=best_candidate_entry.get("http_status") if best_candidate_entry else None,
                    observed_content_type=best_candidate_entry.get("observed_content_type", "") if best_candidate_entry else "",
                    ok=bool(best_candidate_entry.get("ok")) if best_candidate_entry else False,
                    score=float(best_candidate_entry.get("score", 0)) if best_candidate_entry else 0.0,
                    note="promoted_best",
                )
                key = (dep_id, best_candidate_url, row["discovered_by"], row["discovery_method"])
                if key not in history_keys:
                    history_keys.add(key)
                    history_rows.append(row)

            promote_updates[dep_id] = best_candidate_url
            promote_rows.append(
                _as_row(
                    dependency_id=dep_id,
                    dataset_family=dataset_family,
                    link_role="current",
                    url=best_candidate_url,
                    discovered_by="suggest_dependency_updates.py",
                    discovery_method="promoted",
                    discovered_on_utc=discovered_on,
                    http_status=best_candidate_entry.get("http_status") if best_candidate_entry else None,
                    observed_content_type=best_candidate_entry.get("observed_content_type", "") if best_candidate_entry else "",
                    ok=bool(best_candidate_entry.get("ok")) if best_candidate_entry else False,
                    score=float(best_candidate_entry.get("score", 0)) if best_candidate_entry else 0.0,
                    note="promoted",
                )
            )

        suggestions.append(
            {
                "dependency_id": dep_id,
                "current": current_entry,
                "candidates": sorted(candidates, key=lambda c: (c["url"], c.get("note", ""))),
                "best_candidate_url": best_candidate_url,
                "recommendation": "update" if best_candidate_url and best_candidate_url != current_url else "none",
            }
        )

    suggestions_sorted = sorted(suggestions, key=lambda s: s["dependency_id"])
    out_path = resolve_under_repo(Path(args.out))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_payload = {
        "schema_version": "1.0",
        "suggestions": suggestions_sorted,
    }
    out_path.write_text(json.dumps(out_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.write_history and history_path is not None:
        _write_history(history_path, history_rows)

    if args.promote_best:
        for dep_id, new_url in promote_updates.items():
            row = src_index.get(dep_id)
            if row is None:
                print(f"ERROR: dependency_id not found in dependency_sources.csv: {dep_id}", file=sys.stderr)
                return 2
            row["url"] = new_url
            if not args.no_timestamps:
                row["last_verified_utc"] = _now_utc_iso()
        _write_dependency_sources_csv(
            sources_csv_path,
            columns=src_columns,
            rows=src_rows,
            comment_lines=src_comments,
        )

        if args.write_history and history_path is not None:
            for row in promote_rows:
                key = (row["dependency_id"], row["url"], row["discovered_by"], row["discovery_method"])
                if key not in history_keys:
                    history_keys.add(key)
                    history_rows.append(row)
            _write_history(history_path, history_rows)

    print(f"OK: wrote {Path(args.out).as_posix()}")
    if args.write_history and history_path is not None:
        print(f"OK: updated {Path(args.history_csv).as_posix()}")
    if args.promote_best:
        print(f"OK: updated {Path('data_db/dependency_sources.csv').as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
