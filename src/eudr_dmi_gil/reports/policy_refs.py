from __future__ import annotations

from pathlib import Path


def collect_policy_mapping_refs(
    refs: list[str] | None = None,
    ref_files: list[str] | None = None,
) -> list[str]:
    """Collect policy-to-evidence spine references.

    This is intentionally lightweight and non-interpreting:
    - accepts explicit reference strings
    - accepts file paths containing newline-separated references

    It does NOT:
    - interpret regulation text
    - infer compliance
    - validate that a reference exists in any external system

    Empty lines and comment lines (starting with '#') in ref files are ignored.
    Returned refs are de-duplicated and stably sorted.
    """

    out: set[str] = set()

    for r in refs or []:
        r = r.strip()
        if r:
            out.add(r)

    for fp in ref_files or []:
        path = Path(fp)
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            out.add(line)

    return sorted(out)
