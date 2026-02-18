#!/usr/bin/env python3
# scripts/parse_sarif_fail_on_severity.py

import json
import sys
from pathlib import Path

levels = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


def iter_sarif_files(path: Path):
    """Yield SARIF files from a file path or directory path."""
    if path.is_file():
        yield path
    elif path.is_dir():
        # Recursively find SARIF files
        yield from sorted(path.rglob("*.sarif"))


def parse_sarif_file(sarif_file: Path, th: int):
    """Return list of findings >= threshold from one SARIF file."""
    bad = []
    try:
        data = json.loads(sarif_file.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARNING: No se pudo parsear SARIF: {sarif_file} ({e})")
        return bad

    for run in data.get("runs", []):
        for r in run.get("results", []):
            props = r.get("properties", {}) or {}

            # Preferimos properties.severity; fallback a r.level
            sev = (props.get("severity") or r.get("level") or "").upper()
            if sev in levels and levels[sev] >= th:
                bad.append(
                    (
                        sev,
                        r.get("ruleId", ""),
                        (r.get("message", {}) or {}).get("text", ""),
                        str(sarif_file),
                    )
                )
    return bad


def main():
    if len(sys.argv) < 2:
        print("Usage: parse_sarif_fail_on_severity.py <sarif_file_or_dir> [threshold]")
        sys.exit(2)

    sarif_path = Path(sys.argv[1])
    threshold = (sys.argv[2] if len(sys.argv) > 2 else "HIGH").upper()
    th = levels.get(threshold, 2)

    if not sarif_path.exists():
        print(f"SARIF not found: {sarif_path} (not failing)")
        sys.exit(0)

    sarif_files = list(iter_sarif_files(sarif_path))
    if not sarif_files:
        print(f"No SARIF files found at: {sarif_path} (not failing)")
        sys.exit(0)

    bad = []
    for f in sarif_files:
        bad.extend(parse_sarif_file(f, th))

    print(f"SARIF files scanned: {len(sarif_files)}")
    print(f"Findings >= {threshold}: {len(bad)}")

    for sev, rule, msg, src in bad[:30]:
        print(f"- {sev} {rule} ({src}): {msg[:200]}")

    sys.exit(3 if bad else 0)


if __name__ == "__main__":
    main()
