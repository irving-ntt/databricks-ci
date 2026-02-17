#!/usr/bin/env python3
# scripts/parse_sarif_fail_on_severity.py

import json, sys
from pathlib import Path

levels = {"LOW":0, "MEDIUM":1, "HIGH":2, "CRITICAL":3}

def main():
    if len(sys.argv) < 2:
        print("Usage: parse_sarif_fail_on_severity.py <sarif_file> [threshold]")
        sys.exit(2)

    sarif_path = Path(sys.argv[1])
    threshold = (sys.argv[2] if len(sys.argv) > 2 else "HIGH").upper()
    th = levels.get(threshold, 2)

    if not sarif_path.exists():
        print(f"SARIF not found: {sarif_path} (not failing)")
        sys.exit(0)

    data = json.loads(sarif_path.read_text(encoding="utf-8"))
    bad = []

    for run in data.get("runs", []):
        for r in run.get("results", []):
            props = r.get("properties", {}) or {}
            sev = (props.get("severity") or r.get("level") or "").upper()
            if sev in levels and levels[sev] >= th:
                bad.append((sev, r.get("ruleId",""), (r.get("message",{}) or {}).get("text","")))

    print(f"Findings >= {threshold}: {len(bad)}")
    for sev, rule, msg in bad[:30]:
        print(f"- {sev} {rule}: {msg[:200]}")

    sys.exit(3 if bad else 0)

if __name__ == "__main__":
    main()
