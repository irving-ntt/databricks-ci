#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path
import xml.etree.ElementTree as ET

def parse_coverage_xml(path: str):
    tree = ET.parse(path)
    root = tree.getroot()
    files = {}

    def count_lines(line_elems):
        total = 0
        covered = 0
        for line in line_elems:
            hits = line.get("hits") or line.get("count") or line.get("hit")
            total += 1
            try:
                if hits is not None and int(hits) > 0:
                    covered += 1
            except Exception:
                pass
        return covered, total

    for file_el in root.findall(".//file"):
        name = file_el.get("name") or file_el.get("filename")
        if not name:
            continue
        covered, total = count_lines(file_el.findall(".//line"))
        name = name.replace("\\", "/")
        files[name] = {
            "covered": covered,
            "total": total,
            "pct": round((covered / total) * 100, 2) if total > 0 else 0.0
        }

    return files

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coverage-xml", required=True)
    ap.add_argument("--notebooks-dir", default="notebooks")
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    cov = parse_coverage_xml(args.coverage_xml)

    # index por basename
    by_base = {}
    for k, v in cov.items():
        by_base.setdefault(Path(k).name.lower(), []).append((k, v))

    notebook_files = sorted(Path(args.notebooks_dir).rglob("*.py"))

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "notebook_file",
            "matched_coverage_file",
            "covered_lines",
            "total_lines",
            "coverage_percent"
        ])

        for nf in notebook_files:
            rel = nf.as_posix()
            base = nf.name.lower()

            match_key = None
            info = None

            # 1) match por ruta relativa
            for k, v in cov.items():
                if k.lower().endswith(rel.lower()):
                    match_key = k
                    info = v
                    break

            # 2) match por basename
            if not info and base in by_base:
                match_key, info = sorted(
                    by_base[base],
                    key=lambda kv: kv[1].get("total", 0),
                    reverse=True
                )[0]

            if info:
                writer.writerow([
                    rel,
                    match_key,
                    info["covered"],
                    info["total"],
                    info["pct"]
                ])
            else:
                # <-- regla solicitada: si no hay test, coverage = 0%
                writer.writerow([
                    rel,
                    None,
                    0,
                    0,
                    0.0
                ])

    print(f"CSV generado correctamente: {args.output}")

if __name__ == "__main__":
    main()