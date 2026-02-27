#!/usr/bin/env python3
import argparse
import xml.etree.ElementTree as ET
import json
from pathlib import Path

def parse_coverage_xml(path):
    """
    Parse coverage.xml produced by coverage.py.
    Return dict: {filename_in_xml: {"covered": int, "total": int}}
    Handles both <class> entries (some formats) and <file><line .../> formats.
    """
    tree = ET.parse(path)
    root = tree.getroot()
    files = {}

    # primary attempt: look for <class> entries (some coverage xml variants)
    for class_el in root.findall(".//class"):
        filename = class_el.get("filename")
        try:
            covered = int(class_el.get("covered") or 0)
            total = int(class_el.get("lines") or 0)
        except Exception:
            covered = 0
            total = 0
        if filename:
            files[filename] = {"covered": covered, "total": total}

    # fallback: parse <file> elements and count <line hits="N">
    if not files:
        for file_el in root.findall(".//file"):
            filename = file_el.get("name")
            covered = 0
            total = 0
            for line in file_el.findall(".//line"):
                total += 1
                hits = line.get("hits")
                # coverage.py uses hits attr; consider string/int
                try:
                    if hits is not None and int(hits) > 0:
                        covered += 1
                except Exception:
                    pass
            if filename:
                files[filename] = {"covered": covered, "total": total}

    return files

def calculate_per_job(files_map, jobs_dir):
    """
    For each .py under jobs_dir (recursive), try to find matching entry in files_map.
    Matching strategy (in order):
      1) exact endswith("jobs/<name>")
      2) basename match Path(candidate).name == job_file.name
      3) endswith(job_file.name)
      4) substring match (candidate contains job_file.name) -- fallback
    """
    results = []
    jobs = sorted(list(Path(jobs_dir).glob("**/*.py")))

    print(f"DEBUG: Found {len(jobs)} job files under '{jobs_dir}'")

    # prepare a list of candidate filenames from coverage.xml
    candidates = list(files_map.keys())
    print(f"DEBUG: Found {len(candidates)} entries in coverage.xml")

    for job_file in jobs:
        job_name = job_file.stem
        matched_file = None

        # matching heuristics
        for filename in candidates:
            # normalize slash direction for safe matching
            fn = filename.replace("\\", "/")
            if fn.endswith(f"jobs/{job_file.name}"):
                matched_file = filename
                break

        if not matched_file:
            # basename match
            for filename in candidates:
                if Path(filename).name == job_file.name:
                    matched_file = filename
                    break

        if not matched_file:
            for filename in candidates:
                if filename.replace("\\", "/").endswith(job_file.name):
                    matched_file = filename
                    break

        if not matched_file:
            # last resort: substring match (may produce false positives)
            for filename in candidates:
                if job_file.name in filename:
                    matched_file = filename
                    break

        if matched_file:
            cov = files_map.get(matched_file, {})
            covered = cov.get("covered", 0)
            total = cov.get("total", 0)
            coverage_percent = round((covered / total) * 100, 2) if total > 0 else 0.0
            match_info = matched_file
        else:
            covered = 0
            total = 0
            coverage_percent = 0.0
            match_info = None

        results.append({
            "job_name": job_name,
            "job_path": str(job_file),
            "matched_file": match_info,
            "covered_lines": covered,
            "total_lines": total,
            "coverage_percent": coverage_percent
        })

    # summary debug
    matched_count = sum(1 for r in results if r["matched_file"])
    print(f"DEBUG: Matched {matched_count}/{len(results)} job files to coverage entries")

    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-xml", required=True)
    parser.add_argument("--jobs-dir", required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    files_map = parse_coverage_xml(args.coverage_xml)
    results = calculate_per_job(files_map, args.jobs_dir)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump({"per_job": results}, f, indent=2, ensure_ascii=False)

    print("Coverage por Job generado correctamente.")
    # print a brief sample for logs
    for r in results[:20]:
        print(f"{r['job_name']}: {r['coverage_percent']}% (matched: {r['matched_file']})")

if __name__ == "__main__":
    main()