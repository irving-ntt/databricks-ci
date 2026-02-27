#!/usr/bin/env python3

import argparse
import xml.etree.ElementTree as ET
import json
from pathlib import Path


def parse_coverage_xml(path):
    tree = ET.parse(path)
    root = tree.getroot()
    files = {}

    # coverage.py formato estándar
    for class_el in root.findall(".//class"):
        filename = class_el.get("filename")
        covered = int(class_el.get("covered") or 0)
        total = int(class_el.get("lines") or 0)

        files[filename] = {
            "covered": covered,
            "total": total
        }

    return files


def calculate_per_job(files_map, jobs_dir):
    results = []
    jobs = list(Path(jobs_dir).glob("*.py"))

    for job_file in jobs:
        job_name = job_file.stem
        matched_file = None

        for filename in files_map:
            if filename.endswith(f"jobs/{job_file.name}") or filename.endswith(job_file.name):
                matched_file = filename
                break

        if matched_file:
            covered = files_map[matched_file]["covered"]
            total = files_map[matched_file]["total"]
            coverage_percent = round((covered / total) * 100, 2) if total > 0 else 0
        else:
            covered = 0
            total = 0
            coverage_percent = 0

        results.append({
            "job_name": job_name,
            "covered_lines": covered,
            "total_lines": total,
            "coverage_percent": coverage_percent
        })

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
        json.dump({"per_job": results}, f, indent=2)

    print("Coverage por Job generado correctamente.")


if __name__ == "__main__":
    main()