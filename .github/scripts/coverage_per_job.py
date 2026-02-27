#!/usr/bin/env python3
"""
coverage_per_job.py (match tests by task_key -> tests/test_<task_key>.py)

Behavior:
 - Extract notebook tasks from jobs YAMLs (notebook_path used only for reporting)
 - Parse coverage.xml (handles <file> and <class> formats)
 - For each task_key, look ONLY for tests with exact filenames:
       tests/test_<task_key>.py
   with tolerance for case and '-' vs '_' differences.
 - If several candidate basenames match, choose the candidate with HIGHEST coverage percent.
 - If none found, assign coverage_percent = 0.0 and matched_coverage_file = None.

Usage:
 python .github/scripts/coverage_per_job.py --coverage-xml coverage.xml --jobs-dir jobs --output coverage_per_job.json
Requires: pyyaml (install in CI)
"""

import argparse
import xml.etree.ElementTree as ET
import json
from pathlib import Path
import yaml
import re
import sys

def parse_coverage_xml(path):
    """Return dict filename_in_xml -> {'covered':int,'total':int,'pct':float}"""
    tree = ET.parse(path)
    root = tree.getroot()
    files = {}

    # try <class> entries first
    for class_el in root.findall(".//class"):
        filename = class_el.get("filename")
        try:
            covered = int(class_el.get("covered") or 0)
            total = int(class_el.get("lines") or 0)
        except Exception:
            covered = 0
            total = 0
        if filename:
            pct = round((covered / total) * 100, 2) if total > 0 else 0.0
            files[filename] = {"covered": covered, "total": total, "pct": pct}

    # fallback to <file>/<line>
    if not files:
        for file_el in root.findall(".//file"):
            filename = file_el.get("name")
            covered = 0
            total = 0
            for line in file_el.findall(".//line"):
                total += 1
                hits = line.get("hits")
                try:
                    if hits is not None and int(hits) > 0:
                        covered += 1
                except Exception:
                    pass
            if filename:
                pct = round((covered / total) * 100, 2) if total > 0 else 0.0
                files[filename] = {"covered": covered, "total": total, "pct": pct}

    return files

def discover_tasks_from_jobs(jobs_dir):
    """
    Find YAMLs under jobs_dir and extract tasks with notebook_task.notebook_path and task_key.
    Returns list of dicts: {job_id, job_name, job_file, task_key, notebook_path}
    """
    out = []
    p = Path(jobs_dir)
    if not p.exists():
        print(f"ERROR: jobs_dir '{jobs_dir}' does not exist.", file=sys.stderr)
        return out

    yaml_files = sorted(list(p.rglob("*.yml")) + list(p.rglob("*.yaml")))
    for yf in yaml_files:
        try:
            with open(yf, "r", encoding="utf-8") as fh:
                doc = yaml.safe_load(fh) or {}
        except Exception as e:
            print(f"WARN: cannot parse YAML {yf}: {e}", file=sys.stderr)
            continue

        resources = doc.get("resources") or {}
        jobs_section = resources.get("jobs") or {}
        if isinstance(jobs_section, dict) and jobs_section:
            for job_id, job_data in jobs_section.items():
                job_name = job_data.get("name") or job_id
                tasks = job_data.get("tasks") or []
                for task in tasks:
                    task_key = task.get("task_key") or ""
                    nt = task.get("notebook_task") or {}
                    nb_path = nt.get("notebook_path") or nt.get("notebook") or None
                    out.append({
                        "job_id": job_id,
                        "job_name": job_name,
                        "job_file": str(yf),
                        "task_key": task_key,
                        "notebook_path": str(nb_path) if nb_path else ""
                    })
        else:
            # fallback: top-level tasks
            tasks = doc.get("tasks") or []
            for task in tasks:
                task_key = task.get("task_key") or ""
                nt = task.get("notebook_task") or {}
                nb_path = nt.get("notebook_path") or nt.get("notebook") or None
                out.append({
                    "job_id": Path(yf).stem,
                    "job_name": Path(yf).stem,
                    "job_file": str(yf),
                    "task_key": task_key,
                    "notebook_path": str(nb_path) if nb_path else ""
                })
    return out

def generate_test_candidates_for_task(task_key):
    """
    Generate candidate test basenames for a given task_key.
    Examples:
      task_key: ingest_circuits
      candidates: test_ingest_circuits.py
      Also tolerate hyphen/underscore and case differences.
    Returns list of candidate basenames (lowercase).
    """
    tk = str(task_key).strip()
    if not tk:
        return []
    # normalize: lowercase, remove spaces
    tk = tk.lower().replace(" ", "_")
    # variants
    variants = set()
    base = f"test_{tk}.py"
    variants.add(base)
    # swap _ <-> - variants
    variants.add(f"test_{tk.replace('_','-')}.py")
    variants.add(f"test_{tk.replace('-','_')}.py")
    # also add without 'test_' if someone named tests differently (less likely)
    variants.add(f"{tk}.py")
    variants.add(f"{tk}.lower()")
    # return lowercased unique list
    return [v.lower() for v in variants]

def find_best_candidate_from_files(candidates, files_map):
    """
    Given candidate basenames (lowercase) and files_map keys,
    return best_match_filename (original key from files_map) with highest pct, or None.
    """
    best = None
    best_pct = -1.0
    for cand in candidates:
        for fm in files_map.keys():
            if Path(fm).name.lower() == cand:
                info = files_map.get(fm) or {}
                pct = info.get("pct", 0.0)
                if pct > best_pct:
                    best_pct = pct
                    best = fm
    return best, best_pct

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-xml", required=True)
    parser.add_argument("--jobs-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    files_map = parse_coverage_xml(args.coverage_xml)
    print(f"DEBUG: parsed {len(files_map)} coverage files from {args.coverage_xml}")

    tasks = discover_tasks_from_jobs(args.jobs_dir)
    print(f"DEBUG: discovered {len(tasks)} task entries from jobs YAMLs")

    per_job = []
    for t in tasks:
        job_id = t["job_id"]
        job_name = t["job_name"]
        job_file = t["job_file"]
        task_key = t["task_key"]
        nb_path = t["notebook_path"]

        candidates = generate_test_candidates_for_task(task_key)
        matched, matched_pct = find_best_candidate_from_files(candidates, files_map)

        if matched:
            info = files_map.get(matched)
            covered = info.get("covered", 0)
            total = info.get("total", 0)
            pct = info.get("pct", 0.0)
            matched_file = matched
        else:
            covered = 0
            total = 0
            pct = 0.0
            matched_file = None

        per_job.append({
            "job_id": job_id,
            "job_name": job_name,
            "job_file": job_file,
            "task_key": task_key,
            "notebook_path": nb_path,
            "matched_coverage_file": matched_file,
            "covered_lines": covered,
            "total_lines": total,
            "coverage_percent": pct
        })

    out = {"per_job": per_job}
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, ensure_ascii=False)

    print(f"WROTE {len(per_job)} entries to {args.output}")

if __name__ == "__main__":
    main()