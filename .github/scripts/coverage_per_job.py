#!/usr/bin/env python3
"""
coverage_per_job.py (match tests by task_key -> tests/test_<task_key>.py)

Fixes:
 - Build basename map from coverage.xml keys so lookups by basename work.
 - Return real covered/total/pct from parsed coverage data.
 - Debug prints of parsed filenames to help CI debugging.

Usage:
 python .github/scripts/coverage_per_job.py --coverage-xml coverage.xml --jobs-dir jobs --output coverage_per_job.json
Requires: pyyaml
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

    # try <class> format first
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

    # fallback to <file> with <line hits="..."/>
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
    """Extract tasks (job_id, job_name, job_file, task_key, notebook_path) from YAML job files."""
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
                    nb_path = nt.get("notebook_path") or nt.get("notebook") or ""
                    out.append({
                        "job_id": job_id,
                        "job_name": job_name,
                        "job_file": str(yf),
                        "task_key": task_key,
                        "notebook_path": str(nb_path)
                    })
        else:
            tasks = doc.get("tasks") or []
            for task in tasks:
                task_key = task.get("task_key") or ""
                nt = task.get("notebook_task") or {}
                nb_path = nt.get("notebook_path") or nt.get("notebook") or ""
                out.append({
                    "job_id": Path(yf).stem,
                    "job_name": Path(yf).stem,
                    "job_file": str(yf),
                    "task_key": task_key,
                    "notebook_path": str(nb_path)
                })
    return out

def generate_test_candidates_for_task(task_key):
    """Return list of candidate basenames to try (lowercase)."""
    tk = str(task_key).strip()
    if not tk:
        return []
    tk = tk.lower().replace(" ", "_")
    variants = []
    variants.append(f"test_{tk}.py")
    variants.append(f"test_{tk.replace('_','-')}.py")
    variants.append(f"test_{tk.replace('-','_')}.py")
    variants.append(f"{tk}.py")  # fallback
    # unique preserve order
    seen = set()
    out = []
    for v in variants:
        lv = v.lower()
        if lv not in seen:
            seen.add(lv)
            out.append(lv)
    return out

def build_basename_map(files_map):
    """
    Build mapping: basename(lower) -> list of original keys found in coverage.xml
    """
    bmap = {}
    for k in files_map.keys():
        b = Path(k).name.lower()
        bmap.setdefault(b, []).append(k)
    return bmap

def find_best_match_for_task(task_key, files_map, basename_map):
    """
    According to your rule: only consider exact test filenames tests/test_<task_key>.py
    (with _ <-> - tolerance). If multiple matches, choose one with highest pct.
    If none, return (None,0,0,0.0).
    """
    candidates = generate_test_candidates_for_task(task_key)  # basenames
    best = None
    best_pct = -1.0
    best_info = (None, 0, 0, 0.0)
    for cand in candidates:
        if cand in basename_map:
            for orig_key in basename_map[cand]:
                info = files_map.get(orig_key)
                if not info:
                    # try normalized key variants
                    info = files_map.get(orig_key.replace("\\","/"))
                if info:
                    pct = info.get("pct", 0.0)
                    if pct > best_pct:
                        best_pct = pct
                        best = orig_key
                        best_info = (orig_key, info.get("covered",0), info.get("total",0), info.get("pct",0.0))
    return best_info  # (chosen_key, covered, total, pct) or (None,0,0,0.0)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-xml", required=True)
    parser.add_argument("--jobs-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    files_map = parse_coverage_xml(args.coverage_xml)
    print(f"DEBUG: parsed {len(files_map)} coverage files from {args.coverage_xml}")

    # debug: list some keys detected
    sample_keys = list(files_map.keys())[:50]
    print("DEBUG: coverage.xml sample file keys:")
    for k in sample_keys:
        print("  -", k)

    basename_map = build_basename_map(files_map)

    tasks = discover_tasks_from_jobs(args.jobs_dir)
    print(f"DEBUG: discovered {len(tasks)} task entries from jobs YAMLs")

    per_job = []
    for t in tasks:
        job_id = t["job_id"]
        job_name = t["job_name"]
        job_file = t["job_file"]
        task_key = t["task_key"]
        nb_path = t["notebook_path"]

        chosen_key, covered, total, pct = find_best_match_for_task(task_key, files_map, basename_map)

        matched_file = chosen_key if chosen_key else None
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
    # print summary
    for r in per_job:
        print(f"{r['task_key']}: matched={r['matched_coverage_file']}, pct={r['coverage_percent']}")

if __name__ == "__main__":
    main()