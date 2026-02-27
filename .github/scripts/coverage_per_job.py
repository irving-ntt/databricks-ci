#!/usr/bin/env python3
"""
coverage_per_job.py (auto-discover notebook_path in Job YAMLs)

Usage:
 python .github/scripts/coverage_per_job.py --coverage-xml coverage.xml --jobs-dir jobs --output coverage_per_job.json

Requiere: pyyaml (pip install pyyaml)
"""

import argparse
import xml.etree.ElementTree as ET
import json
from pathlib import Path
import yaml
import re
import sys

def parse_coverage_xml(path):
    """Parse coverage.xml -> dict {filename_in_xml: {'covered':int,'total':int}}"""
    tree = ET.parse(path)
    root = tree.getroot()
    files = {}

    # intento 1: formato <class>
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

    # fallback: formato <file> con <line hits="N" />
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
                files[filename] = {"covered": covered, "total": total}

    return files

def discover_notebook_paths_from_jobs(jobs_dir):
    """
    Busca archivos YAML en jobs_dir y extrae notebook_path de cada task:
      resources:
        jobs:
          JOB_ID:
            tasks:
              - task_key: ...
                notebook_task:
                  notebook_path: /notebooks/Whatever
    Retorna lista de dicts: {'job_id','job_name','job_file','task_key','notebook_path'}
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

        # path in example: resources.jobs.<JOB_ID>
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
                    if nb_path:
                        out.append({
                            "job_id": job_id,
                            "job_name": job_name,
                            "job_file": str(yf),
                            "task_key": task_key,
                            "notebook_path": str(nb_path)
                        })
        else:
            # fallback: try top-level 'tasks' list as alternative structure
            tasks = doc.get("tasks") or []
            for task in tasks:
                task_key = task.get("task_key") or ""
                nt = task.get("notebook_task") or {}
                nb_path = nt.get("notebook_path") or nt.get("notebook") or None
                if nb_path:
                    out.append({
                        "job_id": Path(yf).stem,
                        "job_name": Path(yf).stem,
                        "job_file": str(yf),
                        "task_key": task_key,
                        "notebook_path": str(nb_path)
                    })
    return out

def normalize_name(s):
    """Normalize a path or name for matching: lowercase, replace non-alnum by underscore"""
    s = s.lower()
    s = re.sub(r"\\", "/", s)
    # take last path segment
    base = s.split("/")[-1]
    # remove extension if present
    if base.endswith(".py"):
        base = base[:-3]
    # replace non-word by underscore
    base = re.sub(r"[^\w]", "_", base)
    return base

def match_notebook_to_coverage(nb_path, files_map):
    """
    Given a notebook_path (e.g. /notebooks/Ingest_circuits or notebooks/ingest_circuits),
    return best match from files_map (filename keys) by heuristics:
     - exact endswith with .py
     - basename match
     - normalized name match
     - substring match (lowercase)
    Returns (chosen_candidate, covered, total, percent) or (None,0,0,0.0)
    """
    candidates = list(files_map.keys())
    nb = str(nb_path)
    nb_base = Path(nb).name  # e.g. Ingest_circuits
    nb_with_py = nb_base if nb_base.endswith(".py") else nb_base + ".py"

    # 1) exact endswith
    for c in candidates:
        if c.lower().endswith("/" + nb_with_py.lower()) or c.lower().endswith(nb_with_py.lower()):
            info = files_map[c]
            total = info.get("total",0); covered = info.get("covered",0)
            pct = round((covered/total)*100,2) if total>0 else 0.0
            return c, covered, total, pct

    # 2) basename exact
    for c in candidates:
        if Path(c).name.lower() == nb_with_py.lower():
            info = files_map[c]
            total = info.get("total",0); covered = info.get("covered",0)
            pct = round((covered/total)*100,2) if total>0 else 0.0
            return c, covered, total, pct

    # 3) normalized name match
    nb_norm = normalize_name(nb)
    for c in candidates:
        if normalize_name(Path(c).name) == nb_norm:
            info = files_map[c]
            total = info.get("total",0); covered = info.get("covered",0)
            pct = round((covered/total)*100,2) if total>0 else 0.0
            return c, covered, total, pct

    # 4) substring match
    for c in candidates:
        if nb_norm in normalize_name(c):
            info = files_map[c]
            total = info.get("total",0); covered = info.get("covered",0)
            pct = round((covered/total)*100,2) if total>0 else 0.0
            return c, covered, total, pct

    return None, 0, 0, 0.0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-xml", required=True)
    parser.add_argument("--jobs-dir", required=True)   # path to YAML job files
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    files_map = parse_coverage_xml(args.coverage_xml)
    print(f"DEBUG: parsed {len(files_map)} coverage files from {args.coverage_xml}")

    tasks = discover_notebook_paths_from_jobs(args.jobs_dir)
    print(f"DEBUG: discovered {len(tasks)} notebook tasks from jobs YAMLs")

    per_job = []
    for t in tasks:
        job_id = t["job_id"]
        job_name = t["job_name"]
        job_file = t["job_file"]
        task_key = t["task_key"]
        nb_path = t["notebook_path"]

        chosen, covered, total, pct = match_notebook_to_coverage(nb_path, files_map)
        per_job.append({
            "job_id": job_id,
            "job_name": job_name,
            "job_file": job_file,
            "task_key": task_key,
            "notebook_path": nb_path,
            "matched_coverage_file": chosen,
            "covered_lines": covered,
            "total_lines": total,
            "coverage_percent": pct
        })

    # write output
    out = {"per_job": per_job}
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=2, ensure_ascii=False)

    print(f"WROTE {len(per_job)} entries to {args.output}")

if __name__ == "__main__":
    main()