#!/usr/bin/env python3
"""
coverage_per_job.py (agrupa tasks por job)

Salida:
{
  "per_job": [
    {
      "job_id": "WF_ADB",
      "job_name": "WF_ADB",
      "job_file": "jobs/WF_ADB.yml",
      "tasks": [
        { "task_key": "...", "notebook_path": "...", "matched_coverage_file": "...",
          "covered_lines": N, "total_lines": M, "coverage_percent": P },
        ...
      ]
    },
    ...
  ]
}

Uso:
 python .github/scripts/coverage_per_job.py --coverage-xml coverage.xml --jobs-dir jobs --output coverage_per_job.json

Requerimientos: pyyaml
"""

import argparse
import xml.etree.ElementTree as ET
import json
from pathlib import Path
import yaml
import sys
import re

# ---------- parser robusto de coverage.xml ----------
def strip_ns(tag):
    return tag.split("}")[-1] if "}" in tag else tag

def parse_coverage_xml(path):
    tree = ET.parse(path)
    root = tree.getroot()
    files = {}

    def count_lines_from_line_elems(line_elems):
        total = 0
        covered = 0
        for line in line_elems:
            hits = line.get("hits") or line.get("count") or line.get("hit")
            total += 1
            try:
                if hits is not None and int(hits) > 0:
                    covered += 1
            except Exception:
                if str(hits).lower() in ("true","yes","1"):
                    covered += 1
        return covered, total

    # 1) <file> con <line/>
    for file_el in root.findall(".//file"):
        name = file_el.get("name") or file_el.get("filename")
        if not name:
            continue
        line_elems = file_el.findall(".//line")
        covered, total = count_lines_from_line_elems(line_elems)
        files[name] = {"covered": covered, "total": total, "pct": round((covered/total)*100,2) if total>0 else 0.0}

    # 2) <class> variantes
    for class_el in root.findall(".//class"):
        filename = class_el.get("filename") or class_el.get("name")
        if not filename:
            continue
        covered_attr = class_el.get("covered")
        total_attr = class_el.get("lines") or class_el.get("statements")
        if covered_attr is not None and total_attr is not None:
            try:
                covered = int(covered_attr)
                total = int(total_attr)
            except Exception:
                covered = 0; total = 0
        else:
            line_elems = class_el.findall(".//line")
            covered, total = count_lines_from_line_elems(line_elems)
        prev = files.get(filename)
        if prev and prev.get("total",0) >= total:
            continue
        files[filename] = {"covered": covered, "total": total, "pct": round((covered/total)*100,2) if total>0 else 0.0}

    # 3) explorar otros nodos por si contienen filename *.py
    for el in root.iter():
        tag = strip_ns(el.tag)
        # evitar reprocese los ya tratados
        for attr_name in ("filename","name","file","path"):
            val = el.get(attr_name)
            if val and isinstance(val,str) and val.lower().endswith(".py") and val not in files:
                line_elems = el.findall(".//line")
                covered, total = count_lines_from_line_elems(line_elems)
                files[val] = {"covered": covered, "total": total, "pct": round((covered/total)*100,2) if total>0 else 0.0}

    return files

# ---------- extraer tasks de jobs YAML ----------
def discover_tasks_from_jobs(jobs_dir):
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
                        "notebook_path": str(nb_path) if nb_path else ""
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
                    "notebook_path": str(nb_path) if nb_path else ""
                })
    return out

# ---------- candidates y matching (exacto: tests/test_<task>.py) ----------
def generate_test_candidates_for_task(task_key):
    tk = str(task_key).strip().lower().replace(" ", "_")
    if not tk:
        return []
    variants = []
    variants.append(f"test_{tk}.py")
    variants.append(f"test_{tk.replace('_','-')}.py")
    variants.append(f"test_{tk.replace('-','_')}.py")
    variants.append(f"{tk}.py")
    seen = set(); out=[]
    for v in variants:
        lv = v.lower()
        if lv not in seen:
            seen.add(lv); out.append(lv)
    return out

def build_basename_map(files_map):
    bmap = {}
    for k in files_map.keys():
        b = Path(k).name.lower()
        bmap.setdefault(b, []).append(k)
    return bmap

def find_best_match_for_task(task_key, files_map, basename_map):
    candidates = generate_test_candidates_for_task(task_key)
    best = None; best_pct = -1.0; best_info = (None,0,0,0.0)
    for cand in candidates:
        if cand in basename_map:
            for orig_key in basename_map[cand]:
                info = files_map.get(orig_key) or files_map.get(orig_key.replace("\\","/"))
                if info:
                    pct = info.get("pct", 0.0)
                    if pct > best_pct:
                        best_pct = pct
                        best = orig_key
                        best_info = (orig_key, info.get("covered",0), info.get("total",0), info.get("pct",0.0))
    return best_info

# ---------- main: agrupar por job ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-xml", required=True)
    parser.add_argument("--jobs-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    files_map = parse_coverage_xml(args.coverage_xml)
    print(f"DEBUG: parsed {len(files_map)} coverage entries from {args.coverage_xml}")
    # debug sample keys
    for k in list(files_map.keys())[:80]:
        print("DEBUG_KEY:", k)

    basename_map = build_basename_map(files_map)

    tasks = discover_tasks_from_jobs(args.jobs_dir)
    print(f"DEBUG: discovered {len(tasks)} tasks in jobs YAMLs")

    # group tasks by job_id
    jobs = {}
    for t in tasks:
        job_id = t["job_id"]
        if job_id not in jobs:
            jobs[job_id] = {
                "job_id": job_id,
                "job_name": t["job_name"],
                "job_file": t["job_file"],
                "tasks": []
            }
        # find match for this task
        chosen_key, covered, total, pct = find_best_match_for_task(t["task_key"], files_map, basename_map)
        jobs[job_id]["tasks"].append({
            "task_key": t["task_key"],
            "notebook_path": t["notebook_path"],
            "matched_coverage_file": chosen_key,
            "covered_lines": covered,
            "total_lines": total,
            "coverage_percent": pct
        })

    per_job = list(jobs.values())

    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump({"per_job": per_job}, fh, indent=2, ensure_ascii=False)

    print(f"WROTE {len(per_job)} entries to {args.output}")
    for j in per_job:
        print(f"JOB {j['job_id']}: {len(j['tasks'])} tasks")

if __name__ == "__main__":
    main()