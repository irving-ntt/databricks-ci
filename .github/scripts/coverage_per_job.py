#!/usr/bin/env python3
"""
coverage_per_job.py (KPI Test Coverage por Job usando cobertura de notebooks/*.py)

Salida:
{
  "per_job": [
    {
      "job_id": "...",
      "job_name": "...",
      "job_file": "...",
      "tasks": [
        {
          "task_key": "...",
          "notebook_path": "...",
          "matched_coverage_file": "notebooks/<X>.py",
          "covered_lines": N,
          "total_lines": M,
          "coverage_percent": P
        }
      ],
      "summary": {
        "tasks_count": N,
        "matched_tasks": M,
        "unmatched_tasks": N-M,
        "avg_coverage_percent": X.X,
        "weighted_coverage_percent": Y.Y,
        "weighted_assuming_equal_task_size": Z.Z,
        "covered_lines_total": A,
        "total_lines_total": B
      }
    }
  ]
}

Uso:
 python .github/scripts/coverage_per_job.py --coverage-xml coverage.xml --jobs-dir jobs --output coverage_per_job.json

Requiere: pyyaml
"""

import argparse
import xml.etree.ElementTree as ET
import json
from pathlib import Path
import yaml
import sys
from statistics import mean

# -------------------- Coverage XML parsing --------------------

def strip_ns(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag

def parse_coverage_xml(path: str):
    """
    Parse coverage.xml robustly.
    Returns dict: {filename_key: {"covered":int,"total":int,"pct":float}}
    filename_key is exactly what appears in coverage.xml.
    """
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
                if str(hits).lower() in ("true", "yes", "1"):
                    covered += 1
        return covered, total

    # 1) <file name="..."><line .../></file>
    for file_el in root.findall(".//file"):
        name = file_el.get("name") or file_el.get("filename")
        if not name:
            continue
        line_elems = file_el.findall(".//line")
        covered, total = count_lines(line_elems)
        files[name] = {
            "covered": covered,
            "total": total,
            "pct": round((covered / total) * 100, 2) if total > 0 else 0.0
        }

    # 2) <class filename="..."> with attrs or nested lines
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
                covered, total = 0, 0
        else:
            line_elems = class_el.findall(".//line")
            covered, total = count_lines(line_elems)

        prev = files.get(filename)
        if prev and prev.get("total", 0) >= total:
            continue

        files[filename] = {
            "covered": covered,
            "total": total,
            "pct": round((covered / total) * 100, 2) if total > 0 else 0.0
        }

    # 3) Fallback: scan any node that has filename-like attrs ending in .py
    for el in root.iter():
        for attr_name in ("filename", "name", "file", "path"):
            val = el.get(attr_name)
            if val and isinstance(val, str) and val.lower().endswith(".py") and val not in files:
                line_elems = el.findall(".//line")
                covered, total = count_lines(line_elems)
                files[val] = {
                    "covered": covered,
                    "total": total,
                    "pct": round((covered / total) * 100, 2) if total > 0 else 0.0
                }

    return files

def build_basename_map(files_map):
    """basename(lower) -> list(original keys)"""
    bmap = {}
    for k in files_map.keys():
        b = Path(k).name.lower()
        bmap.setdefault(b, []).append(k)
    return bmap

# -------------------- Jobs YAML parsing --------------------

def discover_tasks_from_jobs(jobs_dir: str):
    """
    Extract tasks from YAMLs under jobs_dir.

    Expected structure (your example):
    resources:
      jobs:
        WF_ADB:
          name: WF_ADB
          tasks:
            - task_key: ingest_circuits
              notebook_task:
                notebook_path: /notebooks/Ingest_circuits
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
                    nb_path = nt.get("notebook_path") or nt.get("notebook") or ""
                    out.append({
                        "job_id": job_id,
                        "job_name": job_name,
                        "job_file": str(yf),
                        "task_key": task_key,
                        "notebook_path": str(nb_path) if nb_path else ""
                    })
        else:
            # fallback: top-level tasks (just in case)
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

# -------------------- Notebook matching (uses notebook_path) --------------------

def notebook_candidates_from_notebook_path(notebook_path: str):
    """
    Convert Databricks notebook_path to candidate .py filenames.
    Examples:
      /Workspace/.../notebooks/Ingest_testing  -> notebooks/Ingest_testing.py
      /notebooks/Ingest_testing               -> notebooks/Ingest_testing.py
      notebooks/Ingest_testing                -> notebooks/Ingest_testing.py
    """
    if not notebook_path:
        return []
    p = notebook_path.replace("\\", "/").strip()
    base = p.split("/")[-1]  # Ingest_testing
    if not base:
        return []
    base_py = base if base.lower().endswith(".py") else (base + ".py")
    return [f"notebooks/{base_py}", base_py]

def find_match_for_notebook_path(notebook_path: str, files_map, basename_map):
    """
    Match coverage entry based on notebook_path-derived candidates.
    Match order:
      1) basename exact (Ingest_testing.py)
      2) endswith('notebooks/Ingest_testing.py') for absolute paths in coverage.xml
    If multiple candidates map to multiple entries, choose highest pct.
    """
    cands = notebook_candidates_from_notebook_path(notebook_path)
    if not cands:
        return (None, 0, 0, 0.0)

    # 1) basename exact
    best = (None, 0, 0, 0.0)
    best_pct = -1.0
    for cand in cands:
        b = Path(cand).name.lower()
        if b in basename_map:
            for orig_key in basename_map[b]:
                info = files_map.get(orig_key) or files_map.get(orig_key.replace("\\", "/"))
                if info:
                    pct = info.get("pct", 0.0)
                    if pct > best_pct:
                        best_pct = pct
                        best = (orig_key, info.get("covered", 0), info.get("total", 0), pct)
    if best[0]:
        return best

    # 2) endswith full candidate path
    for cand in cands:
        cand_norm = cand.replace("\\", "/").lower()
        for orig_key, info in files_map.items():
            ok = orig_key.replace("\\", "/").lower()
            if ok.endswith(cand_norm):
                pct = info.get("pct", 0.0)
                if pct > best_pct:
                    best_pct = pct
                    best = (orig_key, info.get("covered", 0), info.get("total", 0), pct)

    return best

# -------------------- Summary --------------------

def compute_job_summary(tasks_list):
    """
    Summary KPIs:
      - avg_coverage_percent: simple avg of tasks (includes 0 for unmatched)
      - weighted_coverage_percent: covered_total/total_total over measured tasks
      - weighted_assuming_equal_task_size: penalize unmatched tasks assuming avg measured size
    """
    tasks_count = len(tasks_list)
    matched = sum(1 for t in tasks_list if t.get("matched_coverage_file"))
    unmatched = tasks_count - matched

    pct_list = [t.get("coverage_percent", 0.0) for t in tasks_list]
    avg_coverage = round(mean(pct_list), 2) if pct_list else 0.0

    covered_total = sum(t.get("covered_lines", 0) for t in tasks_list)
    total_total = sum(t.get("total_lines", 0) for t in tasks_list)
    weighted = round((covered_total / total_total) * 100, 2) if total_total > 0 else 0.0

    totals_for_avg = [t.get("total_lines", 0) for t in tasks_list if t.get("total_lines", 0) > 0]
    avg_total_measured = mean(totals_for_avg) if totals_for_avg else 0.0

    if avg_total_measured <= 0:
        weighted_assumed = avg_coverage
    else:
        assumed_total = avg_total_measured * tasks_count
        weighted_assumed = round((covered_total / assumed_total) * 100, 2) if assumed_total > 0 else 0.0

    return {
        "tasks_count": tasks_count,
        "matched_tasks": matched,
        "unmatched_tasks": unmatched,
        "avg_coverage_percent": avg_coverage,
        "weighted_coverage_percent": weighted,
        "weighted_assuming_equal_task_size": weighted_assumed,
        "covered_lines_total": covered_total,
        "total_lines_total": total_total
    }

# -------------------- Main --------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-xml", required=True)
    parser.add_argument("--jobs-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    files_map = parse_coverage_xml(args.coverage_xml)
    basename_map = build_basename_map(files_map)

    tasks = discover_tasks_from_jobs(args.jobs_dir)

    # group by job_id
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

        chosen_key, covered, total, pct = find_match_for_notebook_path(
            t["notebook_path"], files_map, basename_map
        )

        jobs[job_id]["tasks"].append({
            "task_key": t["task_key"],
            "notebook_path": t["notebook_path"],
            "matched_coverage_file": chosen_key,
            "covered_lines": covered,
            "total_lines": total,
            "coverage_percent": pct
        })

    per_job = []
    for job in jobs.values():
        job_entry = job.copy()
        job_entry["summary"] = compute_job_summary(job_entry["tasks"])
        per_job.append(job_entry)

    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump({"per_job": per_job}, fh, indent=2, ensure_ascii=False)

    print(f"WROTE {len(per_job)} jobs to {args.output}")

if __name__ == "__main__":
    main()