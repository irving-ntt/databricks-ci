#!/usr/bin/env python3

import argparse
import xml.etree.ElementTree as ET
import json
from pathlib import Path
import yaml
import sys
from statistics import mean


# ==========================
# COVERAGE XML PARSER
# ==========================

def parse_coverage_xml(path):
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
        files[name] = {
            "covered": covered,
            "total": total,
            "pct": round((covered / total) * 100, 2) if total > 0 else 0.0
        }

    for class_el in root.findall(".//class"):
        filename = class_el.get("filename") or class_el.get("name")
        if not filename:
            continue
        covered, total = count_lines(class_el.findall(".//line"))
        prev = files.get(filename)
        if prev and prev.get("total", 0) >= total:
            continue
        files[filename] = {
            "covered": covered,
            "total": total,
            "pct": round((covered / total) * 100, 2) if total > 0 else 0.0
        }

    return files


def build_basename_map(files_map):
    bmap = {}
    for k in files_map.keys():
        b = Path(k).name.lower()
        bmap.setdefault(b, []).append(k)
    return bmap


# ==========================
# JOB DISCOVERY (NOTEBOOK TASKS ONLY)
# ==========================

def discover_tasks_from_jobs(jobs_dir):
    """
    Only returns tasks that actually execute notebooks.
    """
    out = []
    p = Path(jobs_dir)

    yaml_files = sorted(list(p.rglob("*.yml")) + list(p.rglob("*.yaml")))

    for yf in yaml_files:
        try:
            with open(yf, "r", encoding="utf-8") as fh:
                doc = yaml.safe_load(fh) or {}
        except Exception:
            continue

        resources = doc.get("resources") or {}
        jobs_section = resources.get("jobs") or {}

        for job_id, job_data in jobs_section.items():
            job_name = job_data.get("name") or job_id
            tasks = job_data.get("tasks") or []

            for task in tasks:
                nt = task.get("notebook_task")
                if not nt:
                    continue  # NOT a notebook task → ignore

                nb_path = nt.get("notebook_path") or nt.get("notebook")
                if not nb_path:
                    continue  # no notebook path → ignore

                out.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "job_file": str(yf),
                    "task_key": task.get("task_key"),
                    "notebook_path": nb_path
                })

    return out


# ==========================
# NOTEBOOK COVERAGE MATCHING
# ==========================

def notebook_candidates(notebook_path):
    if not notebook_path:
        return []
    p = notebook_path.replace("\\", "/").strip()
    base = p.split("/")[-1]
    if not base.lower().endswith(".py"):
        base = base + ".py"
    return [base.lower()]


def find_notebook_coverage(notebook_path, files_map, basename_map):
    candidates = notebook_candidates(notebook_path)
    best = (None, 0, 0, 0.0)
    best_pct = -1.0

    for cand in candidates:
        if cand in basename_map:
            for orig_key in basename_map[cand]:
                info = files_map.get(orig_key)
                if info and info["pct"] > best_pct:
                    best_pct = info["pct"]
                    best = (orig_key, info["covered"], info["total"], info["pct"])

    return best


# ==========================
# KPI SUMMARY
# ==========================

def compute_summary(tasks):
    tasks_count = len(tasks)
    matched = sum(1 for t in tasks if t["matched_coverage_file"])
    unmatched = tasks_count - matched

    pct_list = [t["coverage_percent"] for t in tasks]
    avg_pct = round(mean(pct_list), 2) if pct_list else 0.0

    covered_total = sum(t["covered_lines"] for t in tasks)
    total_total = sum(t["total_lines"] for t in tasks)

    weighted = round((covered_total / total_total) * 100, 2) if total_total > 0 else 0.0

    totals = [t["total_lines"] for t in tasks if t["total_lines"] > 0]
    avg_size = mean(totals) if totals else 0

    if avg_size > 0:
        assumed_total = avg_size * tasks_count
        weighted_assumed = round((covered_total / assumed_total) * 100, 2)
    else:
        weighted_assumed = avg_pct

    return {
        "tasks_count": tasks_count,
        "matched_tasks": matched,
        "unmatched_tasks": unmatched,
        "avg_coverage_percent": avg_pct,
        "weighted_coverage_percent": weighted,
        "weighted_assuming_equal_task_size": weighted_assumed,
        "covered_lines_total": covered_total,
        "total_lines_total": total_total
    }


# ==========================
# MAIN
# ==========================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-xml", required=True)
    parser.add_argument("--jobs-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    files_map = parse_coverage_xml(args.coverage_xml)
    basename_map = build_basename_map(files_map)

    tasks = discover_tasks_from_jobs(args.jobs_dir)

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

        matched, covered, total, pct = find_notebook_coverage(
            t["notebook_path"], files_map, basename_map
        )

        jobs[job_id]["tasks"].append({
            "task_key": t["task_key"],
            "notebook_path": t["notebook_path"],
            "matched_coverage_file": matched,
            "covered_lines": covered,
            "total_lines": total,
            "coverage_percent": pct
        })

    result = []
    for job in jobs.values():
        job["summary"] = compute_summary(job["tasks"])
        result.append(job)

    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump({"per_job": result}, fh, indent=2, ensure_ascii=False)

    print("Coverage per job generado correctamente.")


if __name__ == "__main__":
    main()