#!/usr/bin/env python3
"""
coverage_per_job.py

- Sólo considera tasks que ejecutan notebooks (notebook_task + notebook_path).
- Busca cobertura del notebook en coverage.xml.
- Opcionalmente: fallback a tests por task_key si --allow-test-fallback true.
- Imprime DEBUG para facilitar diagnóstico en CI logs.

Uso:
 python .github/scripts/coverage_per_job.py \
    --coverage-xml coverage.xml --jobs-dir jobs --output coverage_per_job.json \
    [--allow-test-fallback true] [--debug true]

"""
import argparse
import xml.etree.ElementTree as ET
import json
from pathlib import Path
import yaml
import sys
from statistics import mean

# ----------------------------
# Utils / Parser
# ----------------------------
def dbg(msg, enabled):
    if enabled:
        print("DEBUG:", msg)

def parse_coverage_xml(path, debug=False):
    dbg(f"Parsing coverage xml: {path}", debug)
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
                # ignore malformed
                pass
        return covered, total

    # <file> elements
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

    # <class> variants (some tools)
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

    # fallback: any node with filename-like attr ending in .py
    for el in root.iter():
        for attr in ("filename", "name", "file", "path"):
            val = el.get(attr)
            if val and isinstance(val, str) and val.lower().endswith(".py") and val not in files:
                covered, total = count_lines(el.findall(".//line"))
                files[val] = {
                    "covered": covered,
                    "total": total,
                    "pct": round((covered / total) * 100, 2) if total > 0 else 0.0
                }

    dbg(f"Parsed coverage entries: {len(files)}", debug)
    if debug:
        cnt = 0
        for k, v in list(files.items())[:80]:
            print(f"DEBUG_KEY: {k} -> {v}")
            cnt += 1
            if cnt >= 80:
                break

    return files

def build_basename_map(files_map):
    bmap = {}
    for k in files_map.keys():
        b = Path(k).name.lower()
        bmap.setdefault(b, []).append(k)
    return bmap

# ----------------------------
# Jobs discovery (NOTEBOK TASKS ONLY)
# ----------------------------
def discover_tasks_from_jobs(jobs_dir, debug=False):
    """
    Returns only tasks that run notebooks (notebook_task + notebook_path present and non-empty).
    """
    out = []
    p = Path(jobs_dir)
    if not p.exists():
        dbg(f"jobs_dir '{jobs_dir}' not found", debug)
        return out

    yaml_files = sorted(list(p.rglob("*.yml")) + list(p.rglob("*.yaml")))
    dbg(f"Found {len(yaml_files)} YAML files under {jobs_dir}", debug)

    for yf in yaml_files:
        try:
            with open(yf, "r", encoding="utf-8") as fh:
                doc = yaml.safe_load(fh) or {}
        except Exception as e:
            dbg(f"Cannot parse YAML {yf}: {e}", debug)
            continue

        # Flexible access:
        resources = doc.get("resources") or {}
        jobs_section = resources.get("jobs") or doc.get("jobs") or {}
        # jobs_section might be dict-like; if not, skip
        if not isinstance(jobs_section, dict):
            dbg(f"Skipping {yf}: no jobs section", debug)
            continue

        for job_id, job_data in jobs_section.items():
            job_name = job_data.get("name") or job_id
            tasks = job_data.get("tasks") or []
            for task in tasks:
                # only consider notebook_task entries
                nt = task.get("notebook_task")
                if not nt or not isinstance(nt, dict):
                    dbg(f"Ignoring task (no notebook_task) in {job_id} @ {yf}", debug)
                    continue
                nb_path = nt.get("notebook_path") or nt.get("notebook") or nt.get("notebook_name") or ""
                if not nb_path or str(nb_path).strip() == "":
                    dbg(f"Ignoring task (empty notebook_path) in {job_id} task_key={task.get('task_key')} @ {yf}", debug)
                    continue
                out.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "job_file": str(yf),
                    "task_key": task.get("task_key"),
                    "notebook_path": str(nb_path)
                })
    dbg(f"Discovered {len(out)} notebook-tasks from jobs", debug)
    return out

# ----------------------------
# Matching logic
# ----------------------------
def notebook_candidates_from_path(notebook_path):
    if not notebook_path:
        return []
    p = notebook_path.replace("\\", "/").strip()
    base = p.split("/")[-1]
    if not base.lower().endswith(".py"):
        base = base + ".py"
    return [base.lower(), f"notebooks/{base.lower()}"]

def find_notebook_coverage(notebook_path, files_map, basename_map, debug=False):
    cands = notebook_candidates_from_path(notebook_path)
    dbg(f"Notebook candidates for '{notebook_path}': {cands}", debug)
    best = (None, 0, 0, 0.0)
    best_pct = -1.0
    for cand in cands:
        # match by basename_map first
        b = Path(cand).name.lower()
        if b in basename_map:
            for orig in basename_map[b]:
                info = files_map.get(orig) or files_map.get(orig.replace("\\", "/"))
                if info and info.get("pct", 0.0) > best_pct:
                    best_pct = info.get("pct", 0.0)
                    best = (orig, info.get("covered", 0), info.get("total", 0), info.get("pct", 0.0))
    if best[0]:
        dbg(f"Matched notebook by basename: {best[0]} -> pct={best[3]}", debug)
        return best

    # fallback to endswith match on full candidate (for absolute paths)
    for cand in cands:
        cand_norm = cand.replace("\\", "/").lower()
        for orig, info in files_map.items():
            if orig.replace("\\", "/").lower().endswith(cand_norm):
                pct = info.get("pct", 0.0)
                if pct > best_pct:
                    best_pct = pct
                    best = (orig, info.get("covered", 0), info.get("total", 0), pct)
    if best[0]:
        dbg(f"Matched notebook by path-endswith: {best[0]} -> pct={best[3]}", debug)
    else:
        dbg(f"No notebook coverage match for notebook_path '{notebook_path}'", debug)
    return best

def generate_test_candidates(task_key):
    tk = str(task_key or "").strip().lower().replace(" ", "_")
    if not tk:
        return []
    variants = [
        f"test_{tk}.py",
        f"test_{tk.replace('_','-')}.py",
        f"{tk}.py"
    ]
    # unique
    seen = set(); out=[]
    for v in variants:
        if v.lower() not in seen:
            seen.add(v.lower()); out.append(v.lower())
    return out

def find_test_coverage_by_task(task_key, files_map, basename_map, debug=False):
    cands = generate_test_candidates(task_key)
    dbg(f"Test candidates for task_key '{task_key}': {cands}", debug)
    best = (None, 0, 0, 0.0)
    best_pct = -1.0
    for cand in cands:
        if cand in basename_map:
            for orig in basename_map[cand]:
                info = files_map.get(orig) or files_map.get(orig.replace("\\", "/"))
                if info and info.get("pct", 0.0) > best_pct:
                    best_pct = info.get("pct", 0.0)
                    best = (orig, info.get("covered", 0), info.get("total", 0), info.get("pct", 0.0))
    if best[0]:
        dbg(f"Matched test file {best[0]} for task_key {task_key} -> pct={best[3]}", debug)
    else:
        dbg(f"No test coverage match for task_key '{task_key}'", debug)
    return best

# ----------------------------
# Summary computation
# ----------------------------
def compute_job_summary(tasks):
    tasks_count = len(tasks)
    matched = sum(1 for t in tasks if t.get("matched_coverage_file"))
    unmatched = tasks_count - matched
    pct_list = [t.get("coverage_percent", 0.0) for t in tasks]
    avg_pct = round(mean(pct_list), 2) if pct_list else 0.0
    covered_total = sum(t.get("covered_lines", 0) for t in tasks)
    total_total = sum(t.get("total_lines", 0) for t in tasks)
    weighted = round((covered_total / total_total) * 100, 2) if total_total > 0 else 0.0
    totals_for_avg = [t.get("total_lines", 0) for t in tasks if t.get("total_lines", 0) > 0]
    avg_total_measured = mean(totals_for_avg) if totals_for_avg else 0.0
    if avg_total_measured <= 0:
        weighted_assumed = avg_pct
    else:
        assumed_total = avg_total_measured * tasks_count
        weighted_assumed = round((covered_total / assumed_total) * 100, 2) if assumed_total > 0 else 0.0
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

# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-xml", required=True)
    parser.add_argument("--jobs-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--allow-test-fallback", default="false", choices=["true", "false"], help="If true, fallback to matching tests/test_<task_key>.py when notebook coverage not found.")
    parser.add_argument("--debug", default="false", choices=["true", "false"], help="Enable debug prints.")
    args = parser.parse_args()

    debug = args.debug.lower() == "true"
    allow_fallback = args.allow_test_fallback.lower() == "true"

    files_map = parse_coverage_xml(args.coverage_xml)
    basename_map = build_basename_map(files_map)

    tasks = discover_tasks_from_jobs(args.jobs_dir, debug=debug)

    jobs = {}
    for t in tasks:
        job_id = t["job_id"]
        if job_id not in jobs:
            jobs[job_id] = {
                "job_id": job_id,
                "job_name": t.get("job_name"),
                "job_file": t.get("job_file"),
                "tasks": []
            }

        # first try to match notebook coverage
        matched, covered, total, pct = find_notebook_coverage(t["notebook_path"], files_map, basename_map, debug=debug)

        # if no notebook match and fallback allowed, try test match by task_key
        if (not matched) and allow_fallback:
            dbg(f"No notebook coverage for task '{t.get('task_key')}'. Trying test fallback.", debug)
            matched, covered, total, pct = find_test_coverage_by_task(t.get("task_key"), files_map, basename_map, debug=debug)

        jobs[job_id]["tasks"].append({
            "task_key": t.get("task_key"),
            "notebook_path": t.get("notebook_path"),
            "matched_coverage_file": matched,
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