#!/usr/bin/env python3
import json
import csv
import argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)      # coverage_per_job.json
    ap.add_argument("--out-csv", required=True)    # kpi_jobs.csv
    ap.add_argument("--out-json", required=False)  # opcional
    args = ap.parse_args()

    data = json.load(open(args.input, "r", encoding="utf-8"))

    rows = []
    for job in data.get("per_job", []):
        s = job.get("summary", {}) or {}

        tasks_count = int(s.get("tasks_count", 0) or 0)
        matched = int(s.get("matched_tasks", 0) or 0)
        unmatched = int(s.get("unmatched_tasks", 0) or (tasks_count - matched))

        adoption = round((matched / tasks_count) * 100, 2) if tasks_count > 0 else 0.0

        rows.append({
            "job_id": job.get("job_id"),
            "job_name": job.get("job_name"),
            "job_file": job.get("job_file"),
            "tasks_count": tasks_count,
            "matched_tasks": matched,
            "unmatched_tasks": unmatched,
            "task_coverage_adoption_pct": adoption,
            # KPI oficial
            "coverage_kpi_weighted_assuming_equal_task_size": float(s.get("weighted_assuming_equal_task_size", 0.0) or 0.0),
            # métricas auxiliares
            "coverage_avg_percent": float(s.get("avg_coverage_percent", 0.0) or 0.0),
            "coverage_weighted_measured_percent": float(s.get("weighted_coverage_percent", 0.0) or 0.0),
            "covered_lines_total": int(s.get("covered_lines_total", 0) or 0),
            "total_lines_total": int(s.get("total_lines_total", 0) or 0),
        })

    # CSV
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        else:
            # CSV vacío pero válido
            f.write("job_id,job_name,job_file,tasks_count,matched_tasks,unmatched_tasks,task_coverage_adoption_pct,"
                    "coverage_kpi_weighted_assuming_equal_task_size,coverage_avg_percent,coverage_weighted_measured_percent,"
                    "covered_lines_total,total_lines_total\n")

    # JSON opcional
    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as jf:
            json.dump({"kpi_jobs": rows}, jf, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    main()