#!/usr/bin/env python3
# scripts/resolve_and_patch_jobs.py

import os
import sys
from pathlib import Path
import yaml

JOBS_DIR = Path("jobs")

def get_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        print(f"ERROR: required env var {name} not set")
        sys.exit(1)
    return v

def patch_task_dict(task: dict, cluster_id: str) -> bool:
    """Patch single task dict in-place. Return True if changed."""
    changed = False
    if not isinstance(task, dict):
        return False
    # If there's a new_cluster at task level, remove it
    if "new_cluster" in task:
        task.pop("new_cluster", None)
        changed = True
    # Set existing_cluster_id
    prev = task.get("existing_cluster_id")
    if prev != cluster_id:
        task["existing_cluster_id"] = cluster_id
        changed = True
    return changed

def find_and_patch_tasks(obj, cluster_id: str) -> int:
    """
    Recursively walk the object (dicts/lists) and patch any list elements called 'tasks'
    or dicts that look like individual tasks. Returns count patched.
    """
    patched_count = 0

    if isinstance(obj, dict):
        # If this dict has a 'tasks' key that's a list -> iterate tasks
        if "tasks" in obj and isinstance(obj["tasks"], list):
            for t in obj["tasks"]:
                if patch_task_dict(t, cluster_id):
                    patched_count += 1
        # Also handle possible job_clusters -> tasks or other shapes
        # Walk into all dict values recursively
        for k, v in obj.items():
            patched_count += find_and_patch_tasks(v, cluster_id)

    elif isinstance(obj, list):
        for item in obj:
            patched_count += find_and_patch_tasks(item, cluster_id)

    return patched_count

def main():
    # We require CLUSTER_ID to be present (exported to GITHUB_ENV in previous step)
    cluster_id = os.getenv("CLUSTER_ID")
    if not cluster_id:
        print("ERROR: CLUSTER_ID environment variable is required but not set.")
        sys.exit(1)

    print(f"Using CLUSTER_ID={cluster_id}")

    if not JOBS_DIR.exists() or not JOBS_DIR.is_dir():
        print("No jobs/ directory found. Nothing to patch.")
        sys.exit(0)

    job_files = sorted(JOBS_DIR.glob("*.yml"))
    if not job_files:
        print("No job YAML files found in jobs/")
        sys.exit(0)

    total_patched = 0
    for jf in job_files:
        raw = jf.read_text(encoding="utf-8")
        try:
            data = yaml.safe_load(raw) or {}
        except Exception as e:
            print(f"ERROR parsing YAML {jf}: {e}")
            continue

        patched = find_and_patch_tasks(data, cluster_id)

        # Write back only if patched or even always to normalize
        jf.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")

        if patched:
            print(f"Patched {patched} tasks in {jf.name}")
        else:
            print(f"No tasks patched in {jf.name} (check format)")

        total_patched += patched

    print(f"Done. Total patched tasks across files: {total_patched}")

if __name__ == "__main__":
    main()
