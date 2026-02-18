#!/usr/bin/env python3
# scripts/resolve_and_patch_jobs.py

import os
import sys
from pathlib import Path
import yaml
import requests

JOBS_DIR = Path("jobs")


def env_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"Missing required environment variable: {name}")
        sys.exit(1)
    return value


def get_cluster_id(host: str, token: str, cluster_name: str) -> str:
    url = f"{host.rstrip('/')}/api/2.0/clusters/list"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    clusters = response.json().get("clusters", [])

    for cluster in clusters:
        if cluster.get("cluster_name") == cluster_name:
            return cluster.get("cluster_id")

    return None


def main():
    cluster_name = env_required("DATABRICKS_CLUSTER_NAME")

    print(f"Resolving cluster '{cluster_name}'...")

    cluster_id = os.getenv("CLUSTER_ID")  
    print(f"Cluster ID: {cluster_id}")

    if not cluster_id:
        print(f"ERROR: Cluster '{cluster_name}' not found in workspace.")
        sys.exit(1)

    print(f"Cluster ID found: {cluster_id}")

    job_files = sorted(JOBS_DIR.glob("*.yml"))

    if not job_files:
        print("No job files found in jobs/*.yml")
        sys.exit(0)

    for job_file in job_files:
        job_data = yaml.safe_load(job_file.read_text()) or {}

        # Remove any cluster configuration that should not be used
        job_data.pop("new_cluster", None)
        job_data.pop("cluster_name", None)

        # Force existing_cluster_id
        job_data["existing_cluster_id"] = cluster_id

        job_file.write_text(yaml.safe_dump(job_data, sort_keys=False))

        print(f"Updated job: {job_file.name}")

    print("All jobs updated successfully.")


if __name__ == "__main__":
    main()
