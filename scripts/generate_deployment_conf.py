#!/usr/bin/env python3
# scripts/generate_deployment_conf.py

import os
from pathlib import Path
import yaml

JOBS_DIR = Path("jobs")
OUT_FILE = Path("conf/deployment.yml")

def main():
    job_files = sorted([str(p) for p in JOBS_DIR.glob("*.yml")])
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    deployment = {
        "environments": {
            "dev": {"jobs": [{"job": jf} for jf in job_files]},
            "prod": {"jobs": [{"job": jf} for jf in job_files]},
        }
    }

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        yaml.safe_dump(deployment, f, sort_keys=False)

    print(f"Wrote {OUT_FILE} with {len(job_files)} jobs for dev/prod")

if __name__ == "__main__":
    main()
