#!/usr/bin/env python3
import argparse
import csv
import xml.etree.ElementTree as ET
from pathlib import Path

# ----------------------------
# Coverage XML parsing (igual de robusto que coverage_per_job.py)
# ----------------------------
def parse_coverage_xml(path: str):
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

    # <file>
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

    # <class> variants
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

    # normaliza separadores en llaves (opcional pero útil)
    normalized = {}
    for k, v in files.items():
        normalized[k.replace("\\", "/")] = v
    return normalized

def build_basename_map(files_map):
    bmap = {}
    for k in files_map.keys():
        b = Path(k).name.lower()
        bmap.setdefault(b, []).append(k)
    return bmap

# ----------------------------
# Matching logic (replica coverage_per_job.py)
# ----------------------------
def notebook_candidates_from_file(nb_file: Path):
    # coverage_per_job.py genera candidatos desde notebook_path, aquí ya tenemos el archivo
    base = nb_file.name
    if not base.lower().endswith(".py"):
        base = base + ".py"
    return [base.lower(), f"notebooks/{base.lower()}"]

def find_notebook_coverage(nb_file: Path, files_map, basename_map):
    cands = notebook_candidates_from_file(nb_file)
    best = (None, 0, 0, 0.0)
    best_pct = -1.0

    # 1) match por basename
    for cand in cands:
        b = Path(cand).name.lower()
        if b in basename_map:
            for orig in basename_map[b]:
                info = files_map.get(orig) or files_map.get(orig.replace("\\", "/"))
                if info and info.get("pct", 0.0) > best_pct:
                    best_pct = info.get("pct", 0.0)
                    best = (orig, info.get("covered", 0), info.get("total", 0), info.get("pct", 0.0))
    if best[0]:
        return best

    # 2) endswith para rutas absolutas en coverage.xml
    for cand in cands:
        cand_norm = cand.replace("\\", "/").lower()
        for orig, info in files_map.items():
            if orig.replace("\\", "/").lower().endswith(cand_norm):
                pct = info.get("pct", 0.0)
                if pct > best_pct:
                    best_pct = pct
                    best = (orig, info.get("covered", 0), info.get("total", 0), pct)

    return best

def generate_test_candidates(task_key):
    # COPIA 1:1 de coverage_per_job.py
    tk = str(task_key or "").strip().lower().replace(" ", "_")
    if not tk:
        return []
    variants = [
        f"test_{tk}.py",
        f"test_{tk.replace('_','-')}.py",
        f"{tk}.py"
    ]
    seen = set(); out=[]
    for v in variants:
        if v.lower() not in seen:
            seen.add(v.lower()); out.append(v.lower())
    return out

def find_test_coverage_by_notebook(nb_file: Path, files_map, basename_map):
    # Aquí “task_key” = nombre del notebook sin extensión (ej: NB_PATRIF_...)
    task_key = nb_file.stem

    cands = generate_test_candidates(task_key)
    best = (None, 0, 0, 0.0)
    best_pct = -1.0

    for cand in cands:
        if cand in basename_map:
            for orig in basename_map[cand]:
                info = files_map.get(orig) or files_map.get(orig.replace("\\", "/"))
                if info and info.get("pct", 0.0) > best_pct:
                    best_pct = info.get("pct", 0.0)
                    best = (orig, info.get("covered", 0), info.get("total", 0), info.get("pct", 0.0))

    return best

# ----------------------------
# Main -> SOLO CSV
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coverage-xml", required=True)
    ap.add_argument("--notebooks-dir", default="notebooks")
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    files_map = parse_coverage_xml(args.coverage_xml)
    basename_map = build_basename_map(files_map)

    notebooks = sorted(Path(args.notebooks_dir).rglob("*.py"))

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "notebook_file",
            "matched_notebook_coverage_file",
            "notebook_coverage_real_percent",
            "matched_test_file",
            "test_coverage_percent",
            "coverage_percent"  # KPI final: real si existe, si no, hereda del test si hay
        ])

        for nb in notebooks:
            nb_rel = nb.as_posix()

            # coverage REAL del notebook
            nb_cov_file, nb_cov, nb_tot, nb_pct = find_notebook_coverage(nb, files_map, basename_map)
            nb_in_cov = bool(nb_cov_file)

            # relación Notebook->Test (replicando candidates + basename_map)
            test_file, t_cov, t_tot, t_pct = find_test_coverage_by_notebook(nb, files_map, basename_map)

            # KPI final: si notebook está en coverage.xml úsalo; si no, pero hay test, hereda el % del test; si no, 0
            final_pct = nb_pct if nb_in_cov else (t_pct if test_file else 0.0)

            w.writerow([
                nb_rel,
                nb_cov_file,
                nb_pct if nb_in_cov else 0.0,
                test_file,
                t_pct if test_file else 0.0,
                final_pct
            ])

    print(f"WROTE {args.output}")

if __name__ == "__main__":
    main()