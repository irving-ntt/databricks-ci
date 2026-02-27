#!/usr/bin/env python3
"""
coverage_per_job.py (parser robusto)

Uso:
 python .github/scripts/coverage_per_job.py --coverage-xml coverage.xml --jobs-dir jobs --output coverage_per_job.json

Requiere: pyyaml (instalar en CI)
Qué mejora:
 - Detecta múltiples estructuras XML de coverage (class/file/lines/line)
 - Crea mapa basename -> entradas del coverage.xml
 - Devuelve covered_lines, total_lines, coverage_percent correctamente
 - Imprime DEBUG útil para validar en CI
"""
import argparse
import xml.etree.ElementTree as ET
import json
from pathlib import Path
import yaml
import sys
import re

def strip_ns(tag):
    # remove namespace if present
    return tag.split("}")[-1] if "}" in tag else tag

def parse_coverage_xml(path):
    """
    Parse coverage.xml robustly.
    Devuelve dict: {filename_key: {"covered":int,"total":int,"pct":float}}
    filename_key conserva exactamente el valor que aparece en el XML (para trazabilidad).
    """
    tree = ET.parse(path)
    root = tree.getroot()
    files = {}

    # Helper: count lines from a collection of <line> elements (supports different attrs)
    def count_lines_from_line_elems(line_elems):
        total = 0
        covered = 0
        for line in line_elems:
            # hits puede ser 'hits' o 'count' o 'hits' en distintas herramientas
            hits = line.get("hits") or line.get("count") or line.get("hit")
            total += 1
            try:
                if hits is not None and int(hits) > 0:
                    covered += 1
            except Exception:
                # si hits no es entero, probar con 'true'/'false'
                if str(hits).lower() in ("true","yes","1"):
                    covered += 1
        return covered, total

    # 1) Buscar elementos <file> con <line .../>
    for file_el in root.findall(".//file"):
        name = file_el.get("name") or file_el.get("filename")
        if not name:
            continue
        # algunas variantes anidan <line> directo; otras anidan <lines><line/>
        line_elems = file_el.findall(".//line")
        covered, total = count_lines_from_line_elems(line_elems)
        # si no hay <line>, quizá hay atributos directos en file_el (raro), ignorar
        files[name] = {"covered": covered, "total": total, "pct": round((covered/total)*100,2) if total>0 else 0.0}

    # 2) Buscar elementos <class> (Cobertura/otras variantes)
    # muchos coverage.xml tienen <class filename="..."> con atributos o líneas internas
    for class_el in root.findall(".//class"):
        filename = class_el.get("filename") or class_el.get("name")
        if not filename:
            continue
        # intento: atributos 'covered' y 'lines' (algunas variantes)
        covered_attr = class_el.get("covered")
        total_attr = class_el.get("lines") or class_el.get("lines-covered") or class_el.get("statements")
        if covered_attr is not None and total_attr is not None:
            try:
                covered = int(covered_attr)
                total = int(total_attr)
            except Exception:
                covered = 0; total = 0
        else:
            # fallback: contar <line> hijos (o en <lines>)
            line_elems = class_el.findall(".//line")
            covered, total = count_lines_from_line_elems(line_elems)
        # si ya existía la entrada (por file), sumarizar: preferir la que tiene total > 0
        prev = files.get(filename)
        if prev and prev.get("total",0) >= total:
            # mantener prev
            continue
        files[filename] = {"covered": covered, "total": total, "pct": round((covered/total)*100,2) if total>0 else 0.0}

    # 3) A veces cobertura se muestra como <package><class name="..." filename="..."> etc.
    # manejar casos donde filename aparece como atributo en otros nodos
    # Buscamos cualquier elemento que tenga atributo 'filename' o 'name' que parezca ruta *.py
    for el in root.iter():
        tag = strip_ns(el.tag)
        if tag in ("class","file","source","package"):  # ya manejados algunos, pero revisar por seguridad
            continue
        # buscar attrs que contengan '.py'
        for attr_name in ("filename","name","file","path"):
            val = el.get(attr_name)
            if val and isinstance(val,str) and val.lower().endswith(".py"):
                if val not in files:
                    # intentar contar <line> dentro
                    line_elems = el.findall(".//line")
                    covered, total = count_lines_from_line_elems(line_elems)
                    files[val] = {"covered": covered, "total": total, "pct": round((covered/total)*100,2) if total>0 else 0.0}

    return files

def discover_tasks_from_jobs(jobs_dir):
    """Extrae task entries de los YAMLs en jobs_dir."""
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
                        "notebook_path": str(nb_path)
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
                    "notebook_path": str(nb_path)
                })
    return out

def candidates_for_task_key(task_key):
    """Genera nombres de tests exactos que buscamos (lowercase)."""
    tk = (task_key or "").strip().lower().replace(" ", "_")
    if not tk:
        return []
    variants = [
        f"test_{tk}.py",
        f"test_{tk.replace('_','-')}.py",
        f"test_{tk.replace('-','_')}.py",
        f"{tk}.py"
    ]
    # unicidad y preserva orden
    seen = set(); out=[]
    for v in variants:
        lv = v.lower()
        if lv not in seen:
            seen.add(lv); out.append(lv)
    return out

def build_basename_map(files_map):
    """basename(lower) -> list(original keys)"""
    bmap = {}
    for k in files_map.keys():
        b = Path(k).name.lower()
        bmap.setdefault(b, []).append(k)
    return bmap

def find_best_for_task(task_key, files_map, basename_map):
    """Busca solo por los candidatos exactos; devuelve (orig_key, covered, total, pct) o (None,0,0,0.0)"""
    candidates = candidates_for_task_key(task_key)
    best = None; best_pct = -1.0; best_info = (None,0,0,0.0)
    for cand in candidates:
        if cand in basename_map:
            for orig in basename_map[cand]:
                info = files_map.get(orig) or files_map.get(orig.replace("\\","/"))
                if info:
                    pct = info.get("pct",0.0)
                    if pct > best_pct:
                        best_pct = pct
                        best = orig
                        best_info = (orig, info.get("covered",0), info.get("total",0), info.get("pct",0.0))
    return best_info

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage-xml", required=True)
    parser.add_argument("--jobs-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    files_map = parse_coverage_xml(args.coverage_xml)
    print(f"DEBUG: parsed {len(files_map)} coverage entries from {args.coverage_xml}")

    # debug: show sample keys
    sample = list(files_map.keys())[:80]
    print("DEBUG: sample keys from coverage.xml:")
    for k in sample:
        print("  -", k)

    basename_map = build_basename_map(files_map)

    tasks = discover_tasks_from_jobs(args.jobs_dir)
    print(f"DEBUG: discovered {len(tasks)} tasks in jobs YAMLs")

    per_job = []
    for t in tasks:
        job_id = t["job_id"]; job_name = t["job_name"]; job_file = t["job_file"]
        task_key = t["task_key"]; nb_path = t["notebook_path"]
        orig, covered, total, pct = find_best_for_task(task_key, files_map, basename_map)
        per_job.append({
            "job_id": job_id,
            "job_name": job_name,
            "job_file": job_file,
            "task_key": task_key,
            "notebook_path": nb_path,
            "matched_coverage_file": orig,
            "covered_lines": covered,
            "total_lines": total,
            "coverage_percent": pct
        })

    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump({"per_job": per_job}, fh, indent=2, ensure_ascii=False)

    print(f"WROTE {len(per_job)} entries to {args.output}")
    for r in per_job:
        print(f"{r['task_key']}: matched={r['matched_coverage_file']}, pct={r['coverage_percent']}")

if __name__ == "__main__":
    main()