#!/usr/bin/env python3
import argparse
import csv
import re
from pathlib import Path
import xml.etree.ElementTree as ET

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

    for file_el in root.findall(".//file"):
        name = file_el.get("name") or file_el.get("filename")
        if not name:
            continue
        covered, total = count_lines(file_el.findall(".//line"))
        name = name.replace("\\", "/")
        files[name] = {
            "covered": covered,
            "total": total,
            "pct": round((covered / total) * 100, 2) if total > 0 else 0.0
        }

    return files

def build_basename_map(files_map):
    bmap = {}
    for k, v in files_map.items():
        bmap.setdefault(Path(k).name.lower(), []).append((k, v))
    return bmap

def norm_name(stem: str) -> str:
    # NB_PATRIF_REVMOV_0100_EXT_TRN_FOV -> nb_patrif_revmov_0100_ext_trn_fov
    s = stem.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def find_cov_for_basename(basename: str, basename_map):
    """
    Returns best match (path, covered, total, pct) by basename (case-insensitive).
    """
    key = basename.lower()
    if key not in basename_map:
        return (None, 0, 0, 0.0)
    # if duplicates, choose the one with most total lines
    best = sorted(basename_map[key], key=lambda kv: kv[1].get("total", 0), reverse=True)[0]
    path, info = best
    return (path, info.get("covered", 0), info.get("total", 0), info.get("pct", 0.0))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coverage-xml", required=True)
    ap.add_argument("--notebooks-dir", default="notebooks")
    ap.add_argument("--tests-dir", default="tests")
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    cov = parse_coverage_xml(args.coverage_xml)
    cov_by_base = build_basename_map(cov)

    notebooks = sorted(Path(args.notebooks_dir).rglob("*.py"))
    tests = sorted(Path(args.tests_dir).rglob("test_*.py"))

    # index tests by normalized signature
    # test_nb_xxx.py -> "nb_xxx"
    # test_xxx.py    -> "xxx"
    tests_by_sig = {}
    for tf in tests:
        base = tf.name.lower()  # test_....
        stem = tf.stem.lower()  # test_....
        # remove leading "test_"
        sig = stem[5:] if stem.startswith("test_") else stem
        tests_by_sig.setdefault(sig, []).append(tf.as_posix())

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "notebook_file",
            "matched_test_file",
            "test_coverage_percent",
            "notebook_coverage_percent",
            "notebook_in_coverage_xml"
        ])

        for nb in notebooks:
            nb_rel = nb.as_posix()
            nb_stem = nb.stem  # NB_PATRIF...
            sig = norm_name(nb_stem)  # nb_patrif...

            # candidates: test_<sig>.py AND test_nb_<sig>.py AND test_<sig_without_nb_prefix>.py
            candidates = []
            candidates.append(sig)                 # e.g. nb_patrif...
            candidates.append(f"nb_{sig}")         # extra-safe (rare)
            if sig.startswith("nb_"):
                candidates.append(sig[3:])         # patrif... (if tests omit nb_)
            # also common pattern: exact notebook stem lower
            candidates.append(nb_stem.lower())

            # pick first existing candidate in tests_by_sig
            matched_test = None
            for c in candidates:
                if c in tests_by_sig and tests_by_sig[c]:
                    matched_test = tests_by_sig[c][0]  # if multiple, take first
                    break

            # test coverage (from coverage.xml) – based on matched_test basename
            test_pct = 0.0
            if matched_test:
                test_base = Path(matched_test).name
                _, _, _, test_pct = find_cov_for_basename(test_base, cov_by_base)

            # notebook coverage (from coverage.xml) – based on notebook basename
            nb_base = nb.name
            _, _, _, nb_pct = find_cov_for_basename(nb_base, cov_by_base)
            in_cov = (nb_base.lower() in cov_by_base)

            # rule: if notebook not in coverage.xml -> notebook_coverage_percent = 0
            if not in_cov:
                nb_pct = 0.0

            w.writerow([
                nb_rel,
                matched_test,
                test_pct,
                nb_pct,
                "true" if in_cov else "false"
            ])

    print(f"WROTE {args.output}")

if __name__ == "__main__":
    main()