#!/usr/bin/env python3
"""Merge the per-rate-row benchmark JSONs from a split campaign into one file.

The 200-series campaign runs as 4 fresh-JVM launches (rate rows none/5/2/1 — see
the kernel-OOM history in the runbook). Each writes its own timestamped
benchmark_<ts>.json with that row's cells. This concatenates their `results` and
`network` arrays into a single file, after checking protocol identity across the
launches (same W1 series selection, same n_series) — a coherent merge is only
valid if every launch benchmarked the identical setup.

Usage:
  python scripts/merge_rate_rows.py OUT.json IN_none.json IN_5.json IN_2.json IN_1.json
"""
import json
import sys


def main() -> int:
    out_path, *inputs = sys.argv[1:]
    if len(inputs) < 2:
        print("need an output path and >=2 input JSONs", file=sys.stderr)
        return 2

    docs = [json.load(open(p)) for p in inputs]
    base = docs[0]

    # Protocol identity: a merge across launches is only meaningful if they
    # benchmarked the same tables and W1 selection.
    for p, d in zip(inputs[1:], docs[1:]):
        if d.get("w1_series_uids") != base.get("w1_series_uids"):
            print(f"ABORT: w1_series_uids differ in {p} — not a coherent merge",
                  file=sys.stderr)
            return 1
        if d.get("n_series") != base.get("n_series"):
            print(f"ABORT: n_series differ in {p} "
                  f"({d.get('n_series')} vs {base.get('n_series')})", file=sys.stderr)
            return 1

    merged = dict(base)  # carry top-level metadata from the first launch
    merged["results"] = [r for d in docs for r in d["results"]]
    merged["network"] = [n for d in docs for n in d.get("network", [])]
    merged["merged_from"] = inputs

    # Guard against accidentally merging overlapping rows twice.
    cells = [(n["rate_gbit"], n["latency_ms_nominal"]) for n in merged["network"]]
    if len(cells) != len(set(cells)):
        dupes = sorted({c for c in cells if cells.count(c) > 1})
        print(f"ABORT: duplicate cells across inputs: {dupes}", file=sys.stderr)
        return 1

    json.dump(merged, open(out_path, "w"), indent=2)
    print(f"merged {len(inputs)} launches -> {out_path}: "
          f"{len(merged['results'])} results, {len(merged['network'])} network levels")
    return 0


if __name__ == "__main__":
    sys.exit(main())
