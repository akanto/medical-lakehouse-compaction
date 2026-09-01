#!/usr/bin/env python3
"""List the dispersion behind every cell of the campaign.

The paper reports medians throughout, and cites this listing for the spread
those medians were taken over. Two levels are printed:

  Pass totals. One pass is one end-to-end execution of the grid, so a pass
  total sums the sixty cells of one workload: four layouts by fifteen network
  configurations. The three totals give the paper's reproducibility table.

  Per cell. A cell is one (workload, layout, bandwidth limit, round-trip
  time), measured in three passes. A pass value is the total wall-clock time
  of that pass, which for W1 sums the five series reconstructed in turn and
  for W2 and W3 is the single execution. This is the aggregation the figures
  and Table III use, so the medians listed here are the plotted ones.

The standard deviation is the sample deviation over the three passes, and CV
is that deviation over the mean. With n = 3 both are coarse estimates; they
bound the repeatability of a cell rather than describe a distribution.

Usage:
    python evaluation/make_cell_stats.py <campaign.json> [--csv PATH]
"""
import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

LAYOUTS = ["s1", "s2", "s3", "s4"]
WORKLOADS = ["w1", "w2", "w3"]
LABEL = {"s1": "L1", "s2": "L2", "s3": "L3", "s4": "L4"}


def pass_values(results: list, key) -> dict:
    """{key(entry): [pass total, ...]}, one total per repetition."""
    passes = defaultdict(float)
    for entry in results:
        passes[(key(entry), entry["run_index"])] += entry["wall_clock_s"]

    grouped = defaultdict(list)
    for (k, _run), total in passes.items():
        grouped[k].append(total)
    return {k: sorted(v) for k, v in grouped.items()}


def describe(values: list) -> dict:
    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    return {"n": len(values), "median": statistics.median(values),
            "mean": mean, "stdev": stdev,
            "cv": stdev / mean if mean else 0.0}


def pass_table(results: list) -> str:
    """Total execution time of one pass over all four layouts and all cells."""
    totals = pass_values(results, lambda e: e["workload"])
    head = (f"{'Workload':<10}{'Median (s)':>13}{'Mean (s)':>13}"
            f"{'Std. dev. (s)':>15}{'CV':>8}")
    rows = [head, "-" * len(head)]
    for workload in WORKLOADS:
        s = describe(totals[workload])
        rows.append(f"{workload.upper():<10}{s['median']:>13.2f}"
                    f"{s['mean']:>13.2f}{s['stdev']:>15.2f}"
                    f"{s['cv']:>7.1%}")
    return "\n".join(rows)


def cell_rows(results: list) -> list:
    """One row per (workload, layout, rate, latency), in reading order."""
    cells = pass_values(results, lambda e: (e["workload"], e["strategy"],
                                            e["rate_gbit"], e["latency_ms"]))
    rows = []
    for workload in WORKLOADS:
        for layout in LAYOUTS:
            keys = [k for k in cells if k[0] == workload and k[1] == layout]
            for key in sorted(keys, key=lambda k: (-k[2], k[3])):
                s = describe(cells[key])
                rows.append({"workload": workload, "layout": LABEL[layout],
                             "rate_gbit": key[2], "latency_ms": key[3], **s})
    return rows


def cell_table(rows: list) -> str:
    head = (f"{'Workload':<10}{'Layout':<8}{'Gb/s':>6}{'RTT (ms)':>10}"
            f"{'n':>4}{'Median (s)':>13}{'Mean (s)':>13}"
            f"{'Std. dev. (s)':>15}{'CV':>8}")
    out = [head, "-" * len(head)]
    for r in rows:
        out.append(f"{r['workload'].upper():<10}{r['layout']:<8}"
                   f"{r['rate_gbit']:>6}{r['latency_ms']:>10}{r['n']:>4}"
                   f"{r['median']:>13.2f}{r['mean']:>13.2f}"
                   f"{r['stdev']:>15.2f}{r['cv']:>7.1%}")
    return "\n".join(out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("campaign_json")
    parser.add_argument("--csv", metavar="PATH",
                        help="also write the per-cell rows to PATH")
    args = parser.parse_args()

    campaign = json.loads(Path(args.campaign_json).read_text())
    results = campaign["results"]
    rows = cell_rows(results)

    print("Total execution time of one pass over all four layouts and all "
          "fifteen network configurations")
    print(f"  three passes, {len(results)} entries in total\n")
    print(pass_table(results))
    print()
    print("Per cell: median, mean and standard deviation over the three passes")
    print(f"  {len(rows)} cells = 3 workloads x 4 layouts x 15 network "
          "configurations\n")
    print(cell_table(rows))

    if args.csv:
        path = Path(args.csv)
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nwrote {path}")
