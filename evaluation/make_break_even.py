#!/usr/bin/env python3
"""Print the rewrite-cost break-even of each consolidated layout.

The paper (Section V-C) sets the time to build L2, L3 and L4 against the time a
read saves on them, compared with the raw ingested table L1. For one workload,
layout and network configuration:

    saving     = median time on L1 - median time on the layout
    break-even = build time / saving, rounded up to whole reads

Medians are of the three pass totals, the aggregation of the figures and of
make_cell_stats.py. Where the layout is not faster than L1 the saving is not
positive and no break-even count exists; the table shows a dash, as Table V
does.

Build times come from scripts/run_optimization.py. L2 is charged its binpack
rewrite alone (`s2_steps.rewrite_s`): the clone of L1 before it exists only
because the experiment keeps L1 next to L2, and snapshot expiry is
housekeeping that does not change the layout. L3 and L4 are charged their full
write from L1. The builds ran on the unshaped link, so in a hybrid cloud, in
which the rewrite also crosses the WAN, every value here is a lower bound.

Usage:
    python evaluation/make_break_even.py <campaign.json> <build_times_optimize.json> [--cells]
"""
import argparse
import json
import math
import statistics
from pathlib import Path

from make_cell_stats import pass_values

LAYOUTS = ["s2", "s3", "s4"]
WORKLOADS = ["w1", "w2", "w3"]
LABEL = {"s1": "L1", "s2": "L2", "s3": "L3", "s4": "L4"}


def build_seconds(times: dict) -> dict:
    return {"s2": times["s2_steps"]["rewrite_s"],
            "s3": times["s3_s"], "s4": times["s4_s"]}


def break_even(results: list, build: dict) -> dict:
    """{(workload, layout): [(rate, latency, saving_s, reads or None), ...]}."""
    medians = {k: statistics.median(v) for k, v in pass_values(
        results, lambda e: (e["workload"], e["strategy"],
                            e["rate_gbit"], e["latency_ms"])).items()}
    cells = sorted({(r, l) for (_w, _s, r, l) in medians},
                   key=lambda c: (-c[0], c[1]))
    out = {}
    for workload in WORKLOADS:
        for layout in LAYOUTS:
            rows = []
            for rate, lat in cells:
                saving = (medians[(workload, "s1", rate, lat)]
                          - medians[(workload, layout, rate, lat)])
                reads = math.ceil(build[layout] / saving) if saving > 0 else None
                rows.append((rate, lat, saving, reads))
            out[(workload, layout)] = rows
    return out


def text_table(grid: dict) -> str:
    """The paper's Table V as a plain grid, one column triple per workload."""
    head = (f"{'Bandwidth':>9}{'Latency':>9}  " +
            "  ".join("".join(f"{w.upper() + ' ' + LABEL[l]:>8}" for l in LAYOUTS)
                      for w in WORKLOADS))
    rows = [head, "-" * len(head)]
    configs = [(rate, lat) for rate, lat, *_x in grid[(WORKLOADS[0], LAYOUTS[0])]]
    for i, (rate, lat) in enumerate(configs):
        if i and rate != configs[i - 1][0]:
            rows.append("")
        cells = "  ".join(
            "".join(f"{reads:>8,}" if (reads := grid[(w, l)][i][3]) else f"{'-':>8}"
                    for l in LAYOUTS)
            for w in WORKLOADS)
        rows.append(f"{rate:>4} Gb/s{lat:>6} ms  " + cells)
    return "\n".join(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("campaign_json")
    parser.add_argument("build_times_json")
    parser.add_argument("--cells", action="store_true",
                        help="also list every network configuration")
    args = parser.parse_args()

    results = json.loads(Path(args.campaign_json).read_text())["results"]
    build = build_seconds(
        json.loads(Path(args.build_times_json).read_text())["times"])

    print("Build time on the unshaped link (lower bounds for a WAN rewrite)")
    for layout in LAYOUTS:
        print(f"  {LABEL[layout]}  {build[layout]:8.2f} s")
    print()
    grid = break_even(results, build)
    print("Table V — break-even count of each consolidated layout")
    print("  executions after which the accumulated saving against L1 equals the "
          "build time;\n  a dash marks a configuration in which the layout is not "
          "faster than L1\n")
    print(text_table(grid))
    if args.cells:
        for (workload, layout), rows in grid.items():
            print(f"\n{workload.upper()} {LABEL[layout]}")
            for rate, lat, saving, r in rows:
                print(f"  {rate} Gb/s {lat:>2} ms  saving {saving:8.2f} s  "
                      f"{f'{r:,}' if r is not None else '-'}")
