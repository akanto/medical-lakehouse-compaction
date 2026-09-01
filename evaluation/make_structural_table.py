#!/usr/bin/env python3
"""Print the paper's structural-cost table (Table III) from a campaign JSON.

The two structural metrics are captured per benchmark entry by
medical_lakehouse_compaction/metrics/collector.py from live S3A IOStatistics:

  n_req = s3_total_requests = action_http_get_request
                            + action_http_head_request
                            + object_list_request
  RAR   = bytes_on_wire (stream_read_total_bytes) / bytes_useful

Shaping changes timing rather than the query plan, so the table aggregates
over every cell of the grid and reports medians.

Usage:
    python evaluation/make_structural_table.py <campaign.json>
"""
import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

LAYOUTS = ["s1", "s2", "s3", "s4"]
WORKLOADS = ["w1", "w2", "w3"]
LABEL = {"s1": "L1", "s2": "L2", "s3": "L3", "s4": "L4"}


def structural_stats(results: list) -> dict:
    """{(layout, workload): {n_req, rar, n, req_spread, rar_spread}}."""
    grouped = defaultdict(list)
    for entry in results:
        grouped[(entry["strategy"], entry["workload"])].append(entry)

    stats = {}
    for key, entries in grouped.items():
        reqs = [e["s3_total_requests"] for e in entries]
        rars = [e["bytes_on_wire"] / e["bytes_useful"]
                for e in entries if e["bytes_useful"]]
        stats[key] = {
            "n_req": statistics.median(reqs),
            "rar": statistics.median(rars),
            "n": len(entries),
        }
    return stats


def fmt_rar(value: float) -> str:
    """One decimal at 10.0 and above
    (205.8, 44.2), two below it (1.04, 1.96, 0.98, 0.27). The earlier rule broke
    at 1.0 and rendered 1.04 as 1.0 and 1.96 as 2.0, which erased the difference
    the surrounding sentence was drawing. See CLAUDE.md in the paper repo."""
    return f"{value:.1f}" if value >= 10 else f"{value:.2f}"


def text_table(stats: dict) -> str:
    """The paper's Table III as a plain grid, one column pair per workload."""
    head = (f"{'Layout':<8}" +
            "".join(f"{w.upper() + ' n_req':>12}{w.upper() + ' RAR':>10}"
                    for w in WORKLOADS))
    rows = [head, "-" * len(head)]
    for layout in LAYOUTS:
        cells = ""
        for workload in WORKLOADS:
            s = stats[(layout, workload)]
            cells += f"{s['n_req']:>12,.0f}{fmt_rar(s['rar']):>10}"
        rows.append(f"{LABEL[layout]:<8}" + cells)
    return "\n".join(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("campaign_json")
    args = parser.parse_args()

    campaign = json.loads(Path(args.campaign_json).read_text())
    stats = structural_stats(campaign["results"])

    print("Table III — structural cost of one workload execution per layout")
    print(f"  medians over {len(campaign['results'])} entries "
          f"in {len(campaign.get('network', []))} network configurations\n")
    print(text_table(stats))
