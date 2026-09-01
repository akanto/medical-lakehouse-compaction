#!/usr/bin/env python3
"""Print the paper's layout-shape table (Table II) from a table-shapes JSON.

The input is written on the Spark host by scripts/collect_table_stats.py, which
reads the four Iceberg tables' file lists after ingestion and compaction. It is
committed because collecting it again needs the whole live testbed, while the
table it produces is three lines of arithmetic over 24 numbers.

Sizes are reported in decimal MB and GB (10^6 and 10^9 bytes), matching the
paper: the file-size quartiles are compared against the 256 MB compaction
target, which Iceberg's `write.target-file-size-bytes` also counts decimally.

    python evaluation/make_shapes_table.py <table_shapes_raw.json>
"""
import argparse
import json
from pathlib import Path

LAYOUTS = ["s1", "s2", "s3", "s4"]
LABEL = {"s1": "L1", "s2": "L2", "s3": "L3", "s4": "L4"}
MB, GB = 1e6, 1e9


def shape_stats(tables: dict) -> dict:
    """{layout: {files, p25, median, p75, largest, data_gb}}, sizes in MB/GB."""
    stats = {}
    for layout in LAYOUTS:
        t = tables[layout]
        size = t["file_size_bytes"]
        stats[layout] = {
            "files": t["data_files"],
            "p25": size["p25"] / MB,
            "median": size["median"] / MB,
            "p75": size["p75"] / MB,
            "largest": size["max"] / MB,
            "data_gb": t["total_bytes"] / GB,
        }
    return stats


def text_table(stats: dict) -> str:
    head = (f"{'Layout':<8}{'Files':>9}{'p25 (MB)':>11}{'Median (MB)':>13}"
            f"{'p75 (MB)':>11}{'Largest (MB)':>14}{'Data (GB)':>11}")
    rows = [head, "-" * len(head)]
    for layout in LAYOUTS:
        s = stats[layout]
        rows.append(f"{LABEL[layout]:<8}{s['files']:>9,}{s['p25']:>11.2f}"
                    f"{s['median']:>13.2f}{s['p75']:>11.2f}"
                    f"{s['largest']:>14.2f}{s['data_gb']:>11.2f}")
    return "\n".join(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("shapes_json")
    args = ap.parse_args()

    raw = json.loads(Path(args.shapes_json).read_text())
    stats = shape_stats(raw["tables"])

    print("Table II — measured properties of the four layout tables")
    print(f"  {raw['n_series']} series, "
          f"{raw['tables']['s1']['row_count']:,} slices\n")
    print(text_table(stats))


if __name__ == "__main__":
    main()
