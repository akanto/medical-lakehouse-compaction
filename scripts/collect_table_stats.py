#!/usr/bin/env python3
"""Collect per-strategy table-shape statistics and layout fingerprints.

For each strategy table this reads the Iceberg `files` metadata table (exact
data-file count, row count, per-file size distribution — no object-store
listing involved) and runs one unshaped W1 + W3 through the standard
measure() instrumentation. The GET counts of those runs are layout
fingerprints: they are cell-invariant and near-deterministic for a given
physical layout (W1 GETs exact per (strategy, series)
across all 12 cells, W3 within a 3-GET band), so they certify that a rebuilt
table has the same shape the campaign measured. Each fingerprint query runs
twice — the first pass absorbs JVM/S3A/Iceberg-metadata warm-up, matching the
campaign's warm steady-state protocol; only the second is recorded.

Requires the tables built by run_ingestion.py + run_optimization.py. Applies
no shaping and must run on an UNSHAPED network (fingerprints only count
round-trips, but S1's full scans take hours when shaped).

Writes results/table_shapes_raw_<ts>.json for local post-processing."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import datetime
import json
import statistics

from medical_lakehouse_compaction.config import load_profile
from medical_lakehouse_compaction.spark_session import create_spark_session
from medical_lakehouse_compaction.benchmarks.runner import select_w1_series
from medical_lakehouse_compaction.benchmarks.reconstruction import run_w1_benchmark
from medical_lakehouse_compaction.benchmarks.cohort_scan import run_w3_benchmark

TABLES = {
    "s1": "dicom.db.slices_s1",
    "s2": "dicom.db.slices_s2",
    "s3": "dicom.db.slices_s3",
    "s4": "dicom.db.slices_s4",
}

FINGERPRINT_PASSES = ("warmup", "measured")


def table_shape(spark, table: str) -> dict:
    """Exact shape from the Iceberg `files` metadata table (data files only)."""
    rows = (
        spark.read.format("iceberg").load(f"{table}.files")
        .filter("content = 0")
        .select("file_size_in_bytes", "record_count")
        .collect()
    )
    sizes = sorted(r.file_size_in_bytes for r in rows)
    # method="inclusive": the default ("exclusive") extrapolates past the
    # observed range on small samples, so a two-file table reports a p25
    # below its own smallest file and a p75 above its largest.
    q = (statistics.quantiles(sizes, n=4, method="inclusive")
         if len(sizes) > 1 else [sizes[0]] * 3)
    snapshots = spark.read.format("iceberg").load(f"{table}.snapshots").count()
    return {
        "data_files": len(sizes),
        "row_count": sum(r.record_count for r in rows),
        "total_bytes": sum(sizes),
        "file_size_bytes": {
            "min": sizes[0],
            "p25": round(q[0]),
            "median": round(q[1]),
            "p75": round(q[2]),
            "max": sizes[-1],
            "mean": round(statistics.mean(sizes)),
        },
        "snapshots": snapshots,
    }


def fingerprint_fields(r) -> dict:
    return {
        "strategy": r.strategy,
        "workload": r.workload,
        "wall_clock_s": r.wall_clock_s,
        "scan_tasks": r.scan_tasks,
        "bytes_on_wire": r.bytes_on_wire,
        "s3_get_requests": r.s3_get_requests,
        "s3_total_requests": r.s3_total_requests,
        "series_uid": r.series_uid,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="conf/profiles/experiment.yaml")
    parser.add_argument("--output-dir", default="results")
    parser.add_argument("--tables", default=",".join(TABLES),
                        help="comma-separated strategy keys to inspect")
    parser.add_argument("--w1-uid", default=None,
                        help="series UID for the W1 fingerprint (default: "
                             "median of the standard 5-series selection)")
    parser.add_argument("--no-fingerprints", action="store_true",
                        help="shapes only (no W1/W3 runs)")
    args = parser.parse_args()

    cfg = load_profile(args.profile)
    spark = create_spark_session(cfg)
    keys = [k.strip() for k in args.tables.split(",")]

    shapes = {}
    for key in keys:
        shapes[key] = table_shape(spark, TABLES[key])
        s = shapes[key]
        print(f"  shape {key}: {s['data_files']} data files, "
              f"{s['row_count']} rows, {s['total_bytes'] / 2**30:.2f} GiB, "
              f"median file {s['file_size_bytes']['median'] / 2**20:.2f} MiB",
              flush=True)

    fingerprints = None
    if not args.no_fingerprints:
        w1_uid = args.w1_uid or select_w1_series(spark, 5)[2]
        print(f"W1 fingerprint series: {w1_uid}")
        runs = []
        for key in keys:
            table = TABLES[key]
            print(f"  fingerprint {key}", end="  ", flush=True)
            # bytes_useful=0 skips the setup aggregation (an extra unprunable
            # full scan on S1); fingerprints only need request counts.
            for pass_name in FINGERPRINT_PASSES:
                r = run_w1_benchmark(spark, table, w1_uid, strategy=key,
                                     latency_ms=0, bytes_useful=0)
                runs.append({**fingerprint_fields(r), "pass": pass_name})
                print(".", end="", flush=True)
            for pass_name in FINGERPRINT_PASSES:
                r = run_w3_benchmark(spark, table, strategy=key,
                                     latency_ms=0, bytes_useful=0)
                runs.append({**fingerprint_fields(r), "pass": pass_name})
                print(".", end="", flush=True)
            print()
        fingerprints = {"w1_series_uid": w1_uid, "runs": runs}

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.output_dir)
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"table_shapes_raw_{ts}.json"
    out_path.write_text(json.dumps({
        "timestamp": ts,
        "profile": args.profile,
        "n_series": cfg.get("n_series"),
        "tables": shapes,
        "fingerprints": fingerprints,
    }, indent=2))
    print(f"\nResults saved to {out_path}")
    spark.stop()
