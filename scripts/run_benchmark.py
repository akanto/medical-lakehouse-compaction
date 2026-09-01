#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
from medical_lakehouse_compaction.config import load_profile
from medical_lakehouse_compaction.spark_session import create_spark_session
from medical_lakehouse_compaction.benchmarks.runner import select_w1_series, run_all_workloads, save_results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="conf/profiles/local.yaml")
    parser.add_argument("--output-dir", default="results")
    parser.add_argument("--series-uid", default=None,
                        help="SeriesInstanceUID for W1; auto-detected if omitted")
    args = parser.parse_args()

    cfg = load_profile(args.profile)
    spark = create_spark_session(cfg)

    if args.series_uid:
        series_uids = [args.series_uid]
    else:
        series_uids = select_w1_series(spark, cfg.get("w1_n_series", 1))
    print(f"W1 series ({len(series_uids)}, size-ordered): "
          f"...{series_uids[0][-24:]} .. ...{series_uids[-1][-24:]}")

    results = []
    for latency_ms in cfg["latency_ms"]:
        print(f"\n=== Simulated latency: {latency_ms}ms ===")
        results += run_all_workloads(spark, cfg, series_uids, latency_ms)

    out_path = save_results(results, args.output_dir, args.profile, series_uids, cfg)
    print(f"\nResults saved to {out_path}")
    spark.stop()
