#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import datetime
import json
import time
from medical_lakehouse_compaction.config import load_profile
from medical_lakehouse_compaction.spark_session import create_spark_session
from medical_lakehouse_compaction.ingestion.iceberg_writer import ingest_dicom_dir, TABLE_NAME
from medical_lakehouse_compaction.table_metrics import UID_METRICS_TRUNCATE

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="conf/profiles/dev.yaml")
    parser.add_argument("--dicom-dir", required=True)
    parser.add_argument("--table", default=TABLE_NAME)
    parser.add_argument("--output-dir", default="results")
    args = parser.parse_args()

    cfg = load_profile(args.profile)
    spark = create_spark_session(cfg)
    # Must match run_optimization.py: S2/S3/S4 are built from this table, but
    # each sets its own metrics at creation, so a mismatch here would leave S1
    # measured under a different configuration than the rest.
    # Build time, for the rewrite-cost break-even in the paper (Section VI).
    # Includes parsing the DICOM files from local disk, so it bounds the
    # ingestion cost rather than isolating the write.
    t = time.perf_counter()
    n = ingest_dicom_dir(spark, args.dicom_dir, args.table,
                         uid_metrics_truncate=cfg.get("uid_metrics_truncate",
                                                      UID_METRICS_TRUNCATE))
    elapsed = time.perf_counter() - t
    print(f"Ingested {n} slices into {args.table} in {elapsed:.2f} s")

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.output_dir)
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"build_times_ingest_{ts}.json"
    out_path.write_text(json.dumps({
        "timestamp": ts, "profile": args.profile, "dicom_dir": args.dicom_dir,
        "slices": n, "times": {"s1_s": elapsed}}, indent=2))
    print(f"Build time saved to {out_path}")
    spark.stop()
