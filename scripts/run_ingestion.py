#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
from medical_lakehouse_compaction.config import load_profile
from medical_lakehouse_compaction.spark_session import create_spark_session
from medical_lakehouse_compaction.ingestion.iceberg_writer import ingest_dicom_dir, TABLE_NAME
from medical_lakehouse_compaction.table_metrics import UID_METRICS_TRUNCATE

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="conf/profiles/dev.yaml")
    parser.add_argument("--dicom-dir", required=True)
    parser.add_argument("--table", default=TABLE_NAME)
    args = parser.parse_args()

    cfg = load_profile(args.profile)
    spark = create_spark_session(cfg)
    # Must match run_optimization.py: S2/S3/S4 are built from this table, but
    # each sets its own metrics at creation, so a mismatch here would leave S1
    # measured under a different configuration than the rest.
    n = ingest_dicom_dir(spark, args.dicom_dir, args.table,
                         uid_metrics_truncate=cfg.get("uid_metrics_truncate",
                                                      UID_METRICS_TRUNCATE))
    print(f"Ingested {n} slices into {args.table}")
    spark.stop()
