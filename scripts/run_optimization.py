#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
from medical_lakehouse_compaction.config import load_profile
from medical_lakehouse_compaction.spark_session import create_spark_session
from medical_lakehouse_compaction.optimization.generic import run_generic_compaction
from medical_lakehouse_compaction.optimization.domain_aware import run_domain_compaction
from medical_lakehouse_compaction.optimization.partition_spec import run_partition_spec_compaction
from medical_lakehouse_compaction.table_metrics import UID_METRICS_TRUNCATE

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="conf/profiles/dev.yaml")
    parser.add_argument("--source", default="dicom.db.slices_s1")
    parser.add_argument("--s2-table", default="dicom.db.slices_s2")
    parser.add_argument("--s3-table", default="dicom.db.slices_s3")
    parser.add_argument("--s4-table", default="dicom.db.slices_s4")
    args = parser.parse_args()

    cfg = load_profile(args.profile)
    spark = create_spark_session(cfg)

    # Campaign-level setting: every table in a build carries the same UID
    # manifest-bound length, so the strategies differ only by layout. To
    # measure Iceberg's truncate(16) default instead, set uid_metrics_truncate
    # in the profile and rebuild + re-run the whole campaign — the setting must
    # ride table creation, so it can never be mixed within one build
    # (medical_lakehouse_compaction/table_metrics.py).
    truncate = cfg.get("uid_metrics_truncate", UID_METRICS_TRUNCATE)
    print(f"UID manifest bounds: truncate({truncate})" if truncate is not None
          else "UID manifest bounds: Iceberg default truncate(16)")

    print("Running generic compaction (S2, rewrite_data_files binpack)...")
    run_generic_compaction(spark, args.source, args.s2_table,
                           cfg["target_file_size_mb"], uid_metrics_truncate=truncate)
    print("Running domain-aware compaction (S3)...")
    run_domain_compaction(spark, args.source, args.s3_table,
                          uid_metrics_truncate=truncate)
    print("Running native partition spec compaction (S4)...")
    run_partition_spec_compaction(spark, args.source, args.s4_table,
                                  uid_metrics_truncate=truncate)
    print("Done.")
    spark.stop()
