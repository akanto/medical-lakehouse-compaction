from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BinaryType
)
import pydicom
from medical_lakehouse_compaction.ingestion.dicom_reader import dicom_to_row
from medical_lakehouse_compaction.table_metrics import UID_METRICS_TRUNCATE, write_with_uid_metrics

TABLE_NAME = "dicom.db.slices_s1"

SCHEMA = StructType([
    StructField("series_instance_uid", StringType(), False),
    StructField("sop_instance_uid", StringType(), False),
    StructField("patient_id", StringType(), True),
    StructField("study_instance_uid", StringType(), True),
    StructField("instance_number", IntegerType(), True),
    StructField("slice_location", DoubleType(), True),
    StructField("rows", IntegerType(), True),
    StructField("columns", IntegerType(), True),
    StructField("pixel_spacing", StringType(), True),
    StructField("image_position_patient", StringType(), True),
    StructField("pixel_data", BinaryType(), False),
    StructField("source_path", StringType(), True),
])

def ingest_dicom_dir(spark: SparkSession, dicom_dir: str, table_name: str = TABLE_NAME,
                     uid_metrics_truncate: int | None = UID_METRICS_TRUNCATE) -> int:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS dicom.db")

    dicom_root = Path(dicom_dir)
    series_dirs = sorted(d for d in dicom_root.iterdir() if d.is_dir())

    total = 0
    first_write = True
    for i, series_dir in enumerate(series_dirs):
        dcm_paths = sorted(series_dir.glob("*.dcm"))
        rows = [dicom_to_row(pydicom.dcmread(str(p), force=True), str(p)) for p in dcm_paths]
        if not rows:
            continue
        df = spark.createDataFrame(rows, schema=SCHEMA)
        # S1: one Parquet file per slice — range-partition on unique sop_instance_uid
        # so Spark assigns exactly one slice per partition (no hash collisions)
        # Create on the first write that actually happens (not loop index 0,
        # which may be skipped) so stale data from a previous run never
        # survives. The UID metrics properties ride that creation — see
        # medical_lakehouse_compaction/table_metrics.py; manifests written under Iceberg's truncate(16)
        # default would keep truncated bounds for the whole table's life.
        slice_df = df.repartitionByRange(len(rows), "sop_instance_uid")
        if first_write:
            write_with_uid_metrics(slice_df, table_name, uid_metrics_truncate)
            first_write = False
        else:
            slice_df.writeTo(table_name).append()
        total += len(rows)
        print(f"  [{i+1}/{len(series_dirs)}] ingested {len(rows)} slices from {series_dir.name[:40]}...")

    return total
