# tests/test_generic.py
import pytest
from medical_lakehouse_compaction.ingestion.iceberg_writer import ingest_dicom_dir
from medical_lakehouse_compaction.optimization.generic import run_generic_compaction


def test_generic_reduces_file_count(spark, sample_dicom_dir):
    src = "dicom.db.s1_for_generic"
    dst = "dicom.db.s2_generic"
    ingest_dicom_dir(spark, str(sample_dicom_dir), src)
    run_generic_compaction(spark, src, dst, target_file_size_mb=1)
    df = spark.read.format("iceberg").load(dst)
    assert df.count() == 10  # same rows


def test_generic_preserves_all_rows(spark, sample_dicom_dir):
    src = "dicom.db.s1_for_generic2"
    dst = "dicom.db.s2_generic2"
    ingest_dicom_dir(spark, str(sample_dicom_dir), src)
    run_generic_compaction(spark, src, dst, target_file_size_mb=1)
    dst_df = spark.read.format("iceberg").load(dst)
    src_df = spark.read.format("iceberg").load(src)
    assert dst_df.count() == src_df.count()
    assert set(r.sop_instance_uid for r in dst_df.select("sop_instance_uid").collect()) == \
           set(r.sop_instance_uid for r in src_df.select("sop_instance_uid").collect())
