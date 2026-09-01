# tests/test_iceberg_writer.py
import pytest
from medical_lakehouse_compaction.ingestion.iceberg_writer import ingest_dicom_dir, TABLE_NAME

def test_ingest_creates_table(spark, sample_dicom_dir):
    ingest_dicom_dir(spark, str(sample_dicom_dir), TABLE_NAME + "_test_ingest")
    df = spark.read.format("iceberg").load(TABLE_NAME + "_test_ingest")
    assert df.count() == 10  # 2 series × 5 slices

def test_ingest_schema_has_pixel_data(spark, sample_dicom_dir):
    ingest_dicom_dir(spark, str(sample_dicom_dir), TABLE_NAME + "_test_schema")
    df = spark.read.format("iceberg").load(TABLE_NAME + "_test_schema")
    assert "pixel_data" in df.columns
    assert "series_instance_uid" in df.columns

def test_ingest_one_file_per_slice(spark, sample_dicom_dir):
    table = TABLE_NAME + "_test_files"
    ingest_dicom_dir(spark, str(sample_dicom_dir), table)
    df = spark.read.format("iceberg").load(table)
    assert df.count() == 10

def test_ingest_overwrites_stale_table_when_first_dir_empty(spark, sample_dicom_dir):
    table = TABLE_NAME + "_test_stale"
    ingest_dicom_dir(spark, str(sample_dicom_dir), table)
    # An empty dir sorting first must not turn the first real write into an
    # append onto the stale table from the previous ingestion.
    (sample_dicom_dir / "0_empty_series").mkdir()
    ingest_dicom_dir(spark, str(sample_dicom_dir), table)
    df = spark.read.format("iceberg").load(table)
    assert df.count() == 10
