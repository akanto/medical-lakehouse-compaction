import pytest
from medical_lakehouse_compaction.ingestion.iceberg_writer import ingest_dicom_dir
from medical_lakehouse_compaction.optimization.domain_aware import run_domain_compaction


def test_domain_preserves_all_rows(spark, sample_dicom_dir):
    src = "dicom.db.s1_for_domain"
    dst = "dicom.db.s3_domain"
    ingest_dicom_dir(spark, str(sample_dicom_dir), src)
    run_domain_compaction(spark, src, dst)
    df = spark.read.format("iceberg").load(dst)
    assert df.count() == 10


def test_domain_one_file_per_series(spark, sample_dicom_dir):
    src = "dicom.db.s1_for_domain3"
    dst = "dicom.db.s3_domain3"
    ingest_dicom_dir(spark, str(sample_dicom_dir), src)
    run_domain_compaction(spark, src, dst)
    n_series = (spark.read.format("iceberg").load(dst)
                .select("series_instance_uid").distinct().count())
    n_files = spark.read.format("iceberg").load(f"{dst}.files").count()
    assert n_files == n_series


def test_domain_slices_ordered_within_series(spark, sample_dicom_dir):
    src = "dicom.db.s1_for_domain2"
    dst = "dicom.db.s3_domain2"
    ingest_dicom_dir(spark, str(sample_dicom_dir), src)
    run_domain_compaction(spark, src, dst)
    df = spark.read.format("iceberg").load(dst)
    from pyspark.sql.functions import col
    for row in df.select("series_instance_uid").distinct().collect():
        series_df = df.filter(col("series_instance_uid") == row.series_instance_uid) \
                      .select("instance_number").collect()
        numbers = [r.instance_number for r in series_df]
        assert numbers == sorted(numbers)
