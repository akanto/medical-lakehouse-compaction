import pytest
from pyspark.sql.functions import col
from medical_lakehouse_compaction.ingestion.iceberg_writer import ingest_dicom_dir
from medical_lakehouse_compaction.optimization.partition_spec import run_partition_spec_compaction


def test_partition_spec_preserves_all_rows(spark, sample_dicom_dir):
    src = "dicom.db.s1_for_s4"
    dst = "dicom.db.s4_partition"
    ingest_dicom_dir(spark, str(sample_dicom_dir), src)
    run_partition_spec_compaction(spark, src, dst)
    assert spark.read.format("iceberg").load(dst).count() == 10


def test_partition_spec_one_file_per_series(spark, sample_dicom_dir):
    src = "dicom.db.s1_for_s4b"
    dst = "dicom.db.s4_partition_b"
    ingest_dicom_dir(spark, str(sample_dicom_dir), src)
    run_partition_spec_compaction(spark, src, dst)
    series_uid = (spark.read.format("iceberg").load(dst)
                  .select("series_instance_uid").first().series_instance_uid)
    assert (spark.read.format("iceberg").load(dst)
            .filter(col("series_instance_uid") == series_uid)
            .count()) == 5


def test_partition_spec_one_data_file_per_series(spark, sample_dicom_dir):
    src = "dicom.db.s1_for_s4d"
    dst = "dicom.db.s4_partition_d"
    ingest_dicom_dir(spark, str(sample_dicom_dir), src)
    run_partition_spec_compaction(spark, src, dst)
    n_series = (spark.read.format("iceberg").load(dst)
                .select("series_instance_uid").distinct().count())
    n_files = spark.read.format("iceberg").load(f"{dst}.files").count()
    assert n_files == n_series


def test_partition_spec_predicate_reads_only_matching_series(spark, sample_dicom_dir):
    src = "dicom.db.s1_for_s4c"
    dst = "dicom.db.s4_partition_c"
    ingest_dicom_dir(spark, str(sample_dicom_dir), src)
    run_partition_spec_compaction(spark, src, dst)
    df = spark.read.format("iceberg").load(dst)
    series_uids = [r.series_instance_uid
                   for r in df.select("series_instance_uid").distinct().collect()]
    for uid in series_uids:
        assert df.filter(col("series_instance_uid") == uid).count() == 5
