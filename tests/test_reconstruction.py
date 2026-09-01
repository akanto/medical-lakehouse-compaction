# tests/test_reconstruction.py
import numpy as np
import pytest
from medical_lakehouse_compaction.ingestion.iceberg_writer import ingest_dicom_dir
from medical_lakehouse_compaction.optimization.domain_aware import run_domain_compaction
from medical_lakehouse_compaction.benchmarks.reconstruction import reconstruct_volume, run_w1_benchmark
from medical_lakehouse_compaction.metrics.collector import BenchmarkResult

@pytest.fixture
def s3_table(spark, sample_dicom_dir):
    ingest_dicom_dir(spark, str(sample_dicom_dir), "dicom.db.s1_w1")
    run_domain_compaction(spark, "dicom.db.s1_w1", "dicom.db.s3_w1")
    return "dicom.db.s3_w1"

def test_reconstruct_returns_3d_array(spark, s3_table):
    series_uid = spark.read.format("iceberg").load(s3_table) \
        .select("series_instance_uid").first().series_instance_uid
    volume = reconstruct_volume(spark, s3_table, series_uid)
    assert volume.ndim == 3
    assert volume.shape[0] == 5   # 5 slices in fixture
    assert volume.shape[1] == 32  # rows from make_dicom_slice
    assert volume.shape[2] == 32  # cols from make_dicom_slice

def test_reconstruct_dtype_is_int16(spark, s3_table):
    series_uid = spark.read.format("iceberg").load(s3_table) \
        .select("series_instance_uid").first().series_instance_uid
    volume = reconstruct_volume(spark, s3_table, series_uid)
    assert volume.dtype == np.int16

def test_w1_benchmark_returns_result(spark, s3_table):
    series_uid = spark.read.format("iceberg").load(s3_table) \
        .select("series_instance_uid").first().series_instance_uid
    result = run_w1_benchmark(spark, s3_table, series_uid,
                              strategy="s3", latency_ms=1)
    assert isinstance(result, BenchmarkResult)
    assert result.workload == "w1"
    assert result.wall_clock_s > 0
