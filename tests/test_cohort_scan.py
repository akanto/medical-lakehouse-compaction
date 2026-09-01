import pytest
from medical_lakehouse_compaction.ingestion.iceberg_writer import ingest_dicom_dir
from medical_lakehouse_compaction.benchmarks.cohort_scan import COHORT_COLUMNS, run_cohort_scan, run_w3_benchmark
from medical_lakehouse_compaction.metrics.collector import BenchmarkResult


@pytest.fixture
def s1_table(spark, sample_dicom_dir):
    ingest_dicom_dir(spark, str(sample_dicom_dir), "dicom.db.s1_w3")
    return "dicom.db.s1_w3"


def test_cohort_scan_returns_all_rows(spark, s1_table):
    rows = run_cohort_scan(spark, s1_table)
    assert len(rows) == 10  # 2 series × 5 slices


def test_cohort_scan_columns_present(spark, s1_table):
    rows = run_cohort_scan(spark, s1_table)
    assert len(rows) > 0
    for col in COHORT_COLUMNS:
        assert hasattr(rows[0], col)


def test_cohort_scan_no_pixel_data(spark, s1_table):
    rows = run_cohort_scan(spark, s1_table)
    assert not hasattr(rows[0], "pixel_data")


def test_w3_benchmark_returns_result(spark, s1_table):
    result = run_w3_benchmark(spark, s1_table, strategy="s1", latency_ms=0)
    assert isinstance(result, BenchmarkResult)
    assert result.workload == "w3"
    assert result.wall_clock_s > 0


def test_w3_bytes_useful_excludes_pixel_data(spark, s1_table):
    result = run_w3_benchmark(spark, s1_table, strategy="s1", latency_ms=0)
    # 10 rows × (UID ~10 bytes + patient_id ~7 bytes + 12 bytes numeric) = well under 1 MB
    assert 0 < result.bytes_useful < 1024 * 1024


def test_w3_run_index_recorded(spark, s1_table):
    result = run_w3_benchmark(spark, s1_table, strategy="s1", latency_ms=0, run_index=2)
    assert result.run_index == 2
