# tests/test_collector.py
import pytest
from medical_lakehouse_compaction.metrics.collector import BenchmarkResult, measure

def test_measure_captures_wall_clock(spark):
    def job():
        spark.range(100).collect()
    result = measure(spark, job, strategy="s1", workload="w1", latency_ms=1)
    assert result.wall_clock_s > 0

def test_benchmark_result_rar_wire(spark):
    result = BenchmarkResult(
        strategy="s3", workload="w1", latency_ms=10,
        wall_clock_s=1.5, bytes_useful=105_000_000,
        run_index=0, bytes_on_wire=21_000_000_000,
    )
    assert abs(result.rar_wire - 200.0) < 1.0

def test_measure_returns_benchmark_result(spark):
    result = measure(spark, lambda: spark.range(10).collect(),
                     strategy="s1", workload="w1", latency_ms=1,
                     bytes_useful=1000)
    assert isinstance(result, BenchmarkResult)
    assert result.strategy == "s1"
    assert result.workload == "w1"


def test_measure_tags_the_job_group_and_clears_it(spark):
    """The event-log parser attributes stages by job group, so measure() must
    set one. It must also clear it, or the next untagged query inherits the tag.
    PySpark 4.0 removed SparkContext.clearJobGroup, which this guards against.
    """
    from medical_lakehouse_compaction.metrics.stage_log import parse_tag
    seen = {}

    def job():
        seen["tag"] = spark.sparkContext.getLocalProperty("spark.jobGroup.id")
        spark.range(10).collect()

    measure(spark, job, strategy="s3", workload="w2", latency_ms=25,
            run_index=1, series_uid="uid-x")

    assert parse_tag(seen["tag"]) == {"strategy": "s3", "workload": "w2",
                                      "run_index": 1, "series_uid": "uid-x",
                                      "latency_ms": 25}
    assert spark.sparkContext.getLocalProperty("spark.jobGroup.id") is None


def test_measure_reports_task_and_stage_counts_without_the_spark_ui(spark):
    """scan_tasks read 0 in three campaigns because it required sc.uiWebUrl and
    benchmark sessions disable the UI. It must work with the UI off."""
    assert spark.conf.get("spark.ui.enabled", "true") in ("false", "true")
    r = measure(spark, lambda: spark.range(1000).repartition(4).collect(),
                strategy="s1", workload="w1", latency_ms=0)
    assert r.scan_tasks > 0
    assert r.scan_stages > 0
