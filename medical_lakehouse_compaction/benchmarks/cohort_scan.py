from pyspark.sql import SparkSession
from pyspark.sql.functions import count as spark_count, sum as spark_sum, length
from medical_lakehouse_compaction.metrics.collector import BenchmarkResult, measure

COHORT_COLUMNS = ["series_instance_uid", "patient_id", "instance_number", "slice_location"]


def run_cohort_scan(spark: SparkSession, table_path: str) -> list:
    return (
        spark.read.format("iceberg").load(table_path)
        .select(*COHORT_COLUMNS)
        .collect()
    )


def cohort_bytes_useful(spark: SparkSession, table_path: str) -> int:
    """Decoded size of the cohort projection. Cell-invariant full-table
    aggregate: on S1 it scans all files, so grid sweeps should precompute it
    once, unshaped, not once per cell per run."""
    agg = (
        spark.read.format("iceberg").load(table_path)
        .agg(
            spark_count("*").alias("n"),
            spark_sum(length("series_instance_uid")).alias("uid_bytes"),
            spark_sum(length("patient_id")).alias("pid_bytes"),
        )
        .first()
    )
    # instance_number (int32 = 4 bytes) + slice_location (float64 = 8 bytes) per row
    return int(agg.uid_bytes) + int(agg.pid_bytes) + int(agg.n) * 12


def run_w3_benchmark(
    spark: SparkSession,
    table_path: str,
    strategy: str,
    latency_ms: int,
    run_index: int = 0,
    bytes_useful: int | None = None,
) -> BenchmarkResult:
    if bytes_useful is None:
        bytes_useful = cohort_bytes_useful(spark, table_path)

    return measure(
        spark,
        lambda: run_cohort_scan(spark, table_path),
        strategy=strategy,
        workload="w3",
        latency_ms=latency_ms,
        bytes_useful=bytes_useful,
        run_index=run_index,
    )
