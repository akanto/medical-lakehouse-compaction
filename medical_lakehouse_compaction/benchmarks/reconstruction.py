import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as spark_count, first as spark_first
from medical_lakehouse_compaction.metrics.collector import BenchmarkResult, measure

def reconstruct_volume(spark: SparkSession, table_path: str, series_uid: str) -> np.ndarray:
    rows = (
        spark.read.format("iceberg").load(table_path)
        .filter(col("series_instance_uid") == series_uid)
        .orderBy("instance_number")
        .select("pixel_data", "rows", "columns")
        .collect()
    )
    return np.stack([
        np.frombuffer(r.pixel_data, dtype=np.int16).reshape(r.rows, r.columns)
        for r in rows
    ])

def series_bytes_useful(spark: SparkSession, table_path: str,
                        series_uid: str) -> int:
    """Decoded volume size of one series. Cell-invariant: on S1 this scans the
    whole table (UID stats truncation defeats pruning), so callers sweeping a
    network grid should precompute it once, unshaped, not once per cell."""
    agg = (
        spark.read.format("iceberg").load(table_path)
        .filter(col("series_instance_uid") == series_uid)
        .agg(
            spark_count("*").alias("n"),
            spark_first("rows").alias("img_rows"),
            spark_first("columns").alias("img_cols"),
        )
        .first()
    )
    return int(agg.n) * int(agg.img_rows) * int(agg.img_cols) * 2


def run_w1_benchmark(
    spark: SparkSession,
    table_path: str,
    series_uid: str,
    strategy: str,
    latency_ms: int,
    run_index: int = 0,
    bytes_useful: int | None = None,
) -> BenchmarkResult:
    if bytes_useful is None:
        bytes_useful = series_bytes_useful(spark, table_path, series_uid)

    return measure(
        spark,
        lambda: reconstruct_volume(spark, table_path, series_uid),
        strategy=strategy,
        workload="w1",
        latency_ms=latency_ms,
        bytes_useful=bytes_useful,
        run_index=run_index,
        series_uid=series_uid,
    )
