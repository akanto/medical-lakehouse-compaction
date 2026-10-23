import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as spark_count, first as spark_first
from medical_lakehouse_compaction.metrics.collector import BenchmarkResult, measure

def reconstruct_volume(spark: SparkSession, table_path: str, series_uid: str) -> np.ndarray:
    # The slices are ordered on the driver after collection, not with orderBy.
    # A global sort in Spark plans as a range-partitioning exchange, and the
    # range bounds come from a sampling job that scans the whole input, pixel
    # data included, before the sort stage scans it again. Every W1 read of the
    # earlier campaign paid two scans on all four layouts: 6.0 GET requests
    # per slice on L1, against 3.0 in W2, which has no sort. One
    # series fits on the driver, so the local sort returns the same volume
    # from a single scan.
    rows = (
        spark.read.format("iceberg").load(table_path)
        .filter(col("series_instance_uid") == series_uid)
        .select("instance_number", "pixel_data", "rows", "columns")
        .collect()
    )
    rows.sort(key=lambda r: r.instance_number)
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
