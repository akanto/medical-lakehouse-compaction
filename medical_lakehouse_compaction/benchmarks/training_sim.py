import random
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, sum as spark_sum
from medical_lakehouse_compaction.metrics.collector import BenchmarkResult, measure

# org.apache.iceberg.expressions.InclusiveMetricsEvaluator#IN_PREDICATE_LIMIT.
# Verified in iceberg-spark-runtime-4.0_2.13-1.10.1: `in()` compares the
# literal set size against 200 and returns ROWS_MIGHT_MATCH above it, so a
# larger IN list is never evaluated against manifest bounds and prunes
# nothing. This is why a W2 shard is drawn as series, not as individual
# slices: a slice-level batch big enough to matter would exceed the limit and
# silently degrade L1 to a full-table scan.
IN_PREDICATE_LIMIT = 200


def build_metadata_cache(spark: SparkSession, table_path: str) -> dict[str, str]:
    """Returns {sop_instance_uid: series_instance_uid} for the whole table."""
    rows = (
        spark.read.format("iceberg").load(table_path)
        .select("sop_instance_uid", "series_instance_uid")
        .collect()
    )
    return {r.sop_instance_uid: r.series_instance_uid for r in rows}


def sample_shards(metadata_cache: dict, n_shards: int, shard_series: int,
                  seed: int) -> list[list[str]]:
    """Pre-draw each shard's series UIDs with a private seeded RNG over the
    sorted series list, so the same (seed, cache) always yields the same
    shards — across network cells, strategies, and Spark sessions. Unseeded
    sampling makes W2 request counts drift between cells, which destroys
    the comparison the grid exists to make.

    Shards are drawn at *series* (patient study) level rather than at slice
    level. Two reasons, one methodological and one mechanical:

    - Patient-level shuffling is what medical-imaging training pipelines
      actually do, because slice-level shuffling leaks anatomy between the
      train and validation splits.
    - A slice-level batch large enough to make L1's request count matter
      would exceed IN_PREDICATE_LIMIT and stop pruning altogether.
    """
    rng = random.Random(seed)
    all_series = sorted(set(metadata_cache.values()))
    k = min(shard_series, len(all_series))
    return [rng.sample(all_series, k) for _ in range(n_shards)]


def _load_shard(spark: SparkSession, table_path: str,
                shard_series: list[str]) -> int:
    """Read every slice of the shard; return the pixel bytes actually read.

    The pixel data is aggregated, not collected. A loader streams a shard into
    training rather than accumulating it in one process, and collecting a
    30-series shard means ~2.8 GB of driver results: past
    spark.driver.maxResultSize
    and into the heap that kernel-OOM-killed two campaign attempts. Summing
    the length of pixel_data cannot be answered from Parquet metadata, so the
    column is still fully read and decompressed and the S3A byte/request
    counters see the whole transfer. The returned total is checked against
    bytes_useful by the caller — a free correctness assertion that the shard
    read what it was supposed to.
    """
    row = (
        spark.read.format("iceberg").load(table_path)
        .filter(col("series_instance_uid").isin(shard_series))
        .agg(spark_sum(length(col("pixel_data"))).alias("pixel_bytes"))
        .first()
    )
    return int(row.pixel_bytes or 0)


def run_w2_benchmark(
    spark: SparkSession,
    table_path: str,
    metadata_cache: dict,
    n_batches: int,
    batch_series: int,
    strategy: str,
    latency_ms: int,
    run_index: int = 0,
    seed: int = 42,
    slice_dims: tuple[int, int] | None = None,
) -> BenchmarkResult:
    """W2 — training data loading, one patient-level shard per batch.

    Each batch loads the pixel data of `batch_series` randomly drawn series.
    Unlike W1 there is no ordering: this is a loader filling a shuffle buffer,
    not a volume reconstruction, so no shuffle stage is involved.

    The shard size sets which cost dominates each layout. L1 holds one slice
    per file, so it prunes exactly but pays a request per slice; L2's
    bin-packed files carry no series clustering, so any shard touches every
    file and it pays the whole table in bytes. L3 and L4 are series-clustered,
    so they pay neither. The size is an experimental parameter, chosen so that
    both costs are in play rather than one dominating.
    """
    if batch_series > IN_PREDICATE_LIMIT:
        raise ValueError(
            f"batch_series={batch_series} exceeds Iceberg's IN_PREDICATE_LIMIT "
            f"({IN_PREDICATE_LIMIT}); the series filter would stop pruning and "
            f"every layout would full-scan, voiding the comparison")

    if slice_dims is None:
        # Cell-invariant lookup; grid sweeps should pass it precomputed rather
        # than pay a table probe per cell (planning alone lists every S1 file).
        d = spark.read.format("iceberg").load(table_path).select("rows", "columns").first()
        slice_dims = (int(d.rows), int(d.columns))

    shards = sample_shards(metadata_cache, n_batches, batch_series, seed)

    # Series differ in slice count (95-733 in LIDC-IDRI), so the shard's
    # useful bytes are counted from the cache rather than assumed uniform.
    slices_per_series = Counter(metadata_cache.values())
    n_slices = sum(slices_per_series[uid] for shard in shards for uid in shard)
    bytes_useful = n_slices * slice_dims[0] * slice_dims[1] * 2

    read = []

    def job():
        read.extend(_load_shard(spark, table_path, shard) for shard in shards)

    result = measure(
        spark, job,
        strategy=strategy,
        workload="w2",
        latency_ms=latency_ms,
        bytes_useful=bytes_useful,
        run_index=run_index,
    )

    # The shard must have read exactly the pixel bytes it accounted for. A
    # mismatch means the predicate selected the wrong rows or the scan was
    # short-circuited, either of which would silently corrupt the layout
    # comparison. Warn rather than raise: a mid-grid abort costs hours, and
    # the per-cell validator will catch a systematic divergence.
    total_read = sum(read)
    if total_read != bytes_useful:
        print(f"  WARNING {strategy}/w2: read {total_read:,} pixel bytes, "
              f"expected {bytes_useful:,} "
              f"({100 * (total_read - bytes_useful) / bytes_useful:+.2f}%)",
              flush=True)
    return result
