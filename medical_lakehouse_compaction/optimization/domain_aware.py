from pyspark.sql import SparkSession

from medical_lakehouse_compaction.table_metrics import UID_METRICS_TRUNCATE, write_with_uid_metrics


def run_domain_compaction(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    uid_metrics_truncate: int | None = UID_METRICS_TRUNCATE,
) -> None:
    """
    Compact an Iceberg table using domain-aware partitioning by SeriesInstanceUID.

    Groups all slices from the same CT series (SeriesInstanceUID) into the same Parquet file,
    sorted by instance_number. This optimization enables 3D volume reconstruction queries
    to read only ONE file instead of scanning the entire dataset.

    Args:
        spark: SparkSession
        source_table: Fully-qualified source table name (e.g., 'dicom.db.source')
        target_table: Fully-qualified target table name (e.g., 'dicom.db.target')
        uid_metrics_truncate: Manifest bound length for the UID columns.
            Defaults to the full UID length so pruning works; pass None for
            Iceberg's truncate(16) default, which builds the narrow variant
            used to price the pruning loss (medical_lakehouse_compaction/table_metrics.py).
    """
    # Ensure namespace exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS dicom.db")

    df = spark.read.format("iceberg").load(source_table)

    # Exactly one Parquet file per series. Without an explicit count,
    # repartitionByRange falls back to spark.sql.shuffle.partitions and packs
    # multiple series per file, which confounds the S3-vs-S4 comparison with a
    # file-granularity difference. Over-provision 8×: range boundaries come
    # from sampled quantiles, so exactly n_series partitions can still merge
    # two adjacent series (observed 49 files for 50 series); with ~8× the
    # per-partition row target drops well below the smallest series, so every
    # series is isolated in its own partition. A key never spans partitions,
    # and empty partitions write no files.
    n_series = df.select("series_instance_uid").distinct().count()

    sorted_df = df.repartitionByRange(n_series * 8, "series_instance_uid") \
                  .sortWithinPartitions("series_instance_uid", "instance_number")

    # S3 is sorted on the series key, so it is the layout that depends most on
    # the manifest bounds: with them truncated, a single-series read probes
    # every file's footer instead of pruning to the one it needs.
    write_with_uid_metrics(sorted_df, target_table, uid_metrics_truncate)
