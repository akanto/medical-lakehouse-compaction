from pyspark.sql import SparkSession

from medical_lakehouse_compaction.table_metrics import UID_METRICS_TRUNCATE, write_with_uid_metrics


def run_partition_spec_compaction(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    uid_metrics_truncate: int | None = UID_METRICS_TRUNCATE,
) -> None:
    """
    S4: rewrite using Iceberg's native partition spec.

    Unlike S3 (repartitionByRange — file pruning via manifest column stats, O(n
    files)), S4 declares PARTITIONED BY (series_instance_uid) in the Iceberg
    table metadata. Queries filtered on series_instance_uid use O(1) partition
    pruning with no manifest stats scan, yielding measurable gains over S3 at
    scale (200+ series) under high WAN latency.

    The UID metrics setting is inert here — partition values are exempt from
    metrics truncation, so S4 prunes identically at truncate(16) and
    truncate(64). It is applied anyway so that every strategy is built with
    one configuration and the layout stays the only variable
    (medical_lakehouse_compaction/table_metrics.py).
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS dicom.db")

    df = spark.read.format("iceberg").load(source_table)

    # Hash-repartition on the partition key so each series lives in exactly
    # one task, giving one file per partition value. Otherwise the file count
    # per partition depends on how Spark happened to split the source read.
    #
    # The partition count is explicit, 8x the series count as in S3, and the
    # table sets write.distribution-mode=none. With repartition(col) alone the
    # session's shuffle partition count applies, and Iceberg's default hash
    # distribution for partitioned tables adds a second shuffle on the same
    # key, which AQE coalesces to a few write tasks. The build then shuffles
    # the table twice, which inflates its measured rewrite time. The layout,
    # one file per series, is the same either way.
    n_series = df.select("series_instance_uid").distinct().count()
    write_with_uid_metrics(
        df.repartition(n_series * 8, "series_instance_uid")
          .sortWithinPartitions("series_instance_uid", "instance_number"),
        target_table, uid_metrics_truncate,
        partition_columns=("series_instance_uid",),
        properties={"write.distribution-mode": "none"})
