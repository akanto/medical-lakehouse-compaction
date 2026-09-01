from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from medical_lakehouse_compaction.table_metrics import UID_METRICS_TRUNCATE, write_with_uid_metrics


def run_generic_compaction(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    target_file_size_mb: int = 256,
    uid_metrics_truncate: int | None = UID_METRICS_TRUNCATE,
) -> None:
    """
    S2: Iceberg's native generic compaction (`rewrite_data_files`, binpack).

    Two steps, so the measured table is literally what a production optimizer
    produces from the raw layout (not a repartition proxy):

    1. Clone the source's one-file-per-slice layout into the target table
       (same range-partition trick as ingestion), giving binpack the same
       input a generic optimizer would see on the raw table.
    2. `CALL system.rewrite_data_files(strategy => 'binpack')` on the clone,
       then expire the pre-compaction snapshot so only the binpacked files
       remain in storage (production housekeeping; keeps object counts
       meaningful for table verification).

    Note (paper Section III-D): binpack assigns slices to output files in
    whatever order the write tasks deliver them, so series boundaries fall
    differently on every build — S2's layout is non-deterministic across
    builds by nature of the operator.

    Args:
        spark: SparkSession
        source_table: Fully-qualified source table name (e.g., 'dicom.db.source')
        target_table: Fully-qualified target table name (e.g., 'dicom.db.target')
        target_file_size_mb: Target file size in MB (default 256)
        uid_metrics_truncate: Manifest bound length for the UID columns; None
            leaves Iceberg's truncate(16) default. Set on the clone so
            rewrite_data_files inherits it. Inert for S2 in practice — binpack
            leaves every series spread over every file, so the bounds span the
            whole UID range at any truncation — but set identically to the
            other strategies to keep layout the only variable
            (medical_lakehouse_compaction/table_metrics.py).
    """
    # Ensure namespace exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS dicom.db")

    df = spark.read.format("iceberg").load(source_table)

    # One file per slice — unique sop_instance_uid per range partition, no
    # hash collisions (matches the ingestion writer's S1 layout).
    n_slices = df.count()
    write_with_uid_metrics(
        df.repartitionByRange(n_slices, "sop_instance_uid"),
        target_table, uid_metrics_truncate)

    catalog, ident = target_table.split(".", 1)
    size_bytes = target_file_size_mb * 1024 * 1024
    spark.sql(f"""
        CALL {catalog}.system.rewrite_data_files(
          table => '{ident}',
          strategy => 'binpack',
          options => map(
            'target-file-size-bytes', '{size_bytes}',
            'min-input-files', '1'
          )
        )
    """)

    # Drop the pre-compaction snapshot and its per-slice data files. The
    # horizon must be in the future so the just-created snapshots qualify;
    # retain_last keeps the current (binpacked) one. A naive timestamp is
    # interpreted in the Spark session timezone, so +48 h keeps it in the
    # future regardless of the host timezone (a +1 h UTC horizon read as
    # CEST landed in the past and silently expired nothing).
    horizon = (datetime.now(timezone.utc) + timedelta(hours=48)) \
        .strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""
        CALL {catalog}.system.expire_snapshots(
          table => '{ident}',
          older_than => TIMESTAMP '{horizon}',
          retain_last => 1
        )
    """)
