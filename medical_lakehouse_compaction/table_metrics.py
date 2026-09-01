"""Per-column Iceberg manifest statistics for the DICOM UID columns.

Iceberg's `write.metadata.metrics.default` is `truncate(16)`. Every LIDC-IDRI
UID shares the 34-character prefix `1.3.6.1.4.1.14519.5.2.1.6279.6001.`, so a
16-character bound collapses to the same pair for every data file
(`["1.3.6.1.4.1.1451", "1.3.6.1.4.1.1452")`). Manifest-based
file pruning can then exclude nothing, and a query that names a series or a
slice degrades to a full-table footer probe.

UIDs are 64 characters, so `truncate(64)` stores them whole and pruning
becomes exact. Both filter columns need it: W1 and W3 filter on
`series_instance_uid`, W2 additionally on `sop_instance_uid`
(`medical_lakehouse_compaction/benchmarks/training_sim.py`). Widening only the series key leaves W2
pruning to the ~30 series a batch touches — 6,020 of 35,485 files, 17% of the
table — instead of the 32 files it actually reads.

The properties must ride table *creation*: manifests written under the default
keep their truncated bounds, and setting the property afterwards does not
rewrite them.

Layouts differ in whether they can use the statistics at all, which is the
point of the comparison and not a reason to configure them differently:

- L1 (one slice per file) and L3 (sorted on the series key) are clustered on
  the key, so exact bounds make pruning exact.
- L2 (bin-packed, unsorted) spreads every series across every file, so its
  bounds span the whole UID range whatever the truncation. Pruning needs both
  clustering *and* preserved statistics; L2 has only the statistics.
- L4 prunes on partition values, which are exempt from metrics truncation, so
  the setting is inert for it.

Setting all four identically keeps the layout the only variable.
"""

UID_METRICS_COLUMNS = ("series_instance_uid", "sop_instance_uid")

# Full UID length. LIDC-IDRI UIDs are exactly 64 characters; 40 already
# separates all 200 series, but the margin costs only manifest bytes and
# survives datasets with longer roots.
UID_METRICS_TRUNCATE = 64


def uid_metrics_properties(truncate: int | None = UID_METRICS_TRUNCATE) -> dict[str, str]:
    """Table properties widening the manifest bounds of both UID columns.

    `truncate=None` returns an empty mapping, leaving Iceberg's `truncate(16)`
    default in place — the narrow variant used to price the pruning loss.
    """
    if truncate is None:
        return {}
    return {
        f"write.metadata.metrics.column.{column}": f"truncate({truncate})"
        for column in UID_METRICS_COLUMNS
    }


def write_with_uid_metrics(df, table_name: str, truncate: int | None,
                           partition_columns: tuple[str, ...] = ()):
    """Create-or-replace `table_name` from `df` with the UID metrics applied.

    Uses DataFrameWriterV2 because only it can set table properties at
    creation time. Iceberg's write path is the same as `saveAsTable`, so the
    caller's `repartition`/`sortWithinPartitions` layout is preserved exactly
    as before.
    """
    from pyspark.sql.functions import col

    writer = df.writeTo(table_name).using("iceberg")
    for key, value in uid_metrics_properties(truncate).items():
        writer = writer.tableProperty(key, value)
    if partition_columns:
        writer = writer.partitionedBy(*(col(c) for c in partition_columns))
    writer.createOrReplace()
