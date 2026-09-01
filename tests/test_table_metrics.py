"""The UID manifest bounds are what make file pruning possible.

Iceberg's `truncate(16)` default collapses every DICOM UID to its shared
prefix, so every data file reports the same lower/upper bound and no file can
ever be excluded. These tests assert the property is set on every strategy and
that it actually changes the stored bounds — the mechanism, not just the
configuration.
"""
from pyspark.sql.functions import col

from medical_lakehouse_compaction.ingestion.iceberg_writer import ingest_dicom_dir
from medical_lakehouse_compaction.optimization.domain_aware import run_domain_compaction
from medical_lakehouse_compaction.optimization.generic import run_generic_compaction
from medical_lakehouse_compaction.optimization.partition_spec import run_partition_spec_compaction
from medical_lakehouse_compaction.table_metrics import (
    UID_METRICS_COLUMNS, UID_METRICS_TRUNCATE, uid_metrics_properties,
)


def distinct_bound_pairs(spark, table: str, column: str) -> int:
    """Number of distinct (lower, upper) bound pairs across the data files.

    1 means every file looks identical to the planner and pruning on `column`
    can exclude nothing.
    """
    files = spark.read.format("iceberg").load(f"{table}.files")
    return (files.select(
        col(f"readable_metrics.{column}.lower_bound").alias("lo"),
        col(f"readable_metrics.{column}.upper_bound").alias("hi"))
        .distinct().count())


def table_properties(spark, table: str) -> dict:
    return {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}


def test_properties_cover_both_filter_columns():
    props = uid_metrics_properties()
    assert set(props) == {
        f"write.metadata.metrics.column.{c}" for c in UID_METRICS_COLUMNS
    }
    assert set(props.values()) == {f"truncate({UID_METRICS_TRUNCATE})"}
    # W2 filters on sop_instance_uid; widening only the series key leaves it
    # pruning to whole series instead of single slices.
    assert "sop_instance_uid" in UID_METRICS_COLUMNS


def test_none_leaves_the_iceberg_default():
    assert uid_metrics_properties(None) == {}


def test_every_strategy_carries_the_property(spark, sample_dicom_dir):
    s1 = "dicom.db.tm_s1"
    ingest_dicom_dir(spark, str(sample_dicom_dir), s1)
    run_generic_compaction(spark, s1, "dicom.db.tm_s2", target_file_size_mb=256)
    run_domain_compaction(spark, s1, "dicom.db.tm_s3")
    run_partition_spec_compaction(spark, s1, "dicom.db.tm_s4")

    expected = uid_metrics_properties()
    for table in ["dicom.db.tm_s1", "dicom.db.tm_s2", "dicom.db.tm_s3", "dicom.db.tm_s4"]:
        props = table_properties(spark, table)
        for key, value in expected.items():
            assert props.get(key) == value, f"{table} missing {key}"


def test_s1_bounds_are_exact_per_slice(spark, sample_dicom_dir):
    """S1 holds one slice per file, so exact bounds make every file distinct."""
    table = "dicom.db.tm_s1_bounds"
    ingest_dicom_dir(spark, str(sample_dicom_dir), table)
    n_files = spark.read.format("iceberg").load(f"{table}.files").count()
    assert distinct_bound_pairs(spark, table, "sop_instance_uid") == n_files


def test_narrow_metrics_collapse_the_bounds(spark, sample_dicom_dir):
    """The counterfactual: under Iceberg's default every file looks the same.

    A campaign sets `uid_metrics_truncate` once for every table, so the two
    settings are never mixed in one build. This test is where the difference
    between them is pinned down.
    """
    src = "dicom.db.tm_s1_for_narrow"
    ingest_dicom_dir(spark, str(sample_dicom_dir), src)

    wide, narrow = "dicom.db.tm_s3_wide", "dicom.db.tm_s3_narrow"
    run_domain_compaction(spark, src, wide)
    run_domain_compaction(spark, src, narrow, uid_metrics_truncate=None)

    n_series = (spark.read.format("iceberg").load(wide)
                .select("series_instance_uid").distinct().count())
    assert distinct_bound_pairs(spark, wide, "series_instance_uid") == n_series
    assert distinct_bound_pairs(spark, narrow, "series_instance_uid") == 1
