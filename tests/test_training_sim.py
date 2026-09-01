# tests/test_training_sim.py
import pytest
from medical_lakehouse_compaction.ingestion.iceberg_writer import ingest_dicom_dir
from medical_lakehouse_compaction.benchmarks.training_sim import (
    IN_PREDICATE_LIMIT, build_metadata_cache, run_w2_benchmark, sample_shards,
)
from medical_lakehouse_compaction.metrics.collector import BenchmarkResult
from tests.conftest import FIXTURE_COLS, FIXTURE_ROWS

@pytest.fixture
def s1_table(spark, sample_dicom_dir):
    ingest_dicom_dir(spark, str(sample_dicom_dir), "dicom.db.s1_w2")
    return "dicom.db.s1_w2"

def test_metadata_cache_maps_sop_to_series(spark, s1_table):
    cache = build_metadata_cache(spark, s1_table)
    assert len(cache) == 10  # 2 series × 5 slices
    for sop_uid, series_uid in cache.items():
        assert isinstance(sop_uid, str)
        assert isinstance(series_uid, str)

def test_w2_benchmark_returns_result(spark, s1_table):
    cache = build_metadata_cache(spark, s1_table)
    result = run_w2_benchmark(
        spark, s1_table, cache,
        n_batches=2, batch_series=1,
        strategy="s1", latency_ms=1,
    )
    assert isinstance(result, BenchmarkResult)
    assert result.workload == "w2"
    assert result.wall_clock_s > 0

def test_w2_bytes_useful_counts_actual_shard_slices(spark, s1_table):
    """Series differ in slice count, so the shard's useful bytes must be
    summed from the cache, not assumed uniform."""
    cache = build_metadata_cache(spark, s1_table)
    both = run_w2_benchmark(spark, s1_table, cache, n_batches=1, batch_series=2,
                            strategy="s1", latency_ms=1)
    one = run_w2_benchmark(spark, s1_table, cache, n_batches=1, batch_series=1,
                           strategy="s1", latency_ms=1)
    # Derived from the fixture rather than hardcoded: bytes_useful is
    # slices x rows x cols x 2 (16-bit samples), and the fixture's slice
    # dimensions have changed before while this assertion did not.
    slice_bytes = FIXTURE_ROWS * FIXTURE_COLS * 2
    assert both.bytes_useful == 10 * slice_bytes   # 2 series x 5 slices
    assert one.bytes_useful == 5 * slice_bytes

def test_sample_shards_deterministic_per_seed():
    cache = {f"sop{i:03d}": f"series{i % 12}" for i in range(120)}
    a = sample_shards(cache, n_shards=4, shard_series=8, seed=42)
    b = sample_shards(cache, n_shards=4, shard_series=8, seed=42)
    c = sample_shards(cache, n_shards=4, shard_series=8, seed=43)
    assert a == b            # same seed -> identical shards (cell-invariant)
    assert a != c            # different repetition -> different shards
    assert all(len(shard) == 8 for shard in a)
    assert all(len(set(shard)) == 8 for shard in a)   # no repeated series

def test_sample_shards_ignores_cache_insertion_order():
    items = [(f"sop{i:03d}", f"series{i % 10}") for i in range(40)]
    forward, backward = dict(items), dict(reversed(items))
    assert (sample_shards(forward, 2, 5, seed=7)
            == sample_shards(backward, 2, 5, seed=7))

def test_sample_shards_capped_at_series_count():
    cache = {f"sop{i:03d}": f"series{i % 3}" for i in range(30)}
    shards = sample_shards(cache, n_shards=1, shard_series=100, seed=1)
    assert len(shards[0]) == 3            # only 3 distinct series exist

def test_w2_rejects_shard_above_iceberg_in_predicate_limit(spark, s1_table):
    """Above IN_PREDICATE_LIMIT Iceberg stops evaluating the IN list against
    manifest bounds, so every layout would full-scan and the comparison would
    be void. Fail loudly rather than measure nonsense."""
    cache = build_metadata_cache(spark, s1_table)
    with pytest.raises(ValueError, match="IN_PREDICATE_LIMIT"):
        run_w2_benchmark(spark, s1_table, cache,
                         n_batches=1, batch_series=IN_PREDICATE_LIMIT + 1,
                         strategy="s1", latency_ms=1)
