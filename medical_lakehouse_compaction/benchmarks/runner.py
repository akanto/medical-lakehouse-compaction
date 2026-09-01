"""Shared benchmark loop used by both the local (run_benchmark.py) and the
AWS/WAN (run_experiment.py) drivers, so W1/W2/W3 and the output format stay
identical across environments. The drivers differ only in how the network is
shaped (local: latency read from profile, no shaping; experiment: HTB rate cap
+ netem delay applied on the Traffic Shaping Gateway per rate x latency cell)."""
import datetime
import json
from dataclasses import asdict
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import min as spark_min

from medical_lakehouse_compaction.benchmarks.reconstruction import run_w1_benchmark, series_bytes_useful
from medical_lakehouse_compaction.benchmarks.training_sim import build_metadata_cache, run_w2_benchmark
from medical_lakehouse_compaction.benchmarks.cohort_scan import cohort_bytes_useful, run_w3_benchmark

TABLES = {
    "s1": "dicom.db.slices_s1",
    "s2": "dicom.db.slices_s2",
    "s3": "dicom.db.slices_s3",
    "s4": "dicom.db.slices_s4",
}


def default_series_uid(spark: SparkSession, table: str = TABLES["s1"]) -> str:
    """Smallest series UID — deterministic across runs, unlike .first()."""
    return (
        spark.read.format("iceberg").load(table)
        .agg(spark_min("series_instance_uid").alias("uid"))
        .first().uid
    )


def pick_spread(items: list, n: int) -> list:
    """n items evenly spaced over an already-sorted list, endpoints included.

    Deterministic; duplicate indices (n close to len) are collapsed, so the
    result can be shorter than n but never contains repeats."""
    if n >= len(items):
        return list(items)
    if n == 1:
        return [items[len(items) // 2]]
    idx = [round(i * (len(items) - 1) / (n - 1)) for i in range(n)]
    return [items[i] for i in dict.fromkeys(idx)]


def select_w1_series(spark: SparkSession, n: int,
                     table: str = TABLES["s1"]) -> list[str]:
    """n series UIDs spanning the slice-count distribution, smallest to
    largest. Series are ordered by (slice count, uid) so the selection is
    deterministic for a given dataset; endpoints are always included, which
    makes the wall-clock-versus-series-size plot cover the full range."""
    rows = (
        spark.read.format("iceberg").load(table)
        .groupBy("series_instance_uid").count()
        .orderBy("count", "series_instance_uid")
        .collect()
    )
    return pick_spread([r.series_instance_uid for r in rows], n)


def precompute_setup(spark: SparkSession, series_uids: list[str],
                     tables: dict = TABLES) -> dict:
    """All benchmark setup values (metadata cache, slice dims, bytes_useful
    denominators) are cell-invariant: the tables never change across a network
    grid. Computing them per cell is unmeasured work that still pays the WAN
    tax — on S1 every one of these queries is an unprunable full scan
    (~12 extra full scans per cell, tens of minutes at
    10 ms RTT). Grid drivers call this once on an unshaped network and pass
    the result to every run_all_workloads call."""
    setup = {}
    for strategy, table in tables.items():
        d = (spark.read.format("iceberg").load(table)
             .select("rows", "columns").first())
        setup[strategy] = {
            "cache": build_metadata_cache(spark, table),
            "slice_dims": (int(d.rows), int(d.columns)),
            "w1_bytes": {uid: series_bytes_useful(spark, table, uid)
                         for uid in series_uids},
            "w3_bytes": cohort_bytes_useful(spark, table),
        }
        print(f"  setup {strategy}: cache={len(setup[strategy]['cache'])} sops, "
              f"dims={setup[strategy]['slice_dims']}", flush=True)
    return setup


def run_all_workloads(
    spark: SparkSession,
    cfg: dict,
    series_uids,
    latency_ms: int,
    tables: dict = TABLES,
    setup: dict | None = None,
) -> list:
    """Run W1+W2+W3 for every strategy at one latency level.

    All three workloads run `benchmark_runs` times and `run_index` is the
    repetition everywhere, so W1's error bar means the same thing as W2's and
    W3's. W1 additionally sweeps every series in `series_uids`; the series is
    identified by `series_uid` on the row, not by run_index. Repetitions are
    the OUTER loop (all series once, then again) so the replicates of one
    series are spread across the cell rather than run back to back, which
    keeps a transient — a GC pause, a page-cache warm hit on the MinIO side —
    from landing on all replicates of the same series.

    Repeating each series is what gives W1 a repeatability error bar. Using
    the five series themselves as the replicates would instead report the
    between-series variance, which is far larger than the effect being
    measured and answers a different question.

    `setup` is the precomputed cell-invariant state from precompute_setup;
    omitted (single-cell/local callers) it is built here, which costs one
    extra scan set per call."""
    if isinstance(series_uids, str):
        series_uids = [series_uids]
    if setup is None:
        setup = precompute_setup(spark, series_uids, tables)
    results = []
    for strategy, table in tables.items():
        print(f"  {strategy}", end="  ", flush=True)
        s = setup[strategy]
        for run_i in range(cfg["benchmark_runs"]):
            for uid in series_uids:
                results.append(run_w1_benchmark(
                    spark, table, uid,
                    strategy=strategy, latency_ms=latency_ms, run_index=run_i,
                    bytes_useful=s["w1_bytes"][uid]))
                print(".", end="", flush=True)
        for run_i in range(cfg["benchmark_runs"]):
            # Seed varies by repetition (different batches per run) but not by
            # cell or strategy, so W2 request counts are directly comparable.
            results.append(run_w2_benchmark(
                spark, table, s["cache"],
                n_batches=cfg["w2_n_batches"], batch_series=cfg["w2_batch_series"],
                strategy=strategy, latency_ms=latency_ms, run_index=run_i,
                seed=cfg.get("w2_seed", 42) + run_i,
                slice_dims=s["slice_dims"]))
            results.append(run_w3_benchmark(
                spark, table,
                strategy=strategy, latency_ms=latency_ms, run_index=run_i,
                bytes_useful=s["w3_bytes"]))
            print(".", end="", flush=True)
        print()
    return results


def save_results(results: list, output_dir: str, profile: str,
                 series_uid: str, cfg: dict,
                 network_levels: list | None = None,
                 filename: str | None = None,
                 partial: bool = False) -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / (filename or f"benchmark_{ts}.json")
    payload = {
        "timestamp": ts,
        "profile": profile,
        "n_series": cfg.get("n_series"),
        "results": [{**asdict(r), "rar_wire": r.rar_wire} for r in results],
    }
    # Multi-series W1 stores the ordered selection; single-series callers keep
    # the legacy scalar key so older analysis scripts read both formats.
    if isinstance(series_uid, (list, tuple)):
        payload["w1_series_uids"] = list(series_uid)
    else:
        payload["series_uid"] = series_uid
    if partial:
        # Mid-sweep checkpoint: overwritten after every cell so a crash loses
        # at most one cell, not the whole multi-hour run.
        payload["partial"] = True
    if network_levels is not None:
        # Per-cell shaping evidence from the WAN driver: nominal vs measured
        # RTT and the applied rate cap (netem delay is additive on the
        # substrate RTT, so nominal and observed always differ slightly).
        payload["network"] = network_levels
    out_path.write_text(json.dumps(payload, indent=2))
    return out_path
