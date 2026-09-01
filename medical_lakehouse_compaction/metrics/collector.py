import json
import time
import urllib.request
from dataclasses import dataclass
from typing import Callable, Optional
from pyspark.sql import SparkSession
from medical_lakehouse_compaction.metrics.stage_log import make_tag

# S3A IOStatistics counter keys (hadoop-aws 3.4.1). These are live, in-process
# counters maintained by the S3A connector — unlike MinIO's Prometheus request
# counters, which refresh on a ~10s server-side cache and so cannot be deltaed
# per job. Each is one round-trip over the WAN link.
_GET_KEY = "action_http_get_request"      # object data reads
_HEAD_KEY = "action_http_head_request"    # metadata / Parquet footer probes
_LIST_KEY = "object_list_request"         # manifest / partition listing
_BYTES_KEY = "stream_read_total_bytes"    # bytes actually fetched from the store


@dataclass
class BenchmarkResult:
    strategy: str
    workload: str
    latency_ms: int
    wall_clock_s: float
    bytes_useful: int
    run_index: int
    scan_tasks: int = 0
    scan_stages: int = 0
    # Bandwidth cap for this cell (Gb/s); None = uncapped / not applicable
    # (local runs). Set by the WAN driver after run_all_workloads.
    rate_gbit: Optional[float] = None
    # Ground-truth client-side S3A metrics for this job (0 if not S3A-backed).
    bytes_on_wire: int = 0          # authoritative bytes fetched (reliable in local mode)
    s3_get_requests: int = 0        # GET round-trips
    s3_total_requests: int = 0      # GET + HEAD + LIST round-trips
    # W1 only: the series this run reconstructed (None for W2/W3). W1 iterates
    # over series spanning the size distribution, so this keys the
    # wall-clock-versus-series-size analysis.
    series_uid: Optional[str] = None

    # NOTE: a SparkUI-derived `bytes_read`/`rar` pair used to live here; it was
    # removed because SparkUI stage inputBytes undercounts Iceberg DSv2 Parquet
    # reads, which was confirmed against the wire byte counters below. Older
    # result JSONs still carry those unreliable fields — ignore them there too.
    @property
    def rar_wire(self) -> float:
        """Read amplification: wire bytes (compressed) / useful decoded bytes."""
        if self.bytes_useful == 0:
            return float("inf")
        return self.bytes_on_wire / self.bytes_useful


def _collect_scan_tasks(spark: SparkSession, tag: str) -> tuple[int, int]:
    """(tasks, stages) for the Spark jobs belonging to this run's job group.

    Uses statusTracker only. The previous implementation queried the SparkUI
    REST API and bailed out when `sc.uiWebUrl` was unset; benchmark sessions
    disable the UI to keep the driver heap flat over a ten-hour grid, so this
    returned 0 in every run of three consecutive campaigns. statusTracker
    reads the same AppStatusStore the UI
    would have served, and that store is populated whether or not the UI is
    bound to a port.

    Attribution is by job group rather than by diffing the job-id set before and
    after, which is both simpler and immune to another thread starting a job
    inside the measured window. `tag` is unique per (strategy, workload,
    repetition, series, cell).

    This is the total across the run's stages, which is enough to tell an 8-way
    parallel scan from a 1-way one. Per-stage attribution, and the file and
    partition counts the transport-aware model wants, come from the event log
    via `medical_lakehouse_compaction.metrics.stage_log`.
    """
    try:
        tracker = spark.sparkContext.statusTracker()
        stage_ids: set[int] = set()
        for jid in tracker.getJobIdsForGroup(tag) or []:
            info = tracker.getJobInfo(jid)
            if info:
                stage_ids.update(info.stageIds)
        tasks = 0
        for sid in stage_ids:
            si = tracker.getStageInfo(sid)
            if si:
                tasks += si.numTasks
        return tasks, len(stage_ids)
    except Exception:
        return 0, 0


def _set_job_group(sc, tag: Optional[str], description: Optional[str] = None) -> None:
    """Set the Spark job group, or clear it when tag is None.

    Written through local properties rather than SparkContext.setJobGroup /
    clearJobGroup because PySpark 4.0 removed clearJobGroup; only
    cancelJobGroup and the newer job-tag API survive. These two properties are
    exactly what setJobGroup wrote, and they are what lands in
    SparkListenerJobStart, which is all the event-log parser reads.
    """
    sc.setLocalProperty("spark.jobGroup.id", tag)
    sc.setLocalProperty("spark.job.description", description)


def _s3a_filesystem(spark: SparkSession):
    """The cached S3A FileSystem instance Spark reads through, or None.

    FileSystem.get() returns the same cached instance Spark uses, so its
    IOStatistics reflect Spark's actual reads. None when the warehouse isn't
    S3A-backed, so the harness works unchanged against other stores.
    """
    try:
        warehouse = spark.conf.get("spark.sql.catalog.dicom.warehouse", "")
        if not warehouse.startswith("s3a://"):
            return None
        jvm = spark.sparkContext._jvm
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI(warehouse)
        return jvm.org.apache.hadoop.fs.FileSystem.get(uri, hconf)
    except Exception:
        return None


def _s3a_counters(fs) -> dict[str, int]:
    """Snapshot of the S3A FileSystem IOStatistics counters. {} if unavailable."""
    if fs is None:
        return {}
    try:
        ios = fs.getIOStatistics()
        if ios is None:
            return {}
        out: dict[str, int] = {}
        it = ios.counters().entrySet().iterator()
        while it.hasNext():
            e = it.next()
            out[e.getKey()] = int(e.getValue())
        return out
    except Exception:
        return {}


def measure(
    spark: SparkSession,
    job: Callable,
    strategy: str,
    workload: str,
    latency_ms: int,
    bytes_useful: int = 0,
    run_index: int = 0,
    series_uid: Optional[str] = None,
) -> BenchmarkResult:
    sc = spark.sparkContext
    fs = _s3a_filesystem(spark)
    c0 = _s3a_counters(fs)

    # Tag every Spark job this workload starts, so the event log can be split
    # back into runs offline. setJobGroup writes a thread-local property that
    # lands in SparkListenerJobStart, and costs nothing at runtime.
    tag = make_tag(strategy, workload, run_index, series_uid, latency_ms)
    _set_job_group(sc, tag, f"{workload} {strategy} run {run_index}")
    try:
        start = time.perf_counter()
        job()
        elapsed = time.perf_counter() - start
    finally:
        _set_job_group(sc, None)

    scan_tasks, scan_stages = _collect_scan_tasks(spark, tag)
    c1 = _s3a_counters(fs)

    def delta(key: str) -> int:
        return max(c1.get(key, 0) - c0.get(key, 0), 0)

    s3_get = delta(_GET_KEY)
    s3_total = s3_get + delta(_HEAD_KEY) + delta(_LIST_KEY)
    bytes_on_wire = delta(_BYTES_KEY)

    return BenchmarkResult(
        strategy=strategy,
        workload=workload,
        latency_ms=latency_ms,
        wall_clock_s=elapsed,
        bytes_useful=bytes_useful,
        run_index=run_index,
        scan_tasks=scan_tasks,
        scan_stages=scan_stages,
        bytes_on_wire=bytes_on_wire,
        s3_get_requests=s3_get,
        s3_total_requests=s3_total,
        series_uid=series_uid,
    )
