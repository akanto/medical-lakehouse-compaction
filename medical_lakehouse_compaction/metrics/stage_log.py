"""Stage-level telemetry from the Spark event log.

Why the event log and not the SparkUI REST API: benchmark sessions run headless
for hours and issue tens of thousands of stages, so `spark.ui.enabled` is false
(see `medical_lakehouse_compaction/spark_session.py`). That leaves `sc.uiWebUrl` unset, which is why the
original `_collect_scan_tasks` returned 0 in every run of three consecutive
campaigns. The event log has no such dependency: it appends to a file on local
disk, so it costs no driver heap, and it carries strictly more than the UI did.

Why this schema: the fields are named for the transport-aware performance
model published alongside our prior work (see `DATA.md` in
github.com/cloudera/transport-aware-spark-model, section 2,
`raw/stage_measurement_summary.json`), so stage records from this harness can
be fed to that model without a translation layer. The model's
notation is given in the docstrings below: `N_tasks`, `N_partitions`, and the
calibration target `y = stage_duration_s`.

Attribution: `measure()` wraps each workload execution in a Spark job group
whose id encodes the run, so every stage in the log can be traced back to one
(strategy, workload, repetition, series, cell) tuple. Without that tag the log
is one undifferentiated stream of stages.
"""

import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, Optional

# Job-group id format written by measure(). Kept positional and delimiter-based
# rather than JSON because it also shows up in log lines read by a human.
TAG_SEP = "|"
TAG_FIELDS = ("strategy", "workload", "run_index", "series_uid", "latency_ms")

# SQL-metric names carrying the scan structure. Task metrics do not expose these;
# they are driver-side metrics of the scan node, published as accumulator ids in
# SparkListenerDriverAccumUpdates and named in the plan tree of
# SparkListenerSQLExecutionStart / SparkListenerSQLAdaptiveExecutionUpdate.
# Verified against a Spark 4.0.0 log: reading 40 Parquet files
# reported 40 under "number of files read", and one file reported 1.
#
# Those are `FileSourceScanExec` names. Iceberg reads through `BatchScanExec`,
# which publishes an entirely different set, so a first pass over a real
# campaign log resolved every count to 0 while the Parquet smoke test passed.
# The names below were read off a `BatchScan dicom.db.slices_s1` node. Both
# dialects are accepted,
# because the workloads read Iceberg while the unit tests read plain Parquet.
#
# The mapping onto the model's schema is exact rather than approximate:
#   * "number of result data files" is the count of data files the scan
#     resolved to read after pruning, which is what `number_of_files_read`
#     means for a file source.
#   * "number of file splits read" is the count of splits the scan planned,
#     and in DSv2 one split is one input partition of the scan RDD. The
#     model defines `N_partitions` as the input partitions processed by the
#     stage, so this is the same quantity. Its ratio to `num_tasks` is the
#     model's partition density, which is why the two must not be conflated.
#
# NOT used: Exchange's "number of partitions" metric. It counts shuffle
# output partitions, is attached to a shuffle node rather than a scan, and
# would silently substitute `spark.sql.shuffle.partitions` for a scan width.
_ACC_FILES = "number of files read"
_ACC_PARTITIONS = "number of partitions read"
_ACC_STATIC_FILES = "static number of files read"
_ICEBERG_FILES = "number of result data files"
_ICEBERG_SPLITS = "number of file splits read"
# Manifest pruning, reported by Iceberg only: the data files the scan proved it
# could skip from manifest bounds. This is the direct evidence for the
# truncate(64) pruning argument, which is otherwise inferred from GET counts.
_ICEBERG_SKIPPED_FILES = "number of skipped data files"
# Bytes of the data files the scan resolved to read. Spark's own Input Metrics
# ("Bytes Read") under-report badly for a DSv2 batch scan -- on the
# structural pass they came to roughly 1.5% of the file bytes, proportional to
# file count but far below it -- because Iceberg reports its read volume
# through this scan metric rather than through the task input callback. Both
# are kept: `input_read_gb` stays whatever Spark reported, so it remains
# comparable with a non-Iceberg source, and this carries the true figure.
_ICEBERG_FILE_BYTES = "total data file size (bytes)"
_SQL_METRIC_FIELDS = {
    _ACC_FILES: "number_of_files_read",
    _ACC_PARTITIONS: "number_of_partitions_read",
    _ACC_STATIC_FILES: "static_number_of_files_read",
    _ICEBERG_FILES: "number_of_files_read",
    _ICEBERG_SPLITS: "number_of_partitions_read",
    _ICEBERG_SKIPPED_FILES: "number_of_skipped_data_files",
    _ICEBERG_FILE_BYTES: "scan_file_bytes",
}


def event_log_configs(event_log_dir: Optional[str],
                      compress: bool = False) -> dict[str, str]:
    """Spark configs enabling the event log, or {} when it is not requested.

    Compression defaults to off, and deliberately so: Spark writes lz4 frames
    that `parse_event_log` cannot read without an extra dependency, and a
    campaign that produced logs its own parser refuses would repeat the failure
    mode this module exists to fix. A full grid row writes a few hundred MB
    uncompressed, which the instance's EBS volume absorbs without trouble. Turn
    it on only if the log is being archived rather than parsed here, and
    decompress before parsing.
    """
    if not event_log_dir:
        return {}
    Path(event_log_dir).mkdir(parents=True, exist_ok=True)
    return {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": Path(event_log_dir).resolve().as_uri(),
        "spark.eventLog.compress": "true" if compress else "false",
    }


def make_tag(strategy: str, workload: str, run_index: int,
             series_uid: Optional[str], latency_ms: int) -> str:
    """Job-group id encoding one workload execution."""
    return TAG_SEP.join(
        (strategy, workload, str(run_index), series_uid or "", str(latency_ms)))


def parse_tag(tag: str) -> Optional[dict]:
    """Inverse of make_tag. None for job groups this harness did not set."""
    parts = tag.split(TAG_SEP)
    if len(parts) != len(TAG_FIELDS):
        return None
    out = dict(zip(TAG_FIELDS, parts))
    try:
        out["run_index"] = int(out["run_index"])
        out["latency_ms"] = int(out["latency_ms"])
    except ValueError:
        return None
    out["series_uid"] = out["series_uid"] or None
    return out


@dataclass
class StageRecord:
    """One execution stage, named for the transport-aware model's schema."""
    stage_id: int
    job_id: int
    tag: dict
    stage_duration_s: float = 0.0      # model target `y`
    num_tasks: int = 0                 # model feature `N_tasks`
    avg_task_duration_s: float = 0.0
    executor_runtime_s: float = 0.0
    executor_cpu_time_s: float = 0.0
    input_read_gb: float = 0.0
    shuffle_read_gb: float = 0.0
    number_of_files_read: int = 0
    static_number_of_files_read: int = 0
    number_of_partitions_read: int = 0  # model feature `N_partitions`
    # Iceberg only; 0 for a plain-Parquet scan, which does not report it.
    number_of_skipped_data_files: int = 0
    scan_file_bytes: int = 0

    @property
    def scan_file_gb(self) -> float:
        """Iceberg's own read volume, or Spark's when the scan is not Iceberg."""
        return self.scan_file_bytes / 1e9 if self.scan_file_bytes else self.input_read_gb
    _task_runtimes: list = field(default_factory=list, repr=False)

    def finish(self) -> "StageRecord":
        if self._task_runtimes:
            self.avg_task_duration_s = sum(self._task_runtimes) / len(self._task_runtimes)
        return self


def _log_parts(path: Path) -> list[Path]:
    """The event-log files to read, in order.

    Spark 4.0 writes a rolling log: `spark.eventLog.dir` gets a directory named
    `eventlog_v2_<appId>/` holding an empty `appstatus_<appId>` marker and one
    or more `events_<n>_<appId>` parts, each optionally suffixed `.inprogress`
    while the application is live or if it was killed. Verified against a real
    Spark 4.0.0 session. Passing that directory to a file reader
    raises IsADirectoryError, so accept either a single file or the directory
    and sort the parts by their rolling index.
    """
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise FileNotFoundError(path)
    parts = [p for p in path.iterdir()
             if p.name.startswith("events_") and not p.name.startswith(".")]
    if not parts:
        raise FileNotFoundError(
            f"{path} holds no events_* parts; is it a Spark event-log directory?")

    def index(p: Path) -> int:
        try:
            return int(p.name.split("_")[1])
        except (IndexError, ValueError):
            return 0
    return sorted(parts, key=index)


def _iter_events(path: Path) -> Iterator[dict]:
    """Yield event-log records from a log file or a rolling log directory."""
    for part in _log_parts(path):
        if part.suffix in (".lz4", ".snappy", ".zstd", ".gz"):
            raise ValueError(
                f"{part.name} is compressed; decompress it before parsing "
                f"(e.g. `lz4 -d`), or set spark.eventLog.compress=false")
        with open(part, "r") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue      # a truncated tail is normal on a killed run


def _accumulable(stage_info: dict, name: str) -> int:
    """Sum an SQL metric across the stage's accumulables. 0 when absent."""
    total = 0
    for acc in stage_info.get("Accumulables", []):
        if acc.get("Name") != name:
            continue
        try:
            total += int(acc.get("Value", 0))
        except (TypeError, ValueError):
            continue
    return total


def _plan_metric_names(node: dict, out: dict[int, str]) -> None:
    """Walk a sparkPlanInfo tree collecting accumulatorId -> metric name."""
    for m in node.get("metrics") or []:
        acc, name = m.get("accumulatorId"), m.get("name")
        if acc is not None and name:
            out[int(acc)] = name
    for child in node.get("children") or []:
        _plan_metric_names(child, out)


def parse_event_log(path: str | Path) -> list[StageRecord]:
    """Stage records for every job this harness tagged, in completion order.

    Untagged jobs (Spark's own internal jobs, and anything run outside
    `measure()`) are skipped, so setup and table-shape probes do not pollute the
    benchmark rows.

    File and partition counts are driver-side SQL metrics rather than task
    metrics, so they arrive keyed by accumulator id and scoped to a SQL
    execution, not a stage. They are resolved against the plan tree and then
    attributed to the execution's input-reading stage, which is the stage they
    describe.
    """
    path = Path(path)
    stage_to_job: dict[int, int] = {}
    job_tag: dict[int, dict] = {}
    job_exec: dict[int, int] = {}
    stages: dict[int, StageRecord] = {}
    acc_name: dict[int, str] = {}
    exec_metrics: dict[int, dict[str, int]] = defaultdict(dict)

    for ev in _iter_events(path):
        kind = ev.get("Event", "")

        if kind.endswith("SparkListenerSQLExecutionStart") or \
                kind.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
            _plan_metric_names(ev.get("sparkPlanInfo") or {}, acc_name)
            continue

        if kind.endswith("SparkListenerDriverAccumUpdates"):
            eid = ev.get("executionId")
            for pair in ev.get("accumUpdates") or []:
                if len(pair) != 2:
                    continue
                name = acc_name.get(int(pair[0]))
                if name in _SQL_METRIC_FIELDS:
                    # Repeated updates for one execution are cumulative reports
                    # of the same quantity, so the largest is the final value.
                    prev = exec_metrics[eid].get(name, 0)
                    exec_metrics[eid][name] = max(prev, int(pair[1]))
            continue

        if kind == "SparkListenerJobStart":
            props = ev.get("Properties") or {}
            tag = parse_tag(props.get("spark.jobGroup.id", "") or "")
            if tag is None:
                continue
            jid = ev["Job ID"]
            job_tag[jid] = tag
            eid = props.get("spark.sql.execution.id")
            if eid is not None:
                try:
                    job_exec[jid] = int(eid)
                except (TypeError, ValueError):
                    pass
            for sid in ev.get("Stage IDs", []):
                stage_to_job[sid] = jid

        elif kind == "SparkListenerTaskEnd":
            sid = ev.get("Stage ID")
            if sid not in stage_to_job:
                continue
            rec = stages.setdefault(
                sid, StageRecord(stage_id=sid, job_id=stage_to_job[sid],
                                 tag=job_tag[stage_to_job[sid]]))
            tm = ev.get("Task Metrics") or {}
            run_ms = tm.get("Executor Run Time", 0)
            rec.executor_runtime_s += run_ms / 1000.0
            # Spark reports CPU time in nanoseconds and run time in ms.
            rec.executor_cpu_time_s += tm.get("Executor CPU Time", 0) / 1e9
            rec.input_read_gb += (tm.get("Input Metrics") or {}).get("Bytes Read", 0) / 1e9
            rec.shuffle_read_gb += (
                (tm.get("Shuffle Read Metrics") or {}).get("Remote Bytes Read", 0)
                + (tm.get("Shuffle Read Metrics") or {}).get("Local Bytes Read", 0)) / 1e9
            rec._task_runtimes.append(run_ms / 1000.0)

        elif kind == "SparkListenerStageCompleted":
            info = ev.get("Stage Info") or {}
            sid = info.get("Stage ID")
            if sid not in stage_to_job:
                continue
            rec = stages.setdefault(
                sid, StageRecord(stage_id=sid, job_id=stage_to_job[sid],
                                 tag=job_tag[stage_to_job[sid]]))
            rec.num_tasks = info.get("Number of Tasks", 0)
            submit, complete = info.get("Submission Time"), info.get("Completion Time")
            if submit and complete:
                rec.stage_duration_s = (complete - submit) / 1000.0
            rec.number_of_files_read = (_accumulable(info, _ACC_FILES)
                                        or _accumulable(info, _ICEBERG_FILES))
            rec.static_number_of_files_read = _accumulable(info, _ACC_STATIC_FILES)
            rec.number_of_partitions_read = (_accumulable(info, _ACC_PARTITIONS)
                                             or _accumulable(info, _ICEBERG_SPLITS))
            rec.number_of_skipped_data_files = _accumulable(info, _ICEBERG_SKIPPED_FILES)
            rec.scan_file_bytes = _accumulable(info, _ICEBERG_FILE_BYTES)

    out = [r.finish() for r in sorted(stages.values(), key=lambda r: r.stage_id)]

    # Attribute each execution's scan metrics to the stage that read the input.
    by_exec: dict[int, list[StageRecord]] = defaultdict(list)
    for rec in out:
        eid = job_exec.get(rec.job_id)
        if eid is not None:
            by_exec[eid].append(rec)
    for eid, recs in by_exec.items():
        target = scan_stage(recs)
        if target is None:
            continue
        for name, value in exec_metrics.get(eid, {}).items():
            field_name = _SQL_METRIC_FIELDS[name]
            # A value already read from stage accumulables wins only if the SQL
            # metric is absent, which is the case for some connectors.
            if getattr(target, field_name) == 0:
                setattr(target, field_name, value)
    return out


def scan_stage(records: Iterable[StageRecord]) -> Optional[StageRecord]:
    """The input-reading stage of a run: the one that read the most bytes.

    This is the stage whose `num_tasks` bounds read parallelism. Returns None
    when no stage read input,
    which happens for a plan-only execution.
    """
    reading = [r for r in records if r.input_read_gb > 0]
    return max(reading, key=lambda r: r.input_read_gb) if reading else None


def group_by_run(records: Iterable[StageRecord]) -> dict[tuple, list[StageRecord]]:
    """Stage records keyed by (strategy, workload, run_index, series_uid,
    latency_ms), the tuple identifying one workload execution."""
    out: dict[tuple, list[StageRecord]] = defaultdict(list)
    for r in records:
        out[tuple(r.tag[f] for f in TAG_FIELDS)].append(r)
    return dict(out)
