# tests/test_stage_log.py
"""Event-log parsing, tested on synthetic events rather than a live session.

The parser's job is to survive a real log, so the fixtures below carry the
shapes that actually break it: untagged jobs from Spark's own machinery, a
stage with no completion event, and a truncated final line from a killed run.
"""
import json
import pytest
from medical_lakehouse_compaction.metrics.stage_log import (
    event_log_configs, group_by_run, make_tag, parse_event_log, parse_tag,
    scan_stage,
)


def _job_start(job_id, stage_ids, tag=None):
    ev = {"Event": "SparkListenerJobStart", "Job ID": job_id,
          "Stage IDs": stage_ids, "Properties": {}}
    if tag:
        ev["Properties"]["spark.jobGroup.id"] = tag
    return ev


def _task_end(stage_id, run_ms, cpu_ns, input_bytes):
    return {"Event": "SparkListenerTaskEnd", "Stage ID": stage_id,
            "Task Metrics": {"Executor Run Time": run_ms,
                             "Executor CPU Time": cpu_ns,
                             "Input Metrics": {"Bytes Read": input_bytes}}}


def _stage_done(stage_id, num_tasks, submit, complete, files=0, partitions=0):
    return {"Event": "SparkListenerStageCompleted",
            "Stage Info": {"Stage ID": stage_id, "Number of Tasks": num_tasks,
                           "Submission Time": submit, "Completion Time": complete,
                           "Accumulables": [
                               {"Name": "number of files read", "Value": files},
                               {"Name": "number of partitions read", "Value": partitions},
                           ]}}


@pytest.fixture
def log(tmp_path):
    def _write(events, truncated_tail=False):
        p = tmp_path / "eventlog"
        with open(p, "w") as fh:
            for e in events:
                fh.write(json.dumps(e) + "\n")
            if truncated_tail:
                fh.write('{"Event": "SparkListenerTaskEnd", "Stage ID"')
        return p
    return _write


def test_tag_round_trip():
    t = make_tag("s3", "w1", 2, "1.2.840.series", 25)
    assert parse_tag(t) == {"strategy": "s3", "workload": "w1", "run_index": 2,
                            "series_uid": "1.2.840.series", "latency_ms": 25}


def test_tag_absent_series_round_trips_as_none():
    assert parse_tag(make_tag("s2", "w3", 0, None, 0))["series_uid"] is None


@pytest.mark.parametrize("bad", ["", "not-a-tag", "s1|w1|x|uid|0", "s1|w1|0|uid"])
def test_parse_tag_rejects_foreign_job_groups(bad):
    """Spark and other libraries set job groups too; those must not be parsed
    into bogus benchmark rows."""
    assert parse_tag(bad) is None


def test_parses_tasks_and_stage_structure(log):
    tag = make_tag("s1", "w1", 0, "uid-a", 10)
    p = log([
        _job_start(0, [0], tag),
        _task_end(0, run_ms=500, cpu_ns=400_000_000, input_bytes=2_000_000_000),
        _task_end(0, run_ms=700, cpu_ns=600_000_000, input_bytes=1_000_000_000),
        _stage_done(0, num_tasks=8, submit=1000, complete=3500,
                    files=138, partitions=1),
    ])
    (rec,) = parse_event_log(p)
    assert rec.num_tasks == 8                       # model's N_tasks
    assert rec.number_of_partitions_read == 1       # model's N_partitions
    assert rec.number_of_files_read == 138
    assert rec.stage_duration_s == 2.5              # model target y
    assert rec.executor_runtime_s == pytest.approx(1.2)
    assert rec.executor_cpu_time_s == pytest.approx(1.0)
    assert rec.input_read_gb == pytest.approx(3.0)
    assert rec.avg_task_duration_s == pytest.approx(0.6)
    assert rec.tag["strategy"] == "s1" and rec.tag["latency_ms"] == 10


def test_untagged_jobs_are_skipped(log):
    """Setup queries and Spark's internal jobs run in the same session and must
    not appear as benchmark stages."""
    p = log([
        _job_start(0, [0]),                                   # no job group
        _task_end(0, 100, 1, 1),
        _stage_done(0, num_tasks=4, submit=0, complete=100),
        _job_start(1, [1], make_tag("s3", "w2", 1, None, 0)),
        _task_end(1, 200, 1, 5_000_000),
        _stage_done(1, num_tasks=2, submit=0, complete=200),
    ])
    recs = parse_event_log(p)
    assert [r.stage_id for r in recs] == [1]
    assert recs[0].tag["workload"] == "w2"


def test_truncated_final_line_is_tolerated(log):
    """A grid killed mid-run leaves a partial last line; the completed stages
    before it must still be readable."""
    p = log([
        _job_start(0, [0], make_tag("s1", "w1", 0, "uid", 0)),
        _task_end(0, 100, 1, 1_000_000),
        _stage_done(0, num_tasks=3, submit=0, complete=100),
    ], truncated_tail=True)
    assert [r.num_tasks for r in parse_event_log(p)] == [3]


def test_stage_without_completion_event_still_carries_task_metrics(log):
    """A stage cut off by a crash has task metrics but no Number of Tasks."""
    p = log([
        _job_start(0, [0], make_tag("s1", "w1", 0, "uid", 0)),
        _task_end(0, 100, 1, 1_000_000),
    ])
    (rec,) = parse_event_log(p)
    assert rec.num_tasks == 0 and rec.executor_runtime_s == pytest.approx(0.1)


def test_scan_stage_picks_the_input_reading_stage(log):
    """W1 has a scan stage and a sort stage; read parallelism is the scan's."""
    tag = make_tag("s3", "w1", 0, "uid", 0)
    p = log([
        _job_start(0, [0, 1], tag),
        _task_end(0, 100, 1, 9_000_000_000),          # scan
        _stage_done(0, num_tasks=1, submit=0, complete=100),
        _task_end(1, 100, 1, 0),                      # shuffle, reads no input
        _stage_done(1, num_tasks=20, submit=0, complete=200),
    ])
    recs = parse_event_log(p)
    assert scan_stage(recs).stage_id == 0
    assert scan_stage(recs).num_tasks == 1            # the W1 crossover claim


def test_scan_stage_is_none_when_nothing_read_input(log):
    p = log([
        _job_start(0, [0], make_tag("s1", "w3", 0, None, 0)),
        _task_end(0, 10, 1, 0),
        _stage_done(0, num_tasks=1, submit=0, complete=10),
    ])
    assert scan_stage(parse_event_log(p)) is None


def test_group_by_run_separates_repetitions_and_series(log):
    p = log([
        _job_start(0, [0], make_tag("s1", "w1", 0, "uid-a", 5)),
        _stage_done(0, num_tasks=8, submit=0, complete=10),
        _job_start(1, [1], make_tag("s1", "w1", 1, "uid-a", 5)),
        _stage_done(1, num_tasks=8, submit=0, complete=10),
        _job_start(2, [2], make_tag("s1", "w1", 0, "uid-b", 5)),
        _stage_done(2, num_tasks=8, submit=0, complete=10),
    ])
    groups = group_by_run(parse_event_log(p))
    assert len(groups) == 3
    assert ("s1", "w1", 0, "uid-a", 5) in groups


def test_event_log_configs_off_by_default():
    assert event_log_configs(None) == {}
    assert event_log_configs("") == {}


def test_event_log_configs_creates_dir_and_sets_uri(tmp_path):
    d = tmp_path / "evlog"
    cfg = event_log_configs(str(d))
    assert d.is_dir()
    assert cfg["spark.eventLog.enabled"] == "true"
    assert cfg["spark.eventLog.dir"].startswith("file://")


def test_compressed_log_fails_loudly(tmp_path):
    """Silently returning no stages would look like the old scan_tasks bug."""
    p = tmp_path / "eventlog.lz4"
    p.write_bytes(b"\x00")
    with pytest.raises(ValueError, match="compressed"):
        parse_event_log(p)


def test_event_log_is_written_uncompressed_so_the_parser_can_read_it(tmp_path):
    """A campaign must not produce logs parse_event_log refuses. Compression is
    opt-in for archival only."""
    cfg = event_log_configs(str(tmp_path / "d"))
    assert cfg["spark.eventLog.compress"] == "false"
    assert event_log_configs(str(tmp_path / "d"), compress=True)["spark.eventLog.compress"] == "true"


def _rolling_dir(tmp_path, parts):
    """Spark 4.0 writes eventlog_v2_<appId>/ with appstatus_* and events_N_*."""
    d = tmp_path / "eventlog_v2_app-1"
    d.mkdir()
    (d / "appstatus_app-1").write_text("")
    (d / ".appstatus_app-1.crc").write_bytes(b"\x00")
    for i, evs in enumerate(parts, start=1):
        (d / f"events_{i}_app-1").write_text(
            "".join(json.dumps(e) + "\n" for e in evs))
    return d


def test_reads_a_rolling_event_log_directory(tmp_path):
    """Spark 4.0 writes a directory, not a file. Passing it to a file reader
    raised IsADirectoryError, which would have made a campaign's stage data
    unreadable after the fact."""
    d = _rolling_dir(tmp_path, [[
        _job_start(0, [0], make_tag("s1", "w1", 0, "uid", 0)),
        _stage_done(0, num_tasks=8, submit=0, complete=100),
    ]])
    assert [r.num_tasks for r in parse_event_log(d)] == [8]


def test_rolling_parts_are_read_in_index_order(tmp_path):
    """events_10 must not sort before events_2."""
    parts = [[_job_start(0, [0], make_tag("s1", "w1", 0, "uid", 0)),
              _stage_done(0, num_tasks=4, submit=0, complete=10)]]
    parts += [[] for _ in range(8)]
    parts.append([_job_start(1, [1], make_tag("s1", "w2", 0, None, 0)),
                  _stage_done(1, num_tasks=9, submit=0, complete=10)])
    d = _rolling_dir(tmp_path, parts)
    assert [p.name for p in __import__("medical_lakehouse_compaction.metrics.stage_log", fromlist=["x"])._log_parts(d)][-1].endswith("events_10_app-1")
    assert sorted(r.num_tasks for r in parse_event_log(d)) == [4, 9]


def test_directory_without_event_parts_fails_loudly(tmp_path):
    d = tmp_path / "empty"; d.mkdir()
    with pytest.raises(FileNotFoundError, match="events_"):
        parse_event_log(d)


def _sql_start(exec_id, metrics):
    """SparkListenerSQLExecutionStart carrying a plan whose scan node names the
    accumulator ids that DriverAccumUpdates later reports values for."""
    return {"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": exec_id,
            "sparkPlanInfo": {"nodeName": "Project", "metrics": [], "children": [
                {"nodeName": "BatchScan parquet", "children": [],
                 "metrics": [{"name": n, "accumulatorId": a} for n, a in metrics]}]}}


def _driver_accums(exec_id, pairs):
    return {"Event": "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
            "executionId": exec_id, "accumUpdates": [list(p) for p in pairs]}


def _job_start_sql(job_id, stage_ids, tag, exec_id):
    ev = _job_start(job_id, stage_ids, tag)
    ev["Properties"]["spark.sql.execution.id"] = str(exec_id)
    return ev


def test_file_counts_come_from_driver_sql_metrics(log):
    """`number of files read` is a driver-side SQL metric keyed by accumulator
    id, not a stage accumulable, so it has to be resolved against the plan tree.
    Verified against a real Spark 4.0.0 log: 40 files reported 40, one file 1."""
    p = log([
        _sql_start(7, [("number of files read", 375), ("size of files read", 377)]),
        _job_start_sql(0, [0], make_tag("s1", "w1", 0, "uid", 0), exec_id=7),
        _task_end(0, 100, 1, 910_588),
        _stage_done(0, num_tasks=10, submit=0, complete=100),
        _driver_accums(7, [(375, 40), (377, 910_588)]),
    ])
    (rec,) = parse_event_log(p)
    assert rec.num_tasks == 10
    assert rec.number_of_files_read == 40


def test_sql_metrics_land_on_the_scan_stage_not_the_shuffle_stage(log):
    p = log([
        _sql_start(1, [("number of files read", 90)]),
        _job_start_sql(0, [0, 1], make_tag("s3", "w1", 0, "uid", 0), exec_id=1),
        _task_end(0, 100, 1, 5_000_000),          # scan
        _stage_done(0, num_tasks=1, submit=0, complete=100),
        _task_end(1, 100, 1, 0),                  # shuffle
        _stage_done(1, num_tasks=20, submit=0, complete=200),
        _driver_accums(1, [(90, 1)]),
    ])
    recs = {r.stage_id: r for r in parse_event_log(p)}
    assert recs[0].number_of_files_read == 1
    assert recs[1].number_of_files_read == 0


def test_unknown_accumulator_ids_are_ignored(log):
    """Every SQL node publishes metrics; only the scan-structure ones matter."""
    p = log([
        _sql_start(2, [("number of output rows", 11)]),
        _job_start_sql(0, [0], make_tag("s1", "w3", 0, None, 0), exec_id=2),
        _task_end(0, 10, 1, 1_000),
        _stage_done(0, num_tasks=2, submit=0, complete=10),
        _driver_accums(2, [(11, 200000), (999, 5)]),
    ])
    (rec,) = parse_event_log(p)
    assert rec.number_of_files_read == 0 and rec.num_tasks == 2
