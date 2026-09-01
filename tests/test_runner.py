# tests/test_runner.py
import json
from medical_lakehouse_compaction.benchmarks.runner import pick_spread, save_results
from medical_lakehouse_compaction.metrics.collector import BenchmarkResult


def _result():
    return BenchmarkResult("s1", "w1", 10, 5.2, 105_000_000, 0)


def test_save_results_timestamped_final(tmp_path):
    out = save_results([_result()], str(tmp_path), "p.yaml", "uid", {"n_series": 10})
    payload = json.loads(out.read_text())
    assert out.name.startswith("benchmark_")
    assert "partial" not in payload
    assert len(payload["results"]) == 1


def test_save_results_partial_checkpoint_overwrites(tmp_path):
    for n in (1, 2):
        out = save_results([_result()] * n, str(tmp_path), "p.yaml", "uid",
                           {"n_series": 10}, filename="benchmark_partial.json",
                           partial=True)
    assert out.name == "benchmark_partial.json"
    payload = json.loads(out.read_text())
    assert payload["partial"] is True
    assert len(payload["results"]) == 2  # second checkpoint replaced the first
    assert len(list(tmp_path.glob("*.json"))) == 1


def test_save_results_w1_series_list(tmp_path):
    out = save_results([_result()], str(tmp_path), "p.yaml",
                       ["uid-a", "uid-b"], {"n_series": 10})
    payload = json.loads(out.read_text())
    assert payload["w1_series_uids"] == ["uid-a", "uid-b"]
    assert "series_uid" not in payload


def test_pick_spread_endpoints_and_determinism():
    items = list(range(100))
    picked = pick_spread(items, 10)
    assert len(picked) == 10
    assert picked[0] == 0 and picked[-1] == 99   # endpoints included
    assert picked == pick_spread(items, 10)       # deterministic
    assert picked == sorted(set(picked))          # no repeats, ordered


def test_pick_spread_edge_cases():
    assert pick_spread([1, 2, 3], 5) == [1, 2, 3]   # n >= len: all items
    assert pick_spread(list(range(11)), 1) == [5]   # n=1: median element


def test_run_all_workloads_repeats_w1(monkeypatch):
    """W1 must run benchmark_runs times per series, with run_index = the
    repetition (not the series index) so its error bar is repeatability and
    means the same thing as W2's and W3's. Regression guard for the
    2026-08-29 change; see conf/profiles/experiment.yaml."""
    from medical_lakehouse_compaction.benchmarks import runner

    calls = []

    def fake(workload):
        def _f(*args, **kwargs):
            calls.append((workload, kwargs["strategy"], kwargs["run_index"],
                          args[2] if workload == "w1" else None))
            return BenchmarkResult(kwargs["strategy"], workload,
                                   kwargs["latency_ms"], 1.0, 1, 0)
        return _f

    monkeypatch.setattr(runner, "run_w1_benchmark", fake("w1"))
    monkeypatch.setattr(runner, "run_w2_benchmark", fake("w2"))
    monkeypatch.setattr(runner, "run_w3_benchmark", fake("w3"))

    uids = ["uid-a", "uid-b", "uid-c"]
    tables = {"s1": "t1", "s2": "t2"}
    setup = {s: {"cache": {}, "slice_dims": (512, 512),
                 "w1_bytes": {u: 1 for u in uids}, "w3_bytes": 1}
             for s in tables}
    cfg = {"benchmark_runs": 3, "w2_n_batches": 2, "w2_batch_series": 3}

    res = runner.run_all_workloads(None, cfg, uids, 10,
                                   tables=tables, setup=setup)

    # 2 strategies x 3 reps x (3 W1 + 1 W2 + 1 W3)
    assert len(res) == 2 * 3 * (3 + 1 + 1) == 30
    w1 = [c for c in calls if c[0] == "w1"]
    assert len(w1) == 2 * 3 * 3
    # every (strategy, series) gets exactly benchmark_runs replicates,
    # indexed 0..2 — the same run_index domain as W2/W3
    for strat in tables:
        for uid in uids:
            idx = sorted(c[2] for c in w1 if c[1] == strat and c[3] == uid)
            assert idx == [0, 1, 2]
    for workload in ("w2", "w3"):
        assert sorted(c[2] for c in calls if c[0] == workload) == \
            sorted([0, 1, 2] * len(tables))
