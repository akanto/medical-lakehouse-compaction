#!/usr/bin/env python3
"""Read a benchmark JSON and print a human-readable results table."""
import json
import sys
import argparse
import statistics
from pathlib import Path

STRATEGY_LABELS = {
    "s1": "S1  raw (1 file/slice)",
    "s2": "S2  generic compaction",
    "s3": "S3  domain sort",
    "s4": "S4  partition spec",
}
STRATEGIES = ["s1", "s2", "s3", "s4"]
WORKLOAD_LABELS = {
    "w1": "W1 — 3D CT Reconstruction  (single-series read)",
    "w2": "W2 — Training Data Loading  (random-batch read)",
    "w3": "W3 — Metadata Cohort Scan   (full-table, no pixel data)",
}


def mean(xs: list) -> float:
    return sum(xs) / len(xs) if xs else float("nan")


def stdev(xs: list) -> float:
    return statistics.stdev(xs) if len(xs) > 1 else 0.0


def speedup_str(baseline: float, t: float) -> str:
    if t == 0:
        return "     ∞×"
    ratio = baseline / t
    marker = " *" if ratio >= 1.5 else "  "
    return f"{ratio:5.2f}×{marker}"


def mb(b: float) -> float:
    return b / 1024 / 1024


def load_file(path: Path) -> dict:
    return json.loads(path.read_text())


def latest_file(results_dir: str) -> Path:
    files = sorted(Path(results_dir).glob("benchmark_*.json"))
    if not files:
        sys.exit(f"No benchmark_*.json files found in '{results_dir}/'")
    return files[-1]


def section(label: str, width: int) -> str:
    dashes = width - len(label) - 5
    return f"  ── {label} {'─' * max(dashes, 4)}"


def print_report(data: dict, path: Path) -> None:
    W = 74
    results = data["results"]
    latencies = sorted({r["latency_ms"] for r in results})
    n_runs = max((r["run_index"] for r in results), default=0) + 1

    print()
    print("=" * W)
    print("  BENCHMARK RESULTS")
    print("=" * W)
    print(f"  File    : {path.name}")
    print(f"  Profile : {data.get('profile', '?')}")
    print(f"  Series  : {data.get('n_series', '?')}  |  runs/strategy: {n_runs}")
    uid = data.get("series_uid", "")
    print(f"  W1 UID  : ...{uid[-40:]}")
    print("=" * W)

    LABEL_W = 22
    # run times column width: n_runs × "XX.XXX" (6) + separators (2 each)
    RUNS_W = max(n_runs * 8 - 2, 16)

    for workload in ["w1", "w2", "w3"]:
        print(f"\n{WORKLOAD_LABELS[workload]}")

        for latency_ms in latencies:
            subset = [r for r in results
                      if r["workload"] == workload and r["latency_ms"] == latency_ms]
            if not subset:
                continue

            # bytes_useful is identical across strategies/runs for this block
            bu_bytes = mean([r["bytes_useful"] for r in subset if r["bytes_useful"] > 0])
            bu_mb = mb(bu_bytes)

            s1_times = [r["wall_clock_s"] for r in subset if r["strategy"] == "s1"]
            s1_mean = mean(s1_times)

            print(f"\n  Latency: {latency_ms} ms   |   "
                  f"bytes_useful: {bu_mb:.1f} MB/run")

            # ── Timing ─────────────────────────────────────────────────────
            print(section("Timing", W))
            hdr = (f"  {'Strategy':<{LABEL_W}}  "
                   f"{'run times (s)':<{RUNS_W}}  "
                   f"{'mean':>6}  {'std':>5}  {'vs S1':>8}")
            print(hdr)
            print("  " + "─" * (len(hdr) - 2))

            for strat in STRATEGIES:
                times = [r["wall_clock_s"] for r in subset if r["strategy"] == strat]
                if not times:
                    continue
                times_sorted = sorted(times)
                m = mean(times)
                sd = stdev(times)
                sp = speedup_str(s1_mean, m)
                runs_str = "  ".join(f"{t:6.3f}" for t in times_sorted)
                label = STRATEGY_LABELS.get(strat, strat)
                print(f"  {label:<{LABEL_W}}  {runs_str:<{RUNS_W}}  "
                      f"{m:>6.3f}  {sd:>5.3f}  {sp}")

            # ── I/O ────────────────────────────────────────────────────────
            print(section("I/O", W))
            io_hdr = (f"  {'Strategy':<{LABEL_W}}  "
                      f"{'tasks':>5}  {'read MB':>7}  "
                      f"{'RAR':>5}  {'useful MB/s':>11}")
            print(io_hdr)
            print("  " + "─" * (len(io_hdr) - 2))

            for strat in STRATEGIES:
                rows_s = [r for r in subset if r["strategy"] == strat]
                if not rows_s:
                    continue
                m = mean([r["wall_clock_s"] for r in rows_s])

                task_vals = [r.get("scan_tasks", 0) for r in rows_s]
                tasks_str = (f"{mean(task_vals):>5.0f}"
                             if any(t > 0 for t in task_vals) else "  n/a")

                read_vals = [r.get("bytes_on_wire", 0) for r in rows_s]
                read_str = (f"{mb(mean(read_vals)):>7.1f}"
                            if any(b > 0 for b in read_vals) else "    n/a")

                # rar_wire only: the legacy SparkUI-based `rar` in pre-2026-07
                # JSONs undercounts Iceberg reads and must never be displayed.
                rar_vals = []
                for r in rows_s:
                    if r.get("bytes_on_wire", 0) > 0:
                        val = r.get("rar_wire", 0)
                        if val not in (float("inf"),) and val == val:
                            rar_vals.append(val)
                rar_str = f"{mean(rar_vals):>5.2f}" if rar_vals else "  n/a"

                tp = bu_mb / m if m > 0 and bu_bytes > 0 else 0.0
                label = STRATEGY_LABELS.get(strat, strat)
                print(f"  {label:<{LABEL_W}}  {tasks_str}  {read_str}  "
                      f"{rar_str}  {tp:>11.1f}")

    # ── Verdict ────────────────────────────────────────────────────────────
    print()
    print("=" * W)
    print("  VERDICT")
    print("=" * W)
    for workload in ["w1", "w2", "w3"]:
        for latency_ms in latencies:
            wrows = [r for r in results if r["workload"] == workload
                     and r["latency_ms"] == latency_ms]
            if not wrows:
                continue
            bu_bytes = mean([r["bytes_useful"] for r in wrows if r["bytes_useful"] > 0])
            bu_mb = mb(bu_bytes)
            s1_t = mean([r["wall_clock_s"] for r in wrows if r["strategy"] == "s1"])
            ranked = sorted(
                [(s, mean([r["wall_clock_s"] for r in wrows if r["strategy"] == s]))
                 for s in STRATEGIES if any(r["strategy"] == s for r in wrows)],
                key=lambda x: x[1],
            )
            wl_short = WORKLOAD_LABELS[workload].split("—")[0].strip()
            print(f"\n  {wl_short} @ {latency_ms} ms")
            for i, (strat, t) in enumerate(ranked):
                lab = STRATEGY_LABELS.get(strat, strat)
                ratio = s1_t / t if t > 0 else float("inf")
                tp = bu_mb / t if t > 0 and bu_bytes > 0 else 0.0
                marker = "  ← winner" if i == 0 else ""
                print(f"    {lab:<22}  {t:.3f}s  {ratio:.2f}× vs S1  "
                      f"{tp:.1f} MB/s useful{marker}")

    print()
    print("  Notes")
    print("  " + "─" * (W - 2))
    print("  tasks     = Iceberg read tasks (≈ files opened; proxy for WAN round-trips)")
    print("  read MB   = avg bytes fetched from object store per run")
    print("  RAR       = read MB / useful MB  (1.0 = perfect; n/a if SparkUI unavailable)")
    print("              can be < 1.0: wire bytes are Parquet/Snappy-compressed while")
    print("              useful MB is uncompressed logical size — read as transfer")
    print("              efficiency, not literal amplification")
    print("  useful MB/s = bytes_useful / wall_clock_s  (throughput on useful data only)")
    print("  *         = speedup ≥ 1.5×")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate benchmark JSON results.")
    parser.add_argument("--input", default=None,
                        help="Path to benchmark JSON (default: latest in --results-dir)")
    parser.add_argument("--results-dir", default="results")
    args = parser.parse_args()

    if args.input:
        path = Path(args.input)
        if not path.exists():
            sys.exit(f"File not found: {path}")
        data = load_file(path)
    else:
        path = latest_file(args.results_dir)
        data = load_file(path)

    print_report(data, path)
