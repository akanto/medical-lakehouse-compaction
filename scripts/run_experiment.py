#!/usr/bin/env python3
"""AWS/WAN driver: same W1/W2/W3 benchmark and JSON output as run_benchmark.py,
but sweeps the full rate x latency grid, applying each cell on the Traffic
Shaping Gateway over SSH (sudo setup_tsg.sh --rate <gbit|none> --rtt <ms>) and
recording the ping-measured RTT next to the nominal value. Runs ON the Spark
instance, so pings traverse the shaped path. --manual restores the old
prompt-and-wait behaviour (operator applies tc by hand)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import re
import subprocess
import time
from medical_lakehouse_compaction.config import load_profile
from medical_lakehouse_compaction.spark_session import create_spark_session
from medical_lakehouse_compaction.benchmarks.runner import (
    precompute_setup, select_w1_series, run_all_workloads, save_results)

SETTLE_SECONDS = 5  # let in-flight TCP windows adapt to the new shaping

_SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10"]


def apply_tsg_level(cfg: dict, rate, latency_ms: int) -> None:
    """SSH to the TSG and apply one shaping cell. The script lives at
    ~/setup_tsg.sh on the gateway (placed there by `make tsg-setup`)."""
    target = f"{cfg['tsg_ssh_user']}@{cfg['tsg_host']}"
    script = cfg.get("tsg_script_path", "~/setup_tsg.sh")
    cmd = ["ssh", *_SSH_OPTS, target,
           f"sudo bash {script} --rate {rate} --rtt {latency_ms}"]
    subprocess.run(cmd, check=True)


def measure_rtt(host: str, count: int = 10):
    """Average RTT in ms from `ping`, plus the raw summary for the log.
    Returns (None, <error>) instead of raising — a failed ping should be
    visible in the results, not abort a multi-hour sweep."""
    try:
        out = subprocess.run(
            ["ping", "-c", str(count), "-q", host],
            capture_output=True, text=True, timeout=60 + count,
        ).stdout
    except Exception as exc:  # noqa: BLE001 - record and continue
        return None, f"ping failed: {exc}"
    # Linux: "rtt min/avg/max/mdev = 0.301/0.412/..." ; macOS: "round-trip ..."
    m = re.search(r"= [\d.]+/([\d.]+)/", out)
    return (float(m.group(1)) if m else None), out.strip()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="conf/profiles/experiment.yaml")
    parser.add_argument("--output-dir", default="results")
    parser.add_argument("--series-uid", default=None,
                        help="SeriesInstanceUID for W1; auto-detected if omitted")
    parser.add_argument("--manual", action="store_true",
                        help="prompt instead of applying shaping via SSH")
    args = parser.parse_args()

    cfg = load_profile(args.profile)
    spark = create_spark_session(cfg)

    # W1 series set: w1_n_series UIDs spanning the slice-count distribution
    # (distinct series are the W1 replicates — one run per series per cell).
    if args.series_uid:
        series_uids = [args.series_uid]
    else:
        series_uids = select_w1_series(spark, cfg.get("w1_n_series", 1))
    print(f"W1 series ({len(series_uids)}, size-ordered): "
          f"...{series_uids[0][-24:]} .. ...{series_uids[-1][-24:]}")

    # Cell-invariant setup (metadata caches, dims, bytes_useful denominators),
    # computed ONCE on an unshaped network. Running it inside every cell is
    # far more expensive: on S1 each setup query is an unprunable full scan,
    # adding ~12 unmeasured full scans per cell.
    if not args.manual:
        apply_tsg_level(cfg, "none", 0)
        time.sleep(SETTLE_SECONDS)
    print("\n=== Precomputing cell-invariant setup (unshaped) ===")
    setup = precompute_setup(spark, series_uids)

    # Warm-up pass, discarded: the first execution of a fresh session carries
    # JVM/S3A/Iceberg warm-up, which otherwise lands entirely on the first
    # cell of the grid. Runs unshaped so it costs seconds, not minutes.
    if cfg.get("warmup", True):
        print("\n=== Warm-up pass (results discarded) ===")
        # latency_ms=-1 marks these executions in the event-log job tags. The
        # results are dropped here, but the stage records are not: with the
        # real first cell also at latency 0, run_index 0 and series_uids[0],
        # a warm-up execution would otherwise land in the same tag group as
        # the measurement and could supply its `stage_duration_s`, which is
        # the transport-aware model's target. No real cell is negative.
        run_all_workloads(spark, {**cfg, "benchmark_runs": 1}, [series_uids[0]],
                          latency_ms=-1, setup=setup)

    rates = cfg.get("rate_gbit", ["none"])
    results = []
    network_levels = []
    partial_name = "benchmark_partial.json"
    # Rate outer, latency inner: fewer HTB rebuilds per sweep.
    for rate in rates:
        for latency_ms in cfg["latency_ms"]:
            print(f"\n=== Cell: rate {rate} Gbit, nominal RTT {latency_ms} ms ===")
            if args.manual:
                print(f"NOTE: on the TSG run: sudo bash setup_tsg.sh "
                      f"--rate {rate} --rtt {latency_ms}")
                input("Press Enter when tc is configured...")
            else:
                apply_tsg_level(cfg, rate, latency_ms)
                time.sleep(SETTLE_SECONDS)

            rtt_measured, ping_raw = measure_rtt(cfg.get("minio_ping_host", "")) \
                if cfg.get("minio_ping_host") else (None, "minio_ping_host not set")
            print(f"Measured RTT: {rtt_measured} ms (nominal {latency_ms} ms)")
            network_levels.append({
                "rate_gbit": None if rate == "none" else rate,
                "latency_ms_nominal": latency_ms,
                "rtt_ms_measured": rtt_measured,
                "ping_raw": ping_raw,
            })

            cell = run_all_workloads(spark, cfg, series_uids, latency_ms,
                                     setup=setup)
            for r in cell:
                r.rate_gbit = None if rate == "none" else rate
            results += cell

            # Checkpoint after every cell: a crash at hour 9 of a multi-hour
            # sweep loses at most the current cell. Recovery: trim the
            # already-completed values out of rate_gbit/latency_ms in the
            # profile, rerun, and merge with the partial JSON.
            save_results(results, args.output_dir, args.profile, series_uids,
                         cfg, network_levels=network_levels,
                         filename=partial_name, partial=True)
            print(f"Checkpoint: {len(network_levels)} cells, "
                  f"{len(results)} results -> {partial_name}")

    out_path = save_results(results, args.output_dir, args.profile, series_uids,
                            cfg, network_levels=network_levels)
    (Path(args.output_dir) / partial_name).unlink(missing_ok=True)
    print(f"\nResults saved to {out_path}")
    spark.stop()
