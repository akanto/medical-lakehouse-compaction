#!/usr/bin/env python3
"""Per-cell ballpark check for a running (or finished) experiment grid.

Run against `benchmark_partial.json` after each `Checkpoint:` line, or against a
final `benchmark_*.json`. Checks are scale-agnostic (work at 10 and 200 series)
and deliberately do NOT hardcode absolute request counts — the strongest signal
is *cell-invariance*: for a given (strategy, workload, replicate) the request
counts and bytes-on-wire are near-deterministic per layout, so they must be
constant across every completed cell. A drift means a mis-built table or a
corrupted/short read — exactly the "not in the ballpark" case to catch early,
before burning the rest of a launch.

Exit code: 0 = PASS (warnings allowed), 1 = FAIL. Prints one line per finding.

Usage:
  python scripts/validate_cell.py results/aws/benchmark_partial.json
  python scripts/validate_cell.py <json> --per-cell 84   # 84 (200-series) / 20 (smoke)
"""
import argparse
import json
import statistics
import sys
from collections import defaultdict

# Substrate-aware RTT tolerance (ms). The check is on (measured - nominal -
# substrate); netem jitter is a larger *fraction* at 1/3 ms but the absolute
# offset stays small, so a fixed absolute band is right. Matches Step 10.
RTT_TOL_MS = 0.6
# Cell-invariant counts per layout. bytes_on_wire is effectively deterministic;
# GET/total include LIST+HEAD, which wiggle a few counts on the small binpack
# (S2) tables (seen: W3 GETs 217↔220). Flag only drift that is BOTH relative
# and absolutely large — a mis-built table or short read drifts by multiples,
# not by a handful. Bytes get the tight relative band; request counts also need
# an absolute floor to clear the benign S2 noise.
INVARIANCE_TOL = {"bytes_on_wire": 0.01,
                  "s3_get_requests": 0.03, "s3_total_requests": 0.03}
INVARIANCE_ABS_FLOOR = {"bytes_on_wire": 0,  # relative band alone
                        "s3_get_requests": 32, "s3_total_requests": 32}


def _key(r):
    """Identity of a replicate that must repeat identically across cells:
    strategy + workload + which replicate (W1 = series_uid, W2/W3 = run_index)."""
    rep = r.get("series_uid") or r.get("run_index")
    return (r["strategy"], r["workload"], rep)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--per-cell", type=int, default=84,
                    help="expected entries per cell: 4 strategies x (w1_n_series + 2) x benchmark_runs. 84 for the 200-series campaign (5,3), 20 for smoke and explore (3,1)")
    args = ap.parse_args()

    d = json.load(open(args.path))
    res, net = d["results"], d.get("network", [])
    fails, warns, oks = [], [], []

    # --- Network: measured RTT ~ nominal + substrate (substrate = median offset)
    valid = [(n["latency_ms_nominal"], n["rtt_ms_measured"]) for n in net
             if n.get("rtt_ms_measured") is not None]
    if len(valid) != len(net):
        fails.append(f"{len(net) - len(valid)} cell(s) have measured RTT = None "
                     "(TSG SSH/shaping failed) — halt the launch")
    if valid:
        offsets = sorted(m - nom for nom, m in valid)
        substrate = offsets[len(offsets) // 2]
        if not (0.1 < substrate < 1.0):
            warns.append(f"substrate RTT {substrate:.3f} ms outside 0.1–1.0 "
                         "(low-latency netem jitter?) — inspect")
        for nom, m in valid:
            if abs(m - nom - substrate) >= RTT_TOL_MS:
                fails.append(f"RTT mismatch: nominal {nom} ms, measured {m:.3f} "
                             f"ms, off {m - nom - substrate:+.3f} vs substrate "
                             f"{substrate:.3f} — check tc/netem on this cell")
        oks.append(f"network: {len(valid)} cells, substrate {substrate:.3f} ms")

    # --- Per-cell shape + basic sanity
    by_cell = defaultdict(list)
    for r in res:
        by_cell[(r["rate_gbit"], r["latency_ms"])].append(r)
    for cell, rows in sorted(by_cell.items(), key=lambda kv: str(kv[0])):
        if len(rows) != args.per_cell:
            fails.append(f"cell {cell}: {len(rows)} entries, expected {args.per_cell}")
        for r in rows:
            if not (r["wall_clock_s"] > 0):
                fails.append(f"cell {cell} {_key(r)}: wall_clock_s={r['wall_clock_s']}")
            if not (r["bytes_on_wire"] > 0):
                fails.append(f"cell {cell} {_key(r)}: bytes_on_wire={r['bytes_on_wire']}")
        # S1 must be the request hog in every cell (unprunable full scans).
        gets = defaultdict(list)
        for r in rows:
            gets[r["strategy"]].append(r["s3_get_requests"])
        if "s1" in gets:
            s1max = max(gets["s1"])
            for strat in ("s2", "s3", "s4"):
                if strat in gets and max(gets[strat]) >= s1max:
                    fails.append(f"cell {cell}: {strat} GETs ≥ s1 "
                                 f"({max(gets[strat])} vs {s1max}) — layout wrong?")

    # --- Cell-invariance: same replicate must give same counts across all cells
    series = defaultdict(lambda: defaultdict(list))  # key -> metric -> [values]
    for r in res:
        for metric in ("s3_get_requests", "s3_total_requests", "bytes_on_wire"):
            series[_key(r)][metric].append(r[metric])
    drift = 0
    for k, metrics in series.items():
        for metric, vals in metrics.items():
            lo, hi = min(vals), max(vals)
            rel_bad = lo > 0 and (hi - lo) / lo > INVARIANCE_TOL[metric]
            abs_bad = (hi - lo) > INVARIANCE_ABS_FLOOR[metric]
            if rel_bad and abs_bad and len(vals) > 1:
                drift += 1
                if drift <= 8:  # cap the noise
                    warns.append(f"{k} {metric} varies across cells "
                                 f"[{lo}..{hi}] — should be layout-invariant")
    if drift:
        warns.append(f"{drift} (replicate,metric) pairs drift across cells "
                     "— investigate before trusting the launch")
    else:
        oks.append("cell-invariance: all request/byte counts constant across cells")

    for m in oks:
        print(f"OK   {m}")
    for m in warns:
        print(f"WARN {m}")
    for m in fails:
        print(f"FAIL {m}")
    n_cells = len(by_cell)
    print(f"--- {n_cells} cells, {len(res)} entries, "
          f"{len(fails)} fail / {len(warns)} warn")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
