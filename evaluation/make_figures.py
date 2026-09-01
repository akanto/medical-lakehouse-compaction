"""Render fig_w1.png, fig_w2.png and fig_w3.png from the committed campaign.

One figure per workload, each a row of three panels one IEEE column wide: x is
the injected round-trip time, y is the median wall-clock time, and each panel
holds one bandwidth limit, with one line per layout.

L4 is not plotted. It tracks L3 closely enough that the two lines would
overprint; it stays in the tables, where the difference is legible.

No crossing point is marked. The grid measures five round-trip times, and a
marker at an intersection would assert a value between two of them; the
segments already interpolate, which is as far as the data goes.

Log y throughout, since the workloads span several decades and a linear axis
would spend its height on one line. Past two decades the intermediate ticks
collide at this panel size, so W3 is ticked by decade while W1 and W2 use the
1-2-5 ladder; W1 and W2 share ticks and limits so the two can be read against
each other.

Colours are Okabe-Ito. Marker shape and dash pattern repeat the distinction, so
the lines stay separable in greyscale print.

Run it with `make figures`, or directly:
  python evaluation/make_figures.py [--workloads w1,w2] [--preview PATH]

--workloads selects which figures to write, and stacks several workloads into
one figure if they are joined by a plus (w1+w2 writes fig_w1_w2.png with two
rows). --preview renders the first requested figure to PATH alone.
"""

import argparse
import json
import math
import shutil
import statistics as st
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parent.parent
CAMPAIGN = REPO / ("results/campaign-2026-08-30/"
                   "benchmark_campaign_20260830_merged.json")
# figures/ is gitignored: the rendered figures are IEEE copyright once the
# paper is accepted, so they are regenerated rather than committed.
OUT_DIRS = [REPO / "figures"]

RATES = [5, 2, 1]          # left to right, one panel each
LATS = [0, 2, 5, 10, 25]   # x positions, in real units
FIGURES = ["w1", "w2", "w3"]

# Amber is the raw table, carried over from the heatmap's convention, in which
# amber marked the cells the raw table wins. The blue is that figure's #4C9FD0
# and the green is Okabe-Ito's #009E73.
LAYOUTS = [("s1", "L1", "#E69F00", "o", "-"),
           ("s2", "L2", "#4C9FD0", "s", "--"),
           ("s3", "L3", "#009E73", "^", "-.")]

# The 1-2-5 ladder, so every plotted value lands near a tick.
YTICK_CANDIDATES = [0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500]

# W1 and W2 share one y scale, ticks and limits alike. Their ranges overlap
# (1.55 to 142.64 s against 6.40 to 127.19 s), so separate scales invited a
# comparison of heights between the two figures that did not hold. W3 keeps its
# own, since it spans three and a half decades and would flatten both.
SHARED_Y = ("w1", "w2")

d = json.loads(CAMPAIGN.read_text())
rs = [r for r in d["results"] if r["rate_gbit"] in RATES]


def median_wall(w, rate, lat, s):
    v = [r["wall_clock_s"] for r in rs
         if r["workload"] == w and r["rate_gbit"] == rate
         and r["latency_ms"] == lat and r["strategy"] == s]
    return st.median(v)


def ticks_for(lo, hi):
    # Bracket the data rather than sit inside it: with ticks only at or above
    # the minimum, W2's L3 line at 5 Gb/s ran below the lowest tick and the
    # band it occupies carried no reference value.
    if hi / lo > 100:
        # Past two decades the intermediate ticks collide at this panel size:
        # W3 runs 0.30 to 483.10 s and printed 0.3 on top of 0.5.
        decades = range(math.floor(math.log10(lo)),
                        math.floor(math.log10(hi)) + 2)
        return [10.0 ** e for e in decades if 10.0 ** e <= hi * 1.3]
    ticks = [t for t in YTICK_CANDIDATES if lo <= t <= hi]
    below = [t for t in YTICK_CANDIDATES if t < lo]
    above = [t for t in YTICK_CANDIDATES if t > hi]
    if below:
        ticks.insert(0, below[-1])
    if above and above[0] <= hi * 1.4:
        ticks.append(above[0])
    return ticks


def yscale_for(w):
    """Ticks and limits for one workload row, shared across SHARED_Y."""
    keys = SHARED_Y if w in SHARED_Y else (w,)
    vals = [median_wall(k, rate, lat, s) for k in keys for rate in RATES
            for lat in LATS for s, *_ in LAYOUTS]
    lo, hi = min(vals), max(vals)
    ticks = ticks_for(lo, hi)
    # The lowest tick must sit inside the axis or it is not drawn, and the top
    # must clear the data even when the highest tick falls below it.
    return ticks, (min(ticks) * 0.95, max(hi * 1.15, max(ticks) * 1.02))


def render(keys, preview=None):
    """Draw one figure with a row per workload key and save it."""
    series = {(w, rate, s): [median_wall(w, rate, lat, s) for lat in LATS]
              for w in keys for rate in RATES for s, *_ in LAYOUTS}

    plt.rcParams.update({"font.size": 6.5, "axes.labelsize": 6.5,
                         "xtick.labelsize": 6, "ytick.labelsize": 6})
    # Height is per workload row. One row is about 111 pt of column once the
    # caption is added, which is what makes three separate figures affordable.
    fig, axes = plt.subplots(len(keys), len(RATES),
                             figsize=(3.5, 1.55 * len(keys)),
                             sharex=True, sharey="row", squeeze=False,
                             layout="constrained")

    for row, w in enumerate(keys):
        ticks, ylim = yscale_for(w)
        for col, rate in enumerate(RATES):
            ax = axes[row][col]
            for s, label, colour, marker, dash in LAYOUTS:
                ax.plot(LATS, series[(w, rate, s)], color=colour,
                        linestyle=dash, marker=marker, markersize=2.4,
                        linewidth=0.9, markeredgewidth=0, label=label)
            ax.set_yscale("log")
            ax.set_yticks(ticks)
            ax.set_ylim(*ylim)
            # The log locator prints 2x10^1 style labels and adds minor ticks
            # that crowd a panel this size, so both are set by hand.
            ax.set_yticklabels([f"{t:g}" for t in ticks])
            ax.minorticks_off()
            ax.set_xticks(LATS)
            ax.set_xticklabels([str(l) for l in LATS])
            ax.tick_params(length=1.5, pad=1.5)
            for side in ("top", "right"):
                ax.spines[side].set_visible(False)
            for side in ("left", "bottom"):
                ax.spines[side].set_linewidth(0.5)
            if row == 0:
                ax.set_title(f"{rate} Gb/s", fontsize=6.5, pad=2)
            if row == len(keys) - 1:
                ax.set_xlabel("RTT (ms)", fontsize=6.5, labelpad=1)
            if col == 0:
                stem = "Execution time (s)"
                ax.set_ylabel(f"{w.upper()}\n{stem}" if len(keys) > 1 else stem,
                              fontsize=6.5, labelpad=1)

    # One legend per figure, below the row. Three entries fit on one line at
    # this width, and a per-panel legend would cover the crossing in the 5 Gb/s
    # panel, which is the one the eye goes to first.
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="outside lower center", ncol=len(LAYOUTS),
               frameon=False, fontsize=6.5, handlelength=2.4,
               columnspacing=1.6, handletextpad=0.5, borderpad=0.1)

    name = f"fig_{'_'.join(keys)}.png"
    if preview:
        out = Path(preview)
        out.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(out, dpi=600, bbox_inches="tight")
        print(f"wrote {out}  (preview only)")
    else:
        # Rendered once and copied: bbox_inches="tight" recomputes the bounding
        # box on every call and a second render differs by a few pixels.
        first = OUT_DIRS[0] / name
        first.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(first, dpi=600, bbox_inches="tight")
        for out_dir in OUT_DIRS[1:]:
            out_dir.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(first, out_dir / name)
        for out_dir in OUT_DIRS:
            print(f"wrote {out_dir / name}")
    plt.close(fig)


ap = argparse.ArgumentParser()
ap.add_argument("--workloads", help="comma-separated figures to write; join"
                " keys with + to stack them as rows of one figure")
ap.add_argument("--preview", help="render the first figure to this path only")
args = ap.parse_args()

for spec in (args.workloads.split(",") if args.workloads else FIGURES):
    render(spec.split("+"), preview=args.preview)
    if args.preview:
        break
