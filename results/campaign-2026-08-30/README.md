# Campaign of 2026-08-30

The dataset behind the paper's tables and figures. Everything the evaluation
scripts read is here; nothing else is needed to regenerate them.

## Files

| File | What it is |
|---|---|
| `benchmark_campaign_20260830_merged.json` | the campaign, 1,260 entries |
| `benchmark_20260829_212737.json` | the 5 Gb/s row, as launched |
| `benchmark_20260830_004405.json` | the 2 Gb/s row |
| `benchmark_20260830_054155.json` | the 1 Gb/s row |
| `table_shapes_raw_20260829_190144.json` | file counts and size quantiles per layout |

The merged file is the three rate rows combined by `scripts/merge_rate_rows.py`,
which checks that the protocol was identical across the three launches. All
three inputs are committed, so the merge can be repeated rather than trusted.

## Shape of the campaign

200 series, 35,485 slices, written into four layout tables.

15 network cells — `rate_gbit [5, 2, 1]` × `latency_ms [0, 2, 5, 10, 25]` — and
84 entries in each: 4 layouts × 3 repetitions × (5 W1 series + 1 W2 + 1 W3).

Run as three launches, one per bandwidth row, each in a fresh JVM
(`scripts/run_rows.sh 5 2 1`) at `driver_memory: 12g`. 19:02:20Z 2026-08-29 to
05:41:55Z 2026-08-30, 10.6 h in total: 145, 196 and 297 minutes for the 5, 2 and
1 Gb/s rows. Every row completed.

## Checks that were run

On the merged file: 15 network levels, 1,260 entries, 84 per cell; every
`wall_clock_s` and `bytes_on_wire` above zero; 5 W1 series each with exactly 3
repetitions per layout per cell; measured RTT equal to nominal plus a substrate
of 0.42 ms in every cell.

Per cell during the run: HTB rate correct, netem drops zero throughout. Before
the grid: iperf3 at 9.926 Gb/s on the unshaped path, and table shapes matching
the reference build.

## Entry fields

Each element of `results` is one workload execution.

| Field | Meaning |
|---|---|
| `strategy` | layout: `s1` raw, `s2` size-based, `s3` sort-clustered, `s4` partition-clustered |
| `workload` | `w1` reconstruction, `w2` shard load, `w3` metadata scan |
| `rate_gbit` | bandwidth limit applied to the path |
| `latency_ms` | injected round-trip time, nominal |
| `run_index` | repetition, 0-based |
| `series_uid` | the series read, on W1 entries only |
| `wall_clock_s` | execution time |
| `bytes_on_wire` | bytes fetched from the object store (`stream_read_total_bytes`) |
| `bytes_useful` | decoded size of the data requested |
| `rar_wire` | `bytes_on_wire / bytes_useful` |
| `s3_get_requests` | GET count |
| `s3_total_requests` | GET + HEAD + LIST |
| `scan_tasks` | Iceberg read tasks |

Top-level keys carry `n_series`, the profile used, `w1_series_uids` (the five
series W1 samples), `merged_from`, and `network` — one entry per cell with the
nominal and measured RTT, and the ping output the measurement came from. In
that output the object store's address reads `<minio-private-ip>`; the measured
values parsed from it are unmodified.

`table_shapes_raw_*.json` holds, per layout, the data file count, row count,
total bytes, and the min/p25/median/p75/max/mean of file size in bytes.
