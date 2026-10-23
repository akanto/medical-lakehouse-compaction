# Campaign of 2026-10-21

The dataset behind the paper's tables and figures, from the camera-ready
version on. Everything the evaluation scripts read is here; nothing else is
needed to regenerate them.

It replaces the earlier campaign of the submitted manuscript, whose W1 read its
input twice (see "What changed"). The protocol, the network grid, the dataset
and the five W1 series are the same.

## Files

| File | What it is |
|---|---|
| `benchmark_campaign_20261021_merged.json` | the campaign, 1,260 entries |
| `benchmark_20261021_112118.json` | the 5 Gb/s row, as launched |
| `benchmark_20261021_134709.json` | the 2 Gb/s row |
| `benchmark_20261021_171101.json` | the 1 Gb/s row |
| `table_shapes_raw_20261021_092650.json` | file counts, size quantiles and row groups per layout |
| `build_times_ingest_20261027_174226.json` | wall clock of building L1 |
| `build_times_optimize_20261027_180609.json` | wall clock of building L2, L3 and L4 from L1 |
| `table_shapes_raw_20261027_181019.json` | table shapes of the timed build, for comparison |

The merged file is the three rate rows combined by `scripts/merge_rate_rows.py`,
which checks that the protocol was identical across the three launches. All
three inputs are committed, so the merge can be repeated rather than trusted.

## Shape of the campaign

200 series, 35,485 slices, written into four layout tables.

15 network cells — `rate_gbit [5, 2, 1]` × `latency_ms [0, 2, 5, 10, 25]` — and
84 entries in each: 4 layouts × 3 repetitions × (5 W1 series + 1 W2 + 1 W3).

Run as three launches, one per bandwidth row, each in a fresh JVM
(`scripts/run_rows.sh 5 2 1`) at `driver_memory: 12g`. 09:28Z to 17:11Z
2026-10-21, 7.7 h in total: 112, 145 and 203 minutes for the 5, 2 and 1 Gb/s
rows. Every row completed. The saving against the earlier campaign (10.6 h) is W1 alone,
which no longer scans twice.

## Table builds

The build times come from a separate session on a freshly deployed testbed,
with the same configuration and the unshaped link, in which all four tables
were built in turn:

| Build | Wall clock |
|---|---|
| L1, ingestion, including DICOM parsing from local disk | 657.50 s |
| L2, total | 825.99 s |
| — clone of L1's one-file-per-slice layout | 370.30 s |
| — `rewrite_data_files` binpack | 136.22 s |
| — `expire_snapshots` | 316.37 s |
| L3, range-partition and sort, from L1 | 301.74 s |
| L4, partition spec, from L1 | 240.33 s |

The tables of that session match the ones the campaign read:
`table_shapes_raw_20261027_181019.json` and `table_shapes_raw_20261021_092650.json`
agree for L1, L3 and L4 in file count, file sizes, row groups and total bytes.
L2 differs by a few kilobytes, because binpack follows write-arrival order and
packs differently on every build.

Only the binpack rewrite is what a production optimizer pays on an existing
raw table; the clone exists because the experiment keeps L1 next to L2. A
rewrite in a hybrid cloud also crosses the WAN, so these are lower bounds.
`make break-even` sets them against the per-read saving, as in the paper's
Section V-C.

## Checks that were run

On the merged file: 15 network levels, 1,260 entries, 84 per cell; every
`wall_clock_s` and `bytes_on_wire` above zero; 5 W1 series each with exactly 3
repetitions per layout per cell; measured RTT equal to nominal plus a substrate
of 0.61 to 0.64 ms in every cell. `scripts/validate_cell.py` reports no failure
and no warning, and every request and byte count is the same in all fifteen
cells. `scan_tasks` is non-zero in every entry.

Before the grid: substrate RTT 0.64 ms with two hops through the gateway,
iperf3 at 9.93 Gb/s on the unshaped path, and a spot check at 1 Gb/s and 25 ms
giving a 25.60 ms minimum RTT and 904 Mb/s.

Table shapes against the earlier build: L1, L3 and L4 identical in file
count, total bytes, and minimum, median, mean and maximum file size. L2 has the
same 36 files but differs by 2,079 bytes in total, since binpack assigns slices
to files in write-arrival order and therefore differs on every build. The p25
and p75 of every layout are now inclusive quantiles; the earlier shapes were
written with the exclusive method, which is why their L3 and L4 quartiles differ
by 0.02 to 0.03 MB for identical tables.

## What changed against the earlier campaign

- **W1 reads its input once.** The reconstruction query sorted the slices with
  a global `orderBy`, which Spark plans as a range-partitioning exchange: a
  sampling job scans the full input to find the range bounds, and the sort
  stage scans it again. Every earlier W1 value paid two scans, on all
  four layouts (L1 issued 6.0 GET requests per slice in W1, against 3.0 in W2,
  which has no sort). The query now collects the series and orders the slices
  by instance number on the driver
  (`medical_lakehouse_compaction/benchmarks/reconstruction.py`). A check on ten
  series returned identical volumes, array for array, from the old and the new
  query. W1 request counts and wire bytes halved on every layout, and every W1
  value differs from the earlier campaign by design.
- **Object store: SILO replaces MinIO.** The MinIO release of the earlier
  campaign (`RELEASE.2024-11-07T00-52-20Z`) is no longer distributed. This campaign ran
  PGSTY SILO `RELEASE.2026-09-16T00-00-00Z`, a community-maintained AGPL-3.0
  fork of MinIO, pinned by digest in `docker-compose.yml`.
- **Tables rebuilt**, as above.
- **Host image:** Ubuntu 24.04 AMI of 2026-10-17, kernel 7.0.0-1014-aws,
  OpenJDK 17.0.20.1. PySpark 4.0.0, Iceberg 1.10.1 and hadoop-aws 3.4.1 are
  unchanged.

W2 and W3 reproduce the earlier campaign. Their request counts are identical except on
the rebuilt L2 (W2 437 to 432, W3 305 to 304), and the ratio of the L2 and L1
execution times in W2 moved by at most 0.04 in any cell, never crossing 1.0.
The server change and the new host image therefore did not move the
measurements, and no conclusion of the paper changed.

## Entry fields

Each element of `results` is one measured run. W2 and W3 execute once per
pass, so one element is one execution of the workload. W1 reconstructs five
series in turn, so one execution is the sum of its five elements, which is
how the evaluation scripts aggregate a cell before taking the median over
the three repetitions.

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
| `scan_stages` | Spark stages of the read |
| `scan_tasks` | Iceberg read tasks |

Top-level keys carry `n_series`, the profile used, `w1_series_uids` (the five
series W1 samples), `merged_from`, and `network` — one entry per cell with the
nominal and measured RTT, and the ping output the measurement came from. In
that output the object store's address reads `<minio-private-ip>`; this is the
only edit made to the files, and the measured values parsed from the output
are unmodified.

`table_shapes_raw_*.json` holds, per layout, the data file count, row count,
total bytes, the min/p25/median/p75/max/mean of file size in bytes, and the
min/median/max/total of Parquet row groups per data file, counted from the
Iceberg `split_offsets` of each file. `build_times_*.json` hold the wall clock
in seconds of each build, with the L2 steps under `s2_steps`.
