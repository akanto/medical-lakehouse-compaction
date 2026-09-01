#!/usr/bin/env python3
"""Generate/verify a dataset manifest for the S3 backup of LIDC-IDRI.

The manifest is the completeness token for s3://<bucket>/series_<N>/: it is
uploaded LAST, so its presence in S3 means the full DICOM tree beneath
series_<N>/dicom/ arrived intact. `generate` doubles as the TCIA-download
completeness gate — the downloader can print "Done" after a transient
failure with series missing, so we never
trust its exit status alone.

Modes:
  generate --dicom-dir D --expect-series N [--min-slices 50] --output M.json
      Walks D (one subdirectory per SeriesInstanceUID), fails without writing
      if the series count != N or any series has fewer than --min-slices
      files (LIDC-IDRI CT series have 95-154 slices; fewer means truncated).
  verify --dicom-dir D --manifest M.json
      Re-walks D and compares per-series file counts and byte totals against
      the manifest. Used after an S3 restore.
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def scan(dicom_dir: Path) -> dict:
    series = {}
    for d in sorted(p for p in dicom_dir.iterdir() if p.is_dir()):
        files = [f for f in d.rglob("*") if f.is_file()]
        series[d.name] = {
            "files": len(files),
            "bytes": sum(f.stat().st_size for f in files),
        }
    return series


def cmd_generate(args) -> int:
    series = scan(Path(args.dicom_dir))
    problems = []
    if len(series) != args.expect_series:
        problems.append(f"expected {args.expect_series} series, found {len(series)}")
    for uid, s in series.items():
        if s["files"] < args.min_slices:
            problems.append(f"{uid}: only {s['files']} slices (< {args.min_slices})")
    if problems:
        print("INCOMPLETE dataset — manifest NOT written:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 2

    manifest = {
        "schema_version": 1,
        "dataset": "LIDC-IDRI",
        "source": args.source,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_series": len(series),
        "total_files": sum(s["files"] for s in series.values()),
        "total_bytes": sum(s["bytes"] for s in series.values()),
        "series": series,
    }
    Path(args.output).write_text(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"Manifest written: {args.output} "
          f"({manifest['n_series']} series, {manifest['total_files']} files, "
          f"{manifest['total_bytes'] / 1e9:.2f} GB)")
    return 0


def cmd_verify(args) -> int:
    manifest = json.loads(Path(args.manifest).read_text())
    actual = scan(Path(args.dicom_dir))
    expected = manifest["series"]
    problems = []
    for uid in expected.keys() - actual.keys():
        problems.append(f"missing series {uid}")
    for uid in actual.keys() - expected.keys():
        problems.append(f"unexpected series {uid}")
    for uid in expected.keys() & actual.keys():
        if expected[uid] != actual[uid]:
            problems.append(f"{uid}: manifest {expected[uid]} != local {actual[uid]}")
    if problems:
        print("VERIFY FAILED:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 2
    print(f"Verify OK: {len(actual)} series, "
          f"{sum(s['files'] for s in actual.values())} files match {args.manifest}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)

    g = sub.add_parser("generate")
    g.add_argument("--dicom-dir", required=True)
    g.add_argument("--expect-series", type=int, required=True)
    g.add_argument("--min-slices", type=int, default=50)
    g.add_argument("--source", default="tcia")
    g.add_argument("--output", required=True)
    g.set_defaults(func=cmd_generate)

    v = sub.add_parser("verify")
    v.add_argument("--dicom-dir", required=True)
    v.add_argument("--manifest", required=True)
    v.set_defaults(func=cmd_verify)

    args = parser.parse_args()
    sys.exit(args.func(args))
