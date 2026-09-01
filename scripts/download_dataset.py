#!/usr/bin/env python3
"""Download LIDC-IDRI subset from TCIA."""
import argparse
from pathlib import Path
from tcia_utils import nbia

def download_lidc_subset(output_dir: str, n_series: int) -> None:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    print(f"Fetching LIDC-IDRI series list...")
    series_list = nbia.getSeries(collection="LIDC-IDRI", modality="CT")
    subset = series_list[:n_series]

    print(f"Downloading {len(subset)} series to {output}...")
    for i, series in enumerate(subset):
        series_uid = series["SeriesInstanceUID"]
        print(f"  [{i+1}/{len(subset)}] {series_uid}")
        nbia.downloadSeries([series], path=str(output))

    print(f"Done. Downloaded to {output}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-series", type=int, default=10)
    parser.add_argument("--output", default="data/dicom")
    args = parser.parse_args()
    download_lidc_subset(args.output, args.n_series)
