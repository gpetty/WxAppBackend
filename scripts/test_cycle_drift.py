#!/usr/bin/env python
"""
Compare the same valid-time forecast across two consecutive NBM cycles.

Tests whether consecutive hourly cycles produce meaningfully different
extended-range forecasts — i.e., whether retaining multiple cycles for
drift visualization is worthwhile.

Usage:
    python scripts/test_cycle_drift.py
    python scripts/test_cycle_drift.py --date 2026-03-01 --hour 16 --fxx 48
"""

import argparse
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

import cfgrib
import numpy as np
from herbie import Herbie


def fetch_and_read(cycle_dt: datetime, fxx: int) -> np.ndarray | None:
    """Download one NBM file and return the 2m temperature array, or None."""
    H = Herbie(cycle_dt, model="nbm", product="co", fxx=fxx,
               priority=["aws", "nomads"], verbose=False)
    path = H.download()
    if path is None:
        print(f"  {cycle_dt:%H}Z fxx={fxx:03d}: NOT FOUND on S3")
        return None
    datasets = cfgrib.open_datasets(str(path))
    for ds in datasets:
        if "t2m" in ds.data_vars:
            return ds["t2m"].values
    print(f"  {cycle_dt:%H}Z fxx={fxx:03d}: t2m not found in GRIB2 datasets")
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", default="2026-03-01",
                        help="Cycle date YYYY-MM-DD (default: 2026-03-01)")
    parser.add_argument("--hour", type=int, default=16,
                        help="Later cycle hour UTC (default: 16); earlier cycle = hour-1")
    parser.add_argument("--fxx", type=int, default=48,
                        help="Forecast hour to use for the EARLIER cycle (default: 48). "
                             "The later cycle uses fxx-1 to target the same valid time.")
    args = parser.parse_args()

    base = datetime.strptime(args.date, "%Y-%m-%d")
    cycle_a = base.replace(hour=args.hour - 1)   # earlier cycle
    cycle_b = base.replace(hour=args.hour)        # later cycle
    fxx_a = args.fxx
    fxx_b = args.fxx - 1  # same valid time, one hour later issue

    valid_time = cycle_a + timedelta(hours=fxx_a)
    print(f"\nComparing forecast for valid time: {valid_time:%Y-%m-%d %H}Z")
    print(f"  Cycle A: {cycle_a:%Y-%m-%d %H}Z + fxx={fxx_a:03d}")
    print(f"  Cycle B: {cycle_b:%Y-%m-%d %H}Z + fxx={fxx_b:03d}\n")

    arr_a = fetch_and_read(cycle_a, fxx_a)
    arr_b = fetch_and_read(cycle_b, fxx_b)

    if arr_a is None or arr_b is None:
        print("\nCould not fetch both files — comparison aborted.")
        return

    diff = arr_b - arr_a
    print(f"\nResults (t2m in Kelvin):")
    print(f"  Cycle A  mean={arr_a.mean():.4f}  std={arr_a.std():.4f}  sample[200,300]={arr_a[200,300]:.4f}")
    print(f"  Cycle B  mean={arr_b.mean():.4f}  std={arr_b.std():.4f}  sample[200,300]={arr_b[200,300]:.4f}")
    print(f"\nDifference (B - A):")
    print(f"  mean={diff.mean():.6f}  std={diff.std():.6f}")
    print(f"  min={diff.min():.4f}  max={diff.max():.4f}")
    print(f"  pixels with |diff| > 0.01K : {(np.abs(diff) > 0.01).sum()} / {diff.size}")
    print(f"  pixels with |diff| > 0.1K  : {(np.abs(diff) > 0.1).sum()} / {diff.size}")
    print(f"  pixels with |diff| > 1.0K  : {(np.abs(diff) > 1.0).sum()} / {diff.size}")

    if np.allclose(arr_a, arr_b, atol=0.01):
        print("\nConclusion: forecasts are IDENTICAL — consecutive cycles do NOT update extended-range data.")
    elif np.abs(diff).mean() < 0.1:
        print("\nConclusion: forecasts differ NEGLIGIBLY — consecutive cycles add little new information.")
    else:
        print("\nConclusion: forecasts differ MEANINGFULLY — consecutive cycles do update extended-range data.")


if __name__ == "__main__":
    main()
