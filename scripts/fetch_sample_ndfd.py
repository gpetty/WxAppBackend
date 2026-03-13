"""
Download a representative sample of NDFD CONUS element files from S3 for inspection.

Downloads a small set of elements from VP.001-003 and VP.004-007 into a local
staging directory. Prints S3 last-modified timestamps alongside the downloaded files
so we can see whether elements were issued at different times.

Usage:
    python scripts/fetch_sample_ndfd.py [--out-dir /path/to/output]
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import s3fs

BUCKET   = "noaa-ndfd-pds"
PREFIX   = "opnl/AR.conus"
PERIODS  = ["VP.001-003", "VP.004-007"]

# Elements to download — chosen to cover:
#   hourly (temp), wind (wspd), precip (pop12, qpf), sky (sky), visibility (vis),
#   ceiling (cig), apparent temp (apt — VP.004-007 only), weather type (wx),
#   wet bulb (wbgt), precip type (wx)
ELEMENTS = [
    "temp",    # 2m temperature
    "td",      # dew point
    "rhm",     # relative humidity
    "wspd",    # wind speed
    "wdir",    # wind direction
    "wgust",   # wind gust
    "pop12",   # 12-h precip probability
    "qpf",     # quantitative precip forecast
    "sky",     # sky cover
    "vis",     # visibility (VP.001-003 only)
    "cig",     # cloud ceiling (VP.001-003 only)
    "apt",     # apparent temperature (VP.004-007 only)
    "wbgt",    # wet bulb globe temperature
    "wx",      # weather type
    "snow",    # snowfall
]


def main(out_dir: Path) -> None:
    fs = s3fs.S3FileSystem(anon=True)
    now_utc = datetime.now(tz=timezone.utc)
    print(f"Fetch time (UTC): {now_utc:%Y-%m-%d %H:%M:%S}\n")

    for period in PERIODS:
        period_dir = out_dir / period
        period_dir.mkdir(parents=True, exist_ok=True)

        print(f"=== {period} ===")

        # List what's actually available in this period
        prefix = f"{BUCKET}/{PREFIX}/{period}/"
        try:
            available = {
                Path(p).name
                for p in fs.ls(prefix)
                if Path(p).name.startswith("ds.") and Path(p).name.endswith(".bin")
            }
        except Exception as e:
            print(f"  ERROR listing {prefix}: {e}")
            continue

        for elem in ELEMENTS:
            fname = f"ds.{elem}.bin"
            s3_path = f"{BUCKET}/{PREFIX}/{period}/{fname}"
            local = period_dir / fname

            if fname not in available:
                print(f"  {fname:<22} NOT ON S3 for this period — skipped")
                continue

            try:
                info = fs.info(s3_path)
                last_mod = info.get("LastModified") or info.get("last_modified") or "?"
                size_mb  = info.get("size", 0) / 1e6
                print(f"  {fname:<22} S3 last-modified: {last_mod}  size: {size_mb:.1f} MB", end="", flush=True)

                fs.get(s3_path, str(local))
                print(f"  → saved {local.name}")
            except Exception as e:
                print(f"\n  ERROR downloading {s3_path}: {e}")

        print()

    print(f"\nDone. Files in: {out_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download NDFD sample files.")
    parser.add_argument(
        "--out-dir",
        default=str(Path(__file__).resolve().parent.parent / "data" / "ndfd_sample"),
        help="Local directory to save downloaded files.",
    )
    args = parser.parse_args()
    main(Path(args.out_dir))
