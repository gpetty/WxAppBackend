"""
Inspect downloaded NDFD GRIB2 element files.

For each file, prints:
  - GRIB2 Section 1 reference time (from eccodes — the model "issue time")
  - Number of GRIB2 messages
  - cfgrib variable names, shortName, typeOfLevel
  - Valid times (time coordinate) and their step/resolution
  - Grid dimensions and longitude convention

Usage:
    python scripts/inventory_ndfd.py [--sample-dir /path/to/ndfd_sample]
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import eccodes
import cfgrib
import numpy as np


def eccodes_section1(path: Path) -> list[dict]:
    """
    Read GRIB2 Section 1 reference times (and a few other header fields)
    from every message in a file using eccodes (fast, no data load).
    """
    records = []
    with open(path, "rb") as f:
        while True:
            try:
                msg = eccodes.codes_grib_new_from_file(f)
            except eccodes.CodesInternalError:
                break
            if msg is None:
                break
            try:
                rec = {}
                for key in [
                    "dataDate", "dataTime",           # Section 1 reference time
                    "validityDate", "validityTime",   # Valid time
                    "stepRange",                      # Forecast step
                    "shortName",                      # Variable identifier
                    "typeOfLevel",                    # Level type
                    "level",
                    "Ni", "Nj",                       # Grid dimensions
                    "paramId",
                ]:
                    try:
                        rec[key] = eccodes.codes_get(msg, key)
                    except eccodes.KeyValueNotFoundError:
                        rec[key] = None
                records.append(rec)
            finally:
                eccodes.codes_release(msg)
    return records


def summarise_times(records: list[dict]) -> None:
    """Print reference times, valid times, and step statistics."""
    ref_times  = sorted(set(
        (r["dataDate"], r["dataTime"]) for r in records if r["dataDate"]
    ))
    valid_times = sorted(set(
        (r["validityDate"], r["validityTime"]) for r in records if r["validityDate"]
    ))

    def fmt_dt(date, time):
        d = str(date)
        t = f"{int(time):04d}"
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}T{t[:2]}:{t[2:]}Z"

    print(f"    Reference times  ({len(ref_times)} distinct):")
    for d, t in ref_times:
        print(f"      {fmt_dt(d, t)}")

    print(f"    Valid times      ({len(valid_times)} steps):")
    if valid_times:
        first = fmt_dt(*valid_times[0])
        last  = fmt_dt(*valid_times[-1])
        # Compute step gaps
        def to_minutes(date, time):
            d = str(date)
            t = int(time)
            return int(d[:4])*525960 + int(d[4:6])*43800 + int(d[6:8])*1440 + (t//100)*60 + (t%100)

        mins = [to_minutes(d, t) for d, t in valid_times]
        gaps = sorted(set(mins[i+1] - mins[i] for i in range(len(mins)-1)))
        print(f"      First: {first}  Last: {last}")
        print(f"      Step gaps (min): {gaps}  → hours: {[g//60 for g in gaps]}")


def cfgrib_inspect(path: Path) -> None:
    """Open with cfgrib and show variable names, shortNames, levels, and time coords."""
    try:
        datasets = cfgrib.open_datasets(str(path))
    except Exception as e:
        print(f"    cfgrib FAILED: {e}")
        return

    if not datasets:
        print("    cfgrib: no datasets found")
        return

    for i, ds in enumerate(datasets):
        print(f"    cfgrib dataset [{i}]:")
        print(f"      dims:      {dict(ds.dims)}")
        for vname, da in ds.data_vars.items():
            attrs = da.attrs
            print(f"      var: {vname:<20} shortName={attrs.get('GRIB_shortName','?'):<12} "
                  f"typeOfLevel={attrs.get('GRIB_typeOfLevel','?'):<20} "
                  f"units={attrs.get('units','?')}")

        # Time coordinate
        if "time" in ds.coords:
            t = ds.coords["time"].values
            print(f"      time coord: {t}")
        if "valid_time" in ds.coords:
            vt = ds.coords["valid_time"].values
            n = len(np.atleast_1d(vt))
            print(f"      valid_time: {n} steps", end="")
            if n > 0:
                arr = np.atleast_1d(vt)
                print(f"  [{arr[0]}  →  {arr[-1]}]", end="")
            print()
        if "step" in ds.coords:
            step = ds.coords["step"].values
            print(f"      step:       {np.atleast_1d(step)}")

        # Longitude convention
        if "longitude" in ds.coords:
            lon = ds.coords["longitude"].values
            print(f"      lon range:  {float(lon.min()):.3f} – {float(lon.max()):.3f}")
        if "latitude" in ds.coords:
            lat = ds.coords["latitude"].values
            print(f"      lat range:  {float(lat.min()):.3f} – {float(lat.max()):.3f}")

        # Grid shape
        for vname, da in ds.data_vars.items():
            print(f"      grid shape ({vname}): {da.shape}")
            break   # one is enough


def main(sample_dir: Path) -> None:
    periods = ["VP.001-003", "VP.004-007"]

    for period in periods:
        period_dir = sample_dir / period
        if not period_dir.exists():
            print(f"\n=== {period} === (directory not found, run fetch_sample_ndfd.py first)\n")
            continue

        bins = sorted(period_dir.glob("ds.*.bin"))
        if not bins:
            print(f"\n=== {period} === (no .bin files found)\n")
            continue

        print(f"\n{'='*70}")
        print(f"  {period}  ({len(bins)} files)")
        print(f"{'='*70}")

        for path in bins:
            print(f"\n  --- {path.name} ---")
            try:
                records = eccodes_section1(path)
                if not records:
                    print("    No GRIB2 messages found.")
                    continue
                shortnames = sorted(set(r["shortName"] for r in records if r["shortName"]))
                print(f"    Messages: {len(records)}   shortNames: {shortnames}")
                summarise_times(records)
                cfgrib_inspect(path)
            except Exception as e:
                print(f"    ERROR: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inventory NDFD sample files.")
    parser.add_argument(
        "--sample-dir",
        default=str(Path(__file__).resolve().parent.parent / "data" / "ndfd_sample"),
        help="Directory containing downloaded NDFD sample files.",
    )
    args = parser.parse_args()
    main(Path(args.sample_dir))
