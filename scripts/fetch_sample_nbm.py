#!/usr/bin/env python3
"""
Download one NBM CONUS GRIB2 file (forecast hour 1 of the most recent cycle)
for field inventory purposes.

Usage:
    python scripts/fetch_sample_nbm.py

Requires:  pip install herbie-data
Output:    ./sample_nbm.grib2  (symlink or copy of Herbie's cached file)
"""

from herbie import Herbie
import shutil, pathlib, sys

OUT = pathlib.Path("sample_nbm.grib2")

# Try recent cycles, newest first, until one is available on S3
for hours_ago in range(3, 18):
    import datetime
    t = datetime.datetime.utcnow() - datetime.timedelta(hours=hours_ago)
    try:
        H = Herbie(t.strftime("%Y-%m-%d %H:00"), model="nbm", product="co", fxx=1, verbose=False)
        local = H.download()          # downloads to Herbie's cache (~/.local/share/herbie/)
        shutil.copy(local, OUT)
        print(f"Downloaded: {local}")
        print(f"Copied to:  {OUT.resolve()}")
        print(f"Cycle:      {H.date}  fxx={H.fxx}")
        break
    except Exception:
        continue
else:
    sys.exit("No NBM cycle found in the last 18 hours — check your connection.")
