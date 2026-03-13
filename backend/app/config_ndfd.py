"""
NDFD-specific configuration for the Weather Window backend.

All NBM config in config.py remains unchanged.  This module adds only
the constants needed by the parallel NDFD pipeline.

Override any path with environment variables:
    export NDFD_DATA_DIR=/12TB2/NDFD
"""

import os
from pathlib import Path

from .config import DATA_ROOT, REPO_ROOT   # share the same data root by default

# ---------------------------------------------------------------------------
# S3 source
# ---------------------------------------------------------------------------

NDFD_BUCKET  = "noaa-ndfd-pds"
NDFD_PREFIX  = "opnl/AR.conus"
NDFD_PERIODS = ("VP.001-003", "VP.004-007")

# Elements to download from each period.
# Keys are period names; values are lists of element abbreviations.
# An element absent from a period is silently skipped (the S3 listing is
# checked before each download and missing elements are logged as warnings).
NDFD_ELEMENTS: dict[str, list[str]] = {
    "VP.001-003": [
        "temp",    # 2m temperature
        "td",      # dew point
        "rhm",     # relative humidity
        "apt",     # apparent temperature (both periods)
        "wspd",    # wind speed
        "wdir",    # wind direction
        "wgust",   # wind gust
        "qpf",     # quantitative precipitation (VP.001-003 only)
        "ptotsvrtstm",  # total severe thunderstorm probability
        "sky",     # sky cover
        "vis",     # visibility (VP.001-003 only)
        "cig",     # cloud ceiling (VP.001-003 only)
        "wbgt",    # wet bulb globe temperature
        "snow",    # snowfall (VP.001-003 only)
    ],
    "VP.004-007": [
        "temp",
        "td",
        "rhm",
        "apt",
        "wspd",
        "wdir",
        "wgust",
        "ptotsvrtstm",
        "sky",
        "wbgt",
    ],
}

# ---------------------------------------------------------------------------
# Local paths
# ---------------------------------------------------------------------------

NDFD_DATA_ROOT: Path  = Path(os.environ.get("NDFD_DATA_DIR", DATA_ROOT.parent / "ndfd"))
NDFD_INCOMING_DIR: Path = NDFD_DATA_ROOT / "incoming"     # scratch: downloaded before cycle_tag known
NDFD_STAGING_DIR: Path  = NDFD_DATA_ROOT / "staging"      # per-cycle dir after tag is known
NDFD_SLAB_STORE_DIR: Path = NDFD_DATA_ROOT / "slabs"
NDFD_LOCK_FILE: Path    = NDFD_DATA_ROOT / "ingest.lock"

NDFD_VARIABLES_YAML: Path = Path(__file__).parent / "variables_ndfd.yaml"

# ---------------------------------------------------------------------------
# NDFD CONUS 2.5 km grid (Lambert Conformal)
# Empirically verified: 2026-03-12 against ds.temp.bin VP.001-003
# ---------------------------------------------------------------------------

NDFD_IDIM: int = 1377   # rows (y)
NDFD_JDIM: int = 2145   # cols (x)

# ---------------------------------------------------------------------------
# Slab ring buffer dimensions
# ---------------------------------------------------------------------------

# ~65 actual time steps (49 VP.001-003 + 16 VP.004-007) plus margin.
NDFD_SLAB_N_FXX:  int = 70

# Retain 28 snapshots (7 days × 4 snapshots/day at hourly scheduler).
# NDFD doesn't have fixed cycles; snapshots are triggered by changed reference times.
NDFD_SLAB_N_RUNS: int = int(os.environ.get("NDFD_SLAB_N_RUNS", 28))

# ---------------------------------------------------------------------------
# Download concurrency
# ---------------------------------------------------------------------------

# NDFD has ~24 small files per download (vs 99 large NBM files).
# 6 workers is more than enough.
NDFD_DOWNLOAD_WORKERS: int = int(os.environ.get("NDFD_DOWNLOAD_WORKERS", 6))

# Post-processing workers for cfgrib extraction (one worker per element file).
NDFD_POSTPROCESS_WORKERS: int = int(os.environ.get("NDFD_POSTPROCESS_WORKERS", 8))
