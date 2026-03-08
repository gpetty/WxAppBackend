"""
Central configuration for the Weather Window backend.

All paths are derived from REPO_ROOT so the project is relocatable.
Override DATA_DIR via environment variable if needed:
    export DATA_DIR=/Volumes/BigDisk/weatherwindow
"""

import os
from pathlib import Path

# Three levels up from backend/app/config.py → repo root
REPO_ROOT: Path = Path(__file__).resolve().parent.parent.parent

# Data root: default to ./data/nbm inside the repo; override with DATA_DIR env var
DATA_ROOT: Path = Path(os.environ.get("DATA_DIR", REPO_ROOT / "data" / "nbm"))

# Subdirectories
STAGING_DIR: Path = DATA_ROOT / "staging"   # GRIB2 files during download
ZARR_DIR: Path    = DATA_ROOT / "zarr"       # processed Zarr stores

# Lock file — prevents overlapping ingestion runs
LOCK_FILE: Path = DATA_ROOT / "ingest.lock"

# Variable registry
VARIABLES_YAML: Path = Path(__file__).parent / "variables.yaml"

# NBM model parameters
NBM_MODEL:   str = "nbm"
NBM_PRODUCT: str = "co"       # CONUS domain

# Download concurrency (parallel workers for GRIB2 file downloads)
DOWNLOAD_WORKERS: int = int(os.environ.get("DOWNLOAD_WORKERS", 6))

# Post-processing concurrency (parallel workers for cfgrib extraction)
# Each worker opens one GRIB2 file; memory per worker ~300 MB.
POSTPROCESS_WORKERS: int = int(os.environ.get("POSTPROCESS_WORKERS", 8))

# Zarr store retention policy (legacy — will be retired with slab ring buffer)
# Keep stores whose cycle hour is in ZARR_RETAIN_HOURS for the past ZARR_RETAIN_DAYS days.
# All other historical stores are pruned after each successful post-processing run.
# The most recent store (current) is always kept regardless of hour.
ZARR_RETAIN_DAYS:  int       = 7
ZARR_RETAIN_HOURS: tuple[int, ...] = (0, 6, 12, 18)

# ---------------------------------------------------------------------------
# Slab ring buffer store
# ---------------------------------------------------------------------------

# Root directory for the slab store
SLAB_STORE_DIR: Path = DATA_ROOT / "slabs"

# Number of forecast runs retained in the outer ring buffer.
# 28 = 7 days × 4 major cycles (00Z/06Z/12Z/18Z) per day.
SLAB_N_RUNS: int = int(os.environ.get("SLAB_N_RUNS", 28))

# Number of forecast time steps per run (one per GRIB2 file in the NBM cycle).
# 99 = 36 hourly (f001–f036) + 51 three-hourly (f038–f188) + 12 six-hourly (f194–f260)
SLAB_N_FXX: int = 99

# NBM CONUS grid dimensions (fixed by the model grid; do not change)
SLAB_IDIM: int = 1597   # rows (y)
SLAB_JDIM: int = 2345   # cols (x)
