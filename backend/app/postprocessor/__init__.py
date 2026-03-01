"""
Post-processor: converts staged GRIB2 files into a Zarr store optimized for
point time-series queries.

Usage:
    from backend.app.postprocessor import run_postprocessor
    run_postprocessor(staging_dir)

Or via CLI:
    python -m backend.app.postprocessor /path/to/staging/dir
    python -m backend.app.postprocessor --help
"""

from .grib2_to_zarr import run_postprocessor

__all__ = ["run_postprocessor"]
