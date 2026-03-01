"""
CLI entry point for the GRIB2 → Zarr post-processor.

Usage:
    # From repo root, with venv active:
    python -m backend.app.postprocessor STAGING_DIR
    python -m backend.app.postprocessor data/nbm/staging/20260227_18
    python -m backend.app.postprocessor --keep-staging STAGING_DIR
    python -m backend.app.postprocessor --help

The post-processor reads the GRIB2 files in STAGING_DIR, extracts all
native variables from variables.yaml, writes a Zarr store, and atomically
swaps it into the live location.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from ..config import ZARR_DIR
from .grib2_to_zarr import run_postprocessor


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for noisy in ("cfgrib", "eccodes", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m backend.app.postprocessor",
        description="Convert staged GRIB2 files to a Zarr store.",
    )
    parser.add_argument(
        "staging_dir", type=Path,
        help="Path to the staging directory containing GRIB2 files for one cycle "
             "(e.g., data/nbm/staging/20260227_18).",
    )
    parser.add_argument(
        "--keep-staging", action="store_true",
        help="Do not delete the GRIB2 staging directory after Zarr write. "
             "Useful for debugging or re-processing.",
    )
    parser.add_argument(
        "--zarr-out", type=Path, default=None, metavar="DIR",
        help=f"Zarr staging output path (default: {ZARR_DIR / 'staging_new'}).",
    )
    parser.add_argument(
        "--zarr-live", type=Path, default=None, metavar="DIR",
        help=f"Zarr live store path (default: {ZARR_DIR / 'current'}).",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable debug logging.",
    )
    args = parser.parse_args()

    setup_logging(args.verbose)
    log = logging.getLogger(__name__)

    if not args.staging_dir.exists():
        log.error(f"Staging directory not found: {args.staging_dir}")
        sys.exit(1)

    try:
        stats = run_postprocessor(
            staging_dir=args.staging_dir,
            delete_staging=not args.keep_staging,
            zarr_staging=args.zarr_out,
            zarr_live=args.zarr_live,
        )
        # Print stats as JSON for easy machine consumption
        print(json.dumps(stats, indent=2))

    except FileNotFoundError as e:
        log.error(str(e))
        sys.exit(1)

    except RuntimeError as e:
        log.error(str(e))
        sys.exit(1)

    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
        sys.exit(130)


if __name__ == "__main__":
    main()
