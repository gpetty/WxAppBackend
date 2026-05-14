"""
CLI entry point for the ingestion pipeline.

Usage:
    # From repo root, with venv active:
    python -m backend.app.ingest                   # full cycle, 6 workers
    python -m backend.app.ingest --fxx-max 36      # first 36 hours only
    python -m backend.app.ingest --workers 4       # fewer parallel downloads
    python -m backend.app.ingest --dry-run         # find cycle, skip download
    python -m backend.app.ingest --force           # re-download even if staged
    python -m backend.app.ingest --postprocess     # download + convert to Zarr
    python -m backend.app.ingest --status          # show current staging state
"""

import argparse
import logging
import sys

from ..config import (
    DATA_ROOT, STAGING_DIR,
    DOWNLOAD_WORKERS, MAX_RETRIES, NBM_FXX_MAX, RETRY_DELAY_SEC,
)
from ._common import setup_logging
from .ingest import LockError, run_ingestion, read_manifest


def cmd_status() -> None:
    """Print the current state of the staging directory."""
    if not STAGING_DIR.exists():
        print(f"Staging directory does not exist: {STAGING_DIR}")
        return

    cycles = sorted(STAGING_DIR.iterdir())
    if not cycles:
        print("No staged cycles found.")
        return

    for cycle_dir in cycles:
        manifest = read_manifest(cycle_dir)
        if manifest:
            print(f"  {cycle_dir.name}  "
                  f"{manifest['file_count']} files  "
                  f"downloaded {manifest['downloaded']}")
        else:
            # Directory exists but no manifest — partial/failed download
            n = len(list(cycle_dir.rglob("*.grib2")))
            print(f"  {cycle_dir.name}  {n} .grib2 files  [NO MANIFEST — incomplete?]")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m backend.app.ingest",
        description="Download the latest NBM CONUS forecast cycle.",
    )
    parser.add_argument(
        "--fxx-max", type=int, default=NBM_FXX_MAX, metavar="N",
        help=f"Download forecast hours 1 through N (default: {NBM_FXX_MAX} = full cycle). "
             "Use 36 for a fast ~900 MB test run.",
    )
    parser.add_argument(
        "--workers", type=int, default=DOWNLOAD_WORKERS, metavar="N",
        help=f"Parallel download workers (default: {DOWNLOAD_WORKERS}).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Find the latest cycle and report it, but do not download anything.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-download even if this cycle is already staged.",
    )
    parser.add_argument(
        "--postprocess", action="store_true",
        help="After download, run the GRIB2 → Zarr post-processor. "
             "Writes a Zarr store and atomically swaps it live.",
    )
    parser.add_argument(
        "--keep-staging", action="store_true",
        help="When used with --postprocess, keep the GRIB2 files after "
             "Zarr conversion (useful for debugging).",
    )
    parser.add_argument(
        "--max-retries", type=int, default=MAX_RETRIES, metavar="N",
        help=f"Retry failed files up to N times (default: {MAX_RETRIES}). "
             "Each retry waits --retry-delay seconds.",
    )
    parser.add_argument(
        "--retry-delay", type=int, default=RETRY_DELAY_SEC, metavar="SEC",
        help=f"Seconds to wait between retry attempts (default: {RETRY_DELAY_SEC} = 5 min).",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show current staging directory state and exit.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable debug logging.",
    )
    args = parser.parse_args()

    setup_logging(args.verbose)
    log = logging.getLogger(__name__)

    if args.status:
        cmd_status()
        return

    log.info(f"Data root: {DATA_ROOT}")
    log.info(f"fxx_max={args.fxx_max}  workers={args.workers}  "
             f"dry_run={args.dry_run}  force={args.force}  "
             f"postprocess={args.postprocess}  "
             f"max_retries={args.max_retries}  retry_delay={args.retry_delay}s")

    try:
        staging_dir = run_ingestion(
            fxx_max=args.fxx_max,
            workers=args.workers,
            dry_run=args.dry_run,
            force=args.force,
            postprocess=args.postprocess,
            keep_staging=args.keep_staging,
            max_retries=args.max_retries,
            retry_delay=args.retry_delay,
        )
        log.info(f"Done. Files staged at: {staging_dir}")

    except LockError as e:
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
