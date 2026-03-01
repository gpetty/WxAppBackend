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

from ..config import STAGING_DIR, DATA_ROOT
from .ingest import LockError, run_ingestion, read_manifest


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Suppress noisy third-party loggers
    for noisy in ("herbie", "urllib3", "requests", "boto3", "botocore", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


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
        "--fxx-max", type=int, default=264, metavar="N",
        help="Download forecast hours 1 through N (default: 264 = full cycle). "
             "Use 36 for a fast ~900 MB test run.",
    )
    parser.add_argument(
        "--workers", type=int, default=6, metavar="N",
        help="Parallel download workers (default: 6).",
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
             f"postprocess={args.postprocess}")

    try:
        staging_dir = run_ingestion(
            fxx_max=args.fxx_max,
            workers=args.workers,
            dry_run=args.dry_run,
            force=args.force,
            postprocess=args.postprocess,
            keep_staging=args.keep_staging,
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
