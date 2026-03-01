"""
NBM ingestion pipeline.

Downloads the latest NOAA National Blend of Models (NBM) CONUS forecast cycle
from the NOAA AWS S3 bucket and stages the GRIB2 files locally for post-processing.

Typical call:
    from backend.app.ingest import run_ingestion
    run_ingestion()

Or via CLI:
    python -m backend.app.ingest
    python -m backend.app.ingest --fxx-max 36 --workers 4 --dry-run
"""

from __future__ import annotations

import json
import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from herbie import Herbie

from ..config import (
    DATA_ROOT, STAGING_DIR, LOCK_FILE,
    NBM_MODEL, NBM_PRODUCT, DOWNLOAD_WORKERS,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# NBM forecast hour schedule
# ---------------------------------------------------------------------------

def nbm_forecast_hours(fxx_max: int = 262) -> list[int]:
    """
    Return the ordered list of NBM CONUS forecast hours up to fxx_max.

    Verified against noaa-nbm-grib2-pds S3 bucket (2026-02-27):
        f001–f036 : hourly   (step 1)  — available on NOMADS and S3
        f037–f190 : 3-hourly (step 3)  — S3 only
        f196–f262 : 6-hourly (step 6)  — S3 only

    Total: 36 + 52 + 12 = 100 files per cycle.
    """
    fxx: list[int] = []
    fxx += [h for h in range(1,   37)        if h <= fxx_max]   # hourly
    fxx += [h for h in range(37,  191, 3)    if h <= fxx_max]   # 3-hourly
    fxx += [h for h in range(196, 263, 6)    if h <= fxx_max]   # 6-hourly
    return fxx


# ---------------------------------------------------------------------------
# Cycle discovery
# ---------------------------------------------------------------------------

def find_latest_cycle(max_lookback_hours: int = 24) -> Optional[datetime]:
    """
    Walk back from the current UTC hour until we find a cycle available on S3.

    NBM files typically appear ~30–60 min after the cycle time, so we start
    looking 2 hours back and search up to max_lookback_hours.

    Returns a naive UTC datetime on success, or None if not found.
    """
    # Use naive UTC datetime — Herbie does not expect timezone-aware objects.
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    log.info(f"Current UTC: {now:%Y-%m-%d %H:%M}  (searching back up to {max_lookback_hours} hours)")

    for hours_ago in range(2, max_lookback_hours + 1):
        candidate = now - timedelta(hours=hours_ago)
        date_str = candidate.strftime("%Y-%m-%d %H:00")
        try:
            H = Herbie(date_str, model=NBM_MODEL, product=NBM_PRODUCT,
                       fxx=1, verbose=False)
            # H.grib returns the remote URL if the file exists, raises otherwise.
            url = H.grib
            if url:
                log.info(f"  {date_str}Z  ✓  ({url})")
                return candidate
            else:
                log.info(f"  {date_str}Z  — H.grib returned None (skipping)")
        except Exception as exc:
            log.info(f"  {date_str}Z  ✗  {type(exc).__name__}: {exc}")
            continue

    return None


# ---------------------------------------------------------------------------
# Lock file (simple PID-based; prevents overlapping cron runs)
# ---------------------------------------------------------------------------

class LockError(RuntimeError):
    pass


class IngestLock:
    """Context manager that creates/removes a lock file."""

    def __init__(self, lock_path: Path) -> None:
        self.path = lock_path

    def __enter__(self) -> "IngestLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            pid = self.path.read_text().strip()
            raise LockError(
                f"Lock file exists ({self.path}). "
                f"Is another ingestion running? (PID {pid})\n"
                f"If not, delete the lock file and retry."
            )
        import os
        self.path.write_text(str(os.getpid()))
        log.debug(f"Lock acquired: {self.path}")
        return self

    def __exit__(self, *_) -> None:
        try:
            self.path.unlink()
            log.debug(f"Lock released: {self.path}")
        except FileNotFoundError:
            pass


# ---------------------------------------------------------------------------
# Single-file download
# ---------------------------------------------------------------------------

def _download_one(
    cycle_time: datetime,
    fxx: int,
    save_dir: Path,
) -> tuple[int, Optional[Path]]:
    """
    Download one forecast-hour file. Returns (fxx, local_path) or (fxx, None).
    Intended to be called from a thread pool.
    """
    try:
        H = Herbie(
            cycle_time,
            model=NBM_MODEL,
            product=NBM_PRODUCT,
            fxx=fxx,
            save_dir=save_dir,
            verbose=False,
            # Extended-range files (fxx > 36) are on S3 only, not NOMADS.
            # Prefer S3 for all files for consistency; fall back to NOMADS
            # for the hourly segment if S3 is slow or unavailable.
            priority=["aws", "nomads"],
        )
        local = H.download()
        return fxx, Path(local)
    except Exception as exc:
        log.warning(f"  fxx={fxx:03d} FAILED: {exc}")
        return fxx, None


# ---------------------------------------------------------------------------
# Full-cycle download
# ---------------------------------------------------------------------------

def download_cycle(
    cycle_time: datetime,
    staging_dir: Path,
    fxx_max: int = 264,
    workers: int = DOWNLOAD_WORKERS,
    dry_run: bool = False,
) -> dict[int, Path]:
    """
    Download all forecast hours for cycle_time into staging_dir.

    Returns a dict mapping fxx → local file path for every successful download.
    Raises RuntimeError if any files fail.
    """
    fxx_list = nbm_forecast_hours(fxx_max)
    total = len(fxx_list)
    staging_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Cycle: {cycle_time:%Y-%m-%d %H}Z | "
             f"{total} forecast hours | fxx 001–{fxx_max:03d} | "
             f"{workers} workers")

    if dry_run:
        log.info("[DRY RUN] Skipping actual downloads.")
        return {}

    results: dict[int, Path] = {}
    failures: list[int] = []
    t0 = time.monotonic()
    done = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_download_one, cycle_time, fxx, staging_dir): fxx
            for fxx in fxx_list
        }
        for future in as_completed(futures):
            fxx, path = future.result()
            done += 1
            if path:
                results[fxx] = path
            else:
                failures.append(fxx)
            # Progress report every 10 files
            if done % 10 == 0 or done == total:
                elapsed = time.monotonic() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                log.info(f"  {done:3d}/{total}  "
                         f"({len(failures)} failed)  "
                         f"{elapsed:5.0f}s elapsed  "
                         f"ETA ~{eta:4.0f}s")

    elapsed = time.monotonic() - t0
    log.info(f"Download complete: {len(results)}/{total} files in {elapsed:.0f}s")

    if failures:
        raise RuntimeError(
            f"{len(failures)} files failed to download: "
            f"fxx={sorted(failures)}"
        )

    return results


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

def write_manifest(
    staging_dir: Path,
    cycle_time: datetime,
    files: dict[int, Path],
) -> Path:
    """Write a JSON manifest recording the cycle and downloaded file paths."""
    manifest = {
        "cycle":       cycle_time.strftime("%Y-%m-%dT%H:00:00Z"),
        "downloaded":  datetime.now(tz=timezone.utc).isoformat(),
        "file_count":  len(files),
        "files": {
            fxx: str(path) for fxx, path in sorted(files.items())
        },
    }
    path = staging_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2))
    log.info(f"Manifest written: {path}")
    return path


def read_manifest(staging_dir: Path) -> Optional[dict]:
    """Read the manifest from a staging directory. Returns None if not found."""
    path = staging_dir / "manifest.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_ingestion(
    fxx_max: int = 264,
    workers: int = DOWNLOAD_WORKERS,
    dry_run: bool = False,
    force: bool = False,
    postprocess: bool = False,
    keep_staging: bool = False,
) -> Path:
    """
    Run a full ingestion cycle:
      1. Acquire lock
      2. Find latest available NBM CONUS cycle
      3. Check if already staged (skip unless force=True)
      4. Download all forecast hours
      5. Write manifest
      6. (Optional) Run GRIB2 → Zarr post-processing
      7. Release lock

    Parameters
    ----------
    postprocess : bool
        If True, run the GRIB2 → Zarr post-processor after download.
    keep_staging : bool
        If True and postprocess is True, keep the GRIB2 files after post-processing.
        Ignored if postprocess is False.

    Returns the path to the staging directory for this cycle (or the Zarr
    live directory if postprocess=True and staging was deleted).
    Raises LockError, RuntimeError on failure.
    """
    DATA_ROOT.mkdir(parents=True, exist_ok=True)

    with IngestLock(LOCK_FILE):

        # --- Find cycle ---
        log.info("Searching for latest NBM cycle...")
        cycle_time = find_latest_cycle()
        if cycle_time is None:
            raise RuntimeError("No NBM cycle found on S3 in the last 12 hours.")

        cycle_tag = cycle_time.strftime("%Y%m%d_%H")
        cycle_staging = STAGING_DIR / cycle_tag

        # --- Skip if already complete ---
        manifest = read_manifest(cycle_staging)
        if manifest and not force:
            log.info(
                f"Cycle {cycle_tag} already staged "
                f"({manifest['file_count']} files). "
                f"Use --force to re-download."
            )
            # Still allow post-processing of an already-staged cycle
            if postprocess:
                log.info("Running post-processor on existing staged cycle...")
                from ..postprocessor import run_postprocessor
                stats = run_postprocessor(
                    staging_dir=cycle_staging,
                    delete_staging=not keep_staging,
                )
                log.info(f"Post-processing stats: {stats}")
            return cycle_staging

        # --- Download ---
        files = download_cycle(
            cycle_time=cycle_time,
            staging_dir=cycle_staging,
            fxx_max=fxx_max,
            workers=workers,
            dry_run=dry_run,
        )

        # --- Write manifest ---
        if not dry_run:
            write_manifest(cycle_staging, cycle_time, files)

        log.info(f"Ingestion complete. Staged at: {cycle_staging}")

        # --- Post-process ---
        if postprocess and not dry_run:
            log.info("Starting post-processing (GRIB2 → Zarr)...")
            from ..postprocessor import run_postprocessor
            stats = run_postprocessor(
                staging_dir=cycle_staging,
                delete_staging=not keep_staging,
            )
            log.info(f"Post-processing stats: {stats}")

        return cycle_staging
