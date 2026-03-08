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

def nbm_forecast_hours(fxx_max: int = 260) -> list[int]:
    """
    Fallback fxx schedule used when the S3 directory listing is unavailable.

    Verified against noaa-nbm-grib2-pds S3 bucket (2026-03-01, 16Z cycle):
        f001–f036 : hourly   (step 1)  — available on NOMADS and S3
        f038–f188 : 3-hourly (step 3)  — S3 only
        f194–f260 : 6-hourly (step 6)  — S3 only

    The schedule varies between cycles; prefer list_available_fxx() which
    queries S3 directly rather than assuming a fixed pattern.
    """
    fxx: list[int] = []
    fxx += [h for h in range(1,   37)        if h <= fxx_max]   # hourly
    fxx += [h for h in range(38,  189, 3)    if h <= fxx_max]   # 3-hourly
    fxx += [h for h in range(194, 261, 6)    if h <= fxx_max]   # 6-hourly
    return fxx


def list_available_fxx(
    cycle_time: datetime,
    product: str = NBM_PRODUCT,
    fxx_max: int = 270,
) -> list[int]:
    """
    Query the S3 directory listing for *cycle_time* and return the sorted list
    of fxx values actually present for *product* (e.g. ``"co"``).

    This avoids issuing HTTP requests for files that don't exist, eliminating
    spurious "GRIB2 file not found" warnings for non-existent fxx values.

    Falls back to ``nbm_forecast_hours()`` if the S3 listing fails.
    """
    import re
    import s3fs

    bucket_path = (
        f"noaa-nbm-grib2-pds/blend.{cycle_time:%Y%m%d}/{cycle_time:%H}/core"
    )
    pattern = re.compile(
        rf"blend\.t{cycle_time:%H}z\.core\.f(\d{{3}})\.{product}\.grib2$"
    )

    try:
        fs = s3fs.S3FileSystem(anon=True)
        files = fs.ls(bucket_path)
        fxx_list = []
        for f in files:
            m = pattern.search(f)
            if m:
                fxx_val = int(m.group(1))
                if fxx_val <= fxx_max:
                    fxx_list.append(fxx_val)
        if fxx_list:
            log.info(
                f"S3 listing: {len(fxx_list)} {product} files available "
                f"for {cycle_time:%Y-%m-%d %H}Z"
            )
            return sorted(fxx_list)
        log.warning("S3 listing returned no matching files; using fallback schedule")
    except Exception as exc:
        log.warning(f"S3 listing failed ({exc}); using fallback schedule")

    return nbm_forecast_hours(fxx_max)


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
        import os
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            pid_str = self.path.read_text().strip()
            stale = False
            try:
                pid = int(pid_str)
                os.kill(pid, 0)   # signal 0: check existence only, no-op if running
            except (ValueError, ProcessLookupError):
                stale = True      # PID is gone
            except PermissionError:
                stale = False     # process exists but owned by another user — treat as live
            if stale:
                log.warning(f"Removing stale lock file (PID {pid_str} no longer running).")
                self.path.unlink()
            else:
                raise LockError(
                    f"Lock file exists ({self.path}). "
                    f"Ingestion already running as PID {pid_str}."
                )
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
        if local is None:
            log.warning(f"  fxx={fxx:03d} FAILED: Herbie returned None (file not found on any source)")
            return fxx, None
        return fxx, Path(local)
    except Exception as exc:
        log.warning(f"  fxx={fxx:03d} FAILED: {exc}")
        return fxx, None


# ---------------------------------------------------------------------------
# Batch download (one parallel pass through a list of fxx values)
# ---------------------------------------------------------------------------

def _download_batch(
    cycle_time: datetime,
    fxx_list: list[int],
    staging_dir: Path,
    workers: int,
) -> tuple[dict[int, Path], list[int]]:
    """
    Attempt to download a list of forecast-hour files in parallel.
    Returns (successes, failures) where successes maps fxx → local path.
    """
    results: dict[int, Path] = {}
    failures: list[int] = []
    total = len(fxx_list)
    done = 0
    t0 = time.monotonic()

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
            if done % 10 == 0 or done == total:
                elapsed = time.monotonic() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                log.info(f"  {done:3d}/{total}  "
                         f"({len(failures)} failed)  "
                         f"{elapsed:5.0f}s elapsed  "
                         f"ETA ~{eta:4.0f}s")

    return results, failures


# ---------------------------------------------------------------------------
# Full-cycle download
# ---------------------------------------------------------------------------

def download_cycle(
    cycle_time: datetime,
    staging_dir: Path,
    fxx_max: int = 260,
    workers: int = DOWNLOAD_WORKERS,
    dry_run: bool = False,
    max_retries: int = 5,
    retry_delay: int = 300,
) -> dict[int, Path]:
    """
    Download all forecast hours for cycle_time into staging_dir.

    On each pass the function queries the S3 directory listing to learn which
    files exist, then downloads only those not yet retrieved.  This eliminates
    spurious "file not found" warnings for fxx values that do not exist on S3.

    Passes continue (up to max_retries) for two distinct reasons:

      1. Extended-range files (fxx > 36) are posted to S3 progressively,
         typically 30–90 min after the hourly segment.  Re-listing S3 on each
         pass discovers newly-arrived files that weren't there on the previous
         pass.
      2. Transient download failures are retried automatically: a failed fxx
         stays absent from ``results`` and is re-attempted the next time it
         appears in the S3 listing.

    Raises RuntimeError only if a file confirmed present on S3 still cannot be
    downloaded after all retries.  Expected files that never appear on S3 are
    logged as warnings (the schedule can vary by cycle).

    Returns a dict mapping fxx → local file path for every successful download.
    """
    # Static schedule used only as the "target" set for completion check.
    expected = set(nbm_forecast_hours(fxx_max))
    staging_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Cycle: {cycle_time:%Y-%m-%d %H}Z | "
             f"target {len(expected)} forecast hours up to fxx {fxx_max:03d} | "
             f"{workers} workers")

    if dry_run:
        log.info("[DRY RUN] Skipping actual downloads.")
        return {}

    t0 = time.monotonic()
    results: dict[int, Path] = {}
    download_failures: list[int] = []

    for attempt in range(max_retries + 1):
        if attempt > 0:
            log.info(
                f"Pass {attempt + 1}/{max_retries + 1}: waiting {retry_delay}s "
                f"(polling for extended-range files and retrying failures)..."
            )
            time.sleep(retry_delay)

        # Fresh S3 listing — discovers files posted since the previous pass.
        available = list_available_fxx(cycle_time, fxx_max=fxx_max)
        to_download = [f for f in available if f not in results]

        log.info(
            f"Pass {attempt + 1}/{max_retries + 1}: "
            f"{len(available)} on S3  |  "
            f"{len(results)} downloaded  |  "
            f"{len(to_download)} to fetch"
        )

        if to_download:
            batch_results, download_failures = _download_batch(
                cycle_time, to_download, staging_dir, workers
            )
            results.update(batch_results)
        else:
            download_failures = []

        # Early exit: nothing left to fetch from the current S3 listing and no failures.
        remaining = [f for f in available if f not in results]
        if not remaining and not download_failures:
            log.info("All S3-listed files retrieved.")
            break

    elapsed = time.monotonic() - t0
    log.info(f"Download complete: {len(results)}/{len(expected)} expected files "
             f"in {elapsed:.0f}s")

    # Raise only for confirmed S3 files that still couldn't be downloaded.
    if download_failures:
        raise RuntimeError(
            f"{len(download_failures)} files still failing after {max_retries} retries: "
            f"fxx={sorted(download_failures)}"
        )

    # Warn about expected files that never appeared on S3 (schedule can vary).
    missing = expected - set(results)
    if missing:
        log.warning(
            f"{len(missing)} expected fxx values not found on S3 after all passes: "
            f"fxx={sorted(missing)}"
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
    fxx_max: int = 260,
    workers: int = DOWNLOAD_WORKERS,
    dry_run: bool = False,
    force: bool = False,
    postprocess: bool = False,
    keep_staging: bool = False,
    max_retries: int = 5,
    retry_delay: int = 300,
) -> Path:
    """
    Run a full ingestion cycle:
      1. Acquire lock
      2. Find latest available NBM CONUS cycle
      3. Check if already staged (skip unless force=True)
      4. Download all forecast hours
      5. Write manifest
      6. (Optional) Run GRIB2 → slab ring buffer post-processing
      7. Release lock

    Parameters
    ----------
    postprocess : bool
        If True, run the slab ingest post-processor after download.
    keep_staging : bool
        If True and postprocess is True, keep the GRIB2 files after post-processing.
        Ignored if postprocess is False.

    Returns the path to the staging directory for this cycle.
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
                log.info("Running slab ingest on existing staged cycle...")
                from ..postprocessor.slab_ingest import run_slab_ingest
                stats = run_slab_ingest(
                    staging_dir=cycle_staging,
                    cycle_tag=cycle_tag,
                    delete_staging=not keep_staging,
                )
                log.info(f"Slab ingest stats: {stats}")
            return cycle_staging

        # --- Download ---
        files = download_cycle(
            cycle_time=cycle_time,
            staging_dir=cycle_staging,
            fxx_max=fxx_max,
            workers=workers,
            dry_run=dry_run,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )

        # --- Write manifest ---
        if not dry_run:
            write_manifest(cycle_staging, cycle_time, files)

        log.info(f"Ingestion complete. Staged at: {cycle_staging}")

        # --- Post-process ---
        if postprocess and not dry_run:
            log.info("Starting slab ingest (GRIB2 → slab ring buffer)...")
            from ..postprocessor.slab_ingest import run_slab_ingest
            stats = run_slab_ingest(
                staging_dir=cycle_staging,
                cycle_tag=cycle_tag,
                delete_staging=not keep_staging,
            )
            log.info(f"Slab ingest stats: {stats}")

        return cycle_staging
