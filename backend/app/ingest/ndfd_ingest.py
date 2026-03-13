"""
NDFD ingestion pipeline.

Downloads the current NOAA National Digital Forecast Database (NDFD) CONUS
element files from the noaa-ndfd-pds S3 bucket and stages them locally for
post-processing.

Key differences from the NBM pipeline (ingest.py):
  - No Herbie.  Direct S3 access via s3fs (anonymous, public bucket).
  - File-per-element layout:  one ds.{element}.bin per element per period
    (vs one file per forecast hour in NBM).
  - No fixed cycle time in filenames.  The forecast reference time is read
    from inside the GRIB2 data (GRIB2 Section 1) after download.
  - Idempotency: cycle_tag = floor(reference_time, 1h).  If this tag already
    exists in the ring buffer the run is skipped without writing slabs.
  - Two-stage staging: files download to INCOMING/ first, then move to
    staging/{cycle_tag}/ once the reference time is known.

Typical call:
    from backend.app.ingest.ndfd_ingest import run_ndfd_ingestion
    run_ndfd_ingestion()

Or via CLI:
    python -m backend.app.ingest.ndfd_ingest
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import eccodes
import s3fs

from ..config_ndfd import (
    NDFD_BUCKET, NDFD_PREFIX, NDFD_PERIODS, NDFD_ELEMENTS,
    NDFD_INCOMING_DIR, NDFD_STAGING_DIR, NDFD_LOCK_FILE,
    NDFD_DATA_ROOT, NDFD_DOWNLOAD_WORKERS,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lock file  (re-uses same pattern as ingest.py)
# ---------------------------------------------------------------------------

class LockError(RuntimeError):
    pass


class IngestLock:
    def __init__(self, lock_path: Path) -> None:
        self.path = lock_path

    def __enter__(self) -> "IngestLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            pid_str = self.path.read_text().strip()
            stale = False
            try:
                pid = int(pid_str)
                os.kill(pid, 0)
            except (ValueError, ProcessLookupError):
                stale = True
            except PermissionError:
                stale = False
            if stale:
                log.warning(f"Removing stale lock file (PID {pid_str} no longer running).")
                self.path.unlink()
            else:
                raise LockError(
                    f"Lock file exists ({self.path}). "
                    f"NDFD ingestion already running as PID {pid_str}."
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
# S3 helpers
# ---------------------------------------------------------------------------

def _make_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(anon=True)


def list_available_elements(
    fs:     s3fs.S3FileSystem,
    period: str,
) -> set[str]:
    """Return the set of element abbreviations actually present on S3 for *period*."""
    prefix = f"{NDFD_BUCKET}/{NDFD_PREFIX}/{period}/"
    try:
        paths = fs.ls(prefix)
    except Exception as e:
        log.warning(f"S3 listing failed for {prefix}: {e}")
        return set()
    elements = set()
    for p in paths:
        name = Path(p).name
        if name.startswith("ds.") and name.endswith(".bin"):
            elements.add(name[3:-4])   # strip "ds." prefix and ".bin" suffix
    return elements


# ---------------------------------------------------------------------------
# Per-file download
# ---------------------------------------------------------------------------

def _download_one(
    fs:      s3fs.S3FileSystem,
    period:  str,
    element: str,
    dest:    Path,
) -> tuple[str, str, Optional[Path]]:
    """
    Download one element file.  Returns (period, element, local_path_or_None).
    Intended to be called from a ThreadPoolExecutor.
    """
    s3_path = f"{NDFD_BUCKET}/{NDFD_PREFIX}/{period}/ds.{element}.bin"
    try:
        fs.get(s3_path, str(dest))
        return period, element, dest
    except Exception as e:
        log.warning(f"  {period}/ds.{element}.bin  FAILED: {e}")
        return period, element, None


# ---------------------------------------------------------------------------
# Download all configured elements → INCOMING/
# ---------------------------------------------------------------------------

def download_to_incoming(
    incoming_dir: Path,
    workers:      int = NDFD_DOWNLOAD_WORKERS,
    dry_run:      bool = False,
) -> dict[tuple[str, str], Path]:
    """
    Download all configured NDFD element files into INCOMING/.

    The INCOMING/ directory is wiped and recreated fresh on each call so it
    always reflects the current S3 state.

    Returns a dict mapping (period, element) → local_path for every successful
    download.
    """
    if incoming_dir.exists():
        shutil.rmtree(incoming_dir)
    incoming_dir.mkdir(parents=True)

    if dry_run:
        log.info("[DRY RUN] Skipping downloads.")
        return {}

    fs = _make_fs()

    # Build the download list, checking S3 availability per period first.
    tasks: list[tuple[str, str, Path]] = []
    for period in NDFD_PERIODS:
        available = list_available_elements(fs, period)
        period_dir = incoming_dir / period
        period_dir.mkdir()
        for elem in NDFD_ELEMENTS.get(period, []):
            if elem not in available:
                log.info(f"  {period}/ds.{elem}.bin  NOT ON S3 — skipping")
                continue
            dest = period_dir / f"ds.{elem}.bin"
            tasks.append((period, elem, dest))

    log.info(f"Downloading {len(tasks)} element files with {workers} workers…")
    t0 = time.monotonic()
    results: dict[tuple[str, str], Path] = {}
    failures: list[tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_download_one, fs, period, elem, dest): (period, elem)
            for period, elem, dest in tasks
        }
        for future in as_completed(futures):
            period, elem, path = future.result()
            if path:
                results[(period, elem)] = path
            else:
                failures.append((period, elem))

    elapsed = time.monotonic() - t0
    log.info(
        f"Download complete: {len(results)}/{len(tasks)} files in {elapsed:.0f}s"
        + (f"  ({len(failures)} failed: {failures})" if failures else "")
    )
    return results


# ---------------------------------------------------------------------------
# Reference time extraction  (GRIB2 Section 1 via eccodes)
# ---------------------------------------------------------------------------

def read_grib2_reference_time(path: Path) -> Optional[datetime]:
    """
    Read the GRIB2 Section 1 reference time from the first message in *path*.

    Returns a naive UTC datetime, or None if the file cannot be read.
    """
    try:
        with open(path, "rb") as f:
            msg = eccodes.codes_grib_new_from_file(f)
        if msg is None:
            return None
        try:
            date = eccodes.codes_get(msg, "dataDate")   # YYYYMMDD int
            time = eccodes.codes_get(msg, "dataTime")   # HHMM int (e.g. 2330)
            d = str(date)
            hour   = int(time) // 100
            minute = int(time) % 100
            return datetime(
                int(d[:4]), int(d[4:6]), int(d[6:8]),
                hour, minute, 0,
            )
        finally:
            eccodes.codes_release(msg)
    except Exception as e:
        log.warning(f"Could not read reference time from {path}: {e}")
        return None


def determine_cycle_tag(incoming_dir: Path) -> Optional[str]:
    """
    Read the GRIB2 Section 1 reference time from VP.001-003/ds.temp.bin
    and floor it to the nearest hour to produce a cycle_tag.

    VP.001-003 is used as the canonical period (it is always the most
    recently updated of the two periods).  ds.temp.bin is the canonical
    element (always present, standard encoding).

    Returns a string like "20260312_23", or None if the file is unreadable.
    """
    canon = incoming_dir / "VP.001-003" / "ds.temp.bin"
    if not canon.exists():
        log.error(f"Canonical file missing: {canon}. Cannot determine cycle_tag.")
        return None

    ref_time = read_grib2_reference_time(canon)
    if ref_time is None:
        return None

    # Floor to hour (not round — avoids cross-hour false matches when
    # consecutive NDFD updates land on either side of :30)
    floored = ref_time.replace(minute=0, second=0, microsecond=0)
    cycle_tag = floored.strftime("%Y%m%d_%H")
    log.info(
        f"VP.001-003/ds.temp.bin reference time: {ref_time:%Y-%m-%dT%H:%MZ}  "
        f"→ cycle_tag: {cycle_tag}"
    )
    return cycle_tag


def cycle_time_iso(cycle_tag: str) -> str:
    """Convert "20260312_23" → "2026-03-12T23:00:00Z"."""
    dt = datetime.strptime(cycle_tag, "%Y%m%d_%H")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Staging
# ---------------------------------------------------------------------------

def move_incoming_to_staging(
    incoming_dir: Path,
    staging_dir:  Path,
    cycle_tag:    str,
) -> Path:
    """
    Move INCOMING/ → staging/{cycle_tag}/.

    If a staging directory for this cycle_tag already exists (e.g. from a
    previous partially-completed run) it is removed first.
    """
    cycle_staging = staging_dir / cycle_tag
    if cycle_staging.exists():
        shutil.rmtree(cycle_staging)
    staging_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(incoming_dir), str(cycle_staging))
    log.info(f"Staged at: {cycle_staging}")
    return cycle_staging


def write_manifest(
    staging_dir: Path,
    cycle_tag:   str,
    results:     dict[tuple[str, str], Path],
) -> Path:
    """Write a JSON manifest recording the downloaded files."""
    manifest = {
        "source":      "ndfd",
        "cycle_tag":   cycle_tag,
        "cycle_time":  cycle_time_iso(cycle_tag),
        "downloaded":  datetime.now(tz=timezone.utc).isoformat(),
        "file_count":  len(results),
        "files": {
            f"{period}/{elem}": str(path)
            for (period, elem), path in sorted(results.items())
        },
    }
    path = staging_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2))
    log.info(f"Manifest written: {path}")
    return path


def read_manifest(staging_dir: Path) -> Optional[dict]:
    path = staging_dir / "manifest.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


# ---------------------------------------------------------------------------
# Idempotency check against ring buffer
# ---------------------------------------------------------------------------

def cycle_tag_in_ring_buffer(cycle_tag: str, slab_store_dir: Path) -> bool:
    """
    Return True if *cycle_tag* is already committed to the NDFD ring buffer.
    """
    from ..store.ring_state import RingState
    if not (slab_store_dir / "ring_state.json").exists():
        return False
    state = RingState.load(slab_store_dir)
    return state.run_by_tag(cycle_tag) is not None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_ndfd_ingestion(
    workers:      int  = NDFD_DOWNLOAD_WORKERS,
    dry_run:      bool = False,
    force:        bool = False,
    postprocess:  bool = False,
    keep_staging: bool = False,
) -> Optional[Path]:
    """
    Run a full NDFD ingestion cycle:

      1. Acquire lock.
      2. Download all configured element files to INCOMING/.
      3. Read GRIB2 reference time → determine cycle_tag.
      4. Check if cycle_tag already in ring buffer (idempotent skip if so).
      5. Move INCOMING/ → staging/{cycle_tag}/.
      6. Write manifest.
      7. Optionally run slab ingest post-processor.
      8. Release lock.

    Returns the staging directory path, or None on dry-run / idempotent skip.
    """
    from ..config_ndfd import NDFD_SLAB_STORE_DIR
    NDFD_DATA_ROOT.mkdir(parents=True, exist_ok=True)

    with IngestLock(NDFD_LOCK_FILE):

        # --- Download ---
        log.info("Starting NDFD element download…")
        results = download_to_incoming(
            incoming_dir=NDFD_INCOMING_DIR,
            workers=workers,
            dry_run=dry_run,
        )

        if dry_run:
            return None

        if not results:
            log.error("No element files downloaded.  Aborting.")
            return None

        # --- Determine cycle_tag from GRIB2 Section 1 ---
        cycle_tag = determine_cycle_tag(NDFD_INCOMING_DIR)
        if cycle_tag is None:
            log.error("Could not determine cycle_tag.  Aborting.")
            return None

        # --- Idempotency check ---
        if not force and cycle_tag_in_ring_buffer(cycle_tag, NDFD_SLAB_STORE_DIR):
            log.info(
                f"Cycle {cycle_tag} already in ring buffer.  Nothing to do. "
                f"Use --force to re-process."
            )
            # Clean up the fresh incoming download
            if NDFD_INCOMING_DIR.exists():
                shutil.rmtree(NDFD_INCOMING_DIR)
            return None

        # --- Stage ---
        cycle_staging = move_incoming_to_staging(
            incoming_dir=NDFD_INCOMING_DIR,
            staging_dir=NDFD_STAGING_DIR,
            cycle_tag=cycle_tag,
        )

        write_manifest(cycle_staging, cycle_tag, results)

        log.info(f"Ingestion complete. cycle_tag={cycle_tag}  staged at: {cycle_staging}")

        # --- Post-process ---
        if postprocess:
            log.info("Starting NDFD slab ingest (GRIB2 → slab ring buffer)…")
            from ..postprocessor.ndfd_slab_ingest import run_ndfd_slab_ingest
            stats = run_ndfd_slab_ingest(
                staging_dir=cycle_staging,
                cycle_tag=cycle_tag,
                delete_staging=not keep_staging,
            )
            log.info(f"NDFD slab ingest stats: {stats}")

        return cycle_staging


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(description="Download NDFD element files from S3.")
    parser.add_argument("--workers",      type=int, default=NDFD_DOWNLOAD_WORKERS)
    parser.add_argument("--dry-run",      action="store_true")
    parser.add_argument("--force",        action="store_true",
                        help="Re-download and re-process even if cycle_tag already in ring buffer.")
    parser.add_argument("--postprocess",  action="store_true",
                        help="Run slab ingest after download.")
    parser.add_argument("--keep-staging", action="store_true",
                        help="Keep GRIB2 files after slab ingest (default: delete).")
    args = parser.parse_args()

    run_ndfd_ingestion(
        workers=args.workers,
        dry_run=args.dry_run,
        force=args.force,
        postprocess=args.postprocess,
        keep_staging=args.keep_staging,
    )
