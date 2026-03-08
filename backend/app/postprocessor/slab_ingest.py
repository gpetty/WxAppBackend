"""
GRIB2 → Slab ring buffer post-processor.

Replaces the Zarr-based pipeline with a streaming write approach:
each GRIB2 file is extracted and written to a slab immediately,
so the main process never holds more than ~200 MB of array data
at once (one time step across 15 variables).

Peak memory: ~200 MB, regardless of cycle length.
Compare to old Zarr pipeline: ~109 GB peak for a 260-step cycle.

Typical usage::

    from backend.app.postprocessor.slab_ingest import run_slab_ingest
    stats = run_slab_ingest(staging_dir)
"""

from __future__ import annotations

import json
import logging
import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np

from ..config import (
    SLAB_STORE_DIR, SLAB_N_RUNS, SLAB_N_FXX, SLAB_IDIM, SLAB_JDIM,
    POSTPROCESS_WORKERS, VARIABLES_YAML,
)
from ..registry import VariableRegistry
from ..store import SlabMetadata, SlabRingBuffer, write_slab
from ..store.ring_state import RingState

# Re-use the proven per-file extraction worker and helpers from the existing pipeline.
from .grib2_to_zarr import (
    _extract_file_worker,
    _list_grib2_files,
    _should_extract,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Store initialisation (idempotent)
# ---------------------------------------------------------------------------

def _ensure_store(store_dir: Path) -> None:
    """Create the slab store directory and metadata if it does not yet exist."""
    if (store_dir / "metadata.json").exists():
        return
    log.info(f"Initialising new slab store at {store_dir}")
    store_dir.mkdir(parents=True, exist_ok=True)
    meta = SlabMetadata.create(
        idim=SLAB_IDIM,
        jdim=SLAB_JDIM,
        n_runs=SLAB_N_RUNS,
        n_fxx=SLAB_N_FXX,
    )
    meta.validate()
    meta.save(store_dir)
    RingState.empty().save(store_dir)
    for slot in range(SLAB_N_RUNS):
        (store_dir / f"run_{slot:02d}").mkdir(exist_ok=True)
    slab_mb  = meta.slab_nbytes / 1e6
    total_gb = slab_mb * SLAB_N_RUNS * SLAB_N_FXX / 1e3
    log.info(
        f"Store initialised: {SLAB_IDIM}×{SLAB_JDIM} grid | "
        f"{meta.kvars} variables | "
        f"{SLAB_N_RUNS} runs × {SLAB_N_FXX} slabs | "
        f"{slab_mb:.0f} MB/slab | {total_gb:.0f} GB max"
    )


# ---------------------------------------------------------------------------
# valid_time helper
# ---------------------------------------------------------------------------

def _infer_valid_time(cycle_time_str: str, fxx: int) -> str:
    """
    Compute the ISO-8601 valid time for a given fxx from the cycle time string.
    Used as a fallback when the GRIB2 extraction fails to return a valid_time.
    """
    cycle_dt = datetime.strptime(cycle_time_str, "%Y-%m-%dT%H:00:00Z").replace(
        tzinfo=timezone.utc
    )
    vt = cycle_dt + timedelta(hours=fxx)
    return vt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_slab_ingest(
    staging_dir:    Path,
    cycle_tag:      Optional[str] = None,
    delete_staging: bool = True,
    workers:        int  = POSTPROCESS_WORKERS,
    store_dir:      Path = SLAB_STORE_DIR,
) -> dict:
    """
    Extract all native variables from staged GRIB2 files and write them to
    the slab ring buffer, one slab per file.

    Parameters
    ----------
    staging_dir : Path
        Directory containing staged GRIB2 files and a manifest.json.
    cycle_tag : str, optional
        Cycle identifier (e.g. "20260307_19").  Derived from staging_dir.name
        if not given.
    delete_staging : bool
        Delete the GRIB2 staging directory after a successful ingest.
    workers : int
        Number of parallel cfgrib extraction workers.
    store_dir : Path
        Root of the slab store (default: SLAB_STORE_DIR from config).

    Returns
    -------
    dict with timing and count statistics.
    """
    if cycle_tag is None:
        cycle_tag = staging_dir.name

    overall_t0 = time.monotonic()

    # --- Read cycle time from manifest ---
    manifest_path = staging_dir / "manifest.json"
    cycle_time_str = "unknown"
    if manifest_path.exists():
        m = json.loads(manifest_path.read_text())
        cycle_time_str = m.get("cycle", cycle_time_str)

    log.info(f"Slab ingest — cycle: {cycle_time_str}  (tag: {cycle_tag})")
    log.info(f"  staging: {staging_dir}")
    log.info(f"  store:   {store_dir}")

    # --- Ensure store exists ---
    _ensure_store(store_dir)
    ring = SlabRingBuffer.open(store_dir)
    meta = ring.meta

    # --- Discover GRIB2 files ---
    grib2_files = _list_grib2_files(staging_dir)
    if not grib2_files:
        raise FileNotFoundError(f"No GRIB2 files found in {staging_dir}")
    n_files = len(grib2_files)
    log.info(f"  {n_files} GRIB2 files found | {workers} workers")

    # --- Load variable registry ---
    registry = VariableRegistry(VARIABLES_YAML)
    native_vars = registry.native()

    # Map fxx → fxx_idx (0-based sorted index used for slab filenames)
    sorted_fxx  = sorted(grib2_files.keys())
    fxx_to_idx  = {fxx: idx for idx, fxx in enumerate(sorted_fxx)}

    # --- Reserve ring buffer slot ---
    slot = ring.begin_run()

    # --- Build per-file task list ---
    tasks = []
    for fxx, path in grib2_files.items():
        expected = {
            name: var for name, var in native_vars.items()
            if _should_extract(var, fxx)
        }
        if expected:
            tasks.append((fxx, path, expected))

    # --- Parallel extraction + immediate slab write ---
    # valid_times_by_idx: fxx_idx → ISO-8601 valid time string
    valid_times_by_idx: dict[int, str] = {}
    lat_2d = lon_2d = None
    n_extracted = n_missing_total = 0
    done = 0
    n_tasks = len(tasks)
    extract_t0 = time.monotonic()

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_extract_file_worker, fxx, path, exp_vars): fxx
            for fxx, path, exp_vars in tasks
        }

        for future in as_completed(futures):
            result    = future.result()
            fxx       = result["fxx"]
            fxx_idx   = fxx_to_idx[fxx]
            records   = result["records"]   # {var_name: (valid_time_np64, float32_arr)}
            done     += 1

            # Relay worker log messages
            for level, msg in result["logs"]:
                getattr(log, level)(msg)

            # Capture lat/lon from first successful extraction
            if lat_2d is None and result["lat"] is not None:
                lat_2d = result["lat"]
                lon_2d = result["lon"]

            # Determine valid_time for this slab
            if records:
                vt_np64 = next(iter(records.values()))[0]
                vt_dt   = datetime.utcfromtimestamp(
                    vt_np64.astype("datetime64[s]").astype(int)
                ).replace(tzinfo=timezone.utc)
                vt_str  = vt_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                vt_str = _infer_valid_time(cycle_time_str, fxx)

            valid_times_by_idx[fxx_idx] = vt_str

            # Build var_arrays for write_slab
            # Missing variables will be filled with sentinel 255 by write_slab.
            var_arrays = {name: arr for name, (_, arr) in records.items()}

            write_slab(store_dir, meta, slot, fxx_idx, var_arrays)

            n_extracted      += len(records)
            n_missing_total  += len(result["missing"])

            if done % 10 == 0 or done == n_tasks:
                elapsed = time.monotonic() - extract_t0
                log.info(f"  [{done:3d}/{n_tasks}] fxx={fxx:03d} → "
                         f"slab_{fxx_idx:03d}  ({elapsed:.0f}s)")

    extract_time = time.monotonic() - extract_t0

    # --- Save lat/lon grid if captured and not already stored ---
    if lat_2d is not None and not (store_dir / "lat.npy").exists():
        meta.save_grid(store_dir, lat_2d, lon_2d)
        log.info("Grid coordinates saved (lat.npy / lon.npy)")

    # --- Assemble valid_times list in fxx_idx order ---
    n_slabs = len(sorted_fxx)
    valid_times = [
        valid_times_by_idx.get(idx, _infer_valid_time(cycle_time_str, sorted_fxx[idx]))
        for idx in range(n_slabs)
    ]

    # --- Commit run to ring buffer ---
    ring.commit_run(slot, cycle_tag, cycle_time_str, valid_times)

    # --- Optionally clean up staging ---
    if delete_staging and staging_dir.exists():
        log.info(f"Removing staging directory: {staging_dir}")
        shutil.rmtree(staging_dir)

    overall_time = time.monotonic() - overall_t0

    stats = {
        "cycle":            cycle_time_str,
        "cycle_tag":        cycle_tag,
        "slot":             slot,
        "n_files":          n_files,
        "n_slabs_written":  n_slabs,
        "n_vars_extracted": n_extracted,
        "n_vars_missing":   n_missing_total,
        "extraction_time_s": round(extract_time, 1),
        "total_time_s":     round(overall_time, 1),
    }

    log.info(
        f"Slab ingest complete in {overall_time:.1f}s\n"
        f"  Stats: {json.dumps(stats, indent=2)}"
    )
    return stats
