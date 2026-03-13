"""
NDFD GRIB2 → Slab ring buffer post-processor.

Reads staged NDFD element files (one per element per period), extracts
variables, builds a unified forecast time axis, and writes one slab per
time step into the ring buffer.

Key differences from slab_ingest.py (NBM):
  - Element-centric loop: one cfgrib open per element file (not per fxx).
  - All element data is loaded into memory at once (~1-2 GB; fine for 128 GB server).
  - Unified time axis is built from the union of all valid times across elements.
  - Variables absent at a given time step are stored as NaN (uint8=255).
  - Reuses RingState, RunRecord, slab_path, run_dir from the NBM store layer.
  - Uses NDFD-specific store layer: NDFDSlabMetadata, write_ndfd_slab, NDFDStore.

Typical usage::

    from backend.app.postprocessor.ndfd_slab_ingest import run_ndfd_slab_ingest
    stats = run_ndfd_slab_ingest(staging_dir, cycle_tag)
"""

from __future__ import annotations

import logging
import shutil
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

warnings.filterwarnings(
    "ignore",
    message="In a future version of xarray the default value for compat",
    category=FutureWarning,
    module="cfgrib",
)

import cfgrib

from ..config_ndfd import (
    NDFD_SLAB_STORE_DIR, NDFD_SLAB_N_RUNS, NDFD_SLAB_N_FXX,
    NDFD_IDIM, NDFD_JDIM, NDFD_VARIABLES_YAML,
    NDFD_PERIODS,
)
from ..registry_ndfd import NDFDVariableRegistry, NDFDNativeVariable
from ..store.metadata_ndfd import NDFDSlabMetadata
from ..store.writer_ndfd import write_ndfd_slab
from ..store.ring_state import RingState, RunRecord
from ..store.writer import run_dir
from .conversions import get_converter

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Store initialisation (idempotent)
# ---------------------------------------------------------------------------

def _ensure_store(store_dir: Path) -> NDFDSlabMetadata:
    """Create the NDFD slab store and metadata if it does not yet exist."""
    if (store_dir / "metadata.json").exists():
        meta = NDFDSlabMetadata.load(store_dir)
        meta.validate()
        return meta

    log.info(f"Initialising new NDFD slab store at {store_dir}")
    store_dir.mkdir(parents=True, exist_ok=True)
    meta = NDFDSlabMetadata.create(
        idim=NDFD_IDIM,
        jdim=NDFD_JDIM,
        n_runs=NDFD_SLAB_N_RUNS,
        n_fxx=NDFD_SLAB_N_FXX,
    )
    meta.validate()
    meta.save(store_dir)
    RingState.empty().save(store_dir)
    for slot in range(NDFD_SLAB_N_RUNS):
        (store_dir / f"run_{slot:02d}").mkdir(exist_ok=True)
    slab_mb  = meta.slab_nbytes / 1e6
    total_gb = slab_mb * NDFD_SLAB_N_RUNS * NDFD_SLAB_N_FXX / 1e3
    log.info(
        f"NDFD store initialised: {NDFD_IDIM}×{NDFD_JDIM} grid | "
        f"{meta.kvars} variables | "
        f"{NDFD_SLAB_N_RUNS} runs × {NDFD_SLAB_N_FXX} slabs | "
        f"{slab_mb:.0f} MB/slab | {total_gb:.0f} GB max"
    )
    return meta


# ---------------------------------------------------------------------------
# Element file extraction
# ---------------------------------------------------------------------------

def _element_path(staging_dir: Path, period: str, element: str) -> Optional[Path]:
    """Return the local path for an element file, or None if absent."""
    p = staging_dir / period / f"ds.{element}.bin"
    return p if p.exists() else None


def _open_element(path: Path) -> Optional[list]:
    """
    Open a NDFD element GRIB2 file with cfgrib.

    Returns a list of xarray Datasets (cfgrib may split one file into multiple
    datasets when variables differ by level type).  Returns None on failure.
    """
    try:
        datasets = cfgrib.open_datasets(str(path))
        if not datasets:
            log.warning(f"cfgrib returned no datasets for {path.name}")
            return None
        return datasets
    except Exception as e:
        log.warning(f"cfgrib failed on {path.name}: {e}")
        return None


def _extract_element(
    staging_dir: Path,
    period:      str,
    element:     str,
    var_name:    str,
    units_raw:   str,
    units_out:   str,
    var_key:     str,
) -> Optional[tuple[pd.DatetimeIndex, np.ndarray]]:
    """
    Extract the full time series for one element from one period.

    Returns (valid_times, 3D_array) where 3D_array has shape (n_steps, idim, jdim),
    or None if the element file is absent or unreadable.

    For variables cfgrib identifies as 'unknown', the dataset variable
    is literally named 'unknown'.  For known shortNames the cfgrib_var_key
    from variables_ndfd.yaml is used.
    """
    path = _element_path(staging_dir, period, element)
    if path is None:
        return None

    datasets = _open_element(path)
    if datasets is None:
        return None

    convert = get_converter(units_raw, units_out)

    for ds in datasets:
        # Try the expected variable key first; fall back to 'unknown'
        if var_key in ds.data_vars:
            da = ds[var_key]
        elif "unknown" in ds.data_vars:
            da = ds["unknown"]
        else:
            # Try any non-coordinate variable
            candidates = list(ds.data_vars.keys())
            if not candidates:
                continue
            da = ds[candidates[0]]

        # Extract valid times
        if "valid_time" in ds.coords:
            vt = pd.to_datetime(np.atleast_1d(ds.coords["valid_time"].values), utc=True)
        elif "time" in ds.coords:
            vt = pd.to_datetime(np.atleast_1d(ds.coords["time"].values), utc=True)
        else:
            log.warning(f"No time coordinate in {path.name} ({period})")
            continue

        arr = da.values  # shape: (n_steps, y, x)  or  (y, x) if single step
        if arr.ndim == 2:
            arr = arr[np.newaxis, :, :]   # promote single-step to (1, y, x)
            vt  = vt[:1]

        if arr.shape[0] != len(vt):
            log.warning(
                f"{path.name} ({period}): time axis length mismatch "
                f"(arr={arr.shape[0]}, vt={len(vt)}) — skipping"
            )
            continue

        arr = convert(arr.astype(np.float32))
        log.debug(f"  Extracted {var_name} from {period}/{path.name}: {len(vt)} steps")
        return vt, arr

    log.warning(f"Could not extract {var_name} from {path.name} ({period})")
    return None


# ---------------------------------------------------------------------------
# Unified time axis builder
# ---------------------------------------------------------------------------

def build_unified_time_axis(
    element_data: dict[str, dict[str, tuple[pd.DatetimeIndex, np.ndarray]]],
) -> pd.DatetimeIndex:
    """
    Build the sorted union of all valid times across all extracted elements.

    Parameters
    ----------
    element_data : {var_name: {period: (valid_times, arr)}}

    Returns a sorted DatetimeIndex (UTC, no duplicates).
    """
    all_times: set = set()
    for periods in element_data.values():
        for vt, _ in periods.values():
            all_times.update(vt.tolist())
    if not all_times:
        raise ValueError("No valid times found in any element file.")
    return pd.DatetimeIndex(sorted(all_times), tz="UTC")


# ---------------------------------------------------------------------------
# Grid coordinate extraction
# ---------------------------------------------------------------------------

def extract_grid(staging_dir: Path) -> Optional[tuple[np.ndarray, np.ndarray]]:
    """
    Extract lat/lon 2D arrays from the canonical element file (VP.001-003/ds.temp.bin).

    NDFD uses 0–360 east-positive longitude (same convention as NBM).
    Returns (lat, lon) float32 arrays of shape (idim, jdim), or None on failure.
    """
    path = _element_path(staging_dir, "VP.001-003", "temp")
    if path is None:
        log.warning("Canonical file VP.001-003/ds.temp.bin not found; cannot extract grid.")
        return None

    datasets = _open_element(path)
    if not datasets:
        return None

    ds = datasets[0]
    if "latitude" not in ds.coords or "longitude" not in ds.coords:
        log.warning("lat/lon coordinates not found in ds.temp.bin")
        return None

    lat = ds.coords["latitude"].values.astype(np.float32)
    lon = ds.coords["longitude"].values.astype(np.float32)

    # cfgrib returns 2D grids for Lambert Conformal projections; confirm shape.
    if lat.shape != (NDFD_IDIM, NDFD_JDIM):
        log.warning(
            f"Unexpected grid shape: {lat.shape} (expected {NDFD_IDIM}×{NDFD_JDIM}). "
            f"lat.npy / lon.npy will not be written."
        )
        return None

    return lat, lon


# ---------------------------------------------------------------------------
# Main slab-writing loop
# ---------------------------------------------------------------------------

def run_ndfd_slab_ingest(
    staging_dir:    Path,
    cycle_tag:      str,
    delete_staging: bool = True,
) -> dict:
    """
    Extract all NDFD element files in *staging_dir* and write slabs to the
    ring buffer store at NDFD_SLAB_STORE_DIR.

    Parameters
    ----------
    staging_dir : Path
        Directory containing VP.001-003/ and VP.004-007/ subdirectories
        populated by run_ndfd_ingestion().
    cycle_tag : str
        Cycle identifier, e.g. "20260312_23".
    delete_staging : bool
        If True, remove *staging_dir* after successful ingest.

    Returns
    -------
    dict with keys: slabs_written, vars_extracted, elapsed_s, cycle_tag
    """
    t0 = time.monotonic()
    store_dir = NDFD_SLAB_STORE_DIR

    # --- Ensure store exists ---
    meta = _ensure_store(store_dir)

    # --- Load variable registry ---
    registry = NDFDVariableRegistry(NDFD_VARIABLES_YAML)

    # --- Extract all element files into memory ---
    log.info(f"Extracting NDFD elements from {staging_dir} …")
    # element_data: {var_name: {period: (valid_times, 3D_array)}}
    element_data: dict[str, dict[str, tuple[pd.DatetimeIndex, np.ndarray]]] = {}

    for var_name, var_meta in [
        (n, registry.get(n)) for n in registry.all_names()
    ]:
        if not isinstance(var_meta, NDFDNativeVariable):
            continue

        element    = var_meta.s3_element
        var_key    = var_meta.cfgrib_var_key
        units_raw  = var_meta.units_raw
        units_out  = var_meta.units_out
        period_cut = var_meta.period_cutoff

        periods_to_try = [p for p in NDFD_PERIODS
                          if period_cut is None or p == period_cut]

        per_period: dict[str, tuple[pd.DatetimeIndex, np.ndarray]] = {}
        for period in periods_to_try:
            result = _extract_element(
                staging_dir, period, element, var_name,
                units_raw, units_out, var_key,
            )
            if result is not None:
                per_period[period] = result

        if per_period:
            element_data[var_name] = per_period
        else:
            log.warning(f"  {var_name}: no data extracted from any period")

    if not element_data:
        raise RuntimeError("No element data could be extracted.  Aborting slab ingest.")

    log.info(f"Extracted {len(element_data)} variables: {sorted(element_data)}")

    # --- Build unified time axis ---
    unified_times = build_unified_time_axis(element_data)
    n_steps = len(unified_times)
    log.info(
        f"Unified time axis: {n_steps} steps  "
        f"[{unified_times[0].isoformat()}  →  {unified_times[-1].isoformat()}]"
    )

    # Build a fast time→index lookup per variable per period
    # {var_name: {period: {pd.Timestamp: array_row_index}}}
    time_indices: dict[str, dict[str, dict]] = {}
    for var_name, periods in element_data.items():
        time_indices[var_name] = {}
        for period, (vt, _) in periods.items():
            time_indices[var_name][period] = {t: i for i, t in enumerate(vt)}

    # --- Reserve ring buffer slot ---
    state    = RingState.load(store_dir)
    run_slot = state.next_slot(meta.n_runs)
    log.info(f"Writing to ring buffer slot {run_slot:02d} ({n_steps} slabs)…")

    # --- Extract and save grid (first time or on mismatch) ---
    grid_result = extract_grid(staging_dir)
    if grid_result is not None and not (store_dir / "lat.npy").exists():
        lat, lon = grid_result
        meta.save_grid(store_dir, lat, lon)
        log.info(f"Grid coordinates saved to {store_dir}/lat.npy, lon.npy")

    # --- Write one slab per time step ---
    valid_time_strs: list[str] = []
    slabs_written   = 0

    for fxx_idx, vt_ts in enumerate(unified_times):
        var_arrays: dict[str, np.ndarray] = {}

        for var_name, periods in element_data.items():
            arr_2d: Optional[np.ndarray] = None

            # Try periods in order; use first match for this time step.
            for period in NDFD_PERIODS:
                if period not in periods:
                    continue
                vt_arr, data_3d = periods[period]
                t_idx = time_indices[var_name][period].get(vt_ts)
                if t_idx is not None:
                    arr_2d = data_3d[t_idx]  # shape (idim, jdim)
                    break

            if arr_2d is not None:
                var_arrays[var_name] = arr_2d
            # If not found for this step: variable stays absent (written as NaN sentinel)

        write_ndfd_slab(store_dir, meta, run_slot, fxx_idx, var_arrays)
        valid_time_strs.append(vt_ts.strftime("%Y-%m-%dT%H:%M:%SZ"))
        slabs_written += 1

        if slabs_written % 10 == 0 or slabs_written == n_steps:
            log.info(f"  {slabs_written}/{n_steps} slabs written…")

    # --- Commit run to ring buffer ---
    cycle_time = f"{cycle_tag[:4]}-{cycle_tag[4:6]}-{cycle_tag[6:8]}T{cycle_tag[9:11]}:00:00Z"
    record = RunRecord(
        slot=run_slot,
        cycle_tag=cycle_tag,
        cycle_time=cycle_time,
        n_fxx=slabs_written,
        valid_times=valid_time_strs,
    )
    state = RingState.load(store_dir)
    state.add_run(record, meta.n_runs)
    state.save(store_dir)

    elapsed = time.monotonic() - t0
    log.info(
        f"NDFD slab ingest complete: {slabs_written} slabs | "
        f"{len(element_data)} vars | {elapsed:.0f}s"
    )

    # --- Signal API to reload ---
    try:
        import urllib.request
        urllib.request.urlopen(
            "http://localhost:8002/admin/reload",
            data=b"",
            timeout=5,
        )
        log.info("Sent /admin/reload to NDFD API (port 8002)")
    except Exception as e:
        log.debug(f"Admin reload signal failed (API may not be running): {e}")

    # --- Delete staging ---
    if delete_staging and staging_dir.exists():
        shutil.rmtree(staging_dir)
        log.info(f"Staging directory removed: {staging_dir}")

    return {
        "cycle_tag":     cycle_tag,
        "slabs_written": slabs_written,
        "vars_extracted": len(element_data),
        "elapsed_s":     round(elapsed, 1),
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Extract NDFD staged files and write to slab ring buffer."
    )
    parser.add_argument(
        "staging_dir",
        nargs="?",
        help="Path to staging/{cycle_tag}/ directory.  "
             "Defaults to the most recent directory under NDFD_STAGING_DIR.",
    )
    parser.add_argument("--keep-staging", action="store_true",
                        help="Keep staging files after ingest.")
    args = parser.parse_args()

    from ..config_ndfd import NDFD_STAGING_DIR

    if args.staging_dir:
        sd = Path(args.staging_dir)
    else:
        candidates = sorted(NDFD_STAGING_DIR.glob("*"))
        if not candidates:
            raise SystemExit(f"No staging directories found under {NDFD_STAGING_DIR}")
        sd = candidates[-1]
        log.info(f"Using most recent staging dir: {sd}")

    cycle_tag = sd.name
    run_ndfd_slab_ingest(
        staging_dir=sd,
        cycle_tag=cycle_tag,
        delete_staging=not args.keep_staging,
    )
