"""
GRIB2 → Zarr post-processor.

Reads staged GRIB2 files (one per forecast hour), extracts native variables
defined in variables.yaml, applies unit conversions, and writes a single
Zarr store chunked for optimal point time-series queries.

Design notes
------------
- One chunk read per variable per point query: chunk shape ``(n_time, 256, 256)``.
- All time steps for a variable live in one chunk per spatial tile (~6 KB).
- No compression initially (uncompressed float32); add LZ4 later if needed.
- Atomic swap: writes to ``zarr/staging_new/``, then renames to ``zarr/current/``.
- Staging GRIB2 files are deleted after a successful Zarr write.

Usage::

    from backend.app.postprocessor import run_postprocessor
    stats = run_postprocessor(Path("data/nbm/staging/20260227_18"))
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# cfgrib triggers a FutureWarning about xarray's upcoming compat default change;
# this is internal to cfgrib and not actionable here.
warnings.filterwarnings(
    "ignore",
    message="In a future version of xarray the default value for compat",
    category=FutureWarning,
    module="cfgrib",
)

import numpy as np
import xarray as xr

from ..config import (
    ZARR_DIR, VARIABLES_YAML,
    ZARR_RETAIN_DAYS, ZARR_RETAIN_HOURS, POSTPROCESS_WORKERS,
)
from ..registry import VariableRegistry, NativeVariable
from .conversions import get_converter

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fxx_from_path(path: Path) -> Optional[int]:
    """
    Extract the forecast hour from a GRIB2 filename.

    Expected pattern: ``blend.tHHz.core.fXXX.co.grib2``
    Returns the integer fxx or None if the pattern doesn't match.
    """
    name = path.name
    # Find the ".fXXX." segment
    parts = name.split(".")
    for part in parts:
        if part.startswith("f") and part[1:].isdigit():
            return int(part[1:])
    return None


def _list_grib2_files(staging_dir: Path) -> dict[int, Path]:
    """
    Scan the staging directory (recursively) for GRIB2 files and return
    a dict mapping fxx → file path, sorted by fxx.

    Herbie nests downloads under date/model subdirectories, so we search
    recursively for any file matching ``*.grib2``.
    """
    files: dict[int, Path] = {}
    for p in staging_dir.rglob("*.grib2"):
        fxx = _fxx_from_path(p)
        if fxx is not None:
            files[fxx] = p
    return dict(sorted(files.items()))


def _should_extract(var: NativeVariable, fxx: int) -> bool:
    """
    Return True if *var* should be extracted from the file at forecast hour *fxx*.

    Respects both ``fxx_cutoff`` (variable absent after a certain fxx) and
    ``fxx_availability`` (variable only present at specific fxx values).
    """
    if var.fxx_availability is not None:
        return fxx in var.fxx_availability
    if var.fxx_cutoff is not None:
        return fxx <= var.fxx_cutoff
    return True


def _open_all_datasets(grib2_path: Path) -> Optional[list]:
    """
    Open a GRIB2 file with cfgrib and return the list of xarray Datasets.

    cfgrib.open_datasets() splits the file into groups by level type.
    Returns None on failure.
    """
    import cfgrib

    try:
        return cfgrib.open_datasets(
            str(grib2_path),
            backend_kwargs={"indexpath": ""},  # don't write .idx sidecar files
        )
    except Exception as exc:
        log.warning(f"cfgrib failed to open {grib2_path.name}: {exc}")
        return None


def _open_with_accum_filter(
    grib2_path: Path,
    var: NativeVariable,
    fxx: int,
) -> Optional[xr.DataArray]:
    """
    Open a single GRIB2 variable using an explicit stepRange filter.

    Used for variables like total_precipitation where multiple accumulation
    windows (QPF01, QPF06, QPF12) share the same shortName in a file.
    Filtering by stepRange='{fxx-N}-{fxx}' ensures we always extract
    the N-hour accumulation bucket regardless of message order in the file.
    """
    import cfgrib

    accum = var.grib_accum_hours
    step_range = f"{fxx - accum}-{fxx}"
    try:
        ds = cfgrib.open_dataset(
            str(grib2_path),
            filter_by_keys={
                "shortName":    var.grib_shortName,
                "typeOfLevel":  var.grib_typeOfLevel,
                "stepRange":    step_range,
            },
            backend_kwargs={"indexpath": ""},
        )
        if var.cfgrib_var_key in ds.data_vars:
            return ds[var.cfgrib_var_key]
        # cfgrib sometimes renames ambiguous fields; fall back to first data var
        for da in ds.data_vars.values():
            if da.attrs.get("GRIB_shortName") == var.grib_shortName:
                return da
    except Exception as exc:
        log.debug(
            f"  {var.name} fxx={fxx:03d}: accum filter ({step_range}) failed: {exc}"
        )
    return None


def _find_variable_in_datasets(
    datasets: list,
    var: NativeVariable,
) -> Optional[xr.DataArray]:
    """
    Search already-opened cfgrib datasets for a specific variable.

    Matches first by cfgrib_var_key, then validates grib_shortName.
    Returns None if not found.
    """
    for ds in datasets:
        if var.cfgrib_var_key not in ds.data_vars:
            continue
        da = ds[var.cfgrib_var_key]
        sn = da.attrs.get("GRIB_shortName", "")
        if sn == var.grib_shortName:
            return da
        # Accept cfgrib_var_key match even if shortName differs slightly
        log.debug(
            f"  {var.name}: found {var.cfgrib_var_key} with shortName={sn!r} "
            f"(expected {var.grib_shortName!r}); accepting"
        )
        return da
    return None


def _get_valid_time(da: xr.DataArray, var_name: str, fxx: int) -> Optional[np.datetime64]:
    """Extract the valid_time scalar from a cfgrib DataArray."""
    vt = da.coords.get("valid_time", da.coords.get("time", None))
    if vt is not None:
        return np.datetime64(vt.values, "ns")
    # Fallback: reference_time + step
    step = da.coords.get("step", None)
    ref  = da.coords.get("time", None)
    if step is not None and ref is not None:
        return np.datetime64(ref.values, "ns") + step.values
    log.warning(f"  {var_name} fxx={fxx:03d}: can't determine valid_time")
    return None


# ---------------------------------------------------------------------------
# Parallel file worker  (top-level so ProcessPoolExecutor can pickle it)
# ---------------------------------------------------------------------------

def _extract_file_worker(
    fxx: int,
    grib2_path: Path,
    expected_vars: dict[str, "NativeVariable"],
) -> dict:
    """
    Extract all expected variables from one GRIB2 file.

    Designed to run in a worker subprocess via ProcessPoolExecutor.
    Must be a top-level function so it can be pickled.

    Returns a dict with keys:
      fxx      : int
      records  : {var_name: (valid_time_np64, float32_array)}
      lat      : 2-D latitude array or None
      lon      : 2-D longitude array or None
      missing  : [var_name, ...]  — expected but not found
      logs     : [(level_str, message), ...]  — emitted by main process
    """
    logs: list[tuple[str, str]] = []
    records: dict[str, tuple] = {}
    missing: list[str] = []
    lat_2d = lon_2d = None

    datasets = _open_all_datasets(grib2_path)
    if datasets is None:
        logs.append(("warning", f"fxx={fxx:03d}: cfgrib failed to open {grib2_path.name}"))
        return {"fxx": fxx, "records": {}, "lat": None, "lon": None,
                "missing": list(expected_vars), "logs": logs}

    # Capture lat/lon from first dataset that has spatial coordinates
    for ds in datasets:
        if not ds.data_vars:
            continue
        da_ref = ds[list(ds.data_vars)[0]]
        if "latitude" in da_ref.coords:
            lat_2d = da_ref.coords["latitude"].values
            lon_2d = da_ref.coords["longitude"].values
        elif "lat" in da_ref.coords:
            lat_2d = da_ref.coords["lat"].values
            lon_2d = da_ref.coords["lon"].values
        if lat_2d is not None:
            break

    for var_name, var in expected_vars.items():
        if var.grib_accum_hours is not None:
            da = _open_with_accum_filter(grib2_path, var, fxx)
        else:
            da = _find_variable_in_datasets(datasets, var)

        if da is None:
            logs.append(("debug", f"  {var_name}: not found in fxx={fxx:03d}"))
            missing.append(var_name)
            continue

        vt = _get_valid_time(da, var_name, fxx)
        if vt is None:
            missing.append(var_name)
            continue

        arr = get_converter(var.units_raw, var.units_out)(da.values).astype(np.float32)
        records[var_name] = (vt, arr)

    logs.append(("info",
        f"[{fxx:03d}] {grib2_path.name}: "
        f"{len(records)} extracted, {len(missing)} missing"
    ))
    return {"fxx": fxx, "records": records, "lat": lat_2d, "lon": lon_2d,
            "missing": missing, "logs": logs}


# ---------------------------------------------------------------------------
# Core extraction loop  (file-centric: one cfgrib open per GRIB2 file)
# ---------------------------------------------------------------------------

def extract_variables(
    staging_dir: Path,
    registry: VariableRegistry,
    workers: int = POSTPROCESS_WORKERS,
) -> xr.Dataset:
    """
    Extract all native variables from staged GRIB2 files into an xarray
    Dataset with dimensions ``(valid_time, y, x)``.

    **File-centric design:** each GRIB2 file is opened exactly once with
    ``cfgrib.open_datasets()``, and all applicable variables are extracted
    in that single pass. This is ~15x faster than the alternative of
    iterating files once per variable.

    **Parallel extraction:** files are processed concurrently using
    ``ProcessPoolExecutor``.  Each worker subprocess handles one GRIB2 file
    independently, returning extracted arrays to the main process for assembly.
    Use ``workers=1`` to disable parallelism (useful for debugging).

    Variables missing from a file where they are expected log a warning and
    contribute a NaN time step. xarray aligns variables with different
    valid_time arrays when the Dataset is assembled, filling NaN as needed.
    """
    grib2_files = _list_grib2_files(staging_dir)
    if not grib2_files:
        raise FileNotFoundError(f"No GRIB2 files found in {staging_dir}")

    n_files = len(grib2_files)
    log.info(f"Found {n_files} GRIB2 files in {staging_dir}  |  workers={workers}")

    native_vars = registry.native()

    # Per-variable accumulators: list of (valid_time_ns, 2D float32 array)
    var_records: dict[str, list[tuple[np.datetime64, np.ndarray]]] = {
        name: [] for name in native_vars
    }
    # Counts for summary logging
    var_skipped: dict[str, int] = {name: 0 for name in native_vars}
    var_missing:  dict[str, int] = {name: 0 for name in native_vars}

    lat_2d: Optional[np.ndarray] = None
    lon_2d: Optional[np.ndarray] = None

    # ---- Build per-file task list -------------------------------------------
    # Determine which variables are expected for each fxx and track skips.
    tasks: list[tuple[int, Path, dict]] = []
    for fxx, grib2_path in grib2_files.items():
        expected = {
            name: var for name, var in native_vars.items()
            if _should_extract(var, fxx)
        }
        for name in set(native_vars) - set(expected):
            var_skipped[name] += 1
        if not expected:
            log.debug(f"fxx={fxx:03d}: no variables expected, skipping")
            continue
        tasks.append((fxx, grib2_path, expected))

    # ---- Parallel extraction via ProcessPoolExecutor ------------------------
    done = 0
    n_tasks = len(tasks)
    t0 = time.monotonic()

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_extract_file_worker, fxx, path, exp_vars): fxx
            for fxx, path, exp_vars in tasks
        }
        for future in as_completed(futures):
            result = future.result()
            done += 1

            # Relay worker log messages through the main-process logger
            for level, msg in result["logs"]:
                getattr(log, level)(msg)

            if lat_2d is None and result["lat"] is not None:
                lat_2d = result["lat"]
                lon_2d = result["lon"]

            for var_name, (vt, arr) in result["records"].items():
                var_records[var_name].append((vt, arr))
            for var_name in result["missing"]:
                var_missing[var_name] += 1

            if done % 10 == 0 or done == n_tasks:
                elapsed = time.monotonic() - t0
                log.info(f"  Extracted {done}/{n_tasks} files in {elapsed:.0f}s")

    # ---- Assemble per-variable DataArrays -----------------------------------
    data_vars: dict[str, xr.DataArray] = {}

    for var_name, records in var_records.items():
        var = native_vars[var_name]
        skipped = var_skipped[var_name]
        missing  = var_missing[var_name]

        if not records:
            log.warning(f"  {var_name}: no data extracted from any file — skipping variable")
            continue

        # Sort by valid_time
        records.sort(key=lambda r: r[0])
        vt_array  = np.array([r[0] for r in records], dtype="datetime64[ns]")
        arr_stack = np.stack([r[1] for r in records], axis=0)  # (n_time, y, x)

        data_vars[var_name] = xr.DataArray(
            data=arr_stack,
            dims=["valid_time", "y", "x"],
            coords={"valid_time": vt_array},
            attrs={
                "units":          var.units_out,
                "description":    var.description,
                "grib_shortName": var.grib_shortName,
            },
        )

        log.info(
            f"  {var_name}: {len(records)} time steps extracted, "
            f"{skipped} skipped (fxx out of range), {missing} missing"
        )

    if not data_vars:
        raise RuntimeError("No variables were successfully extracted.")

    # ---- Build Dataset and attach coordinates -------------------------------
    # xarray aligns variables with different valid_time extents automatically,
    # filling NaN where a variable lacks a time step that another has.
    ds = xr.Dataset(data_vars)

    if lat_2d is not None:
        ds.coords["latitude"]  = (["y", "x"], lat_2d)
        ds.coords["longitude"] = (["y", "x"], lon_2d)

    return ds


# ---------------------------------------------------------------------------
# Zarr writing
# ---------------------------------------------------------------------------

def write_zarr(
    ds: xr.Dataset,
    zarr_staging: Path,
    chunk_y: int = 256,
    chunk_x: int = 256,
) -> dict:
    """
    Rechunk and write the dataset to a Zarr store.

    Chunk shape: ``(n_time, chunk_y, chunk_x)`` — all time steps in one
    chunk per spatial tile, optimised for point time-series queries.

    With a 2345×1597 grid and 256×256 tiles this yields ~70 chunks per
    variable — fast to write and still sub-100ms for point queries from
    SSD or page cache (~25 MB per chunk for a 100-step cycle).

    Returns a dict of size/timing stats.
    """
    zarr_staging.parent.mkdir(parents=True, exist_ok=True)

    # Remove any previous failed staging attempt
    if zarr_staging.exists():
        log.info(f"Removing previous staging Zarr: {zarr_staging}")
        shutil.rmtree(zarr_staging)

    # Use ds.sizes (dict-like) instead of deprecated ds.dims mapping
    sizes = dict(ds.sizes)
    n_time = sizes["valid_time"]
    n_y = sizes["y"]
    n_x = sizes["x"]
    chunk_spec = (n_time, chunk_y, chunk_x)

    # Build per-variable encoding to set chunk sizes without requiring dask.
    # xarray's to_zarr() accepts encoding dicts that specify chunk shapes
    # directly — the data stays as in-memory numpy arrays.
    encoding = {}
    for var_name in ds.data_vars:
        encoding[var_name] = {"chunks": chunk_spec}

    log.info(
        f"Writing Zarr store: {zarr_staging}\n"
        f"  Dimensions: valid_time={n_time}, y={n_y}, x={n_x}\n"
        f"  Chunks: {chunk_spec}\n"
        f"  Variables: {list(ds.data_vars)}"
    )

    t0 = time.monotonic()
    ds.to_zarr(str(zarr_staging), mode="w", encoding=encoding, consolidated=True)
    write_time = time.monotonic() - t0

    # Measure store size
    total_bytes = sum(
        f.stat().st_size for f in zarr_staging.rglob("*") if f.is_file()
    )
    total_gb = total_bytes / (1024 ** 3)

    stats = {
        "write_time_s": round(write_time, 1),
        "store_size_gb": round(total_gb, 2),
        "n_variables": len(ds.data_vars),
        "n_time_steps": n_time,
        "grid_shape": [n_y, n_x],
        "chunk_shape": list(chunk_spec),
    }

    log.info(
        f"Zarr write complete in {write_time:.1f}s  |  "
        f"Store size: {total_gb:.2f} GB  |  "
        f"{len(ds.data_vars)} variables, {n_time} time steps"
    )

    return stats


# ---------------------------------------------------------------------------
# Atomic swap
# ---------------------------------------------------------------------------

def atomic_swap(staging: Path, zarr_dir: Path, cycle_tag: str) -> Path:
    """
    Move the newly-written staging Zarr store to a named cycle store and
    atomically update the ``current`` symlink to point to it.

    Named stores live at ``zarr_dir/{cycle_tag}/`` (e.g. ``zarr/20260301_16/``).
    The ``current`` symlink is updated via an atomic rename so the API never
    reads a partially-written or missing store.

    Returns the path to the named cycle store.
    """
    zarr_dir.mkdir(parents=True, exist_ok=True)
    cycle_store = zarr_dir / cycle_tag
    current_link = zarr_dir / "current"

    # Remove any previous store for this cycle tag (re-run scenario)
    if cycle_store.exists():
        log.info(f"Removing previous store for cycle {cycle_tag}")
        shutil.rmtree(cycle_store)

    # Rename staging_new → cycle_tag (fast, same filesystem)
    staging.rename(cycle_store)
    log.info(f"Zarr store saved: {cycle_store}")

    # Atomically update the `current` symlink via a temporary link + os.replace()
    tmp_link = zarr_dir / "current.new"
    if tmp_link.exists() or tmp_link.is_symlink():
        tmp_link.unlink()
    os.symlink(cycle_tag, tmp_link)   # relative symlink within zarr_dir
    os.replace(tmp_link, current_link)
    log.info(f"Symlink updated: {current_link.name} → {cycle_tag}")

    return cycle_store


# ---------------------------------------------------------------------------
# Retention pruning
# ---------------------------------------------------------------------------

def prune_zarr_stores(
    zarr_dir: Path,
    current_tag: str,
    keep_days: int = ZARR_RETAIN_DAYS,
    keep_hours: tuple[int, ...] = ZARR_RETAIN_HOURS,
) -> None:
    """
    Remove old named Zarr cycle stores, keeping:

    - The current (latest) store, always.
    - Stores from the past ``keep_days`` days whose cycle hour is in
      ``keep_hours`` (default: 00Z, 06Z, 12Z, 18Z — the major NWP cycles).

    Everything else is deleted.
    """
    from datetime import timedelta
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=keep_days)
    reserved = {"current", "current.new", "staging_new", current_tag}

    for store in sorted(zarr_dir.iterdir()):
        if store.name in reserved or store.is_symlink() or not store.is_dir():
            continue
        try:
            cycle_dt = datetime.strptime(store.name, "%Y%m%d_%H").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            continue  # not a cycle store directory

        within_window = cycle_dt >= cutoff
        major_cycle   = cycle_dt.hour in keep_hours

        if within_window and major_cycle:
            continue  # keep

        log.info(f"Pruning Zarr store: {store.name}")
        shutil.rmtree(store)


def prune_staging_dirs(staging_root: Path, keep_tag: str) -> None:
    """
    Delete all GRIB2 staging directories except the most recently completed
    cycle (``keep_tag``), which is retained for debugging.
    """
    if not staging_root.exists():
        return
    for d in sorted(staging_root.iterdir()):
        if d.is_dir() and d.name != keep_tag:
            log.info(f"Pruning old staging dir: {d.name}")
            shutil.rmtree(d)


# ---------------------------------------------------------------------------
# Staging cleanup
# ---------------------------------------------------------------------------

def cleanup_staging(staging_dir: Path) -> None:
    """
    Delete the GRIB2 staging directory after a successful Zarr write.
    """
    if staging_dir.exists():
        log.info(f"Cleaning up staging directory: {staging_dir}")
        shutil.rmtree(staging_dir)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_postprocessor(
    staging_dir: Path,
    cycle_tag: Optional[str] = None,
    delete_staging: bool = True,
    zarr_staging: Optional[Path] = None,
    workers: int = POSTPROCESS_WORKERS,
) -> dict:
    """
    Run the full GRIB2 → Zarr post-processing pipeline:

    1. Load the variable registry
    2. Extract all native variables from staged GRIB2 files
    3. Write Zarr store with optimised chunking
    4. Atomic swap: rename to ``zarr/{cycle_tag}/`` and update ``current`` symlink
    5. Prune old Zarr stores (keep 7 days of 00Z/06Z/12Z/18Z cycles)
    6. Prune old GRIB2 staging dirs (keep most recent for debug)

    Parameters
    ----------
    staging_dir : Path
        Directory containing the staged GRIB2 files for one cycle.
    cycle_tag : str, optional
        Cycle identifier used to name the Zarr store (e.g. ``"20260301_16"``).
        Derived from ``staging_dir.name`` if not provided.
    delete_staging : bool
        If True (default), prune old GRIB2 staging directories, retaining
        only the most recent cycle for debugging.  The just-completed cycle's
        staging dir is always kept (it is the most recent).
    zarr_staging : Path, optional
        Temporary write path (default: ``ZARR_DIR / "staging_new"``).

    Returns
    -------
    dict
        Processing statistics (timing, sizes, variable counts).
    """
    if cycle_tag is None:
        cycle_tag = staging_dir.name
    if zarr_staging is None:
        zarr_staging = ZARR_DIR / "staging_new"

    overall_t0 = time.monotonic()

    # --- Read manifest for metadata ---
    manifest_path = staging_dir / "manifest.json"
    cycle_str = "unknown"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        cycle_str = manifest.get("cycle", cycle_str)
    log.info(f"Post-processing cycle: {cycle_str}  (tag: {cycle_tag})")
    log.info(f"  Staging dir:  {staging_dir}")
    log.info(f"  Zarr staging: {zarr_staging}")
    log.info(f"  Zarr store:   {ZARR_DIR / cycle_tag}")

    # --- Load variable registry ---
    registry = VariableRegistry(VARIABLES_YAML)
    log.info(f"  Registry: {registry}")

    # --- Extract variables ---
    extract_t0 = time.monotonic()
    ds = extract_variables(staging_dir, registry, workers=workers)
    extract_time = time.monotonic() - extract_t0
    log.info(f"Extraction complete in {extract_time:.1f}s")

    # --- Add cycle metadata as a global attribute ---
    ds.attrs["cycle"] = cycle_str
    ds.attrs["created"] = datetime.now(tz=timezone.utc).isoformat()

    # --- Write Zarr to staging location ---
    zarr_stats = write_zarr(ds, zarr_staging)

    # --- Atomic swap: staging_new → cycle_tag, update current symlink ---
    atomic_swap(zarr_staging, ZARR_DIR, cycle_tag)
    log.info(f"Live store updated: {ZARR_DIR / 'current'} → {cycle_tag}")

    # --- Prune old Zarr stores ---
    prune_zarr_stores(ZARR_DIR, current_tag=cycle_tag)

    # --- Prune old GRIB2 staging directories (keep current cycle for debug) ---
    if delete_staging:
        prune_staging_dirs(staging_dir.parent, keep_tag=cycle_tag)

    overall_time = time.monotonic() - overall_t0

    stats = {
        "cycle": cycle_str,
        "cycle_tag": cycle_tag,
        "extraction_time_s": round(extract_time, 1),
        **zarr_stats,
        "total_time_s": round(overall_time, 1),
    }

    log.info(
        f"Post-processing complete in {overall_time:.1f}s\n"
        f"  Stats: {json.dumps(stats, indent=2)}"
    )

    return stats
