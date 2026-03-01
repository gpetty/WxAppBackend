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
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import xarray as xr

from ..config import ZARR_DIR, VARIABLES_YAML
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
# Core extraction loop  (file-centric: one cfgrib open per GRIB2 file)
# ---------------------------------------------------------------------------

def extract_variables(
    staging_dir: Path,
    registry: VariableRegistry,
) -> xr.Dataset:
    """
    Extract all native variables from staged GRIB2 files into an xarray
    Dataset with dimensions ``(valid_time, y, x)``.

    **File-centric design:** each GRIB2 file is opened exactly once with
    ``cfgrib.open_datasets()``, and all applicable variables are extracted
    in that single pass. This is ~15x faster than the alternative of
    iterating files once per variable.

    Variables missing from a file where they are expected log a warning and
    contribute a NaN time step. xarray aligns variables with different
    valid_time arrays when the Dataset is assembled, filling NaN as needed.
    """
    import cfgrib  # noqa: F401 — imported here to defer the heavy eccodes load

    grib2_files = _list_grib2_files(staging_dir)
    if not grib2_files:
        raise FileNotFoundError(f"No GRIB2 files found in {staging_dir}")

    n_files = len(grib2_files)
    log.info(f"Found {n_files} GRIB2 files in {staging_dir}")

    native_vars = registry.native()

    # Precompute unit converters (avoid repeated registry lookups)
    converters = {
        name: get_converter(var.units_raw, var.units_out)
        for name, var in native_vars.items()
    }

    # Per-variable accumulators: list of (valid_time_ns, 2D float32 array)
    var_records: dict[str, list[tuple[np.datetime64, np.ndarray]]] = {
        name: [] for name in native_vars
    }
    # Counts for summary logging
    var_skipped: dict[str, int] = {name: 0 for name in native_vars}
    var_missing:  dict[str, int] = {name: 0 for name in native_vars}

    lat_2d: Optional[np.ndarray] = None
    lon_2d: Optional[np.ndarray] = None

    # ---- Main loop: one cfgrib open per file --------------------------------
    for i, (fxx, grib2_path) in enumerate(grib2_files.items(), start=1):

        # Which variables are expected in this file?
        expected = {
            name: var for name, var in native_vars.items()
            if _should_extract(var, fxx)
        }
        not_expected = set(native_vars) - set(expected)
        for name in not_expected:
            var_skipped[name] += 1

        if not expected:
            log.debug(f"[{i:3d}/{n_files}] fxx={fxx:03d}: no variables expected, skipping")
            continue

        log.info(f"[{i:3d}/{n_files}] fxx={fxx:03d}  {grib2_path.name}")

        datasets = _open_all_datasets(grib2_path)
        if datasets is None:
            for name in expected:
                var_missing[name] += 1
            continue

        # Capture lat/lon once from the first successfully opened file
        if lat_2d is None:
            for ds in datasets:
                for da_check in ds.data_vars.values():
                    da_ref = ds[list(ds.data_vars)[0]]
                    if "latitude" in da_ref.coords:
                        lat_2d = da_ref.coords["latitude"].values
                        lon_2d = da_ref.coords["longitude"].values
                    elif "lat" in da_ref.coords:
                        lat_2d = da_ref.coords["lat"].values
                        lon_2d = da_ref.coords["lon"].values
                    break
                if lat_2d is not None:
                    break

        # Extract each expected variable from the already-opened datasets
        for var_name, var in expected.items():
            # Variables with grib_accum_hours set (e.g. total_precipitation) have
            # multiple tp messages in the same file (QPF01, QPF06, QPF12).  cfgrib
            # picks whichever message appears first, which alternates between files
            # and produces the spurious oscillation.  Open separately with an
            # explicit stepRange filter to always get the 1-hour bucket.
            if var.grib_accum_hours is not None:
                da = _open_with_accum_filter(grib2_path, var, fxx)
            else:
                da = _find_variable_in_datasets(datasets, var)
            if da is None:
                log.debug(f"  {var_name}: not found in fxx={fxx:03d}")
                var_missing[var_name] += 1
                continue

            vt = _get_valid_time(da, var_name, fxx)
            if vt is None:
                var_missing[var_name] += 1
                continue

            converted = converters[var_name](da.values).astype(np.float32)
            var_records[var_name].append((vt, converted))

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

def atomic_swap(staging: Path, live: Path) -> None:
    """
    Atomically replace the live Zarr store with the newly-written staging one.

    Strategy: rename staging → live. If live already exists, rename it to a
    backup first, then rename staging to live, then delete the backup.
    This ensures the API never reads a partially-written store.
    """
    live.parent.mkdir(parents=True, exist_ok=True)

    backup = live.parent / f"{live.name}.backup"

    if live.exists():
        log.info(f"Swapping: {staging.name} → {live.name} (backup: {backup.name})")
        # Move current live → backup
        if backup.exists():
            shutil.rmtree(backup)
        live.rename(backup)
    else:
        log.info(f"No existing live store; moving {staging.name} → {live.name}")

    # Move staging → live
    staging.rename(live)

    # Clean up backup
    if backup.exists():
        shutil.rmtree(backup)
        log.info("Backup removed.")


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
    delete_staging: bool = True,
    zarr_staging: Optional[Path] = None,
    zarr_live: Optional[Path] = None,
) -> dict:
    """
    Run the full GRIB2 → Zarr post-processing pipeline:

    1. Load the variable registry
    2. Extract all native variables from staged GRIB2 files
    3. Write Zarr store with optimised chunking
    4. Atomic swap to make it the live store
    5. Delete staging GRIB2 files (unless delete_staging=False)

    Parameters
    ----------
    staging_dir : Path
        Directory containing the staged GRIB2 files for one cycle.
    delete_staging : bool
        If True (default), delete the GRIB2 staging directory after success.
    zarr_staging : Path, optional
        Where to write the new Zarr store (default: ZARR_DIR / "staging_new").
    zarr_live : Path, optional
        Path to the live Zarr store (default: ZARR_DIR / "current").

    Returns
    -------
    dict
        Processing statistics (timing, sizes, variable counts).
    """
    if zarr_staging is None:
        zarr_staging = ZARR_DIR / "staging_new"
    if zarr_live is None:
        zarr_live = ZARR_DIR / "current"

    overall_t0 = time.monotonic()

    # --- Read manifest for metadata ---
    manifest_path = staging_dir / "manifest.json"
    cycle_str = "unknown"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        cycle_str = manifest.get("cycle", cycle_str)
    log.info(f"Post-processing cycle: {cycle_str}")
    log.info(f"  Staging dir:  {staging_dir}")
    log.info(f"  Zarr output:  {zarr_staging}")
    log.info(f"  Zarr live:    {zarr_live}")

    # --- Load variable registry ---
    registry = VariableRegistry(VARIABLES_YAML)
    log.info(f"  Registry: {registry}")

    # --- Extract variables ---
    extract_t0 = time.monotonic()
    ds = extract_variables(staging_dir, registry)
    extract_time = time.monotonic() - extract_t0
    log.info(f"Extraction complete in {extract_time:.1f}s")

    # --- Add cycle metadata as a global attribute ---
    ds.attrs["cycle"] = cycle_str
    ds.attrs["created"] = datetime.now(tz=timezone.utc).isoformat()

    # --- Write Zarr ---
    zarr_stats = write_zarr(ds, zarr_staging)

    # --- Atomic swap ---
    atomic_swap(zarr_staging, zarr_live)
    log.info(f"Live Zarr store updated: {zarr_live}")

    # --- Cleanup ---
    if delete_staging:
        cleanup_staging(staging_dir)

    overall_time = time.monotonic() - overall_t0

    stats = {
        "cycle": cycle_str,
        "extraction_time_s": round(extract_time, 1),
        **zarr_stats,
        "total_time_s": round(overall_time, 1),
    }

    log.info(
        f"Post-processing complete in {overall_time:.1f}s\n"
        f"  Stats: {json.dumps(stats, indent=2)}"
    )

    return stats
