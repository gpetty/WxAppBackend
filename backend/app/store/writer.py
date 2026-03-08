"""
SlabWriter — assemble and write one slab file.

Each slab is a numpy array of shape (idim, jdim, kvars) in C order,
dtype uint8.  Stored as a .npy file so dtype/shape are self-describing
and memory mapping works natively with np.load(..., mmap_mode='r').

Write procedure:
  1. Assemble slab from per-variable float32 arrays.
  2. Pack each variable to uint8 using packing.pack().
  3. Write to a temporary .tmp file (np.save).
  4. Atomically rename .tmp → final path.

The rename makes each slab write visible atomically.  The ring_state.json
is only updated after ALL slabs for a run are written, so readers never
see a partially-ingested run.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from .metadata import SlabMetadata
from .packing import pack

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Path helpers (used by both writer and nbm_store)
# ---------------------------------------------------------------------------

def run_dir(store_dir: Path, slot: int) -> Path:
    return store_dir / f"run_{slot:02d}"


def slab_path(store_dir: Path, run_slot: int, fxx_idx: int) -> Path:
    return run_dir(store_dir, run_slot) / f"slab_{fxx_idx:03d}.npy"


# ---------------------------------------------------------------------------
# Public write function
# ---------------------------------------------------------------------------

def write_slab(
    store_dir:  Path,
    meta:       SlabMetadata,
    run_slot:   int,
    fxx_idx:    int,
    var_arrays: dict[str, np.ndarray],
) -> Path:
    """
    Pack and write one slab to disk.

    Parameters
    ----------
    store_dir : Path
        Root of the slab store.
    meta : SlabMetadata
        Store metadata (used for shape/dtype validation).
    run_slot : int
        Physical run slot index (0 .. n_runs-1).
    fxx_idx : int
        Forecast-time index within the run (0 .. n_fxx-1).
    var_arrays : dict
        Mapping of variable name → float32 ndarray of shape (idim, jdim).
        Variables absent from this dict are filled with the missing sentinel.

    Returns
    -------
    Path to the written slab file.
    """
    # Validate array shapes for any provided variables
    expected_shape = (meta.idim, meta.jdim)
    for name, arr in var_arrays.items():
        if arr.shape != expected_shape:
            raise ValueError(
                f"Variable '{name}': expected shape {expected_shape}, got {arr.shape}"
            )

    # Assemble (idim, jdim, kvars) uint8 slab in C order
    slab = np.full(meta.slab_shape, fill_value=255, dtype=np.uint8, order="C")
    for k, name in enumerate(meta.variable_names):
        arr = var_arrays.get(name)
        if arr is not None:
            slab[:, :, k] = pack(name, arr)

    if not slab.flags["C_CONTIGUOUS"]:
        slab = np.ascontiguousarray(slab)

    dest = slab_path(store_dir, run_slot, fxx_idx)
    dest.parent.mkdir(parents=True, exist_ok=True)

    # np.save appends .npy if not already present, so use a .npy temp name.
    tmp = dest.with_name(dest.stem + ".tmp.npy")
    np.save(tmp, slab)
    tmp.rename(dest)

    return dest
