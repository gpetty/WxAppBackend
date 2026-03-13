"""
write_ndfd_slab — pack and write one NDFD slab file.

Parallel to writer.py (NBM) but uses NDFD packing from packing_ndfd.py.
Reuses slab_path() and run_dir() helpers from writer.py since those are
path-only utilities with no variable-specific coupling.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from .metadata_ndfd import NDFDSlabMetadata
from .packing_ndfd import ndfd_pack
from .writer import slab_path, run_dir   # path helpers are generic — safe to reuse

log = logging.getLogger(__name__)


def write_ndfd_slab(
    store_dir:  Path,
    meta:       NDFDSlabMetadata,
    run_slot:   int,
    fxx_idx:    int,
    var_arrays: dict[str, np.ndarray],
) -> Path:
    """
    Pack and write one NDFD slab to disk.

    Parameters
    ----------
    store_dir : Path
        Root of the NDFD slab store.
    meta : NDFDSlabMetadata
        Store metadata (used for shape/dtype validation).
    run_slot : int
        Physical run slot index (0 .. n_runs-1).
    fxx_idx : int
        Forecast-time index within the run (0 .. n_fxx-1).
    var_arrays : dict
        Mapping variable name → float32 ndarray of shape (idim, jdim).
        Variables absent from this dict are filled with the missing sentinel (255).

    Returns
    -------
    Path to the written slab file.
    """
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
            slab[:, :, k] = ndfd_pack(name, arr)

    if not slab.flags["C_CONTIGUOUS"]:
        slab = np.ascontiguousarray(slab)

    dest = slab_path(store_dir, run_slot, fxx_idx)
    dest.parent.mkdir(parents=True, exist_ok=True)

    tmp = dest.with_name(dest.stem + ".tmp.npy")
    np.save(tmp, slab)
    tmp.rename(dest)

    return dest
