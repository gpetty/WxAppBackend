"""
SlabMetadata — static store configuration, persisted as metadata.json.

Written once when the store is initialized. Never modified while the store
is in service (changing kvars, idim, jdim, or variable order requires a
fresh store and re-ingestion).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .packing import VARIABLE_ORDER, KVARS, PACKING, packing_table


@dataclass
class SlabMetadata:
    idim:           int           # spatial rows  (y)
    jdim:           int           # spatial cols  (x)
    kvars:          int           # number of stored variables (= KVARS = 15)
    n_runs:         int           # outer ring buffer size (number of retained cycles)
    n_fxx:          int           # inner ring buffer size (forecast time steps per cycle)
    variable_names: tuple[str, ...] = tuple(VARIABLE_ORDER)

    # -----------------------------------------------------------------------
    # Factory
    # -----------------------------------------------------------------------

    @classmethod
    def create(cls, idim: int, jdim: int, n_runs: int, n_fxx: int) -> SlabMetadata:
        return cls(
            idim=idim,
            jdim=jdim,
            kvars=KVARS,
            n_runs=n_runs,
            n_fxx=n_fxx,
            variable_names=tuple(VARIABLE_ORDER),
        )

    # -----------------------------------------------------------------------
    # Serialization
    # -----------------------------------------------------------------------

    def save(self, store_dir: Path) -> None:
        """Write metadata.json to store_dir."""
        d = {
            "idim":           self.idim,
            "jdim":           self.jdim,
            "kvars":          self.kvars,
            "n_runs":         self.n_runs,
            "n_fxx":          self.n_fxx,
            "variable_names": list(self.variable_names),
            "slab_dtype":     "uint8",
            "slab_shape":     [self.idim, self.jdim, self.kvars],
            "memory_order":   "C",
            "packing":        packing_table(),
        }
        (store_dir / "metadata.json").write_text(json.dumps(d, indent=2))

    @classmethod
    def load(cls, store_dir: Path) -> SlabMetadata:
        d = json.loads((store_dir / "metadata.json").read_text())
        return cls(
            idim=d["idim"],
            jdim=d["jdim"],
            kvars=d["kvars"],
            n_runs=d["n_runs"],
            n_fxx=d["n_fxx"],
            variable_names=tuple(d["variable_names"]),
        )

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    @property
    def slab_shape(self) -> tuple[int, int, int]:
        """Shape of one slab array: (idim, jdim, kvars)."""
        return (self.idim, self.jdim, self.kvars)

    @property
    def slab_nbytes(self) -> int:
        """Uncompressed size of one slab in bytes (uint8 → 1 byte/element)."""
        return self.idim * self.jdim * self.kvars

    def var_index(self, name: str) -> int:
        return list(self.variable_names).index(name)

    def validate(self) -> None:
        """Raise ValueError if metadata is internally inconsistent."""
        if self.kvars != len(self.variable_names):
            raise ValueError(
                f"kvars={self.kvars} does not match "
                f"len(variable_names)={len(self.variable_names)}"
            )
        if self.kvars != KVARS:
            raise ValueError(
                f"kvars={self.kvars} does not match expected KVARS={KVARS}"
            )
        unknown = set(self.variable_names) - set(VARIABLE_ORDER)
        if unknown:
            raise ValueError(f"Unknown variables in metadata: {unknown}")
        if self.idim <= 0 or self.jdim <= 0:
            raise ValueError(f"Invalid grid dimensions: {self.idim}×{self.jdim}")
        if self.n_runs <= 0 or self.n_fxx <= 0:
            raise ValueError(f"Invalid ring sizes: n_runs={self.n_runs}, n_fxx={self.n_fxx}")

    # -----------------------------------------------------------------------
    # Grid coordinates (optional, stored as lat.npy / lon.npy)
    # -----------------------------------------------------------------------

    def save_grid(self, store_dir: Path, lat: np.ndarray, lon: np.ndarray) -> None:
        """Save 2-D lat/lon arrays alongside the store metadata."""
        if lat.shape != (self.idim, self.jdim) or lon.shape != (self.idim, self.jdim):
            raise ValueError(
                f"Grid arrays must have shape ({self.idim}, {self.jdim}); "
                f"got lat={lat.shape}, lon={lon.shape}"
            )
        np.save(store_dir / "lat.npy", lat.astype(np.float32))
        np.save(store_dir / "lon.npy", lon.astype(np.float32))

    def load_grid(self, store_dir: Path) -> tuple[np.ndarray, np.ndarray]:
        """Load and return (lat, lon) float32 arrays of shape (idim, jdim)."""
        lat_path = store_dir / "lat.npy"
        lon_path = store_dir / "lon.npy"
        if not lat_path.exists():
            raise FileNotFoundError(
                f"Grid coordinate files not found in {store_dir}. "
                f"Run ingest at least once to populate lat.npy / lon.npy."
            )
        return np.load(lat_path), np.load(lon_path)
