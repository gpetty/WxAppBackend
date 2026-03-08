"""
NBMStore — memory-mapped slab store for point time-series queries.

The store keeps a lazy cache of memory-mapped .npy file handles.
Mmaps are opened on first access and retained until the slot is refreshed
by a new ingest run.  On reload(), only the updated slot's mmaps are closed
and reopened — all other slots remain mapped.

Typical API usage:

    store = NBMStore.open(store_dir)

    # Current forecast for a grid point:
    values, valid_times = store.get_point_timeseries(i, j)

    # Specific historical run (for drift queries):
    values, valid_times = store.get_point_timeseries(i, j, cycle_tag="20260307_12")

    # After ingest signals a reload:
    store.reload()
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np

from .metadata import SlabMetadata
from .ring_state import RingState, RunRecord
from .packing import unpack
from .writer import slab_path

log = logging.getLogger(__name__)


class NBMStore:

    def __init__(self, store_dir: Path, meta: SlabMetadata, state: RingState) -> None:
        self._store_dir = store_dir
        self.meta = meta
        self._state = state
        # Lazy mmap cache: (run_slot, fxx_idx) → np.ndarray (mmap'd, shape slab_shape)
        self._mmaps: dict[tuple[int, int], np.ndarray] = {}

    # -----------------------------------------------------------------------
    # Construction / reload
    # -----------------------------------------------------------------------

    @classmethod
    def open(cls, store_dir: Path) -> NBMStore:
        """Open an existing slab store."""
        if not (store_dir / "metadata.json").exists():
            raise FileNotFoundError(
                f"No slab store found at {store_dir}. "
                f"Run 'python -m backend.app.store init' first."
            )
        meta = SlabMetadata.load(store_dir)
        meta.validate()
        state = RingState.load(store_dir)
        return cls(store_dir, meta, state)

    def reload(self) -> bool:
        """
        Re-read ring_state.json and refresh mmaps for any changed slots.

        Called by the API admin endpoint after the ingest pipeline signals
        that a new run has been committed.

        Returns True if the state changed (a new run was published).
        """
        new_state = RingState.load(self._store_dir)
        if new_state.updated == self._state.updated:
            return False

        # Find slots whose cycle_tag changed (newly written runs)
        changed_slots: set[int] = set()
        for new_run in new_state.runs:
            old_run = self._state.run_by_slot(new_run.slot)
            if old_run is None or old_run.cycle_tag != new_run.cycle_tag:
                changed_slots.add(new_run.slot)

        # Evict stale mmaps for changed slots
        stale = [k for k in self._mmaps if k[0] in changed_slots]
        for k in stale:
            del self._mmaps[k]

        self._state = new_state
        log.info(
            f"NBMStore reloaded — {len(self._state.runs)} runs, "
            f"{len(changed_slots)} slot(s) refreshed, "
            f"{len(self._mmaps)} mmaps retained"
        )
        return True

    # -----------------------------------------------------------------------
    # Core query
    # -----------------------------------------------------------------------

    def get_point_timeseries(
        self,
        i:            int,
        j:            int,
        cycle_tag:    Optional[str] = None,
        unpack_values: bool = True,
    ) -> tuple[np.ndarray, list[str]]:
        """
        Return the full forecast time series for grid point (i, j).

        Parameters
        ----------
        i, j : int
            Grid indices (row, col) in the NBM CONUS grid.
        cycle_tag : str, optional
            Cycle identifier (e.g. "20260307_12") to query a specific
            historical run.  If None, the current (newest) run is used.
        unpack_values : bool
            If True (default), return float32 physical values.
            If False, return raw uint8 packed values (for diagnostics).

        Returns
        -------
        values : ndarray, shape (kvars, n_fxx)
            float32 physical values or uint8 packed values, depending on
            unpack_values.  Rows are variables in VARIABLE_ORDER;
            columns are forecast time steps in fxx order.
        valid_times : list[str]
            ISO-8601 UTC strings, one per forecast time step.

        Raises
        ------
        IndexError  : (i, j) out of grid bounds.
        KeyError    : requested cycle_tag not found in the ring buffer.
        RuntimeError: store is empty (no runs ingested yet).
        """
        self._check_bounds(i, j)
        run = self._resolve_run(cycle_tag)

        n = run.n_fxx
        raw = np.empty((self.meta.kvars, n), dtype=np.uint8)

        for fxx_idx in range(n):
            mm = self._get_mmap(run.slot, fxx_idx)
            if mm is None:
                raw[:, fxx_idx] = 255   # missing sentinel for unreadable slab
            else:
                raw[:, fxx_idx] = mm[i, j, :]

        if not unpack_values:
            return raw, run.valid_times

        out = np.empty((self.meta.kvars, n), dtype=np.float32)
        for k, name in enumerate(self.meta.variable_names):
            out[k] = unpack(name, raw[k])

        return out, run.valid_times

    # -----------------------------------------------------------------------
    # Convenience properties for the API layer
    # -----------------------------------------------------------------------

    @property
    def is_ready(self) -> bool:
        return self._state.current_run() is not None

    @property
    def current_cycle_tag(self) -> Optional[str]:
        run = self._state.current_run()
        return run.cycle_tag if run else None

    @property
    def current_cycle_time(self) -> Optional[str]:
        run = self._state.current_run()
        return run.cycle_time if run else None

    @property
    def n_runs_available(self) -> int:
        return len(self._state.runs)

    @property
    def available_runs(self) -> list[dict]:
        """Sorted newest-first list of {cycle_tag, cycle_time, n_fxx, slot}."""
        return [
            {
                "cycle_tag":  r.cycle_tag,
                "cycle_time": r.cycle_time,
                "n_fxx":      r.n_fxx,
                "slot":       r.slot,
            }
            for r in self._state.runs_newest_first()
        ]

    @property
    def variable_names(self) -> tuple[str, ...]:
        return self.meta.variable_names

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _check_bounds(self, i: int, j: int) -> None:
        if not (0 <= i < self.meta.idim and 0 <= j < self.meta.jdim):
            raise IndexError(
                f"Grid index ({i}, {j}) out of bounds "
                f"(grid is {self.meta.idim} × {self.meta.jdim})"
            )

    def _resolve_run(self, cycle_tag: Optional[str]) -> RunRecord:
        if cycle_tag is None:
            run = self._state.current_run()
            if run is None:
                raise RuntimeError("NBMStore has no ingested runs yet.")
            return run
        run = self._state.run_by_tag(cycle_tag)
        if run is None:
            available = [r.cycle_tag for r in self._state.runs_newest_first()]
            raise KeyError(
                f"Cycle '{cycle_tag}' not in ring buffer. "
                f"Available: {available}"
            )
        return run

    def _get_mmap(self, run_slot: int, fxx_idx: int) -> Optional[np.ndarray]:
        """Return a cached memory-mapped slab array, opening it on first access."""
        key = (run_slot, fxx_idx)
        if key not in self._mmaps:
            path = slab_path(self._store_dir, run_slot, fxx_idx)
            if path.exists():
                self._mmaps[key] = np.load(path, mmap_mode="r")
            else:
                return None
        return self._mmaps[key]
