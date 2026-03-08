"""
SlabRingBuffer — manages run slot allocation and atomic publication.

Typical ingest usage (one complete cycle):

    ring = SlabRingBuffer.open(store_dir)
    slot = ring.begin_run(cycle_tag, cycle_time)

    valid_times = []
    for fxx_idx, (fxx, grib2_path) in enumerate(sorted_files):
        var_arrays = extract_one_file(grib2_path)       # → {name: float32 array}
        write_slab(store_dir, ring.meta, slot, fxx_idx, var_arrays)
        valid_times.append(valid_time_for_this_fxx)

    ring.commit_run(slot, cycle_tag, cycle_time, valid_times)

The ring_state.json is updated atomically only in commit_run(), so readers
never see a partially-written run.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .metadata import SlabMetadata
from .ring_state import RingState, RunRecord

log = logging.getLogger(__name__)


class SlabRingBuffer:
    """
    Manages the outer (per-run) ring buffer.

    Does not write slab data directly — callers use write_slab() for that.
    Responsible only for slot allocation and publishing ring state.
    """

    def __init__(self, store_dir: Path, meta: SlabMetadata) -> None:
        self._store_dir = store_dir
        self.meta = meta

    @classmethod
    def open(cls, store_dir: Path) -> SlabRingBuffer:
        meta = SlabMetadata.load(store_dir)
        meta.validate()
        return cls(store_dir, meta)

    # -----------------------------------------------------------------------
    # Ingest interface
    # -----------------------------------------------------------------------

    def begin_run(self) -> int:
        """
        Reserve a physical slot for the next ingest run.

        Reads the current ring state to determine which slot is oldest
        (or unoccupied) and returns its index.  The slot is not locked —
        only one ingest process runs at a time (oneshot systemd service).

        Returns
        -------
        int : physical slot index to write into.
        """
        state = RingState.load(self._store_dir)
        slot = state.next_slot(self.meta.n_runs)
        log.info(f"SlabRingBuffer: reserved slot {slot:02d} for next run")
        return slot

    def commit_run(
        self,
        slot:        int,
        cycle_tag:   str,
        cycle_time:  str,
        valid_times: list[str],
    ) -> None:
        """
        Atomically publish a completed run.

        Must be called only after ALL slab files for the run have been
        fully written and flushed to disk.  Reloads ring state from disk
        before updating to avoid races with any concurrent metadata reads.

        Parameters
        ----------
        slot : int
            Physical slot index returned by begin_run().
        cycle_tag : str
            Cycle identifier, e.g. "20260307_19".
        cycle_time : str
            ISO-8601 UTC of the forecast reference time.
        valid_times : list[str]
            One ISO-8601 UTC string per slab written, in fxx order.
        """
        record = RunRecord(
            slot=slot,
            cycle_tag=cycle_tag,
            cycle_time=cycle_time,
            n_fxx=len(valid_times),
            valid_times=valid_times,
        )
        state = RingState.load(self._store_dir)
        state.add_run(record, self.meta.n_runs)
        state.save(self._store_dir)
        log.info(
            f"SlabRingBuffer: committed {cycle_tag} to slot {slot:02d} "
            f"({len(valid_times)} slabs | {len(state.runs)} runs retained)"
        )

    # -----------------------------------------------------------------------
    # Inspection
    # -----------------------------------------------------------------------

    def current_state(self) -> RingState:
        return RingState.load(self._store_dir)
