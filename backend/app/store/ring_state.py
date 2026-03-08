"""
RingState — dynamic ring buffer state, persisted as ring_state.json.

Updated atomically (write to ring_state.json.new, then os.replace) after
every successful ingest run.  Readers always see either the old or the new
complete state — never a partial update.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RunRecord:
    slot:        int          # physical slot index 0 .. n_runs-1
    cycle_tag:   str          # e.g. "20260307_19"
    cycle_time:  str          # ISO-8601 UTC of forecast reference time
    n_fxx:       int          # number of valid slab files written for this run
    valid_times: list[str]    # ISO-8601 UTC, one entry per slab (length n_fxx)


@dataclass
class RingState:
    runs:    list[RunRecord]  # all occupied slots, in arbitrary order
    updated: str              # ISO-8601 UTC of last ring_state.json write

    # -----------------------------------------------------------------------
    # Factory / persistence
    # -----------------------------------------------------------------------

    @classmethod
    def empty(cls) -> RingState:
        return cls(runs=[], updated=_utcnow())

    @classmethod
    def load(cls, store_dir: Path) -> RingState:
        path = store_dir / "ring_state.json"
        if not path.exists():
            return cls.empty()
        d = json.loads(path.read_text())
        runs = [RunRecord(**r) for r in d["runs"]]
        return cls(runs=runs, updated=d.get("updated", ""))

    def save(self, store_dir: Path) -> None:
        """Atomically replace ring_state.json."""
        self.updated = _utcnow()
        d = {
            "runs":    [vars(r) for r in self.runs],
            "updated": self.updated,
        }
        tmp = store_dir / "ring_state.json.new"
        tmp.write_text(json.dumps(d, indent=2))
        os.replace(tmp, store_dir / "ring_state.json")

    # -----------------------------------------------------------------------
    # Ring buffer logic
    # -----------------------------------------------------------------------

    def next_slot(self, n_runs: int) -> int:
        """
        Return the physical slot index to write for the next ingest run.

        Fills unoccupied slots first (store grows from empty).
        Once all n_runs slots are occupied, overwrites the oldest run.
        """
        occupied = {r.slot for r in self.runs}
        for s in range(n_runs):
            if s not in occupied:
                return s
        oldest = min(self.runs, key=lambda r: r.cycle_time)
        return oldest.slot

    def add_run(self, record: RunRecord, n_runs: int) -> None:
        """
        Register a newly completed run.

        Replaces any existing record for the same slot, then trims the list
        to at most n_runs entries (keeping the most recent by cycle_time).
        """
        self.runs = [r for r in self.runs if r.slot != record.slot]
        self.runs.append(record)
        if len(self.runs) > n_runs:
            self.runs.sort(key=lambda r: r.cycle_time)
            self.runs = self.runs[-n_runs:]

    # -----------------------------------------------------------------------
    # Query helpers
    # -----------------------------------------------------------------------

    def current_run(self) -> Optional[RunRecord]:
        """Return the most recent run record, or None if the store is empty."""
        return max(self.runs, key=lambda r: r.cycle_time) if self.runs else None

    def runs_newest_first(self) -> list[RunRecord]:
        return sorted(self.runs, key=lambda r: r.cycle_time, reverse=True)

    def run_by_slot(self, slot: int) -> Optional[RunRecord]:
        return next((r for r in self.runs if r.slot == slot), None)

    def run_by_tag(self, cycle_tag: str) -> Optional[RunRecord]:
        return next((r for r in self.runs if r.cycle_tag == cycle_tag), None)

    def validate(self, n_runs: int) -> None:
        """Raise ValueError if ring state is internally inconsistent."""
        slots = [r.slot for r in self.runs]
        if len(slots) != len(set(slots)):
            raise ValueError(f"Duplicate slot indices in ring state: {slots}")
        for r in self.runs:
            if not (0 <= r.slot < n_runs):
                raise ValueError(
                    f"Slot {r.slot} out of range for n_runs={n_runs}"
                )
            if len(r.valid_times) != r.n_fxx:
                raise ValueError(
                    f"Run {r.cycle_tag}: n_fxx={r.n_fxx} but "
                    f"len(valid_times)={len(r.valid_times)}"
                )


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _utcnow() -> str:
    return datetime.now(tz=timezone.utc).isoformat()
