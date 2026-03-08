"""
CLI for managing the NBM slab ring buffer store.

Commands
--------
init    — create a new store directory with metadata.json and run slots
status  — show ring state (which cycles are retained)
inspect — dump the packed values at a specific grid point from a given run
packing — print the packing parameter table

Usage examples::

    python -m backend.app.store init   --store-dir /12TB2/NBM/slabs --n-runs 28
    python -m backend.app.store status --store-dir /12TB2/NBM/slabs
    python -m backend.app.store inspect --store-dir /12TB2/NBM/slabs --i 800 --j 1200
    python -m backend.app.store packing
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np


def cmd_init(args: argparse.Namespace) -> None:
    from .metadata   import SlabMetadata
    from .ring_state import RingState
    from ..config    import SLAB_N_FXX

    store_dir = Path(args.store_dir)
    store_dir.mkdir(parents=True, exist_ok=True)

    meta = SlabMetadata.create(
        idim   = 1597,
        jdim   = 2345,
        n_runs = args.n_runs,
        n_fxx  = SLAB_N_FXX,
    )
    meta.validate()
    meta.save(store_dir)

    state = RingState.empty()
    state.save(store_dir)

    for slot in range(args.n_runs):
        (store_dir / f"run_{slot:02d}").mkdir(exist_ok=True)

    slab_mb  = meta.slab_nbytes / 1e6
    total_gb = slab_mb * args.n_runs * meta.n_fxx / 1e3

    print(f"Initialized slab store at {store_dir}")
    print(f"  Grid       : {meta.idim} × {meta.jdim}")
    print(f"  Variables  : {meta.kvars}  ({', '.join(meta.variable_names)})")
    print(f"  n_runs     : {meta.n_runs}")
    print(f"  n_fxx      : {meta.n_fxx}")
    print(f"  Slab size  : {slab_mb:.1f} MB")
    print(f"  Max store  : {total_gb:.1f} GB ({args.n_runs} runs × {meta.n_fxx} slabs)")


def cmd_status(args: argparse.Namespace) -> None:
    from .metadata   import SlabMetadata
    from .ring_state import RingState

    store_dir = Path(args.store_dir)
    meta  = SlabMetadata.load(store_dir)
    state = RingState.load(store_dir)

    print(f"Store     : {store_dir}")
    print(f"Grid      : {meta.idim} × {meta.jdim}  |  {meta.kvars} variables")
    print(f"Capacity  : {meta.n_runs} runs × {meta.n_fxx} slabs")
    print(f"Runs held : {len(state.runs)} / {meta.n_runs}")
    print(f"Updated   : {state.updated}")
    print()
    for run in state.runs_newest_first():
        current = "  ← current" if run == state.current_run() else ""
        t0 = run.valid_times[0]  if run.valid_times else "?"
        tn = run.valid_times[-1] if run.valid_times else "?"
        print(
            f"  slot={run.slot:02d}  {run.cycle_tag:16s}  "
            f"{run.n_fxx:3d} slabs  {t0} … {tn}{current}"
        )


def cmd_inspect(args: argparse.Namespace) -> None:
    from .nbm_store import NBMStore

    store  = NBMStore.open(Path(args.store_dir))
    tag    = args.cycle_tag or None
    values, vtimes = store.get_point_timeseries(
        args.i, args.j, cycle_tag=tag, unpack_values=True
    )

    print(f"Point ({args.i}, {args.j})  —  "
          f"cycle: {tag or store.current_cycle_tag}  —  "
          f"{values.shape[1]} time steps")
    print()
    header = f"{'valid_time':28s}" + "".join(
        f"{n:>10s}" for n in store.variable_names
    )
    print(header)
    print("-" * len(header))
    for t_idx in range(values.shape[1]):
        row = f"{vtimes[t_idx]:28s}" + "".join(
            f"{values[k, t_idx]:10.2f}" for k in range(store.meta.kvars)
        )
        print(row)


def cmd_packing(_args: argparse.Namespace) -> None:
    from .packing import packing_table

    rows = packing_table()
    print(f"{'k':>3}  {'variable':<28}  {'scale':>8}  {'offset':>8}  {'missing_unpacked'}")
    print("-" * 70)
    for r in rows:
        print(
            f"{r['k']:>3}  {r['variable']:<28}  "
            f"{r['scale_factor']:>8.4f}  {r['add_offset']:>8.1f}  "
            f"{r['unpack_missing']}"
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m backend.app.store",
        description="NBM slab ring buffer — store management CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init", help="Initialize a new store")
    p.add_argument("--store-dir", required=True)
    p.add_argument("--n-runs", type=int, default=28,
                   help="Number of forecast runs to retain (default: 28)")

    p = sub.add_parser("status", help="Show ring buffer state")
    p.add_argument("--store-dir", required=True)

    p = sub.add_parser("inspect", help="Dump time series for a grid point")
    p.add_argument("--store-dir", required=True)
    p.add_argument("--i", type=int, required=True, help="Grid row index")
    p.add_argument("--j", type=int, required=True, help="Grid col index")
    p.add_argument("--cycle-tag", default=None,
                   help="Cycle tag to inspect (default: current)")

    sub.add_parser("packing", help="Print packing parameter table")

    args = parser.parse_args()
    {
        "init":    cmd_init,
        "status":  cmd_status,
        "inspect": cmd_inspect,
        "packing": cmd_packing,
    }[args.command](args)


if __name__ == "__main__":
    main()
