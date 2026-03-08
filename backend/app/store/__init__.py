"""
NBM slab ring buffer storage layer.

Public API
----------
SlabMetadata   — static store configuration (metadata.json)
RingState      — dynamic ring buffer state  (ring_state.json)
SlabRingBuffer — manages run slot allocation and publication
write_slab     — pack and write one (run, fxx) slab to disk
NBMStore       — memory-mapped point query interface

Typical ingest usage:

    from backend.app.store import SlabRingBuffer, write_slab

    ring = SlabRingBuffer.open(store_dir)
    slot = ring.begin_run()
    valid_times = []

    for fxx_idx, (valid_time, var_arrays) in enumerate(extracted_slabs):
        write_slab(store_dir, ring.meta, slot, fxx_idx, var_arrays)
        valid_times.append(valid_time)

    ring.commit_run(slot, cycle_tag, cycle_time, valid_times)

Typical API usage:

    from backend.app.store import NBMStore

    store = NBMStore.open(store_dir)
    values, valid_times = store.get_point_timeseries(i, j)
"""

from .metadata    import SlabMetadata
from .ring_state  import RingState, RunRecord
from .ring_buffer import SlabRingBuffer
from .writer      import write_slab, slab_path, run_dir
from .nbm_store   import NBMStore
from .packing     import VARIABLE_ORDER, KVARS, PACKING, pack, unpack

__all__ = [
    "SlabMetadata",
    "RingState",
    "RunRecord",
    "SlabRingBuffer",
    "write_slab",
    "slab_path",
    "run_dir",
    "NBMStore",
    "VARIABLE_ORDER",
    "KVARS",
    "PACKING",
    "pack",
    "unpack",
]
