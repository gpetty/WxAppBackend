"""
GET /status — return metadata about the currently-loaded slab store.

Useful for the frontend to display "forecast as of …" and for monitoring
whether the hourly ingestion pipeline is keeping up.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

from ..config import SLAB_STORE_DIR
from .models import StatusResponse

router = APIRouter()


@router.get("/status", response_model=StatusResponse)
def get_status(request: Request) -> StatusResponse:
    """Return cycle and health information for the slab ring buffer store."""
    state = request.app.state
    store = state.store

    if store is None or not store.is_ready:
        return StatusResponse(
            runtime=None,
            store_path=str(SLAB_STORE_DIR),
            n_variables=0,
            n_time_steps=0,
            last_loaded=state.last_loaded,
            available_cycles=[],
        )

    current_run = store._state.current_run()
    return StatusResponse(
        runtime=store.current_cycle_time,
        store_path=str(SLAB_STORE_DIR),
        n_variables=len(store.variable_names),
        n_time_steps=current_run.n_fxx if current_run else 0,
        last_loaded=state.last_loaded,
        available_cycles=sorted(r["cycle_tag"] for r in store.available_runs),
    )
