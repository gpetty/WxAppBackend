"""
GET /status — return metadata about the currently-loaded Zarr store.

Useful for the frontend to display "forecast as of …" and for monitoring
whether the hourly ingestion pipeline is keeping up.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

from .models import StatusResponse

router = APIRouter()


@router.get("/status", response_model=StatusResponse)
def get_status(request: Request) -> StatusResponse:
    """Return cycle and health information for the current Zarr store."""
    state = request.app.state

    ds        = state.ds
    n_vars    = len(ds.data_vars)
    n_steps   = int(ds.sizes.get("valid_time", 0))
    cycle     = ds.attrs.get("cycle", None)

    return StatusResponse(
        runtime=cycle,
        store_path=str(state.zarr_path),
        n_variables=n_vars,
        n_time_steps=n_steps,
        last_loaded=state.last_loaded,
    )
