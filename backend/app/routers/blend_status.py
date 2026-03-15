"""
GET /status — blend service health and cycle information.

Reports the current state of both the NBM and NDFD slab stores.
Either store may be None (not yet initialised); the blend service operates
in degraded mode when one source is unavailable.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Request

from .blend_models import BlendStatusResponse

router = APIRouter()


@router.get("/status", response_model=BlendStatusResponse)
def get_blend_status(request: Request) -> BlendStatusResponse:
    """
    Return cycle and health information for both the NBM and NDFD slab stores.

    Either store may report as not ready (e.g. first ingest not yet complete).
    The blend /forecast endpoint degrades gracefully, serving whatever sources
    are available.
    """
    state      = request.app.state
    nbm_store  = state.nbm_store
    ndfd_store = state.ndfd_store

    nbm_ready  = nbm_store  is not None and nbm_store.is_ready
    ndfd_ready = ndfd_store is not None and ndfd_store.is_ready

    return BlendStatusResponse(
        nbm_runtime           = nbm_store.current_cycle_time  if nbm_ready  else None,
        ndfd_runtime          = ndfd_store.current_cycle_time if ndfd_ready else None,
        nbm_available_cycles  = (sorted(r["cycle_tag"] for r in nbm_store.available_runs)
                                 if nbm_ready else []),
        ndfd_available_cycles = (sorted(r["cycle_tag"] for r in ndfd_store.available_runs)
                                 if ndfd_ready else []),
        nbm_store_path        = str(state.nbm_store_dir),
        ndfd_store_path       = str(state.ndfd_store_dir),
        nbm_n_variables       = len(nbm_store.variable_names)  if nbm_ready  else 0,
        ndfd_n_variables      = len(ndfd_store.variable_names) if ndfd_ready else 0,
        nbm_ready             = nbm_ready,
        ndfd_ready            = ndfd_ready,
        last_loaded           = state.last_loaded,
    )
