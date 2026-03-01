"""
Weather Window — FastAPI application entry point.

Startup
-------
The Zarr store and variable registry are loaded once during the lifespan
startup phase and stored on ``app.state``.  Every request handler receives
them via ``request.app.state`` — no repeated file I/O per request.

Reload
------
When the ingestion pipeline writes a new Zarr store, it can trigger a reload
by calling ``POST /admin/reload``.  This is an internal endpoint (no auth in
prototype; restrict in production via nginx allow/deny rules).

Development
-----------
    uvicorn backend.app.main:app --reload --port 8000

Production
----------
    uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --workers 1

    Single worker only: the Zarr store lives in app.state and is not shared
    across processes.  Multi-worker setups would need a shared memory or
    reload broadcast mechanism — defer until needed.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from .config import ZARR_DIR, VARIABLES_YAML
from .registry import VariableRegistry
from .extraction import open_zarr_store
from .routers import forecast, variables, status

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ZARR_LIVE: Path = ZARR_DIR / "current"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_state(app: FastAPI) -> None:
    """
    (Re)load the Zarr store and registry into app.state.

    Called once at startup and again on POST /admin/reload.
    Raises FileNotFoundError if the Zarr store does not exist yet.
    """
    registry = VariableRegistry(VARIABLES_YAML)
    ds = open_zarr_store(ZARR_LIVE)

    app.state.ds          = ds
    app.state.registry    = registry
    app.state.zarr_path   = ZARR_LIVE
    app.state.last_loaded = datetime.now(tz=timezone.utc)

    log.info(
        f"State loaded — cycle: {ds.attrs.get('cycle', 'unknown')}  "
        f"| {len(ds.data_vars)} variables  "
        f"| {ds.sizes.get('valid_time', '?')} time steps"
    )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load the Zarr store at startup; nothing special on shutdown."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )

    if not ZARR_LIVE.exists():
        log.warning(
            f"Zarr store not found at {ZARR_LIVE}. "
            f"Run the ingestion + post-processor first, then restart the API. "
            f"The /forecast endpoint will return 503 until the store is available."
        )
        app.state.ds          = None
        app.state.registry    = VariableRegistry(VARIABLES_YAML)
        app.state.zarr_path   = ZARR_LIVE
        app.state.last_loaded = None
    else:
        _load_state(app)

    yield
    # Nothing to clean up on shutdown — xarray/zarr handles its own file handles.


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Weather Window API",
    description=(
        "Point forecast time series from the NOAA National Blend of Models (NBM). "
        "Returns 1-hour resolution data for any CONUS location out to ~11 days."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — allow all origins during prototype; restrict in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(forecast.router)
app.include_router(variables.router)
app.include_router(status.router)


# ---------------------------------------------------------------------------
# Internal admin endpoint
# ---------------------------------------------------------------------------

@app.post("/admin/reload", include_in_schema=False)
def admin_reload(request: Request):
    """
    Reload the Zarr store from disk.

    Called by the ingestion script after a successful atomic swap.  Not
    authenticated in the prototype — restrict at the nginx level in production
    so only localhost can reach this path.
    """
    if not ZARR_LIVE.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Zarr store not found at {ZARR_LIVE}. Run ingestion first.",
        )
    try:
        _load_state(request.app)
        return {
            "status":      "ok",
            "cycle":       request.app.state.ds.attrs.get("cycle", "unknown"),
            "last_loaded": request.app.state.last_loaded.isoformat(),
        }
    except Exception as exc:
        log.exception("Reload failed")
        raise HTTPException(status_code=500, detail=f"Reload failed: {exc}")


# ---------------------------------------------------------------------------
# Store-not-ready guard
# ---------------------------------------------------------------------------

@app.middleware("http")
async def require_store(request: Request, call_next):
    """
    Return 503 for data endpoints if the Zarr store hasn't been loaded yet.
    Pass through /variables, /status, /admin/*, and /docs regardless.
    """
    data_endpoints = {"/forecast"}
    if request.url.path in data_endpoints and request.app.state.ds is None:
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=503,
            content={"detail": "Zarr store not yet available. Check /status."},
        )
    return await call_next(request)
