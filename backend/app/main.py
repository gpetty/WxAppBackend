"""
Weather Window — FastAPI application entry point.

Startup
-------
The slab ring buffer store and variable registry are loaded once during the
lifespan startup phase and stored on ``app.state``.  Every request handler
receives them via ``request.app.state``.

Reload
------
POST /admin/reload re-reads ring_state.json and evicts stale memory-mapped
slabs for any slot that was refreshed since the last reload.  The store object
itself stays open — only the ring state is re-read.  Called by the ingest
pipeline after a successful slab write.

Development
-----------
    uvicorn backend.app.main:app --reload --port 8000

Production
----------
    uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --workers 1

    Single worker only: the slab store lives in app.state and is not shared
    across processes.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .config import SLAB_STORE_DIR, VARIABLES_YAML, REPO_ROOT
from .registry import VariableRegistry
from .store import NBMStore
from .routers import forecast, variables, status

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def _open_store(app: FastAPI) -> None:
    """
    Open (or re-open) the slab store and load the lat/lon grid into app.state.

    Safe to call at startup even if the store is empty (no runs yet) or if
    lat.npy hasn't been written yet (first ingest pending).
    """
    app.state.registry = VariableRegistry(VARIABLES_YAML)

    if not (SLAB_STORE_DIR / "metadata.json").exists():
        log.warning(
            f"Slab store not initialised at {SLAB_STORE_DIR}. "
            f"Run 'python -m backend.app.store init' and then ingest first. "
            f"The /forecast endpoint will return 503 until the store is ready."
        )
        app.state.store    = None
        app.state.lat_grid = None
        app.state.lon_grid = None
        app.state.last_loaded = None
        return

    store = NBMStore.open(SLAB_STORE_DIR)

    lat_grid = lon_grid = None
    if (SLAB_STORE_DIR / "lat.npy").exists():
        lat_grid, lon_grid = store.meta.load_grid(SLAB_STORE_DIR)

    app.state.store       = store
    app.state.lat_grid    = lat_grid
    app.state.lon_grid    = lon_grid
    app.state.last_loaded = datetime.now(tz=timezone.utc)

    log.info(
        f"Slab store opened — {store.n_runs_available} run(s) available "
        f"| is_ready={store.is_ready} "
        f"| lat/lon grid={'loaded' if lat_grid is not None else 'pending'}"
    )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open the slab store at startup; nothing special on shutdown."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )
    await asyncio.to_thread(_open_store, app)
    yield
    # Slab mmaps are closed by the OS when the process exits; no explicit cleanup.


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Weather Window API",
    description=(
        "Point forecast time series from the NOAA National Blend of Models (NBM). "
        "Returns 1-hour resolution data for any CONUS location out to ~11 days."
    ),
    version="0.2.0",
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
    Reload the slab ring buffer state from disk.

    Called by the ingest script after a successful slab write.  Not
    authenticated in the prototype — restrict at the nginx level in production
    so only localhost can reach this path.
    """
    state = request.app.state

    # First-time initialisation after store was created post-startup
    if state.store is None:
        if not (SLAB_STORE_DIR / "metadata.json").exists():
            raise HTTPException(
                status_code=503,
                detail=f"Slab store not initialised at {SLAB_STORE_DIR}.",
            )
        _open_store(request.app)
        state = request.app.state
    else:
        changed = state.store.reload()
        # Load lat/lon grid if it wasn't available at startup
        if state.lat_grid is None and (SLAB_STORE_DIR / "lat.npy").exists():
            state.lat_grid, state.lon_grid = state.store.meta.load_grid(SLAB_STORE_DIR)
        state.last_loaded = datetime.now(tz=timezone.utc)
        log.info(f"Reload — state changed: {changed}")

    store = state.store
    return {
        "status":      "ok",
        "cycle":       store.current_cycle_time if store else None,
        "is_ready":    store.is_ready if store else False,
        "last_loaded": state.last_loaded.isoformat() if state.last_loaded else None,
    }


# ---------------------------------------------------------------------------
# Developer tools
# ---------------------------------------------------------------------------

_REVIEW_HTML = REPO_ROOT / "forecast_review.html"

@app.get("/review", include_in_schema=False)
def review_page():
    """Serve the forecast review tool (single-page HTML + Chart.js)."""
    return FileResponse(_REVIEW_HTML, media_type="text/html")


# ---------------------------------------------------------------------------
# Store-not-ready guard
# ---------------------------------------------------------------------------

@app.middleware("http")
async def require_store(request: Request, call_next):
    """
    Return 503 for data endpoints if the slab store is not ready.
    Pass through /variables, /status, /admin/*, and /docs regardless.
    """
    data_endpoints = {"/forecast"}
    if request.url.path in data_endpoints:
        state = request.app.state
        not_ready = (
            state.store is None
            or not state.store.is_ready
            or state.lat_grid is None
        )
        if not_ready:
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=503,
                content={"detail": "Slab store not yet available. Check /status."},
            )
    return await call_next(request)
