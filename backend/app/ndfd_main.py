"""
Weather Window NDFD API — FastAPI application entry point.

Parallel to main.py (NBM) but serves the NDFD slab store on port 8001.
Uses the same routers (forecast, variables, status) which are duck-typed
and work with any store that implements the NBMStore interface.

Development:
    uvicorn backend.app.ndfd_main:app --reload --port 8001

Production:
    uvicorn backend.app.ndfd_main:app --host 0.0.0.0 --port 8001 --workers 1

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

from .config_ndfd import NDFD_SLAB_STORE_DIR, NDFD_VARIABLES_YAML
from .config import REPO_ROOT
from .registry import VariableRegistry
from .store.ndfd_store import NDFDStore
from .routers import forecast, variables, status

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def _open_store(app: FastAPI) -> None:
    """Open (or re-open) the NDFD slab store and load the lat/lon grid."""
    app.state.registry = VariableRegistry(NDFD_VARIABLES_YAML)

    if not (NDFD_SLAB_STORE_DIR / "metadata.json").exists():
        log.warning(
            f"NDFD slab store not initialised at {NDFD_SLAB_STORE_DIR}. "
            f"Run ndfd_ingest --postprocess at least once. "
            f"The /forecast endpoint will return 503 until the store is ready."
        )
        app.state.store    = None
        app.state.lat_grid = None
        app.state.lon_grid = None
        app.state.last_loaded = None
        return

    store = NDFDStore.open(NDFD_SLAB_STORE_DIR)

    lat_grid = lon_grid = None
    if (NDFD_SLAB_STORE_DIR / "lat.npy").exists():
        lat_grid, lon_grid = store.meta.load_grid(NDFD_SLAB_STORE_DIR)

    app.state.store       = store
    app.state.lat_grid    = lat_grid
    app.state.lon_grid    = lon_grid
    app.state.last_loaded = datetime.now(tz=timezone.utc)

    log.info(
        f"NDFD store opened — {store.n_runs_available} run(s) available "
        f"| is_ready={store.is_ready} "
        f"| lat/lon grid={'loaded' if lat_grid is not None else 'pending'}"
    )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )
    await asyncio.to_thread(_open_store, app)
    yield


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Weather Window NDFD API",
    description=(
        "Point forecast time series from the NOAA National Digital Forecast Database (NDFD). "
        "Returns data at native NDFD resolution (hourly days 1–3, 3-hourly days 4–7) "
        "interpolated to 1-hour output for any CONUS location out to ~7 days."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routers  (same as NBM API — duck-typed, work with NDFDStore)
# ---------------------------------------------------------------------------

app.include_router(forecast.router)
app.include_router(variables.router)
app.include_router(status.router)


# ---------------------------------------------------------------------------
# Admin reload endpoint
# ---------------------------------------------------------------------------

@app.post("/admin/reload", include_in_schema=False)
def admin_reload(request: Request):
    """
    Reload the NDFD slab ring buffer state from disk.
    Called by ndfd_slab_ingest after a successful slab write.
    """
    state = request.app.state

    if state.store is None:
        if not (NDFD_SLAB_STORE_DIR / "metadata.json").exists():
            raise HTTPException(
                status_code=503,
                detail=f"NDFD slab store not initialised at {NDFD_SLAB_STORE_DIR}.",
            )
        _open_store(request.app)
        state = request.app.state
    else:
        changed = state.store.reload()
        if state.lat_grid is None and (NDFD_SLAB_STORE_DIR / "lat.npy").exists():
            state.lat_grid, state.lon_grid = state.store.meta.load_grid(NDFD_SLAB_STORE_DIR)
        state.last_loaded = datetime.now(tz=timezone.utc)
        log.info(f"NDFD reload — state changed: {changed}")

    store = state.store
    return {
        "status":      "ok",
        "source":      "ndfd",
        "cycle":       store.current_cycle_time if store else None,
        "is_ready":    store.is_ready if store else False,
        "last_loaded": state.last_loaded.isoformat() if state.last_loaded else None,
    }


# ---------------------------------------------------------------------------
# Store-not-ready guard
# ---------------------------------------------------------------------------

@app.middleware("http")
async def require_store(request: Request, call_next):
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
                content={"detail": "NDFD slab store not yet available. Check /status."},
            )
    return await call_next(request)


# ---------------------------------------------------------------------------
# Developer tools
# ---------------------------------------------------------------------------

_REVIEW_HTML = REPO_ROOT / "forecast_review.html"

@app.get("/review", include_in_schema=False)
def review_page():
    return FileResponse(_REVIEW_HTML, media_type="text/html")
