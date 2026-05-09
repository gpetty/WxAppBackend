"""
Weather Window Blend API — FastAPI application entry point.

Opens both the NBM and NDFD slab stores at startup and serves a merged
forecast that prefers NDFD where available and backfills with NBM.

Port 8004 — Caddy reverse-proxies /blend/* to here.

Development:
    uvicorn backend.app.blend_main:app --reload --port 8004

Production:
    gunicorn backend.app.blend_main:app -k uvicorn.workers.UvicornWorker \
        -w 1 --bind 127.0.0.1:8004 --root-path /blend

Single worker only: the slab stores live in app.state and are not safe
to share across processes.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .config import SLAB_STORE_DIR, VARIABLES_YAML, REPO_ROOT
from .config_ndfd import NDFD_SLAB_STORE_DIR, NDFD_VARIABLES_YAML
from .registry import VariableRegistry
from .store.nbm_store import NBMStore
from .store.ndfd_store import NDFDStore
from .archive import archive_nbm_sweep, archive_ndfd_sweep
from .routers import blend_forecast, blend_status, blend_variables

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def _open_nbm(app: FastAPI) -> None:
    """Open (or re-open) the NBM slab store into app.state."""
    app.state.nbm_registry = VariableRegistry(VARIABLES_YAML)
    app.state.nbm_store_dir = SLAB_STORE_DIR

    if not (SLAB_STORE_DIR / "metadata.json").exists():
        log.warning(f"NBM slab store not found at {SLAB_STORE_DIR}. "
                    f"Blend will operate in NDFD-only mode until NBM is available.")
        app.state.nbm_store    = None
        app.state.nbm_lat_grid = None
        app.state.nbm_lon_grid = None
        return

    store = NBMStore.open(SLAB_STORE_DIR)
    lat_grid = lon_grid = None
    if (SLAB_STORE_DIR / "lat.npy").exists():
        lat_grid, lon_grid = store.meta.load_grid(SLAB_STORE_DIR)

    app.state.nbm_store    = store
    app.state.nbm_lat_grid = lat_grid
    app.state.nbm_lon_grid = lon_grid
    log.info(f"NBM store opened — {store.n_runs_available} run(s) | "
             f"is_ready={store.is_ready} | "
             f"lat/lon grid={'loaded' if lat_grid is not None else 'pending'}")


def _open_ndfd(app: FastAPI) -> None:
    """Open (or re-open) the NDFD slab store into app.state."""
    app.state.ndfd_registry = VariableRegistry(NDFD_VARIABLES_YAML)
    app.state.ndfd_store_dir = NDFD_SLAB_STORE_DIR

    if not (NDFD_SLAB_STORE_DIR / "metadata.json").exists():
        log.warning(f"NDFD slab store not found at {NDFD_SLAB_STORE_DIR}. "
                    f"Blend will operate in NBM-only mode until NDFD is available.")
        app.state.ndfd_store    = None
        app.state.ndfd_lat_grid = None
        app.state.ndfd_lon_grid = None
        return

    store = NDFDStore.open(NDFD_SLAB_STORE_DIR)
    lat_grid = lon_grid = None
    if (NDFD_SLAB_STORE_DIR / "lat.npy").exists():
        lat_grid, lon_grid = store.meta.load_grid(NDFD_SLAB_STORE_DIR)

    app.state.ndfd_store    = store
    app.state.ndfd_lat_grid = lat_grid
    app.state.ndfd_lon_grid = lon_grid
    log.info(f"NDFD store opened — {store.n_runs_available} run(s) | "
             f"is_ready={store.is_ready} | "
             f"lat/lon grid={'loaded' if lat_grid is not None else 'pending'}")


def _open_stores(app: FastAPI) -> None:
    _open_nbm(app)
    _open_ndfd(app)
    app.state.last_loaded = datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )
    await asyncio.to_thread(_open_stores, app)
    yield


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Weather Window Blend API",
    description=(
        "Blended point forecast merging NDFD (days 1–7) and NBM (days 8–11). "
        "NDFD is preferred where available; NBM provides extended-range backfill. "
        "Each variable array is accompanied by a _source array ('ndfd'/'nbm'/null)."
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
# Routers
# ---------------------------------------------------------------------------

app.include_router(blend_forecast.router)
app.include_router(blend_status.router)
app.include_router(blend_variables.router)


# ---------------------------------------------------------------------------
# Review page
# ---------------------------------------------------------------------------

_REVIEW_HTML = REPO_ROOT / "blend_review.html"

@app.get("/review", include_in_schema=False)
def review_page():
    """Serve the blend forecast review tool (single-page HTML + Chart.js)."""
    return FileResponse(_REVIEW_HTML, media_type="text/html")


# ---------------------------------------------------------------------------
# Admin reload endpoint
# ---------------------------------------------------------------------------

@app.post("/admin/reload", include_in_schema=False)
def admin_reload(request: Request, background_tasks: BackgroundTasks):
    """
    Reload both slab stores from disk.

    Called by both wxingest.service and wxndfdingest.service after each
    successful slab write so the blend service serves fresh data promptly.
    """
    state = request.app.state

    # Reload NBM
    nbm_changed = False
    if state.nbm_store is None:
        if (SLAB_STORE_DIR / "metadata.json").exists():
            _open_nbm(request.app)
            nbm_changed = True
    else:
        nbm_changed = state.nbm_store.reload()
        if state.nbm_lat_grid is None and (SLAB_STORE_DIR / "lat.npy").exists():
            state.nbm_lat_grid, state.nbm_lon_grid = (
                state.nbm_store.meta.load_grid(SLAB_STORE_DIR)
            )

    # Reload NDFD
    ndfd_changed = False
    if state.ndfd_store is None:
        if (NDFD_SLAB_STORE_DIR / "metadata.json").exists():
            _open_ndfd(request.app)
            ndfd_changed = True
    else:
        ndfd_changed = state.ndfd_store.reload()
        if state.ndfd_lat_grid is None and (NDFD_SLAB_STORE_DIR / "lat.npy").exists():
            state.ndfd_lat_grid, state.ndfd_lon_grid = (
                state.ndfd_store.meta.load_grid(NDFD_SLAB_STORE_DIR)
            )

    state.last_loaded = datetime.now(tz=timezone.utc)
    log.info(f"Blend reload — NBM changed: {nbm_changed}, NDFD changed: {ndfd_changed}")

    if nbm_changed:
        background_tasks.add_task(archive_nbm_sweep, state)
    if ndfd_changed:
        background_tasks.add_task(archive_ndfd_sweep, state)

    return {
        "status":       "ok",
        "nbm_changed":  nbm_changed,
        "ndfd_changed": ndfd_changed,
        "nbm_cycle":    state.nbm_store.current_cycle_time  if state.nbm_store  else None,
        "ndfd_cycle":   state.ndfd_store.current_cycle_time if state.ndfd_store else None,
        "last_loaded":  state.last_loaded.isoformat(),
    }
