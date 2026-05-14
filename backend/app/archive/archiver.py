"""
Forecast archive sweeps — called as FastAPI BackgroundTasks from admin_reload.

archive_nbm_sweep(state)  — fires when NBM store has a new cycle
archive_ndfd_sweep(state) — fires when NDFD store has a new cycle

Output: /12TB1/FCST_series/<station_id>/{nbm,ndfd}/<variable>.jsonl
One JSON record per line, one line per (cycle, station, variable).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml

from ..extraction import query_forecast
from .config import ARCHIVE_ROOT, STATIONS_CONFIG, NBM_VARS_CONFIG, NDFD_VARS_CONFIG

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config loaders (cached at module level after first call)
# ---------------------------------------------------------------------------

_stations: Optional[list[dict]] = None
_nbm_vars: Optional[list[str]]  = None
_ndfd_vars: Optional[list[str]] = None


def _load_stations() -> list[dict]:
    global _stations
    if _stations is None:
        with open(STATIONS_CONFIG) as f:
            _stations = yaml.safe_load(f)["stations"]
    return _stations


def _load_nbm_vars() -> list[str]:
    global _nbm_vars
    if _nbm_vars is None:
        with open(NBM_VARS_CONFIG) as f:
            _nbm_vars = yaml.safe_load(f)["variables"]
    return _nbm_vars


def _load_ndfd_vars() -> list[str]:
    global _ndfd_vars
    if _ndfd_vars is None:
        with open(NDFD_VARS_CONFIG) as f:
            _ndfd_vars = yaml.safe_load(f)["variables"]
    return _ndfd_vars


# ---------------------------------------------------------------------------
# JSONL helpers
# ---------------------------------------------------------------------------

def _last_record_field(path: Path, field: str) -> Optional[str]:
    """Return `field` from the last record in a JSONL file, or None."""
    if not path.exists() or path.stat().st_size == 0:
        return None
    with open(path, "rb") as f:
        f.seek(0, 2)
        end = f.tell()
        if end == 0:
            return None
        pos = end - 1
        while pos > 0:
            f.seek(pos)
            ch = f.read(1)
            # pos < end - 1 skips a sole trailing newline of the final line
            if ch == b"\n" and pos < end - 1:
                break
            pos -= 1
        line = f.read().strip()
    if not line:
        return None
    try:
        return json.loads(line).get(field)
    except Exception:
        return None


def _append_record(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(record, separators=(",", ":")) + "\n")
        f.flush()


# ---------------------------------------------------------------------------
# Core sweep
# ---------------------------------------------------------------------------

def _run_sweep(
    source:    str,           # "nbm" or "ndfd"
    store,
    lat_grid,
    lon_grid,
    registry,
    variables: list[str],
    cycle_tag: str,
    cycle_time: str,
) -> None:
    stations = _load_stations()
    archived_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    records_written = 0
    stations_ok     = 0
    stations_failed = 0

    log.info(f"Archive sweep [{source}] cycle={cycle_tag}  stations={len(stations)}  vars={variables}")

    for stn in stations:
        sid  = stn["id"]
        lat  = stn["lat"]
        lon  = stn["lon"]
        try:
            df, grid_lat, grid_lon = query_forecast(
                store     = store,
                lat_grid  = lat_grid,
                lon_grid  = lon_grid,
                lat       = lat,
                lon       = lon,
                variables = variables,
                registry  = registry,
            )
        except Exception as exc:
            log.warning(f"Archive [{source}] {sid}: query failed — {exc}")
            stations_failed += 1
            continue

        times = [t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in df.index]

        for var in variables:
            if var not in df.columns:
                log.warning(f"Archive [{source}] {sid}/{var}: not in query result, skipping")
                continue

            out_path = ARCHIVE_ROOT / sid / source / f"{var}.jsonl"

            # Duplicate-cycle guard
            if _last_record_field(out_path, "cycle_tag") == cycle_tag:
                log.debug(f"Archive [{source}] {sid}/{var}: cycle {cycle_tag} already written, skipping")
                continue

            values = [
                None if v != v else round(float(v), 2)   # nan → None
                for v in df[var]
            ]

            record = {
                "archived_at": archived_at,
                "source":      source,
                "cycle_tag":   cycle_tag,
                "cycle_time":  cycle_time,
                "station_id":  sid,
                "req_lat":     lat,
                "req_lon":     lon,
                "grid_lat":    round(float(grid_lat), 4),
                "grid_lon":    round(float(grid_lon), 4),
                "variable":    var,
                "length":      len(times),
                "times":       times,
                "values":      values,
            }

            try:
                _append_record(out_path, record)
                records_written += 1
            except Exception as exc:
                log.warning(f"Archive [{source}] {sid}/{var}: write failed — {exc}")

        stations_ok += 1

    log.info(
        f"Archive sweep [{source}] done — "
        f"stations ok/failed={stations_ok}/{stations_failed}  "
        f"records written={records_written}"
    )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def archive_nbm_sweep(state) -> None:
    store = state.nbm_store
    if store is None or not store.is_ready:
        log.warning("Archive NBM sweep skipped — store not ready")
        return
    cycle_tag  = store.current_cycle_tag
    cycle_time = store.current_cycle_time
    if not cycle_tag:
        log.warning("Archive NBM sweep skipped — no cycle tag")
        return
    _run_sweep(
        source     = "nbm",
        store      = store,
        lat_grid   = state.nbm_lat_grid,
        lon_grid   = state.nbm_lon_grid,
        registry   = state.nbm_registry,
        variables  = _load_nbm_vars(),
        cycle_tag  = cycle_tag,
        cycle_time = cycle_time,
    )


def archive_ndfd_sweep(state) -> None:
    store = state.ndfd_store
    if store is None or not store.is_ready:
        log.warning("Archive NDFD sweep skipped — store not ready")
        return
    cycle_tag  = store.current_cycle_tag
    cycle_time = store.current_cycle_time
    if not cycle_tag:
        log.warning("Archive NDFD sweep skipped — no cycle tag")
        return
    _run_sweep(
        source     = "ndfd",
        store      = store,
        lat_grid   = state.ndfd_lat_grid,
        lon_grid   = state.ndfd_lon_grid,
        registry   = state.ndfd_registry,
        variables  = _load_ndfd_vars(),
        cycle_tag  = cycle_tag,
        cycle_time = cycle_time,
    )
