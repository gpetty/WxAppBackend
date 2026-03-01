"""
GET /forecast — compact parallel-array time series for one location.

Response shape
--------------
{
  "runtime":   "2026-02-27T18:00:00Z",   // NBM model run
  "latitude":  43.0731,                  // actual grid point (±180)
  "longitude": -89.4068,
  "length":    264,                      // elements in every array
  "times":     ["2026-02-27T19:00:00Z", ...],  // UTC, Z suffix
  "temperature_F":          [34.2, 33.8, ...],
  "wind_speed_mph":         [8.5,  9.1,  ...],
  "visibility_mi":          [10.0, 9.5, null, ...]  // null = absent
}

Variable names in `vars` are plain (e.g. "temperature"); the response keys
carry the unit suffix (e.g. "temperature_F").  Call /variables to see the
full mapping and discover which names are valid.

Query parameters
----------------
lat   float   Latitude, decimal degrees (-90 – 90)
lon   float   Longitude, decimal degrees (±180 or 0–360)
vars  str     Comma-separated plain variable names
start str     Optional ISO-8601 UTC datetime
end   str     Optional ISO-8601 UTC datetime
"""

from __future__ import annotations

import math
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request

from ..extraction import query_forecast
from ..registry import NativeVariable, DerivedVariable

log = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Unit suffix table
# Maps units_out values (from variables.yaml) → response key suffix.
# Suffix is appended as  f"{var_name}_{suffix}"  to form the response key.
# Empty string → no suffix (e.g. categorical codes where the name is enough).
# ---------------------------------------------------------------------------

_UNIT_SUFFIX: dict[str, str] = {
    "F":                   "F",
    "%":                   "pct",
    "mph":                 "mph",
    "Degree true":         "deg",
    "degrees":             "deg",
    "mm":                  "mm",
    "miles":               "mi",
    "feet":                "ft",
    "J kg**-1":            "Jkg",
    "W m**-2":             "Wm2",
    "(Code table 4.201)":  "code",
    "kg m**-2":            "mm",   # fallback if any old store still carries this label
}

# Decimal places to round each variable's values before serialisation.
# Meteorological precision: more than this is noise in float32 NBM data.
_DECIMAL_PLACES: dict[str, int] = {
    "temperature":              1,
    "dewpoint":                 1,
    "apparent_temperature":     1,
    "relative_humidity":        0,
    "wind_speed":               1,
    "wind_gust":                1,
    "wind_direction":           0,
    "total_precipitation":      2,
    "precip_type":              0,
    "thunderstorm_probability": 0,
    "cape":                     0,
    "cloud_cover":              0,
    "solar_radiation":          0,
    "visibility":               2,
    "cloud_ceiling":            0,
    "sun_elevation":            1,
}
_DEFAULT_DECIMALS = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _response_key(var_name: str, units_out: str) -> str:
    """Build the response key from a plain variable name and its output units."""
    suffix = _UNIT_SUFFIX.get(units_out, "")
    return f"{var_name}_{suffix}" if suffix else var_name


def _round_val(val: float, decimals: int) -> Optional[float]:
    """Round a single value; return None for NaN (serialises to JSON null)."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    return round(float(val), decimals)


def _parse_timestamp(value: str, param: str) -> pd.Timestamp:
    try:
        ts = pd.Timestamp(value)
        return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    except Exception:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid datetime for '{param}': {value!r}. "
                   "Expected ISO-8601, e.g. '2026-02-27T18:00:00Z'.",
        )


def _normalise_runtime(raw: str) -> str:
    """Return a clean UTC ISO-8601 string with Z suffix."""
    try:
        ts = pd.Timestamp(raw)
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return raw   # return as-is if unparseable


# ---------------------------------------------------------------------------
# Store selection
# ---------------------------------------------------------------------------

def _select_store(state, age_hours: int):
    """
    Return the xr.Dataset for the most-recent cycle that is at least
    ``age_hours`` older than the current cycle.

    age_hours=0 → current cycle (fast path, no tag parsing).
    """
    if age_hours == 0:
        return state.ds

    current_dt = datetime.strptime(state.current_tag, "%Y%m%d_%H")
    target_dt  = current_dt - timedelta(hours=age_hours)

    candidates = [
        (tag, datetime.strptime(tag, "%Y%m%d_%H"))
        for tag in state.stores
        if datetime.strptime(tag, "%Y%m%d_%H") <= target_dt
    ]

    if not candidates:
        raise HTTPException(
            status_code=404,
            detail={
                "message":          f"No cycle available ≥ {age_hours}h before current.",
                "current_cycle":    state.current_tag,
                "requested_age_hours": age_hours,
                "available_cycles": sorted(state.stores.keys()),
            },
        )

    best_tag = max(candidates, key=lambda x: x[1])[0]
    return state.stores[best_tag]


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.get("/forecast")
def get_forecast(
    request: Request,
    lat:       float         = Query(..., ge=-90.0,  le=90.0,
                                     description="Latitude (decimal degrees)"),
    lon:       float         = Query(..., ge=-180.0, le=360.0,
                                     description="Longitude (decimal degrees, ±180 or 0–360)"),
    vars:      str           = Query(...,
                                     description="Comma-separated variable names (plain, no unit suffix)"),
    start:     Optional[str] = Query(None, description="Start time, ISO-8601 UTC (inclusive)"),
    end:       Optional[str] = Query(None, description="End time, ISO-8601 UTC (inclusive)"),
    age_hours: int           = Query(0, ge=0,
                                     description="Return the most-recent cycle at least this many "
                                                 "hours older than the current run (0 = current). "
                                                 "Use /status for available cycles."),
) -> dict:
    """
    Return a compact parallel-array forecast for the nearest NBM grid point.

    Every array in the response has the same length (`length`).  Variable
    keys carry their units as a suffix so the response is self-documenting
    without a separate units lookup.  Absent values (e.g. visibility beyond
    its forecast horizon) are JSON `null`.
    """
    state    = request.app.state
    ds       = _select_store(state, age_hours)
    registry = state.registry

    # Normalise lon to ±180
    if lon > 180.0:
        lon -= 360.0

    # Parse and validate variable list
    requested = [v.strip() for v in vars.split(",") if v.strip()]
    if not requested:
        raise HTTPException(status_code=422,
                            detail="'vars' must contain at least one variable name.")

    unknown = [v for v in requested if registry.get(v) is None]
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown variable(s): {unknown}. Call /variables for valid names.",
        )

    # Parse optional time bounds
    start_ts = _parse_timestamp(start, "start") if start else None
    end_ts   = _parse_timestamp(end,   "end")   if end   else None
    if start_ts and end_ts and start_ts >= end_ts:
        raise HTTPException(status_code=422, detail="'start' must be before 'end'.")

    # Query
    try:
        df, actual_lat, actual_lon = query_forecast(
            ds=ds, lat=lat, lon=lon,
            variables=requested, registry=registry,
            start=start_ts, end=end_ts,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        log.exception("Unexpected error in query_forecast")
        raise HTTPException(status_code=500, detail=f"Internal error: {exc}")

    # Build shared time axis (UTC, Z suffix, 20 chars each)
    times = [ts.strftime("%Y-%m-%dT%H:%M:%SZ") for ts in df.index]

    # Assemble response
    response: dict = {
        "runtime":   _normalise_runtime(ds.attrs.get("cycle", "unknown")),
        "latitude":  round(actual_lat, 4),
        "longitude": round(actual_lon, 4),
        "length":    len(df),
        "times":     times,
    }

    for var_name in requested:
        if var_name not in df.columns:
            continue
        var_meta = registry.get(var_name)
        units_out = var_meta.units_out if var_meta else ""
        key      = _response_key(var_name, units_out)
        decimals = _DECIMAL_PLACES.get(var_name, _DEFAULT_DECIMALS)
        response[key] = [_round_val(v, decimals) for v in df[var_name]]

    return response
