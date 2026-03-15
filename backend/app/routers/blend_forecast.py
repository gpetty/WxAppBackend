"""
GET /forecast — blended parallel-array time series for one location.

Merges NBM (~11-day) and NDFD (~7-day) forecasts into a single response.
NDFD is preferred where available; NBM provides the extended-range backfill.

Response shape
--------------
{
  "nbm_runtime":   "2026-03-14T12:00:00Z",
  "ndfd_runtime":  "2026-03-14T12:00:00Z",
  "latitude":      43.0731,
  "longitude":     -89.4068,
  "length":        264,
  "times":         ["2026-03-14T13:00:00Z", ...],

  "temperature_F":               [34.2, 33.8, ...],
  "temperature_F_source":        ["ndfd", "ndfd", ..., "nbm", "nbm", ...],

  "thunderstorm_probability_pct":        [2, 3, ...],   // NBM general tstm
  "thunderstorm_probability_pct_source": ["nbm", ...],
  "thunderstorm_probability_severe_pct": [0, 1, ...],   // NDFD severe tstm
  "thunderstorm_probability_severe_pct_source": ["ndfd", ...],

  "interpolated": [false, true, true, false, ...]       // from NBM (dominant source)
}

Merge rules
-----------
BLEND_RULES is the authoritative registry for how each variable is served.
Strategy values:
  "ndfd_preferred"  Use NDFD while it has data (ndfd_h hours); NBM beyond that.
  "nbm_only"        Always from NBM. NDFD has no equivalent.
  "ndfd_only"       Always from NDFD. NBM has no equivalent.
  "derived"         Computed from lat/lon/time; no store needed.

Incompatible variables
----------------------
thunderstorm_probability (NBM) and thunderstorm_probability_severe (NDFD) measure
different quantities and are served as separate response keys. Clients may request
either or both by name.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request

from ..extraction import query_forecast, find_nearest_grid_point
from ..extraction.derived import compute_derived
from .helpers import (
    _UNIT_SUFFIX, _DECIMAL_PLACES, _DEFAULT_DECIMALS,
    _response_key, _round_val, _parse_timestamp, _normalise_runtime,
    select_cycle_tag_for_store, cycle_time_for_tag,
)

log = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Blend rule registry
# ---------------------------------------------------------------------------
#
# Keys are the variable names as seen by the blend API client.
# Fields:
#   strategy     : "ndfd_preferred" | "nbm_only" | "ndfd_only" | "derived"
#   ndfd_var     : variable name to request from the NDFD registry/store
#                  (omitted when strategy is "nbm_only" or "derived")
#   nbm_var      : variable name to request from the NBM registry/store
#                  (omitted when strategy is "ndfd_only" or "derived")
#   ndfd_h       : hours of NDFD coverage (data returned null beyond this)
#   nbm_h        : hours of NBM coverage
#
# thunderstorm_probability         → NBM general tstm (response key: *_pct)
# thunderstorm_probability_severe  → NDFD severe tstm (response key: *_pct)
# These are intentionally separate; see module docstring.

BLEND_RULES: dict[str, dict] = {
    # NDFD-preferred: NDFD days 1–7, NBM days 8–11
    "temperature":          {"strategy": "ndfd_preferred", "ndfd_var": "temperature",         "nbm_var": "temperature",         "ndfd_h": 168, "nbm_h": 264},
    "dewpoint":             {"strategy": "ndfd_preferred", "ndfd_var": "dewpoint",             "nbm_var": "dewpoint",            "ndfd_h": 168, "nbm_h": 264},
    "relative_humidity":    {"strategy": "ndfd_preferred", "ndfd_var": "relative_humidity",    "nbm_var": "relative_humidity",   "ndfd_h": 168, "nbm_h": 264},
    "apparent_temperature": {"strategy": "ndfd_preferred", "ndfd_var": "apparent_temperature", "nbm_var": "apparent_temperature","ndfd_h": 168, "nbm_h": 264},
    "wind_speed":           {"strategy": "ndfd_preferred", "ndfd_var": "wind_speed",           "nbm_var": "wind_speed",          "ndfd_h": 168, "nbm_h": 264},
    "wind_direction":       {"strategy": "ndfd_preferred", "ndfd_var": "wind_direction",       "nbm_var": "wind_direction",      "ndfd_h": 168, "nbm_h": 264},
    "wind_gust":            {"strategy": "ndfd_preferred", "ndfd_var": "wind_gust",            "nbm_var": "wind_gust",           "ndfd_h": 168, "nbm_h": 264},
    "cloud_cover":          {"strategy": "ndfd_preferred", "ndfd_var": "cloud_cover",          "nbm_var": "cloud_cover",         "ndfd_h": 168, "nbm_h": 264},
    # Both sources cut off at ~3 days; NDFD preferred while it has data
    "visibility":           {"strategy": "ndfd_preferred", "ndfd_var": "visibility",           "nbm_var": "visibility",          "ndfd_h": 72,  "nbm_h": 76},
    "cloud_ceiling":        {"strategy": "ndfd_preferred", "ndfd_var": "cloud_ceiling",        "nbm_var": "cloud_ceiling",       "ndfd_h": 72,  "nbm_h": 82},
    # NDFD has QPF only days 1–3; NBM covers full horizon
    "total_precipitation":  {"strategy": "ndfd_preferred", "ndfd_var": "total_precipitation",  "nbm_var": "total_precipitation", "ndfd_h": 72,  "nbm_h": 264},
    # NBM-only
    "precip_type":          {"strategy": "nbm_only",       "nbm_var": "precip_type",                                            "nbm_h": 264},
    "cape":                 {"strategy": "nbm_only",       "nbm_var": "cape",                                                   "nbm_h": 264},
    "solar_radiation":      {"strategy": "nbm_only",       "nbm_var": "solar_radiation",                                        "nbm_h": 264},
    # Incompatible thunderstorm fields — served as separate named variables
    "thunderstorm_probability":        {"strategy": "nbm_only",  "nbm_var":  "thunderstorm_probability",                        "nbm_h":  190},
    "thunderstorm_probability_severe": {"strategy": "ndfd_only", "ndfd_var": "thunderstorm_probability",                        "ndfd_h": 168},
    # NDFD-only
    "wet_bulb_globe_temp":  {"strategy": "ndfd_only",      "ndfd_var": "wet_bulb_globe_temp",                                   "ndfd_h": 168},
    "snowfall":             {"strategy": "ndfd_only",       "ndfd_var": "snowfall",                                              "ndfd_h": 72},
    # Derived — computed from lat/lon/time; no store needed
    "sun_elevation":        {"strategy": "derived"},
}

# Response key overrides for incompatible thunderstorm variables.
# Both map to the same units suffix ("pct") but need distinct response keys.
_RESPONSE_KEY_OVERRIDE: dict[str, str] = {
    "thunderstorm_probability":        "thunderstorm_probability_pct",
    "thunderstorm_probability_severe": "thunderstorm_probability_severe_pct",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _blend_response_key(blend_name: str, units_out: str) -> str:
    if blend_name in _RESPONSE_KEY_OVERRIDE:
        return _RESPONSE_KEY_OVERRIDE[blend_name]
    return _response_key(blend_name, units_out)


def _units_for(blend_name: str, state) -> str:
    """Look up units_out for a blend variable from the appropriate registry."""
    rule = BLEND_RULES[blend_name]
    strategy = rule["strategy"]
    if strategy in ("ndfd_only", "ndfd_preferred"):
        v = state.ndfd_registry.get(rule.get("ndfd_var", blend_name))
        if v:
            return v.units_out
    if strategy in ("nbm_only", "ndfd_preferred"):
        v = state.nbm_registry.get(rule.get("nbm_var", blend_name))
        if v:
            return v.units_out
    # derived — check either registry
    v = state.nbm_registry.get(blend_name) or state.ndfd_registry.get(blend_name)
    return v.units_out if v else ""


def _merge_series(
    blend_name: str,
    rule:       dict,
    nbm_df:     Optional[pd.DataFrame],
    ndfd_df:    Optional[pd.DataFrame],
    unified_index: pd.DatetimeIndex,
) -> tuple[pd.Series, pd.Series]:
    """
    Return (values_series, source_series) on unified_index for one variable.

    values_series : float values or NaN
    source_series : "ndfd", "nbm", or None per time step
    """
    strategy  = rule["strategy"]
    ndfd_var  = rule.get("ndfd_var", blend_name)
    nbm_var   = rule.get("nbm_var",  blend_name)
    ndfd_h    = rule.get("ndfd_h")
    nbm_h     = rule.get("nbm_h")

    n = len(unified_index)
    values  = np.full(n, np.nan, dtype=np.float64)
    sources = np.full(n, None, dtype=object)

    # Get raw series on the unified index (NaN outside each source's range)
    nbm_series  = None
    ndfd_series = None

    if nbm_df is not None and nbm_var in nbm_df.columns:
        nbm_series = nbm_df[nbm_var].reindex(unified_index)

    if ndfd_df is not None and ndfd_var in ndfd_df.columns:
        ndfd_series = ndfd_df[ndfd_var].reindex(unified_index)

    if strategy == "nbm_only":
        if nbm_series is not None:
            for i, (t, v) in enumerate(zip(unified_index, nbm_series)):
                if nbm_h is not None and i >= nbm_h:
                    break
                if not np.isnan(float(v)) if v is not None else False:
                    try:
                        fv = float(v)
                        if np.isfinite(fv):
                            values[i]  = fv
                            sources[i] = "nbm"
                    except (TypeError, ValueError):
                        pass

    elif strategy == "ndfd_only":
        if ndfd_series is not None:
            for i, (t, v) in enumerate(zip(unified_index, ndfd_series)):
                if ndfd_h is not None and i >= ndfd_h:
                    break
                if v is not None:
                    try:
                        fv = float(v)
                        if np.isfinite(fv):
                            values[i]  = fv
                            sources[i] = "ndfd"
                    except (TypeError, ValueError):
                        pass

    elif strategy == "ndfd_preferred":
        for i, t in enumerate(unified_index):
            # Try NDFD first, within its horizon
            used_ndfd = False
            if ndfd_series is not None and (ndfd_h is None or i < ndfd_h):
                v = ndfd_series.iloc[i] if i < len(ndfd_series) else np.nan
                if v is not None:
                    try:
                        fv = float(v)
                        if np.isfinite(fv):
                            values[i]  = fv
                            sources[i] = "ndfd"
                            used_ndfd  = True
                    except (TypeError, ValueError):
                        pass
            # Fall back to NBM
            if not used_ndfd and nbm_series is not None:
                if nbm_h is None or i < nbm_h:
                    v = nbm_series.iloc[i] if i < len(nbm_series) else np.nan
                    if v is not None:
                        try:
                            fv = float(v)
                            if np.isfinite(fv):
                                values[i]  = fv
                                sources[i] = "nbm"
                        except (TypeError, ValueError):
                            pass

    return (
        pd.Series(values,  index=unified_index, name=blend_name),
        pd.Series(sources, index=unified_index, name=f"{blend_name}_source"),
    )


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.get("/forecast")
def get_blend_forecast(
    request:   Request,
    lat:       float         = Query(..., ge=-90.0,  le=90.0,
                                     description="Latitude (decimal degrees)"),
    lon:       float         = Query(..., ge=-180.0, le=360.0,
                                     description="Longitude (decimal degrees, ±180 or 0–360)"),
    vars:      str           = Query(...,
                                     description="Comma-separated blend variable names"),
    start:     Optional[str] = Query(None, description="Start time, ISO-8601 UTC (inclusive)"),
    end:       Optional[str] = Query(None, description="End time, ISO-8601 UTC (inclusive)"),
    age_hours: int           = Query(0, ge=0,
                                     description="Drift: use cycles at least this many hours "
                                                 "older than current. 0 = current cycle."),
) -> dict:
    """
    Return a blended parallel-array forecast merging NBM and NDFD sources.

    NDFD is preferred where available; NBM provides extended-range backfill.
    Each variable array is accompanied by a _source array indicating 'ndfd',
    'nbm', or null per time step.

    thunderstorm_probability and thunderstorm_probability_severe are distinct
    variables (NBM general vs NDFD severe) and must be requested separately.
    """
    state = request.app.state

    # Normalise lon to ±180
    if lon > 180.0:
        lon -= 360.0

    # Parse and validate variable list
    requested = [v.strip() for v in vars.split(",") if v.strip()]
    if not requested:
        raise HTTPException(status_code=422,
                            detail="'vars' must contain at least one variable name.")

    unknown = [v for v in requested if v not in BLEND_RULES]
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown blend variable(s): {unknown}. Call /variables for valid names.",
        )

    # Parse optional time bounds
    start_ts = _parse_timestamp(start, "start") if start else None
    end_ts   = _parse_timestamp(end,   "end")   if end   else None
    if start_ts and end_ts and start_ts >= end_ts:
        raise HTTPException(status_code=422, detail="'start' must be before 'end'.")

    # Determine which store variables to fetch
    nbm_vars_needed:  set[str] = set()
    ndfd_vars_needed: set[str] = set()
    derived_needed:   list[str] = []

    for blend_name in requested:
        rule     = BLEND_RULES[blend_name]
        strategy = rule["strategy"]
        if strategy == "derived":
            derived_needed.append(blend_name)
        if strategy in ("ndfd_preferred", "ndfd_only"):
            ndfd_vars_needed.add(rule.get("ndfd_var", blend_name))
        if strategy in ("ndfd_preferred", "nbm_only"):
            nbm_vars_needed.add(rule.get("nbm_var", blend_name))

    nbm_store  = state.nbm_store
    ndfd_store = state.ndfd_store

    nbm_ready  = nbm_store  is not None and nbm_store.is_ready  and state.nbm_lat_grid  is not None
    ndfd_ready = ndfd_store is not None and ndfd_store.is_ready and state.ndfd_lat_grid is not None

    if not nbm_ready and not ndfd_ready:
        raise HTTPException(status_code=503,
                            detail="Neither NBM nor NDFD store is available. Check /status.")

    # Cycle selection (for drift queries)
    nbm_cycle_tag  = select_cycle_tag_for_store(nbm_store,  age_hours) if nbm_ready  else None
    ndfd_cycle_tag = select_cycle_tag_for_store(ndfd_store, age_hours) if ndfd_ready else None

    if age_hours > 0 and not nbm_ready and ndfd_cycle_tag is None:
        raise HTTPException(
            status_code=404,
            detail=f"No cycle available ≥ {age_hours}h before current in either store.",
        )

    # Query NBM store
    nbm_df    = None
    nbm_lat   = nbm_lon = None
    nbm_runtime = None
    if nbm_ready and nbm_vars_needed:
        try:
            nbm_df, nbm_lat, nbm_lon = query_forecast(
                store    = nbm_store,
                lat_grid = state.nbm_lat_grid,
                lon_grid = state.nbm_lon_grid,
                lat      = lat, lon = lon,
                variables = list(nbm_vars_needed),
                registry  = state.nbm_registry,
                cycle_tag = nbm_cycle_tag,
            )
            nbm_runtime = _normalise_runtime(
                cycle_time_for_tag(nbm_store, nbm_cycle_tag)
            )
        except Exception as exc:
            log.warning(f"NBM query failed (degraded mode): {exc}")
            nbm_df = None

    # Query NDFD store
    ndfd_df   = None
    ndfd_lat  = ndfd_lon = None
    ndfd_runtime = None
    if ndfd_ready and ndfd_vars_needed:
        try:
            ndfd_df, ndfd_lat, ndfd_lon = query_forecast(
                store    = ndfd_store,
                lat_grid = state.ndfd_lat_grid,
                lon_grid = state.ndfd_lon_grid,
                lat      = lat, lon = lon,
                variables = list(ndfd_vars_needed),
                registry  = state.ndfd_registry,
                cycle_tag = ndfd_cycle_tag,
            )
            ndfd_runtime = _normalise_runtime(
                cycle_time_for_tag(ndfd_store, ndfd_cycle_tag)
            )
        except Exception as exc:
            log.warning(f"NDFD query failed (degraded mode): {exc}")
            ndfd_df = None

    if nbm_df is None and ndfd_df is None and not derived_needed:
        raise HTTPException(status_code=503,
                            detail="All store queries failed. Check /status.")

    # Build unified time axis: hourly from earliest to latest valid time across both
    starts, ends = [], []
    if nbm_df is not None and len(nbm_df):
        starts.append(nbm_df.index[0])
        ends.append(nbm_df.index[-1])
    if ndfd_df is not None and len(ndfd_df):
        starts.append(ndfd_df.index[0])
        ends.append(ndfd_df.index[-1])

    if not starts:
        raise HTTPException(status_code=503, detail="No forecast data available.")

    unified_start = min(starts)
    unified_end   = max(ends)
    unified_index = pd.date_range(start=unified_start, end=unified_end, freq="1h", tz="UTC")

    # Apply optional time filters to the unified index
    if start_ts is not None:
        unified_index = unified_index[unified_index >= start_ts]
    if end_ts is not None:
        unified_index = unified_index[unified_index <= end_ts]

    if len(unified_index) == 0:
        raise HTTPException(status_code=422,
                            detail="Time filter excludes all forecast hours.")

    # Determine canonical display lat/lon: prefer NDFD grid point (finer)
    if ndfd_lat is not None:
        actual_lat, actual_lon = ndfd_lat, ndfd_lon
    elif nbm_lat is not None:
        actual_lat, actual_lon = nbm_lat, nbm_lon
    else:
        actual_lat, actual_lon = lat, lon

    # Warn if grids are far apart (sanity check)
    if nbm_lat is not None and ndfd_lat is not None:
        if abs(nbm_lat - ndfd_lat) > 0.05 or abs(nbm_lon - ndfd_lon) > 0.05:
            log.warning(
                f"NBM/NDFD grid points differ: NBM=({nbm_lat:.4f},{nbm_lon:.4f}) "
                f"NDFD=({ndfd_lat:.4f},{ndfd_lon:.4f})"
            )

    # Build response
    times = [ts.strftime("%Y-%m-%dT%H:%M:%SZ") for ts in unified_index]

    # The interpolated flag comes from whichever store has the broader time axis
    # (NBM as primary if available; else NDFD).  It indicates upsampled hours
    # within whichever source covered that step.
    interpolated_series = None
    if nbm_df is not None and "interpolated" in nbm_df.columns:
        interpolated_series = nbm_df["interpolated"].reindex(unified_index).fillna(True)
    elif ndfd_df is not None and "interpolated" in ndfd_df.columns:
        interpolated_series = ndfd_df["interpolated"].reindex(unified_index).fillna(True)

    response: dict = {
        "nbm_runtime":  nbm_runtime,
        "ndfd_runtime": ndfd_runtime,
        "latitude":     round(actual_lat, 4),
        "longitude":    round(actual_lon, 4),
        "length":       len(unified_index),
        "times":        times,
    }

    # Merge each requested variable
    for blend_name in requested:
        rule     = BLEND_RULES[blend_name]
        strategy = rule["strategy"]

        if strategy == "derived":
            # Compute sun_elevation on the unified time axis
            try:
                from ..extraction.derived import _compute_sun_elevation
                elev = _compute_sun_elevation(unified_index, actual_lat, actual_lon)
                decimals   = _DECIMAL_PLACES.get(blend_name, _DEFAULT_DECIMALS)
                units_out  = _units_for(blend_name, state)
                resp_key   = _blend_response_key(blend_name, units_out)
                response[resp_key]            = [_round_val(v, decimals) for v in elev]
                response[f"{resp_key}_source"] = ["derived"] * len(unified_index)
            except Exception as exc:
                log.error(f"Failed to compute derived variable '{blend_name}': {exc}")
            continue

        val_series, src_series = _merge_series(
            blend_name, rule, nbm_df, ndfd_df, unified_index
        )

        units_out = _units_for(blend_name, state)
        resp_key  = _blend_response_key(blend_name, units_out)
        decimals  = _DECIMAL_PLACES.get(blend_name, _DEFAULT_DECIMALS)

        response[resp_key]             = [
            _round_val(v, decimals) if (v is not None and not np.isnan(float(v)) if isinstance(v, float) else v is not None) else None
            for v in val_series
        ]
        response[f"{resp_key}_source"] = [s for s in src_series]

    if interpolated_series is not None:
        response["interpolated"] = [bool(v) for v in interpolated_series]

    return response
