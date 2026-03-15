"""
Shared utilities for API routers.

Pure functions and constants used by forecast.py, variables.py, and the
blend routers.  Importing from here (rather than from forecast.py) avoids
circular imports when blend routers need the same helpers.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from fastapi import HTTPException


# ---------------------------------------------------------------------------
# Unit suffix table  (response key = "{var_name}_{suffix}")
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
    "kg m**-2":            "mm",
}

_DECIMAL_PLACES: dict[str, int] = {
    "temperature":                  1,
    "dewpoint":                     1,
    "apparent_temperature":         1,
    "relative_humidity":            0,
    "wind_speed":                   1,
    "wind_gust":                    1,
    "wind_direction":               0,
    "total_precipitation":          2,
    "snowfall":                     2,
    "precip_type":                  0,
    "thunderstorm_probability":     0,
    "thunderstorm_probability_severe": 0,
    "cape":                         0,
    "cloud_cover":                  0,
    "solar_radiation":              0,
    "visibility":                   2,
    "cloud_ceiling":                0,
    "wet_bulb_globe_temp":          1,
    "sun_elevation":                1,
}
_DEFAULT_DECIMALS = 1


# ---------------------------------------------------------------------------
# Response key builder
# ---------------------------------------------------------------------------

def _response_key(var_name: str, units_out: str) -> str:
    suffix = _UNIT_SUFFIX.get(units_out, "")
    return f"{var_name}_{suffix}" if suffix else var_name


def _suffix(units_out: str) -> str:
    return _UNIT_SUFFIX.get(units_out, "")


# ---------------------------------------------------------------------------
# Value formatting
# ---------------------------------------------------------------------------

def _round_val(val, decimals: int) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return round(f, decimals)


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _parse_timestamp(value: str, param: str) -> pd.Timestamp:
    try:
        ts = pd.Timestamp(value)
        return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    except Exception:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid datetime for '{param}': {value!r}. "
                   "Expected ISO-8601, e.g. '2026-03-14T12:00:00Z'.",
        )


def _normalise_runtime(raw: Optional[str]) -> Optional[str]:
    """Return a clean UTC ISO-8601 string with Z suffix, or None."""
    if not raw:
        return None
    try:
        ts = pd.Timestamp(raw)
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return raw


# ---------------------------------------------------------------------------
# Cycle selection helpers
# ---------------------------------------------------------------------------

def select_cycle_tag_for_store(store, age_hours: int) -> Optional[str]:
    """
    Return the cycle_tag for the most-recent run at least ``age_hours`` older
    than the current run.  Returns None if age_hours=0 (meaning current run)
    or if no qualifying run is found (caller decides how to handle absence).

    Unlike the router-level _select_cycle_tag(), this never raises — the blend
    layer uses None to degrade gracefully when one source has no matching cycle.
    """
    if age_hours == 0:
        return None

    current_tag = store.current_cycle_tag
    if not current_tag:
        return None

    current_dt = datetime.strptime(current_tag, "%Y%m%d_%H")
    target_dt  = current_dt - timedelta(hours=age_hours)

    candidates = [
        (r["cycle_tag"], datetime.strptime(r["cycle_tag"], "%Y%m%d_%H"))
        for r in store.available_runs
        if datetime.strptime(r["cycle_tag"], "%Y%m%d_%H") <= target_dt
    ]

    if not candidates:
        return None   # no retained cycle old enough — caller decides

    return max(candidates, key=lambda x: x[1])[0]


def cycle_time_for_tag(store, cycle_tag: Optional[str]) -> Optional[str]:
    """Look up the ISO-8601 cycle_time for a given tag (or current if None)."""
    if cycle_tag is None:
        return store.current_cycle_time
    for run in store.available_runs:
        if run["cycle_tag"] == cycle_tag:
            return run["cycle_time"]
    return cycle_tag
