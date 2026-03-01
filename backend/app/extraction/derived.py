"""
Derived variable computation for the Weather Window query layer.

Derived variables are computed at query time from the 1-hour interpolated
DataFrame produced by zarr_query.py.  They are NOT stored in the Zarr store.

Currently supported
-------------------
sun_elevation
    Solar elevation angle (degrees above horizon), computed astronomically
    via pysolar.  Requires no native weather columns — only the DataFrame's
    DatetimeIndex (UTC) and the grid point's lat/lon.

    Positive values = sun above horizon.
    Negative values = sun below horizon (night).

    No atmospheric refraction correction is applied; for display purposes
    the difference is negligible (< 0.6° near the horizon).

Intentionally omitted
---------------------
heat_index, wind_chill
    The NBM natively provides ``apparent_temperature`` (aptmp), a pre-blended
    feels-like field that covers both roles with better calibration than
    hand-computed formulas.  See variables.yaml for details.
"""

from __future__ import annotations

import logging
from typing import Callable

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Registry: which native columns each derived variable needs.
# sun_elevation uses only the DataFrame index (UTC timestamps) + lat/lon,
# so it requires no native variable columns.
# ---------------------------------------------------------------------------

DERIVED_REQUIREMENTS: dict[str, list[str]] = {
    "sun_elevation": [],
}

# ---------------------------------------------------------------------------
# Individual computations
# ---------------------------------------------------------------------------

def _compute_sun_elevation(
    index: pd.DatetimeIndex,
    lat: float,
    lon: float,
) -> pd.Series:
    """
    Compute solar elevation angle (degrees) for every hour in *index*.

    Parameters
    ----------
    index : hourly UTC DatetimeIndex
    lat   : latitude of the grid point (decimal degrees)
    lon   : longitude of the grid point (decimal degrees, ±180)

    Returns
    -------
    pd.Series with the same index, values in degrees.
    Positive = above horizon; negative = below horizon.
    """
    try:
        from pysolar.solar import get_altitude
    except ImportError:
        raise ImportError(
            "pysolar is required for sun_elevation. "
            "Install with: pip install pysolar"
        )

    elevations = np.empty(len(index), dtype=np.float32)
    for i, ts in enumerate(index):
        # pysolar requires a timezone-aware datetime; pandas UTC timestamps
        # convert cleanly via .to_pydatetime()
        dt = ts.to_pydatetime()
        elevations[i] = get_altitude(lat, lon, dt)

    return pd.Series(elevations, index=index, name="sun_elevation")


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_COMPUTE: dict[str, Callable] = {
    "sun_elevation": _compute_sun_elevation,
}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compute_derived(
    df: pd.DataFrame,
    derived_names: list[str],
    lat: float,
    lon: float,
) -> pd.DataFrame:
    """
    Add derived variable columns to *df* and return the extended DataFrame.

    Parameters
    ----------
    df           : 1-hour interpolated DataFrame from zarr_query (not modified in place).
    derived_names: Names of derived variables to compute.
    lat, lon     : Coordinates of the nearest grid point (±180 lon).

    Returns
    -------
    DataFrame with the same index and additional columns for each derived variable.
    Unrecognised names are logged and skipped.
    """
    result = df.copy()

    for name in derived_names:
        if name not in _COMPUTE:
            log.warning(f"No computation function for derived variable '{name}' — skipping")
            continue
        if name in result.columns:
            log.debug(f"Derived variable '{name}' already present — skipping")
            continue

        log.debug(f"Computing derived variable: {name}")
        try:
            if name == "sun_elevation":
                result[name] = _compute_sun_elevation(result.index, lat, lon)
            else:
                # Generic path for future derived variables that use native columns
                reqs = DERIVED_REQUIREMENTS.get(name, [])
                missing = [r for r in reqs if r not in result.columns]
                if missing:
                    log.warning(
                        f"Derived variable '{name}' requires {missing} "
                        f"which are not in the DataFrame — skipping"
                    )
                    continue
                result[name] = _COMPUTE[name](result, lat, lon)
        except Exception as exc:
            log.error(f"Error computing '{name}': {exc}")

    return result
