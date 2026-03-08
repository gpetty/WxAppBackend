"""
slab_query — point time-series extraction from the NBM slab ring buffer.

Public API::

    df, actual_lat, actual_lon = query_forecast(
        store=store,
        lat_grid=lat_grid,
        lon_grid=lon_grid,
        lat=43.07, lon=-89.40,
        variables=["temperature", "wind_speed", "wind_direction"],
        registry=registry,
    )

Interpolation strategy
----------------------
Most variables
    Linear interpolation (``pandas.Series.interpolate(method='time')``).
    Does NOT extrapolate past the last valid value, so NaN beyond hard fxx
    cutoffs (visibility, ceiling, thunderstorm) propagates naturally.

``wind_direction``
    Circular interpolation: decompose to sin/cos, interpolate each linearly,
    reconstruct via atan2.  Handles the 350°→10° wraparound correctly.

``precip_type``
    Forward-fill: precipitation type persists until the next reported change.

Longitude convention
--------------------
NBM grid uses 0–360 east-positive longitude.  lat_grid/lon_grid carry this
convention.  The client passes ±180 longitude; conversion is handled internally.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

from ..registry import VariableRegistry, NativeVariable, DerivedVariable
from ..store import NBMStore

log = logging.getLogger(__name__)

_CIRCULAR_VARS    = {"wind_direction"}
_CATEGORICAL_VARS = {"precip_type"}


# ---------------------------------------------------------------------------
# Grid point lookup
# ---------------------------------------------------------------------------

def find_nearest_grid_point(
    lat_grid: np.ndarray,
    lon_grid: np.ndarray,
    lat: float,
    lon: float,
) -> tuple[int, int, float, float]:
    """
    Find the NBM grid point nearest to (lat, lon).

    Parameters
    ----------
    lat_grid : 2-D ndarray of shape (idim, jdim), NBM latitudes.
    lon_grid : 2-D ndarray of shape (idim, jdim), NBM longitudes (0–360).
    lat      : decimal degrees, -90 to +90.
    lon      : decimal degrees, -180 to +180.

    Returns
    -------
    i_idx, j_idx  : integer row/col indices into the (idim, jdim) grid.
    actual_lat    : latitude of the matched grid point.
    actual_lon    : longitude of the matched grid point (±180 convention).
    """
    lon_360 = lon + 360.0 if lon < 0.0 else float(lon)
    dist2 = (lat_grid - lat) ** 2 + (lon_grid - lon_360) ** 2
    i_idx, j_idx = np.unravel_index(int(np.argmin(dist2)), dist2.shape)
    i_idx, j_idx = int(i_idx), int(j_idx)

    actual_lat     = float(lat_grid[i_idx, j_idx])
    actual_lon_360 = float(lon_grid[i_idx, j_idx])
    actual_lon     = actual_lon_360 - 360.0 if actual_lon_360 > 180.0 else actual_lon_360

    log.debug(
        f"Grid lookup: ({lat:.4f}, {lon:.4f}) → "
        f"({actual_lat:.4f}, {actual_lon:.4f}) [i={i_idx}, j={j_idx}]"
    )
    return i_idx, j_idx, actual_lat, actual_lon


# ---------------------------------------------------------------------------
# Interpolation helpers
# ---------------------------------------------------------------------------

def _interp_circular(series: pd.Series) -> pd.Series:
    """
    Interpolate a wind direction series (degrees) with correct 0/360 wraparound.

    Decomposes into sin and cos components, interpolates each with
    pandas method='time', then reconstructs the angle via atan2.
    """
    rad = np.radians(series.values.astype(float))
    sin_s = pd.Series(np.sin(rad), index=series.index)
    cos_s = pd.Series(np.cos(rad), index=series.index)

    sin_i = sin_s.interpolate(method="time")
    cos_i = cos_s.interpolate(method="time")

    degrees = np.degrees(np.arctan2(sin_i.values, cos_i.values)) % 360.0
    return pd.Series(degrees, index=series.index, name=series.name, dtype=np.float32)


def _upsample_to_hourly(
    raw_df: pd.DataFrame,
    slab_valid_times: pd.DatetimeIndex,
) -> pd.DataFrame:
    """
    Upsample the irregular NBM time series to a uniform 1-hour grid.

    Parameters
    ----------
    raw_df           : DataFrame indexed by NBM valid_times (irregular spacing).
    slab_valid_times : The original slab valid times, used to mark which output
                       rows are interpolated vs native.
    """
    hourly_index = pd.date_range(
        start=raw_df.index[0],
        end=raw_df.index[-1],
        freq="1h",
        tz="UTC",
    )

    interpolated_mask = ~hourly_index.isin(slab_valid_times)
    df = raw_df.reindex(hourly_index)

    result_cols: dict[str, pd.Series] = {}
    for col in df.columns:
        if col in _CIRCULAR_VARS:
            result_cols[col] = _interp_circular(df[col])
        elif col in _CATEGORICAL_VARS:
            result_cols[col] = df[col].ffill()
        else:
            result_cols[col] = df[col].interpolate(method="time")

    result = pd.DataFrame(result_cols, index=hourly_index)
    result["interpolated"] = interpolated_mask
    return result


# ---------------------------------------------------------------------------
# Public query function
# ---------------------------------------------------------------------------

def query_forecast(
    store:     NBMStore,
    lat_grid:  np.ndarray,
    lon_grid:  np.ndarray,
    lat:       float,
    lon:       float,
    variables: list[str],
    registry:  VariableRegistry,
    start:     Optional[pd.Timestamp] = None,
    end:       Optional[pd.Timestamp] = None,
    cycle_tag: Optional[str] = None,
) -> tuple[pd.DataFrame, float, float]:
    """
    Extract a 1-hour resolution forecast time series from the slab ring buffer.

    Parameters
    ----------
    store     : Open NBMStore.
    lat_grid  : 2-D lat array of shape (idim, jdim) (from store lat.npy).
    lon_grid  : 2-D lon array of shape (idim, jdim), 0–360 (from store lon.npy).
    lat       : Requested latitude (decimal degrees).
    lon       : Requested longitude (decimal degrees, ±180).
    variables : Variable names to return.  Mix of native and derived is supported.
    registry  : VariableRegistry loaded from variables.yaml.
    start     : Optional inclusive start filter (timezone-aware or naive UTC).
    end       : Optional inclusive end filter.
    cycle_tag : Optional cycle identifier (e.g. "20260307_12") for a specific
                historical run.  None = current (newest) run.

    Returns
    -------
    df         : DataFrame, hourly UTC DatetimeIndex, columns = requested variables
                 + ``interpolated`` (bool).
    actual_lat : Latitude of the nearest NBM grid point.
    actual_lon : Longitude of the nearest NBM grid point (±180).

    Raises
    ------
    ValueError  if no valid variables are requested.
    KeyError    if cycle_tag is not found in the ring buffer.
    RuntimeError if the store has no ingested runs yet.
    """
    # --- Classify requested variables ---
    native_names  = [v for v in variables if isinstance(registry.get(v), NativeVariable)]
    derived_names = [v for v in variables if isinstance(registry.get(v), DerivedVariable)]
    unknown_names = [v for v in variables
                     if registry.get(v) is None and v not in ("interpolated",)]
    if unknown_names:
        log.warning(f"Unknown variables (ignored): {unknown_names}")

    if not native_names and not derived_names:
        raise ValueError(f"No recognised variables in request: {variables}")

    # --- Find nearest grid point ---
    i_idx, j_idx, actual_lat, actual_lon = find_nearest_grid_point(
        lat_grid, lon_grid, lat, lon
    )

    # --- All native columns needed (own + derived requirements) ---
    from .derived import DERIVED_REQUIREMENTS
    extra_for_derived: set[str] = set()
    for dv in derived_names:
        for req in DERIVED_REQUIREMENTS.get(dv, []):
            if isinstance(registry.get(req), NativeVariable):
                extra_for_derived.add(req)
    extra_for_derived -= set(native_names)

    all_native = native_names + sorted(extra_for_derived)

    # --- Extract raw time series from slab store ---
    values, valid_time_strs = store.get_point_timeseries(i_idx, j_idx, cycle_tag=cycle_tag)
    # values: shape (kvars, n_fxx), float32; valid_time_strs: list[str] ISO-8601

    slab_valid_times = pd.to_datetime(valid_time_strs, utc=True)

    var_to_k = {name: k for k, name in enumerate(store.variable_names)}
    cols: dict[str, np.ndarray] = {}
    for var in all_native:
        k = var_to_k.get(var)
        if k is None:
            log.warning(f"'{var}' not found in slab store — skipping")
            continue
        cols[var] = values[k]   # shape (n_fxx,), float32

    if not cols:
        raise ValueError(
            f"None of the requested variables found in slab store: {variables}"
        )

    raw_df = pd.DataFrame(cols, index=slab_valid_times)

    # --- Upsample to uniform 1-hour grid ---
    df = _upsample_to_hourly(raw_df, slab_valid_times)

    # --- Compute derived variables ---
    if derived_names:
        from .derived import compute_derived
        df = compute_derived(df, derived_names, actual_lat, actual_lon)

    # --- Drop extra native columns fetched only for derived computation ---
    cols_to_drop = [v for v in extra_for_derived if v not in variables]
    df = df.drop(columns=cols_to_drop, errors="ignore")

    # --- Apply time window ---
    def _to_utc(ts: pd.Timestamp) -> pd.Timestamp:
        return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")

    if start is not None:
        df = df[df.index >= _to_utc(pd.Timestamp(start))]
    if end is not None:
        df = df[df.index <= _to_utc(pd.Timestamp(end))]

    # --- Order columns: requested first, then 'interpolated' ---
    present = [v for v in variables if v in df.columns]
    df = df[present + ["interpolated"]]

    return df, actual_lat, actual_lon
