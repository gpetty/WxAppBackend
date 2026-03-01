"""
zarr_query — point time-series extraction from the NBM Zarr store.

Public API::

    ds = open_zarr_store(zarr_path)           # call once at application startup

    df, actual_lat, actual_lon = query_forecast(
        ds, lat=43.07, lon=-89.40,
        variables=["temperature", "wind_speed", "wind_direction"],
        registry=registry,
    )

Returned DataFrame
------------------
- Index   : hourly ``DatetimeIndex`` (UTC), spanning the full Zarr valid_time range.
- Columns : one per requested variable (native + derived).
- Column ``interpolated`` : bool — True for hours that were not direct NBM valid
  times (i.e. hours filled in by interpolation between 3-hourly or 6-hourly steps).

Interpolation strategy
----------------------
Most variables
    Linear interpolation (``pandas.Series.interpolate(method='time')``).
    Does NOT extrapolate past the last valid value, so NaN beyond hard fxx
    cutoffs (visibility f076, ceiling f082, thunderstorm f190) propagates
    naturally without special-casing.

``wind_direction``
    Circular interpolation: decompose to sin/cos, interpolate each linearly,
    reconstruct via atan2.  Handles the 350°→10° wraparound correctly
    (midpoint = 0°, not 180°).  Crucially, ``wind_speed`` is interpolated
    independently so its magnitude is fully preserved — a rotation from
    0°@10 mph to 90°@10 mph yields 45°@10 mph at the midpoint, not 7 mph.

``precip_type``
    Forward-fill: precipitation type persists until the next reported change.
    Arithmetic interpolation between categorical codes is meaningless.

Longitude convention
--------------------
NBM GRIB2 (and the Zarr store) uses 0–360 east-positive longitude.  The
client passes ±180 longitude; the conversion is handled internally.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import xarray as xr

from ..registry import VariableRegistry, NativeVariable, DerivedVariable

log = logging.getLogger(__name__)

# Variables that need non-standard interpolation
_CIRCULAR_VARS    = {"wind_direction"}
_CATEGORICAL_VARS = {"precip_type"}


# ---------------------------------------------------------------------------
# Store management
# ---------------------------------------------------------------------------

def open_zarr_store(zarr_path: Path | str) -> xr.Dataset:
    """
    Open the NBM Zarr store and return an xr.Dataset.

    Call this once at application startup and pass the result to every
    ``query_forecast()`` call.  Keeping the dataset open avoids re-reading
    chunk metadata on each request and allows the OS page cache to warm up
    across queries.

    Raises FileNotFoundError if the path does not exist.
    """
    zarr_path = Path(zarr_path)
    if not zarr_path.exists():
        raise FileNotFoundError(f"Zarr store not found: {zarr_path}")

    # Use consolidated metadata when available (written by grib2_to_zarr);
    # fall back gracefully for stores written without it (e.g. test fixtures).
    try:
        ds = xr.open_zarr(str(zarr_path), consolidated=True)
    except KeyError:
        ds = xr.open_zarr(str(zarr_path), consolidated=False)
    log.info(
        f"Opened Zarr store: {zarr_path}  "
        f"| {len(ds.data_vars)} variables  "
        f"| {ds.sizes.get('valid_time', '?')} time steps  "
        f"| cycle: {ds.attrs.get('cycle', 'unknown')}"
    )
    return ds


# ---------------------------------------------------------------------------
# Grid point lookup
# ---------------------------------------------------------------------------

def _find_nearest_grid_point(
    ds: xr.Dataset,
    lat: float,
    lon: float,
) -> tuple[int, int, float, float]:
    """
    Find the NBM grid point nearest to (lat, lon).

    Parameters
    ----------
    ds  : open Zarr Dataset with 2D 'latitude' and 'longitude' coordinates.
    lat : decimal degrees, -90 to +90.
    lon : decimal degrees, -180 to +180.

    Returns
    -------
    y_idx, x_idx   : integer indices into the (y, x) grid dimensions.
    actual_lat     : latitude of the matched grid point.
    actual_lon     : longitude of the matched grid point (±180 convention).

    Notes
    -----
    NBM CONUS longitude is stored 0–360; the client lon is converted before
    the distance calculation.  The returned actual_lon is converted back to
    ±180 for the API response.

    Squared Euclidean distance in (lat, lon) space is used.  At 2.5 km grid
    spacing the flat-earth approximation error (~0.3% at 50° latitude) is
    well under one grid cell and negligible for point queries.
    """
    lon_360 = lon + 360.0 if lon < 0.0 else float(lon)

    lat_grid = ds["latitude"].values    # shape (n_y, n_x)
    lon_grid = ds["longitude"].values   # shape (n_y, n_x), 0–360

    dist2 = (lat_grid - lat) ** 2 + (lon_grid - lon_360) ** 2
    y_idx, x_idx = np.unravel_index(int(np.argmin(dist2)), dist2.shape)
    y_idx, x_idx = int(y_idx), int(x_idx)

    actual_lat     = float(lat_grid[y_idx, x_idx])
    actual_lon_360 = float(lon_grid[y_idx, x_idx])
    actual_lon     = actual_lon_360 - 360.0 if actual_lon_360 > 180.0 else actual_lon_360

    log.debug(
        f"Grid lookup: ({lat:.4f}, {lon:.4f}) → "
        f"({actual_lat:.4f}, {actual_lon:.4f}) [y={y_idx}, x={x_idx}]"
    )
    return y_idx, x_idx, actual_lat, actual_lon


# ---------------------------------------------------------------------------
# Zarr point extraction
# ---------------------------------------------------------------------------

def _extract_point_series(
    ds: xr.Dataset,
    var_names: list[str],
    y_idx: int,
    x_idx: int,
) -> pd.DataFrame:
    """
    Extract a time series at grid point (y_idx, x_idx) for each variable.

    Returns a DataFrame indexed by the Zarr valid_time coordinate (UTC),
    with one column per variable.  Time steps are the raw NBM valid times:
    hourly for f001–f036, then every 3 h, then every 6 h — irregular spacing.
    """
    valid_times = pd.to_datetime(ds["valid_time"].values, utc=True)

    cols: dict[str, np.ndarray] = {}
    for var in var_names:
        if var not in ds.data_vars:
            log.warning(f"'{var}' not found in Zarr store — skipping")
            continue
        cols[var] = ds[var].values[:, y_idx, x_idx]   # (n_time,) float32

    if not cols:
        raise ValueError(
            f"None of the requested variables found in Zarr store: {var_names}"
        )

    return pd.DataFrame(cols, index=valid_times)


# ---------------------------------------------------------------------------
# Interpolation helpers
# ---------------------------------------------------------------------------

def _interp_circular(series: pd.Series) -> pd.Series:
    """
    Interpolate a wind direction series (degrees) with correct 0/360 wraparound.

    Decomposes into sin and cos components, interpolates each with
    pandas method='time', then reconstructs the angle via atan2.

    Result: the direction interpolates along the shortest arc, e.g.:
        350° → 10°  midpoint = 0°   (not 180°)
          0° → 90°  midpoint = 45°

    Magnitude (wind speed) is unaffected because it is interpolated
    independently as a separate column.
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
    zarr_valid_times: pd.DatetimeIndex,
) -> pd.DataFrame:
    """
    Upsample the irregular NBM time series to a uniform 1-hour grid.

    Parameters
    ----------
    raw_df           : DataFrame with NBM valid_times as index (irregular).
    zarr_valid_times : The full valid_time coordinate from the Zarr store,
                       used to mark which output rows are interpolated vs native.

    Steps
    -----
    1. Build a uniform hourly DatetimeIndex spanning raw_df.
    2. Record which of those hours are NOT in the Zarr valid_times
       (these are the interpolated rows).
    3. Reindex raw_df to the hourly grid (inserts NaN at new rows).
    4. Interpolate each column with the variable-appropriate method.
    5. Attach the ``interpolated`` boolean column.

    NaN propagation
    ---------------
    pandas ``interpolate(method='time')`` never extrapolates beyond the
    last non-NaN value.  Variables with hard fxx cutoffs (e.g. visibility
    ends at f076) therefore have NaN in the output from their last valid
    time onward — no special-casing required.
    """
    hourly_index = pd.date_range(
        start=raw_df.index[0],
        end=raw_df.index[-1],
        freq="1h",
        tz="UTC",
    )

    # Mark added rows (not native NBM valid times) BEFORE filling
    interpolated_mask = ~hourly_index.isin(zarr_valid_times)

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
    ds: xr.Dataset,
    lat: float,
    lon: float,
    variables: list[str],
    registry: VariableRegistry,
    start: Optional[pd.Timestamp] = None,
    end: Optional[pd.Timestamp] = None,
) -> tuple[pd.DataFrame, float, float]:
    """
    Extract a 1-hour resolution forecast time series for the nearest grid point.

    Parameters
    ----------
    ds        : Open Zarr Dataset from ``open_zarr_store()``.
    lat       : Requested latitude (decimal degrees).
    lon       : Requested longitude (decimal degrees, ±180).
    variables : Variable names to return.  Mix of native and derived names is
                supported.  Unknown names are logged and omitted from the result.
    registry  : VariableRegistry loaded from variables.yaml.
    start     : Optional inclusive start filter (timezone-aware or naive UTC).
    end       : Optional inclusive end filter (timezone-aware or naive UTC).

    Returns
    -------
    df         : DataFrame, hourly UTC DatetimeIndex, columns = requested variables
                 + ``interpolated`` (bool).
    actual_lat : Latitude of the nearest NBM grid point.
    actual_lon : Longitude of the nearest NBM grid point (±180).

    Raises
    ------
    ValueError  if no valid variables are requested.
    FileNotFoundError (propagated from open_zarr_store) if store is missing.
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
    y_idx, x_idx, actual_lat, actual_lon = _find_nearest_grid_point(ds, lat, lon)

    # --- Identify all native columns needed (own + derived requirements) ---
    from .derived import DERIVED_REQUIREMENTS
    extra_for_derived: set[str] = set()
    for dv in derived_names:
        for req in DERIVED_REQUIREMENTS.get(dv, []):
            if isinstance(registry.get(req), NativeVariable):
                extra_for_derived.add(req)
    extra_for_derived -= set(native_names)

    all_native = native_names + sorted(extra_for_derived)

    # --- Extract raw irregular time series from Zarr ---
    zarr_valid_times = pd.to_datetime(ds["valid_time"].values, utc=True)
    raw_df = _extract_point_series(ds, all_native, y_idx, x_idx)

    # --- Upsample to uniform 1-hour grid ---
    df = _upsample_to_hourly(raw_df, zarr_valid_times)

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
    present    = [v for v in variables if v in df.columns]
    df = df[present + ["interpolated"]]

    return df, actual_lat, actual_lon
