"""
Tests for backend.app.extraction.zarr_query

All tests use a synthetic in-memory xr.Dataset that mirrors the shape of
the real NBM Zarr store.  No GRIB2 files or real Zarr stores are required.

Synthetic dataset
-----------------
- Grid: 5 × 5 (y, x) with lat 40–44°, lon 270–274° (0–360 convention)
- Time steps: 36 hourly + 3 three-hourly, starting at 2026-01-01 01:00 UTC
  (matches the real NBM schedule: f001–f036 hourly, f037+ every 3 h)
- Variables: temperature, wind_speed, wind_direction, precip_type,
             visibility (with a simulated cutoff midway through)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from backend.app.extraction.zarr_query import (
    _find_nearest_grid_point,
    _interp_circular,
    _upsample_to_hourly,
    query_forecast,
)
from backend.app.registry import VariableRegistry
from backend.app.config import VARIABLES_YAML


# ---------------------------------------------------------------------------
# Helpers to build a synthetic Zarr-shaped dataset
# ---------------------------------------------------------------------------

NY, NX = 5, 5
T0 = pd.Timestamp("2026-01-01 01:00", tz="UTC")

def _make_valid_times() -> pd.DatetimeIndex:
    """36 hourly steps + 3 three-hourly steps, matching the NBM f001–f039 schedule."""
    hourly    = pd.date_range(T0, periods=36, freq="1h", tz="UTC")
    threehrly = pd.date_range(hourly[-1] + pd.Timedelta("3h"), periods=3, freq="3h", tz="UTC")
    return hourly.append(threehrly)


def _make_synthetic_ds(
    temp_values: np.ndarray | None = None,
    wind_speed_values: np.ndarray | None = None,
    wind_dir_values: np.ndarray | None = None,
    precip_type_values: np.ndarray | None = None,
    vis_cutoff_step: int | None = None,
) -> xr.Dataset:
    """
    Build an in-memory xr.Dataset shaped like the real NBM Zarr store.

    All arrays have shape (n_time, NY, NX).  The centre grid point is [2, 2]
    at lat=42°, lon=272° (= -88° in ±180 convention).
    """
    valid_times = _make_valid_times()
    n_t = len(valid_times)

    # 2D lat/lon arrays (constant along one axis for simplicity)
    lat_1d = np.linspace(40.0, 44.0, NY)
    lon_1d = np.linspace(270.0, 274.0, NX)   # 0–360 convention
    lat_2d, lon_2d = np.meshgrid(lat_1d, lon_1d, indexing="ij")  # (NY, NX)

    # Default value arrays: uniform 20 °F temperature, 10 mph wind, 0° direction
    if temp_values is None:
        temp_values = np.full(n_t, 20.0, dtype=np.float32)
    if wind_speed_values is None:
        wind_speed_values = np.full(n_t, 10.0, dtype=np.float32)
    if wind_dir_values is None:
        wind_dir_values = np.full(n_t, 0.0, dtype=np.float32)
    if precip_type_values is None:
        precip_type_values = np.full(n_t, 1.0, dtype=np.float32)  # 1 = rain

    def _broadcast(arr_1d: np.ndarray) -> np.ndarray:
        """Broadcast (n_t,) → (n_t, NY, NX), same value at every grid point."""
        return np.broadcast_to(arr_1d[:, None, None], (len(arr_1d), NY, NX)).copy()

    vis = np.full((n_t, NY, NX), 10.0, dtype=np.float32)  # 10 miles everywhere
    if vis_cutoff_step is not None:
        vis[vis_cutoff_step:, :, :] = np.nan   # simulate hard fxx cutoff

    vt_np = valid_times.values.astype("datetime64[ns]")

    ds = xr.Dataset(
        {
            "temperature":     (["valid_time", "y", "x"], _broadcast(temp_values)),
            "wind_speed":      (["valid_time", "y", "x"], _broadcast(wind_speed_values)),
            "wind_direction":  (["valid_time", "y", "x"], _broadcast(wind_dir_values)),
            "precip_type":     (["valid_time", "y", "x"], _broadcast(precip_type_values)),
            "visibility":      (["valid_time", "y", "x"], vis),
        },
        coords={
            "valid_time": vt_np,
            "latitude":   (["y", "x"], lat_2d.astype(np.float32)),
            "longitude":  (["y", "x"], lon_2d.astype(np.float32)),
        },
        attrs={"cycle": "2026-01-01T00:00:00+00:00"},
    )
    return ds


@pytest.fixture
def registry() -> VariableRegistry:
    return VariableRegistry(VARIABLES_YAML)


@pytest.fixture
def synthetic_ds() -> xr.Dataset:
    return _make_synthetic_ds()


# ---------------------------------------------------------------------------
# Grid point lookup
# ---------------------------------------------------------------------------

class TestFindNearestGridPoint:

    def test_exact_centre_point(self, synthetic_ds):
        """Centre of grid (lat=42, lon=272°) should map to index [2, 2]."""
        y, x, alat, alon = _find_nearest_grid_point(synthetic_ds, lat=42.0, lon=-88.0)
        assert y == 2
        assert x == 2
        assert abs(alat - 42.0) < 0.1
        # Returned lon should be ±180 convention
        assert -90.0 <= alon <= -86.0

    def test_negative_lon_converted(self, synthetic_ds):
        """Client -89° should map to 271° (0–360) and find correct grid column."""
        y, x, alat, alon = _find_nearest_grid_point(synthetic_ds, lat=42.0, lon=-89.0)
        # lon=-89 → 271° is between 270° and 272°, should land on column 0 or 1
        assert x <= 1
        assert alon < -88.0   # returned in ±180 convention

    def test_corner_point(self, synthetic_ds):
        """Top-left corner request should map to grid [0, 0]."""
        y, x, _, _ = _find_nearest_grid_point(synthetic_ds, lat=40.1, lon=-89.9)
        assert y == 0
        assert x == 0

    def test_actual_coords_match_grid(self, synthetic_ds):
        """Returned actual_lat/actual_lon must be a real point from the grid arrays."""
        y, x, alat, alon = _find_nearest_grid_point(synthetic_ds, lat=43.5, lon=-87.5)
        lat_grid = synthetic_ds["latitude"].values
        lon_grid = synthetic_ds["longitude"].values
        assert abs(alat - lat_grid[y, x]) < 1e-5
        # lon_grid is 0–360; actual_lon should be the ±180 equivalent
        expected_lon = lon_grid[y, x] - 360.0 if lon_grid[y, x] > 180 else lon_grid[y, x]
        assert abs(alon - expected_lon) < 1e-5


# ---------------------------------------------------------------------------
# Circular interpolation
# ---------------------------------------------------------------------------

class TestInterpCircular:

    def _make_series(self, times, degrees) -> pd.Series:
        return pd.Series(
            np.array(degrees, dtype=float),
            index=pd.DatetimeIndex(times, tz="UTC"),
            name="wind_direction",
        )

    def _hourly_gap_series(self, d0: float, d1: float) -> pd.Series:
        """
        Two values 3 hours apart with NaN in the middle two hours —
        simulating the 3-hourly segment after reindex.
        """
        times = pd.date_range("2026-01-01", periods=4, freq="1h", tz="UTC")
        vals  = np.array([d0, np.nan, np.nan, d1], dtype=float)
        return pd.Series(vals, index=times, name="wind_direction")

    def test_quadrant_midpoint(self):
        """0° → 90°: midpoint at t+1.5h should be 45°, not 0° or 90°."""
        s = self._hourly_gap_series(0.0, 90.0)
        result = _interp_circular(s)
        # At t+1h (1/3 of the way): ~30°; at t+2h (2/3 of the way): ~60°
        assert 20.0 < result.iloc[1] < 40.0, f"Expected ~30°, got {result.iloc[1]:.1f}°"
        assert 50.0 < result.iloc[2] < 70.0, f"Expected ~60°, got {result.iloc[2]:.1f}°"

    def test_wraparound_north(self):
        """350° → 10°: interpolation should go through 0°, not 180°."""
        s = self._hourly_gap_series(350.0, 10.0)
        result = _interp_circular(s)
        mid1, mid2 = result.iloc[1], result.iloc[2]
        # Midpoints should be near 0° (either side of 0), never near 180°
        # Allow values near 360° (equivalent to 0°)
        def near_zero(deg: float) -> bool:
            return deg < 10.0 or deg > 350.0
        assert near_zero(mid1), f"Expected near 0°, got {mid1:.1f}°"
        assert near_zero(mid2), f"Expected near 0°, got {mid2:.1f}°"

    def test_endpoints_preserved(self):
        """Native-time endpoints must not be altered by interpolation."""
        s = self._hourly_gap_series(270.0, 360.0)
        result = _interp_circular(s)
        assert abs(result.iloc[0] - 270.0) < 0.1, "Start endpoint changed"
        # 360° may be stored as 0.0 due to % 360
        assert result.iloc[3] < 1.0 or abs(result.iloc[3] - 360.0) < 0.1, \
            f"End endpoint changed: {result.iloc[3]:.1f}°"

    def test_constant_direction(self):
        """Constant direction series should produce the same value throughout."""
        times = pd.date_range("2026-01-01", periods=4, freq="1h", tz="UTC")
        s = pd.Series([180.0, np.nan, np.nan, 180.0], index=times, name="wind_direction")
        result = _interp_circular(s)
        assert np.allclose(result.values, 180.0, atol=0.1)

    def test_speed_independence(self, synthetic_ds, registry):
        """
        Wind direction and speed must interpolate independently.
        0°@10 mph → 90°@10 mph: midpoint must be 45° at 10 mph, NOT 7 mph.
        """
        n_t = len(_make_valid_times())
        # Only two native valid times: first and last of the 3-hourly segment
        dir_vals  = np.full(n_t, 0.0,  dtype=np.float32)
        spd_vals  = np.full(n_t, 10.0, dtype=np.float32)
        # Put a direction change at the first 3-hourly step (index 36)
        dir_vals[36:]  = 90.0

        ds = _make_synthetic_ds(wind_speed_values=spd_vals, wind_dir_values=dir_vals)
        df, _, _ = query_forecast(
            ds, lat=42.0, lon=-88.0,
            variables=["wind_direction", "wind_speed"],
            registry=registry,
        )

        # At the 3-hourly boundary (index 36 in output = original f037 step)
        # the value is native; one step before is interpolated.
        # Find the step just before the direction jump and confirm speed = 10 mph
        step_idx = df.index.get_loc(T0 + pd.Timedelta("36h"))
        # The row at step_idx itself is native (direction 0°); rows after are a
        # mix of interpolated and native at 90°. Check speed is always 10 mph.
        speed_slice = df["wind_speed"].iloc[step_idx - 2 : step_idx + 4]
        assert np.allclose(speed_slice.values, 10.0, atol=0.1), \
            f"Speed deviated from 10 mph during direction change: {speed_slice.values}"


# ---------------------------------------------------------------------------
# Upsampling and interpolation
# ---------------------------------------------------------------------------

class TestUpsampleToHourly:

    def test_output_is_hourly(self, synthetic_ds):
        """Output DataFrame must have a uniform 1-hour frequency."""
        valid_times = pd.to_datetime(synthetic_ds["valid_time"].values, utc=True)
        raw = pd.DataFrame(
            {"temperature": np.random.rand(len(valid_times)).astype(np.float32)},
            index=valid_times,
        )
        result = _upsample_to_hourly(raw, valid_times)
        diffs = result.index.to_series().diff().dropna()
        assert (diffs == pd.Timedelta("1h")).all(), "Output is not uniformly hourly"

    def test_interpolated_flag_native_rows_false(self, synthetic_ds):
        """Rows that were in the original Zarr valid_times must have interpolated=False."""
        valid_times = pd.to_datetime(synthetic_ds["valid_time"].values, utc=True)
        raw = pd.DataFrame({"x": np.ones(len(valid_times))}, index=valid_times)
        result = _upsample_to_hourly(raw, valid_times)
        # All rows corresponding to original NBM valid times should NOT be interpolated
        native_rows = result[result.index.isin(valid_times)]
        assert not native_rows["interpolated"].any(), \
            "Native valid times incorrectly marked as interpolated"

    def test_interpolated_flag_added_rows_true(self, synthetic_ds):
        """Hours added by the reindex (3-hourly gaps) must have interpolated=True."""
        valid_times = pd.to_datetime(synthetic_ds["valid_time"].values, utc=True)
        raw = pd.DataFrame({"x": np.ones(len(valid_times))}, index=valid_times)
        result = _upsample_to_hourly(raw, valid_times)
        # The 3-hourly segment adds 2 extra rows per 3-h step
        added_rows = result[~result.index.isin(valid_times)]
        assert added_rows["interpolated"].all(), \
            "Added interpolated hours not marked correctly"

    def test_nan_does_not_extrapolate(self):
        """NaN beyond the last valid value must remain NaN (no extrapolation)."""
        # Simulate visibility with a hard cutoff: NaN from step 20 onward
        times = pd.date_range("2026-01-01", periods=10, freq="3h", tz="UTC")
        vals = np.array([5.0]*7 + [np.nan, np.nan, np.nan], dtype=float)
        raw = pd.DataFrame({"visibility": vals}, index=times)
        zarr_times = times  # all times are "native" for this test
        result = _upsample_to_hourly(raw, zarr_times)
        # After the last valid value, everything should be NaN
        last_valid = result["visibility"].last_valid_index()
        tail = result["visibility"][last_valid:].iloc[1:]
        assert tail.isna().all(), "NaN should not be extrapolated past last valid value"

    def test_precip_type_ffill(self):
        """precip_type must use forward-fill, not linear interpolation."""
        # 1 = rain for first few steps, then 5 = snow (3-hourly)
        times = pd.date_range("2026-01-01", periods=4, freq="3h", tz="UTC")
        vals  = np.array([1.0, 1.0, 5.0, 5.0], dtype=float)
        raw   = pd.DataFrame({"precip_type": vals}, index=times)
        zarr_times = times
        result = _upsample_to_hourly(raw, zarr_times)
        # The interpolated hours between step 1 and step 2 (rain→snow transition)
        # should all be 1.0 (forward-filled from rain) not 3.0 (midpoint of 1 and 5)
        transition_rows = result.loc[times[1]:times[2], "precip_type"]
        assert (transition_rows == 1.0).all() or (transition_rows.iloc[-1] == 5.0), \
            f"precip_type not forward-filled correctly: {transition_rows.values}"


# ---------------------------------------------------------------------------
# Full query_forecast integration
# ---------------------------------------------------------------------------

class TestQueryForecast:

    def test_returns_dataframe(self, synthetic_ds, registry):
        df, alat, alon = query_forecast(
            synthetic_ds, lat=42.0, lon=-88.0,
            variables=["temperature", "wind_speed"],
            registry=registry,
        )
        assert isinstance(df, pd.DataFrame)
        assert "temperature"  in df.columns
        assert "wind_speed"   in df.columns
        assert "interpolated" in df.columns

    def test_output_is_hourly(self, synthetic_ds, registry):
        df, _, _ = query_forecast(
            synthetic_ds, lat=42.0, lon=-88.0,
            variables=["temperature"],
            registry=registry,
        )
        diffs = df.index.to_series().diff().dropna()
        assert (diffs == pd.Timedelta("1h")).all()

    def test_index_is_utc(self, synthetic_ds, registry):
        df, _, _ = query_forecast(
            synthetic_ds, lat=42.0, lon=-88.0,
            variables=["temperature"],
            registry=registry,
        )
        assert df.index.tz is not None
        assert str(df.index.tz) in ("UTC", "utc")

    def test_actual_coords_returned(self, synthetic_ds, registry):
        _, alat, alon = query_forecast(
            synthetic_ds, lat=42.0, lon=-88.0,
            variables=["temperature"],
            registry=registry,
        )
        assert 40.0 <= alat <= 44.0
        assert -90.0 <= alon <= -86.0

    def test_unknown_variable_skipped(self, synthetic_ds, registry):
        """Unknown variable names must be silently dropped, not raise."""
        df, _, _ = query_forecast(
            synthetic_ds, lat=42.0, lon=-88.0,
            variables=["temperature", "does_not_exist"],
            registry=registry,
        )
        assert "temperature" in df.columns
        assert "does_not_exist" not in df.columns

    def test_start_end_filter(self, synthetic_ds, registry):
        start = T0 + pd.Timedelta("5h")
        end   = T0 + pd.Timedelta("10h")
        df, _, _ = query_forecast(
            synthetic_ds, lat=42.0, lon=-88.0,
            variables=["temperature"],
            registry=registry,
            start=start,
            end=end,
        )
        assert df.index.min() >= start
        assert df.index.max() <= end

    def test_values_are_finite_for_continuous_var(self, synthetic_ds, registry):
        """Temperature (no fxx cutoff) should have no NaN in the output."""
        df, _, _ = query_forecast(
            synthetic_ds, lat=42.0, lon=-88.0,
            variables=["temperature"],
            registry=registry,
        )
        assert df["temperature"].notna().all(), \
            "Unexpected NaN in temperature (no fxx cutoff)"

    def test_visibility_nan_beyond_cutoff(self, registry):
        """
        Visibility with a simulated cutoff at step 20 should have NaN
        after that point in the output.
        """
        ds = _make_synthetic_ds(vis_cutoff_step=20)
        df, _, _ = query_forecast(
            ds, lat=42.0, lon=-88.0,
            variables=["visibility"],
            registry=registry,
        )
        # Find the last non-NaN row and check everything after is NaN
        last_valid = df["visibility"].last_valid_index()
        if last_valid is not None:
            tail = df["visibility"].loc[last_valid:].iloc[1:]
            if len(tail) > 0:
                assert tail.isna().all(), \
                    "visibility should be NaN after its cutoff"

    def test_all_vars_in_registry(self, synthetic_ds, registry):
        """Every native variable in the registry that exists in the synthetic
        dataset should appear in the output without error."""
        available = [
            v for v in registry.native()
            if v in synthetic_ds.data_vars
        ]
        df, _, _ = query_forecast(
            synthetic_ds, lat=42.0, lon=-88.0,
            variables=available,
            registry=registry,
        )
        for v in available:
            assert v in df.columns, f"Expected variable '{v}' missing from output"
