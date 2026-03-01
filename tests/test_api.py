"""
Tests for the FastAPI application (Phase 1.7) — updated for compact
parallel-array response format with unit-suffixed keys.

Uses FastAPI's TestClient with a synthetic in-memory Zarr store written
to a temp directory — no real NBM data or network access required.

The synthetic dataset mirrors test_zarr_query.py: 5×5 grid, 36 hourly
+ 3 three-hourly steps, centre point at lat=42°, lon=272° (=-88°).
"""

from __future__ import annotations

import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.config import VARIABLES_YAML
from backend.app.registry import VariableRegistry


# ---------------------------------------------------------------------------
# Synthetic store fixture
# ---------------------------------------------------------------------------

NY, NX = 5, 5
T0 = pd.Timestamp("2026-01-01 01:00", tz="UTC")


def _make_valid_times() -> pd.DatetimeIndex:
    hourly    = pd.date_range(T0, periods=36, freq="1h", tz="UTC")
    threehrly = pd.date_range(hourly[-1] + pd.Timedelta("3h"), periods=3, freq="3h", tz="UTC")
    return hourly.append(threehrly)


def _write_synthetic_zarr(tmp_path: Path) -> Path:
    """
    Write a minimal Zarr store shaped like the real NBM output.
    Returns the path to the written store.
    """
    valid_times = _make_valid_times()
    n_t = len(valid_times)

    lat_1d = np.linspace(40.0, 44.0, NY)
    lon_1d = np.linspace(270.0, 274.0, NX)
    lat_2d, lon_2d = np.meshgrid(lat_1d, lon_1d, indexing="ij")

    def _block(val: float) -> np.ndarray:
        return np.full((n_t, NY, NX), val, dtype=np.float32)

    ds = xr.Dataset(
        {
            "temperature":         (["valid_time", "y", "x"], _block(55.0)),
            "wind_speed":          (["valid_time", "y", "x"], _block(12.0)),
            "wind_direction":      (["valid_time", "y", "x"], _block(270.0)),
            "apparent_temperature":(["valid_time", "y", "x"], _block(50.0)),
            "relative_humidity":   (["valid_time", "y", "x"], _block(65.0)),
            "cloud_cover":         (["valid_time", "y", "x"], _block(40.0)),
            "total_precipitation": (["valid_time", "y", "x"], _block(0.5)),
            "precip_type":         (["valid_time", "y", "x"], _block(1.0)),
        },
        coords={
            "valid_time": valid_times.values.astype("datetime64[ns]"),
            "latitude":   (["y", "x"], lat_2d.astype(np.float32)),
            "longitude":  (["y", "x"], lon_2d.astype(np.float32)),
        },
        attrs={
            "cycle":   "2026-01-01T00:00:00+00:00",
            "created": "2026-01-01T01:30:00+00:00",
        },
    )

    zarr_path = tmp_path / "current"
    ds.to_zarr(str(zarr_path), mode="w")
    return zarr_path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def tmp_store(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("zarr")
    return _write_synthetic_zarr(tmp)


@pytest.fixture(scope="module")
def client(tmp_store):
    """
    TestClient with the Zarr store and registry pre-loaded into app.state.
    We bypass the lifespan by setting app.state directly before creating
    the client.
    """
    from backend.app.extraction import open_zarr_store

    ds       = open_zarr_store(tmp_store)
    registry = VariableRegistry(VARIABLES_YAML)

    app.state.ds          = ds
    app.state.registry    = registry
    app.state.zarr_path   = tmp_store
    app.state.last_loaded = datetime.now(tz=timezone.utc)

    return TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# GET /variables
# ---------------------------------------------------------------------------

class TestVariablesEndpoint:

    def test_200(self, client):
        r = client.get("/variables")
        assert r.status_code == 200

    def test_has_native_and_derived(self, client):
        body = client.get("/variables").json()
        assert "native"  in body
        assert "derived" in body

    def test_temperature_in_native(self, client):
        body = client.get("/variables").json()
        assert "temperature" in body["native"]
        assert body["native"]["temperature"]["units"] == "F"

    def test_response_key_suffix_present(self, client):
        """Each variable entry must expose its response_key_suffix."""
        body = client.get("/variables").json()
        temp = body["native"]["temperature"]
        assert "response_key_suffix" in temp
        assert temp["response_key_suffix"] == "F"

    def test_wind_speed_suffix(self, client):
        body = client.get("/variables").json()
        ws = body["native"]["wind_speed"]
        assert ws["response_key_suffix"] == "mph"

    def test_sun_elevation_in_derived(self, client):
        body = client.get("/variables").json()
        assert "sun_elevation" in body["derived"]

    def test_no_heat_index(self, client):
        """heat_index and wind_chill were intentionally removed."""
        body = client.get("/variables").json()
        assert "heat_index"  not in body["derived"]
        assert "wind_chill"  not in body["derived"]


# ---------------------------------------------------------------------------
# GET /status
# ---------------------------------------------------------------------------

class TestStatusEndpoint:

    def test_200(self, client):
        assert client.get("/status").status_code == 200

    def test_fields_present(self, client):
        body = client.get("/status").json()
        assert "runtime"      in body
        assert "n_variables"  in body
        assert "n_time_steps" in body
        assert "last_loaded"  in body

    def test_runtime_is_utc_string(self, client):
        body = client.get("/status").json()
        rt = body["runtime"]
        assert rt is not None
        # Should be parseable as a timestamp
        pd.Timestamp(rt)

    def test_n_time_steps_positive(self, client):
        body = client.get("/status").json()
        assert body["n_time_steps"] > 0


# ---------------------------------------------------------------------------
# GET /forecast
# ---------------------------------------------------------------------------

class TestForecastEndpoint:

    BASE = {"lat": 42.0, "lon": -88.0, "vars": "temperature"}

    def test_200(self, client):
        r = client.get("/forecast", params=self.BASE)
        assert r.status_code == 200

    def test_top_level_structure(self, client):
        """Response must have runtime, latitude, longitude, length, times."""
        body = client.get("/forecast", params=self.BASE).json()
        assert "runtime"   in body
        assert "latitude"  in body
        assert "longitude" in body
        assert "length"    in body
        assert "times"     in body

    def test_no_old_keys(self, client):
        """Old field names must not appear in the new format."""
        body = client.get("/forecast", params=self.BASE).json()
        assert "cycle"      not in body
        assert "lat_actual" not in body
        assert "lon_actual" not in body
        assert "variables"  not in body   # was the old wrapper dict

    def test_temperature_key_is_suffixed(self, client):
        """Variable keys must carry the unit suffix: temperature_F, not temperature."""
        body = client.get("/forecast", params=self.BASE).json()
        assert "temperature_F" in body
        assert "temperature"   not in body   # bare name must NOT appear

    def test_temperature_array_is_list(self, client):
        body = client.get("/forecast", params=self.BASE).json()
        assert isinstance(body["temperature_F"], list)

    def test_length_matches_arrays(self, client):
        body = client.get("/forecast", params=self.BASE).json()
        n = body["length"]
        assert len(body["times"])         == n
        assert len(body["temperature_F"]) == n

    def test_times_are_utc_with_z_suffix(self, client):
        body = client.get("/forecast", params=self.BASE).json()
        for t in body["times"][:5]:
            assert t.endswith("Z"), f"Expected Z suffix on time string, got: {t!r}"

    def test_times_are_hourly(self, client):
        body = client.get("/forecast", params=self.BASE).json()
        times = body["times"]
        assert len(times) >= 2
        t0 = pd.Timestamp(times[0])
        t1 = pd.Timestamp(times[1])
        assert t1 - t0 == pd.Timedelta("1h")

    def test_no_interpolated_flag(self, client):
        """The 'interpolated' field must not appear anywhere in the response."""
        raw = client.get("/forecast", params=self.BASE).text
        assert "interpolated" not in raw

    def test_multiple_vars_suffixed_keys(self, client):
        r = client.get("/forecast", params={
            "lat": 42.0, "lon": -88.0,
            "vars": "temperature,wind_speed,cloud_cover",
        })
        assert r.status_code == 200
        body = r.json()
        assert "temperature_F"  in body
        assert "wind_speed_mph" in body
        assert "cloud_cover_pct" in body

    def test_multiple_vars_same_length(self, client):
        body = client.get("/forecast", params={
            "lat": 42.0, "lon": -88.0,
            "vars": "temperature,wind_speed",
        }).json()
        n = body["length"]
        assert len(body["temperature_F"])  == n
        assert len(body["wind_speed_mph"]) == n

    def test_lon_360_accepted(self, client):
        """Client may pass lon in 0–360 convention; API should normalise it."""
        r = client.get("/forecast", params={
            "lat": 42.0, "lon": 272.0,
            "vars": "temperature",
        })
        assert r.status_code == 200

    def test_actual_lat_lon_in_grid_range(self, client):
        body = client.get("/forecast", params=self.BASE).json()
        assert 40.0 <= body["latitude"]  <= 44.0
        assert -90.0 <= body["longitude"] <= -86.0

    def test_runtime_normalised(self, client):
        """runtime must be a UTC ISO-8601 string with Z suffix."""
        body = client.get("/forecast", params=self.BASE).json()
        rt = body["runtime"]
        assert isinstance(rt, str)
        assert rt.endswith("Z"), f"Expected Z suffix on runtime, got: {rt!r}"

    def test_unknown_var_returns_422(self, client):
        r = client.get("/forecast", params={
            "lat": 42.0, "lon": -88.0, "vars": "not_a_real_variable",
        })
        assert r.status_code == 422

    def test_start_end_filter(self, client):
        start = (T0 + pd.Timedelta("5h")).isoformat()
        end   = (T0 + pd.Timedelta("10h")).isoformat()
        body  = client.get("/forecast", params={
            "lat": 42.0, "lon": -88.0, "vars": "temperature",
            "start": start, "end": end,
        }).json()
        times = [pd.Timestamp(t) for t in body["times"]]
        assert times[0]  >= T0 + pd.Timedelta("5h")
        assert times[-1] <= T0 + pd.Timedelta("10h")

    def test_start_after_end_returns_422(self, client):
        r = client.get("/forecast", params={
            "lat": 42.0, "lon": -88.0, "vars": "temperature",
            "start": "2026-01-02T00:00:00Z",
            "end":   "2026-01-01T00:00:00Z",
        })
        assert r.status_code == 422

    def test_invalid_lat_returns_422(self, client):
        r = client.get("/forecast", params={"lat": 999, "lon": -88, "vars": "temperature"})
        assert r.status_code == 422

    def test_no_nan_in_json(self, client):
        """NaN must never appear literally in the JSON response — use null instead."""
        raw = client.get("/forecast", params=self.BASE).text
        assert "NaN"      not in raw, "NaN must not appear literally in JSON output"
        assert "Infinity" not in raw

    def test_values_are_float_or_null(self, client):
        body = client.get("/forecast", params=self.BASE).json()
        for v in body["temperature_F"]:
            assert v is None or isinstance(v, (int, float)), \
                f"Expected float or null, got {type(v)}: {v!r}"

    def test_temperature_rounded_to_1dp(self, client):
        """Temperature values must be rounded to 1 decimal place."""
        body = client.get("/forecast", params=self.BASE).json()
        for v in body["temperature_F"]:
            if v is not None:
                # Check that it has at most 1 decimal place
                s = f"{v}"
                if "." in s:
                    assert len(s.split(".")[1]) <= 1, \
                        f"temperature_F should be 1dp, got {v!r}"

    def test_cloud_cover_rounded_to_0dp(self, client):
        """Cloud cover (%) must be rounded to 0 decimal places (integers)."""
        body = client.get("/forecast", params={
            "lat": 42.0, "lon": -88.0, "vars": "cloud_cover",
        }).json()
        assert "cloud_cover_pct" in body
        for v in body["cloud_cover_pct"]:
            if v is not None:
                assert v == int(v), f"cloud_cover_pct should be integer-valued, got {v!r}"

    def test_precip_key_is_mm(self, client):
        """total_precipitation → total_precipitation_mm."""
        r = client.get("/forecast", params={
            "lat": 42.0, "lon": -88.0, "vars": "total_precipitation",
        })
        assert r.status_code == 200
        body = r.json()
        assert "total_precipitation_mm" in body

    def test_wind_direction_key_is_deg(self, client):
        r = client.get("/forecast", params={
            "lat": 42.0, "lon": -88.0, "vars": "wind_direction",
        })
        assert r.status_code == 200
        body = r.json()
        assert "wind_direction_deg" in body
