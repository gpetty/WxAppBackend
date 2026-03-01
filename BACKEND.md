# BACKEND: Weather Window API Reference

*Last updated: 2026-03-01*

This document is the primary reference for building the frontend.
It describes the live API: endpoints, request parameters, response shapes, variable names, and error handling.
Backend operational details (ingestion pipeline, server config) are in a separate section at the bottom.

---

## Base URL

```
https://precip.aos.wisc.edu/wxapp
```

CORS is open to all origins (prototype). All endpoints are GET except the internal `/admin/reload`.

OpenAPI/Swagger docs: `https://precip.aos.wisc.edu/wxapp/docs`

---

## Quick Start

```
GET /wxapp/variables                         → discover variable names + response key suffixes
GET /wxapp/status                            → current cycle time, available historical cycles
GET /wxapp/forecast?lat=43.07&lon=-89.4&vars=temperature,wind_speed
GET /wxapp/forecast?lat=43.07&lon=-89.4&vars=temperature&age_hours=6   → 6h-old cycle (drift)
```

---

## Endpoints

---

### `GET /variables`

Returns the complete variable registry. Call this once on startup to build your variable picker UI and to discover the exact response key suffix for each variable.

**No parameters.**

**Response:**

```json
{
  "native": {
    "temperature": {
      "units": "F",
      "response_key_suffix": "F",
      "description": "2-metre air temperature",
      "grib_shortName": "2t",
      "fxx_cutoff": null
    },
    "wind_speed": {
      "units": "mph",
      "response_key_suffix": "mph",
      "description": "10-metre wind speed",
      "grib_shortName": "10si",
      "fxx_cutoff": null
    },
    "visibility": {
      "units": "miles",
      "response_key_suffix": "mi",
      "description": "Horizontal visibility at surface",
      "grib_shortName": "vis",
      "fxx_cutoff": 76
    },
    ...
  },
  "derived": {
    "sun_elevation": {
      "units": "degrees",
      "response_key_suffix": "deg",
      "description": "Solar elevation angle above horizon (astronomical, no atmospheric correction)",
      "requires": ["lat", "lon", "valid_time"]
    }
  }
}
```

**Building a response key from the registry:**
The `/forecast` response uses `{variable_name}_{response_key_suffix}` as its key.
Example: variable `temperature`, suffix `F` → response key `temperature_F`.
If `response_key_suffix` is empty, the response key is just the variable name.

**`fxx_cutoff`:** variables with a non-null cutoff return `null` beyond that forecast hour. The frontend should treat `null` as "no forecast available" for that time step (not zero).

---

### `GET /status`

Returns the current cycle time and the list of all retained historical cycles available for drift queries.

**No parameters.**

**Response:**

```json
{
  "runtime":          "2026-03-01T21:00:00Z",
  "store_path":       "/12TB2/NBM/zarr/current",
  "n_variables":      15,
  "n_time_steps":     98,
  "last_loaded":      "2026-03-01T23:36:57.081725Z",
  "available_cycles": ["20260301_18", "20260301_21"]
}
```

| Field | Description |
|---|---|
| `runtime` | Issue time of the cycle currently serving requests. |
| `n_time_steps` | Number of hours in the current forecast (varies: 98–260 depending on how far into the cycle the ingest ran). |
| `last_loaded` | UTC timestamp of the last successful store load (after ingestion or manual reload). |
| `available_cycles` | Sorted list of retained cycle tags (`YYYYMMDD_HH`). Use to determine valid `age_hours` values. |

---

### `GET /forecast`

Returns a parallel-array time series for one location, upsampled to **1-hour resolution**.

**Query parameters:**

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `lat` | float | **yes** | — | Latitude, decimal degrees (−90 to +90) |
| `lon` | float | **yes** | — | Longitude, decimal degrees (±180 or 0–360) |
| `vars` | string | **yes** | — | Comma-separated variable names (plain names, no unit suffix). E.g. `temperature,wind_speed,sun_elevation` |
| `start` | string | no | first forecast hour | Inclusive start, ISO-8601 UTC. E.g. `2026-03-01T18:00:00Z` |
| `end` | string | no | last forecast hour | Inclusive end, ISO-8601 UTC. |
| `age_hours` | int ≥ 0 | no | `0` | Forecast drift: return the most-recent retained cycle that is **at least** this many hours older than the current run. `0` = current cycle. See [Forecast Drift](#forecast-drift) below. |

**Response (200 OK):**

```json
{
  "runtime":   "2026-03-01T21:00:00Z",
  "latitude":  43.0731,
  "longitude": -89.4068,
  "length":    260,
  "times":     ["2026-03-01T22:00:00Z", "2026-03-01T23:00:00Z", "2026-03-02T00:00:00Z", ...],

  "temperature_F":                [34.2, 33.8, 33.1, ...],
  "dewpoint_F":                   [28.5, 28.0, 27.6, ...],
  "apparent_temperature_F":       [26.1, 25.7, ...],
  "wind_speed_mph":               [8.5,  9.1,  9.4,  ...],
  "wind_gust_mph":                [14.2, 15.0, ...],
  "wind_direction_deg":           [270,  268,  271,  ...],
  "relative_humidity_pct":        [72,   71,   70,   ...],
  "cloud_cover_pct":              [85,   90,   88,   ...],
  "solar_radiation_Wm2":          [0,    0,    0,    ...],
  "total_precipitation_mm":       [0.0,  0.05, 0.0,  ...],
  "precip_type_code":             [0,    1,    0,    ...],
  "thunderstorm_probability_pct": [2,    3,    2,    ...],
  "cape_Jkg":                     [0,    0,    12,   ...],
  "visibility_mi":                [10.0, 9.5,  null, ...],
  "cloud_ceiling_ft":             [2500, 2400, null, ...],
  "sun_elevation_deg":            [0.0,  0.0,  12.3, ...],

  "interpolated": [false, true, true, false, true, true, false, ...]
}
```

**Key response fields:**

| Field | Description |
|---|---|
| `runtime` | Issue time of the NBM cycle actually served. With `age_hours=0` this is the current cycle. With `age_hours>0` it will be an older cycle. |
| `latitude`, `longitude` | Coordinates of the **nearest NBM grid point** (±180 convention), not the exact requested coordinates. Use these for display. |
| `length` | Number of elements in every array. All arrays are exactly this long. |
| `times` | UTC ISO-8601 strings, one per hour, Z-suffixed. |
| `{var}_{suffix}` | Data arrays — one per requested variable. |
| `interpolated` | Boolean per time step. `true` = this hour was not a native NBM valid time; it was filled in by upsampling. See [Interpolation](#interpolation) below. |

**Null values:** `null` in a data array means "no forecast available" — either the variable has no data beyond its `fxx_cutoff` for that hour, or the grid point had a NaN in the source GRIB2. Do not treat `null` as zero.

---

#### Interpolation

The raw NBM forecast is hourly only for the first 36 hours, then 3-hourly, then 6-hourly. The API upsamples to a uniform 1-hour grid at query time:

| Variable | Interpolation method |
|---|---|
| All continuously-varying | **Linear** between adjacent valid times. Does not extrapolate past the last valid value. |
| `wind_direction` | **Circular**: decompose to sin/cos, interpolate each linearly, reconstruct with atan2. Correctly handles the 350°→10° wraparound (midpoint = 0°, not 180°). |
| `precip_type` | **Forward-fill** — the most recent valid type persists until the next change. Arithmetic interpolation between categorical type codes is meaningless. |

The `interpolated` flag in the response marks hours filled in this way. The frontend can use this to render interpolated steps with lighter styling (dotted line, reduced opacity) if desired.

---

#### Forecast Drift

The `age_hours` parameter allows the frontend to overlay multiple historical forecasts on the same time axis, showing how the forecast for a given period has evolved over successive model runs.

**Selection rule:** find the most-recent retained cycle whose issue time is ≤ `current_cycle_time − age_hours`. This gives "at least N hours back" semantics.

**Example** — current cycle is 20Z, retained cycles are [06Z, 12Z, 18Z, 20Z]:

| `age_hours` | Target time | Cycle served | `runtime` in response |
|---|---|---|---|
| 0 | current (20Z) | 20Z | `2026-03-01T20:00:00Z` |
| 2 | 18Z | 18Z | `2026-03-01T18:00:00Z` |
| 6 | 14Z → nearest is 12Z | 12Z | `2026-03-01T12:00:00Z` |
| 8 | 12Z | 12Z | `2026-03-01T12:00:00Z` |
| 12 | 08Z → nearest is 06Z | 06Z | `2026-03-01T06:00:00Z` |
| 200 | (nothing retained that far back) | **404** | — |

**Workflow for building a drift visualization:**
1. Call `GET /status` to get `available_cycles` and the current `runtime`.
2. For each historical cycle you want to overlay, compute `age_hours` from the difference between `runtime` and the target cycle time, then call `/forecast?...&age_hours=N`.
3. The `runtime` field in each response confirms which cycle was actually served.
4. All responses use the same 1-hour time grid, so you can align them directly.

**Note:** The backend always serves the latest complete cycle by default. It does not push updates to the client. The frontend is responsible for polling and building the drift history locally (see ARCHITECTURE.md for the client-side snapshot store design).

---

### Error Responses

All errors return JSON.

**HTTP 422 — Validation error:**

```json
{
  "detail": "Unknown variable(s): ['foo']. Call /variables for valid names."
}
```

Common causes: unknown variable name, lat/lon out of range, malformed ISO-8601 datetime, `start >= end`.

**HTTP 404 — No retained cycle old enough (only when `age_hours > 0`):**

```json
{
  "detail": {
    "message":             "No cycle available ≥ 200h before current.",
    "current_cycle":       "20260301_21",
    "requested_age_hours": 200,
    "available_cycles":    ["20260301_18", "20260301_21"]
  }
}
```

**HTTP 503 — Store not yet available:**

```json
{
  "detail": "Zarr store not yet available. Check /status."
}
```

This means the server started but the first ingestion cycle has not completed yet. Retry after a few minutes.

---

## Complete Variable Reference

All 16 variables (15 native + 1 derived) available to request in `vars=`:

| `vars=` name | Response key | Units | Forecast horizon | Notes |
|---|---|---|---|---|
| `temperature` | `temperature_F` | °F | full | 2-metre air temp |
| `dewpoint` | `dewpoint_F` | °F | full | 2-metre dewpoint |
| `relative_humidity` | `relative_humidity_pct` | % | full | 2-metre RH |
| `apparent_temperature` | `apparent_temperature_F` | °F | full | NBM blended feels-like; covers heat index + wind chill |
| `wind_speed` | `wind_speed_mph` | mph | full | 10-metre sustained speed |
| `wind_direction` | `wind_direction_deg` | ° | full | Degrees from north (meteorological convention) |
| `wind_gust` | `wind_gust_mph` | mph | full | Instantaneous 10-metre gust |
| `total_precipitation` | `total_precipitation_mm` | mm | full | 1-hour liquid-equivalent accumulation |
| `precip_type` | `precip_type_code` | code | full | 1=rain, 3=freezing rain, 5=snow, 8=sleet |
| `thunderstorm_probability` | `thunderstorm_probability_pct` | % | f001–f190 (~8 days) | `null` beyond f190 |
| `cape` | `cape_Jkg` | J/kg | full | Convective available potential energy |
| `cloud_cover` | `cloud_cover_pct` | % | full | Total cloud, all layers |
| `solar_radiation` | `solar_radiation_Wm2` | W/m² | full | Surface downward short-wave flux |
| `visibility` | `visibility_mi` | miles | f001–f076 (~3 days) | `null` beyond f076 |
| `cloud_ceiling` | `cloud_ceiling_ft` | feet | f001–f082 (~3.5 days) | `null` beyond f082 |
| `sun_elevation` | `sun_elevation_deg` | ° | full | Derived from lat/lon + time; negative = sun below horizon |

"Full" forecast horizon = the complete cycle length, currently ~260 hours (~11 days).

---

## Forecast Coverage

Each ingestion cycle covers:

| Segment | Hours ahead | Time step |
|---|---|---|
| f001 – f036 | 1–36 h | 1 h |
| f038 – f188 | 38–188 h (~8 days) | 3 h (upsampled to 1 h by API) |
| f194 – f260 | 194–260 h (~11 days) | 6 h (upsampled to 1 h by API) |

Total: 99 source files, ~260 hours of hourly-interpolated output after upsampling.

The `n_time_steps` field in `/status` reflects the actual step count for the current store (typically 260, or fewer for a partial ingest).

---

## Notes for Frontend Development

**Location resolution:** The API snaps to the nearest NBM grid point (~2.5 km resolution). The response's `latitude`/`longitude` fields contain the actual grid point coordinates. Display these to users so they understand which location the forecast is valid for.

**Time zone handling:** All times in the API are UTC with a `Z` suffix. Convert to local time for display.

**Variable subsets:** Request only the variables needed for the current view. There is no cost to requesting all 16 at once (single chunk read per variable from RAM), but smaller payloads are still preferable on mobile.

**Polling / refresh:** The backend updates roughly once per hour. A reasonable refresh policy is: check `/status` every 10–15 minutes; if `runtime` has changed, re-fetch the forecast and store the new snapshot for drift comparison.

**Null handling:** Variables with `fxx_cutoff` have `null` in their arrays beyond the cutoff. The frontend should treat these as "data unavailable" — mask them in the UI rather than plotting zero or interpolating across the gap.

**`precip_type` codes:** Only meaningful when `total_precipitation` > 0. At dry time steps, the value may be 0 (no precipitation type defined) or carry the previous type forward (due to forward-fill interpolation). Guard against this in your display logic.

---

---

## Backend Operations Reference

*(This section is for backend maintenance, not frontend development.)*

### Server

| Item | Value |
|---|---|
| Host | `precip.aos.wisc.edu` (Ubuntu, 32 cores, 128 GiB RAM, 10 TB disk) |
| Internal binding | `127.0.0.1:8001` (nginx reverse-proxies `/wxapp/` → here) |
| Python venv | `/home/gpetty/WxApp/.venv` |
| Data root | `/12TB2/NBM/` (env var `DATA_DIR`) |
| Zarr stores | `/12TB2/NBM/zarr/{YYYYMMDD_HH}/`, `current` symlink |

### Systemd Services

- **`wxapi.service`**: gunicorn, `-w 1` (single worker required), `--timeout 30`. Restart: `systemctl restart wxapi` or graceful reload: `kill -HUP $(pgrep -f 'gunicorn.*wxapp')`.
- **`wxingest.service`** + **`wxingest.timer`**: runs `python -m backend.app.ingest --postprocess` hourly (random delay 0–5 min). Posts to `/admin/reload` on completion.
- Check logs: `journalctl -u wxapi.service -f` or `journalctl -u wxingest.service -n 50`.

### Zarr Store Format

- Library: zarr 3.1.5 — **Zarr v3 format** (`zarr.json` at root, no `.zmetadata`).
- Grid: `(valid_time: 99, y: 1597, x: 2345)`, ~2.5 km resolution, uncompressed float32.
- Chunks: `(n_time, 256, 256)`. A single point query reads one chunk per variable (~25 MB).
- Total size: ~22 GB per cycle. Retained cycles: 00Z/06Z/12Z/18Z for 7 days + current.
- **Longitude convention in store:** 0–360 east-positive. The API converts ±180 input internally.

### Key Config (`backend/app/config.py`)

| Constant | Default | Env var override |
|---|---|---|
| `DATA_ROOT` | `./data/nbm` | `DATA_DIR` |
| `DOWNLOAD_WORKERS` | 6 | `DOWNLOAD_WORKERS` |
| `POSTPROCESS_WORKERS` | 8 | `POSTPROCESS_WORKERS` |
| `ZARR_RETAIN_DAYS` | 7 | (code only) |
| `ZARR_RETAIN_HOURS` | (0, 6, 12, 18) | (code only) |

### Performance

- Ingestion: ~210s download + ~300s post-processing per 99-file cycle.
- Query latency: effectively instantaneous. The 128 GiB server holds the entire ~22 GB Zarr store in the OS page cache after first access.
