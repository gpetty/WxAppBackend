# BACKEND: Weather Window API Reference

*Last updated: 2026-03-14*

This document is the primary reference for building the frontend.
It describes the live API: endpoints, request parameters, response shapes, variable names, and error handling.
Backend operational details (ingestion pipeline, server config) are in a separate section at the bottom.

---

## Overview: Three Services

The backend runs two parallel ingestion pipelines and three FastAPI services:

| Service | Source | Port | Caddy path | Forecast horizon | Update cadence |
|---|---|---|---|---|---|
| NBM API | NOAA National Blend of Models | 8001 | `/wxapp/` | ~11 days (~260 h) | Every 3 hours |
| NDFD API | NOAA National Digital Forecast Database | 8002 | `/wxndfd/` | ~7 days (~168 h) | Every 3 hours |
| Blend API | NBM + NDFD merged | 8004 | `/blend/` | ~11 days | Every 3 hours |

**The Blend API is the primary endpoint for frontend development.** The NBM and NDFD APIs remain available for debugging and comparison.

---

## Base URLs

```
https://precip.aos.wisc.edu/blend/     ← Blend API  ✦ primary for frontend
https://precip.aos.wisc.edu/wxapp/     ← NBM API    (debug/comparison)
https://precip.aos.wisc.edu/wxndfd/    ← NDFD API   (debug/comparison)
```

CORS is open to all origins (prototype). All endpoints are GET except the internal `/admin/reload`.

OpenAPI/Swagger docs:
- `https://precip.aos.wisc.edu/blend/docs`
- `https://precip.aos.wisc.edu/wxapp/docs`
- `https://precip.aos.wisc.edu/wxndfd/docs`

---

## Quick Start

```
GET /blend/variables                                → merged catalog with source annotations
GET /blend/status                                   → both NBM and NDFD cycle times
GET /blend/forecast?lat=43.07&lon=-89.4&vars=temperature,wind_speed
GET /blend/forecast?lat=43.07&lon=-89.4&vars=temperature&age_hours=6  → drift
```

The individual NBM and NDFD APIs have the same endpoint structure but serve a single source:
```
GET /wxapp/forecast?lat=43.07&lon=-89.4&vars=temperature
GET /wxndfd/forecast?lat=43.07&lon=-89.4&vars=temperature
```

---

## Blend API Endpoints

### `GET /blend/variables`

Returns the merged variable catalog with source and merge-rule annotations.

**Response:**

```json
{
  "variables": {
    "temperature": {
      "units": "F",
      "response_key_suffix": "F",
      "description": "2-metre air temperature",
      "sources": ["ndfd", "nbm"],
      "merge_rule": "ndfd_preferred",
      "ndfd_horizon_h": 168,
      "nbm_horizon_h": 264
    },
    "thunderstorm_probability": {
      "units": "%",
      "response_key_suffix": "pct",
      "description": "Thunderstorm probability",
      "sources": ["nbm"],
      "merge_rule": "nbm_only",
      "ndfd_horizon_h": null,
      "nbm_horizon_h": 190
    },
    "thunderstorm_probability_severe": {
      "units": "%",
      "response_key_suffix": "pct",
      "description": "Total severe thunderstorm probability ...",
      "sources": ["ndfd"],
      "merge_rule": "ndfd_only",
      "ndfd_horizon_h": 168,
      "nbm_horizon_h": null
    },
    ...
  }
}
```

---

### `GET /blend/status`

```json
{
  "nbm_runtime":           "2026-03-14T12:00:00Z",
  "ndfd_runtime":          "2026-03-14T12:00:00Z",
  "nbm_available_cycles":  ["20260314_06", "20260314_09", "20260314_12"],
  "ndfd_available_cycles": ["20260314_06", "20260314_09", "20260314_12"],
  "nbm_store_path":        "/12TB2/NBM/slabs",
  "ndfd_store_path":       "/12TB2/NDFD/slabs",
  "nbm_n_variables":       15,
  "ndfd_n_variables":      14,
  "nbm_ready":             true,
  "ndfd_ready":            true,
  "last_loaded":           "2026-03-14T12:05:23Z"
}
```

---

### `GET /blend/forecast`

Same query parameters as the individual APIs (`lat`, `lon`, `vars`, `start`, `end`, `age_hours`).

Variable names come from the blend catalog (see `/blend/variables`). Note the two thunderstorm fields:
- `thunderstorm_probability` → NBM general tstm
- `thunderstorm_probability_severe` → NDFD severe tstm

**Response:**

```json
{
  "nbm_runtime":   "2026-03-14T12:00:00Z",
  "ndfd_runtime":  "2026-03-14T12:00:00Z",
  "latitude":      43.0731,
  "longitude":     -89.4068,
  "length":        264,
  "times":         ["2026-03-14T13:00:00Z", ...],

  "temperature_F":               [34.2, 33.8, ..., 28.1],
  "temperature_F_source":        ["ndfd", "ndfd", ..., "nbm"],

  "thunderstorm_probability_pct":               [2, 3, ...],
  "thunderstorm_probability_pct_source":        ["nbm", "nbm", ...],
  "thunderstorm_probability_severe_pct":        [0, 1, ...],
  "thunderstorm_probability_severe_pct_source": ["ndfd", "ndfd", ...],

  "wet_bulb_globe_temp_F":        [78.2, 79.1, ..., null],
  "wet_bulb_globe_temp_F_source": ["ndfd", ..., null],

  "interpolated": [false, true, true, false, ...]
}
```

**Key differences from individual APIs:**
- `runtime` is replaced by `nbm_runtime` and `ndfd_runtime`
- Every variable array has a companion `{key}_source` array
- Time axis extends to `max(nbm_end, ndfd_end)` — variables not available for that range are `null`
- Degraded mode: if one store is down, affected variables return all-null; the other source serves normally

---

## Individual API Endpoints (NBM and NDFD)

Both services share the same endpoint set. Examples use `/wxapp/` — substitute `/wxndfd/` for NDFD.

(The rest of this section is unchanged from before.)

---

### `GET /variables`

Returns the complete variable registry for this service. Call this once on startup to build your variable picker UI and to discover the exact response key suffix for each variable.

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
  "runtime":          "2026-03-14T12:00:00Z",
  "store_path":       "/12TB2/NBM/slabs",
  "n_variables":      15,
  "n_time_steps":     260,
  "last_loaded":      "2026-03-14T12:36:57Z",
  "available_cycles": ["20260314_06", "20260314_12"]
}
```

| Field | Description |
|---|---|
| `runtime` | Issue time of the cycle currently serving requests. |
| `n_time_steps` | Number of hours in the current forecast (after 1-h upsampling). |
| `last_loaded` | UTC timestamp of the last successful store load. |
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
| `start` | string | no | first forecast hour | Inclusive start, ISO-8601 UTC. E.g. `2026-03-14T12:00:00Z` |
| `end` | string | no | last forecast hour | Inclusive end, ISO-8601 UTC. |
| `age_hours` | int ≥ 0 | no | `0` | Forecast drift: return the most-recent retained cycle that is **at least** this many hours older than the current run. `0` = current cycle. See [Forecast Drift](#forecast-drift) below. |

**Response (200 OK):**

```json
{
  "runtime":   "2026-03-14T12:00:00Z",
  "latitude":  43.0731,
  "longitude": -89.4068,
  "length":    260,
  "times":     ["2026-03-14T13:00:00Z", "2026-03-14T14:00:00Z", ...],

  "temperature_F":                [34.2, 33.8, 33.1, ...],
  "wind_speed_mph":               [8.5,  9.1,  9.4,  ...],
  ...

  "interpolated": [false, true, true, false, ...]
}
```

**Key response fields:**

| Field | Description |
|---|---|
| `runtime` | Issue time of the cycle actually served. |
| `latitude`, `longitude` | Coordinates of the **nearest grid point** (±180 convention). Use these for display. |
| `length` | Number of elements in every array. All arrays are exactly this long. |
| `times` | UTC ISO-8601 strings, one per hour, Z-suffixed. |
| `{var}_{suffix}` | Data arrays — one per requested variable. |
| `interpolated` | Boolean per time step. `true` = filled in by upsampling (not a native valid time). |

**Null values:** `null` means "no forecast available" — either the variable has no data beyond its cutoff for that hour, or the grid point had a NaN in the source. Do not treat `null` as zero.

---

#### Interpolation

Both APIs upsample their native time steps to a uniform 1-hour grid at query time:

| Variable | Interpolation method |
|---|---|
| All continuously-varying | **Linear** between adjacent valid times |
| `wind_direction` | **Circular**: decompose to sin/cos, interpolate, reconstruct with atan2 — correctly handles 350°→10° wraparound |
| `precip_type` (NBM only) | **Forward-fill** — categorical codes are not arithmetic |

The `interpolated` flag marks hours filled in this way.

---

#### Forecast Drift

The `age_hours` parameter allows the frontend to overlay multiple historical forecasts on the same time axis.

**Selection rule:** find the most-recent retained cycle whose issue time is ≤ `current_cycle_time − age_hours`.

**Workflow:**
1. Call `GET /status` to get `available_cycles` and the current `runtime`.
2. For each historical cycle to overlay, compute `age_hours` from the difference, then call `/forecast?...&age_hours=N`.
3. The `runtime` field in each response confirms which cycle was served.
4. All responses use the same 1-hour time grid — align them directly.

---

### Error Responses

All errors return JSON.

**HTTP 422 — Validation error:**
```json
{ "detail": "Unknown variable(s): ['foo']. Call /variables for valid names." }
```

**HTTP 404 — No retained cycle old enough:**
```json
{
  "detail": {
    "message": "No cycle available ≥ 200h before current.",
    "current_cycle": "20260314_12",
    "requested_age_hours": 200,
    "available_cycles": ["20260314_06", "20260314_12"]
  }
}
```

**HTTP 503 — Store not yet available:**
```json
{ "detail": "Slab store not yet available. Check /status." }
```

---

## Variable Catalog

### NBM Variables (`/wxapp/forecast`)

Source: NOAA National Blend of Models. ~11-day forecast horizon, updated hourly.
Grid: CONUS 2.5 km Lambert Conformal (2345 × 1597).
Native time steps: hourly f001–f036, 3-hourly f038–f188, 6-hourly f194–f260 — all upsampled to 1-h.

| `vars=` name | Response key | Units | Horizon | Notes |
|---|---|---|---|---|
| `temperature` | `temperature_F` | °F | full (~11 days) | 2-metre air temp |
| `dewpoint` | `dewpoint_F` | °F | full | 2-metre dewpoint |
| `relative_humidity` | `relative_humidity_pct` | % | full | 2-metre RH |
| `apparent_temperature` | `apparent_temperature_F` | °F | full | NBM blended feels-like; covers heat index + wind chill |
| `wind_speed` | `wind_speed_mph` | mph | full | 10-metre sustained speed |
| `wind_direction` | `wind_direction_deg` | ° | full | Degrees from north (met convention) |
| `wind_gust` | `wind_gust_mph` | mph | full | Instantaneous 10-metre gust |
| `total_precipitation` | `total_precipitation_mm` | mm | full | 1-hour liquid-equivalent accumulation (QPF01) |
| `precip_type` | `precip_type_code` | code | full | 1=rain, 3=freezing rain, 5=snow, 8=sleet |
| `thunderstorm_probability` | `thunderstorm_probability_pct` | % | f001–f190 (~8 days) | `null` beyond f190 |
| `cape` | `cape_Jkg` | J/kg | full | Convective available potential energy |
| `cloud_cover` | `cloud_cover_pct` | % | full | Total cloud, all layers |
| `solar_radiation` | `solar_radiation_Wm2` | W/m² | full | Surface downward short-wave flux |
| `visibility` | `visibility_mi` | miles | f001–f076 (~3 days) | `null` beyond f076 |
| `cloud_ceiling` | `cloud_ceiling_ft` | feet | f001–f082 (~3.5 days) | `null` beyond f082 |
| `sun_elevation` | `sun_elevation_deg` | ° | full | *Derived.* Astronomical solar elevation; negative = below horizon |

"Full" = complete cycle length, currently ~260 hours (~11 days).

---

### NDFD Variables (`/wxndfd/forecast`)

Source: NOAA National Digital Forecast Database. ~7-day forecast horizon, updated every 3 hours.
Grid: CONUS 2.5 km Lambert Conformal (2145 × 1377).
Native time steps: hourly days 1–3 (VP.001-003), 3-hourly days 4–7 (VP.004-007) — all upsampled to 1-h.

| `vars=` name | Response key | Units | Horizon | Notes |
|---|---|---|---|---|
| `temperature` | `temperature_F` | °F | full (~7 days) | 2-metre air temp |
| `dewpoint` | `dewpoint_F` | °F | full | 2-metre dewpoint |
| `relative_humidity` | `relative_humidity_pct` | % | full | 2-metre RH |
| `apparent_temperature` | `apparent_temperature_F` | °F | full | NDFD official feels-like; encompasses heat index / wind chill |
| `wind_speed` | `wind_speed_mph` | mph | full | 10-metre sustained speed |
| `wind_direction` | `wind_direction_deg` | ° | full | Degrees from north (met convention) |
| `wind_gust` | `wind_gust_mph` | mph | full | Instantaneous 10-metre gust |
| `total_precipitation` | `total_precipitation_mm` | mm | days 1–3 only | QPF liquid-equivalent; `null` days 4–7 |
| `thunderstorm_probability` | `thunderstorm_probability_pct` | % | full (~7 days) | Total **severe** tstm probability (wind/hail/tornado combined) — not equivalent to NBM general tstm |
| `cloud_cover` | `cloud_cover_pct` | % | full | Sky cover 0–100 % |
| `visibility` | `visibility_mi` | miles | days 1–3 only | `null` days 4–7 |
| `cloud_ceiling` | `cloud_ceiling_ft` | feet | days 1–3 only | `null` days 4–7 |
| `wet_bulb_globe_temp` | `wet_bulb_globe_temp_F` | °F | full | WBGT heat stress index (NDFD-only — not in NBM) |
| `snowfall` | `snowfall_mm` | mm | days 1–3 only | Snowfall water equivalent; `null` days 4–7 |
| `sun_elevation` | `sun_elevation_deg` | ° | full | *Derived.* Astronomical solar elevation; negative = below horizon |

---

### Variable Comparison: NBM vs. NDFD

| Variable | NBM | NDFD | Notes |
|---|---|---|---|
| `temperature` | ✓ ~11 days | ✓ ~7 days | Same name/units on both APIs |
| `dewpoint` | ✓ ~11 days | ✓ ~7 days | |
| `relative_humidity` | ✓ ~11 days | ✓ ~7 days | |
| `apparent_temperature` | ✓ ~11 days | ✓ ~7 days | Both are official feels-like; NDFD is NWS-authoritative |
| `wind_speed` | ✓ ~11 days | ✓ ~7 days | |
| `wind_direction` | ✓ ~11 days | ✓ ~7 days | |
| `wind_gust` | ✓ ~11 days | ✓ ~7 days | |
| `total_precipitation` | ✓ ~11 days | ✓ days 1–3 | NBM provides full horizon |
| `precip_type` | ✓ ~11 days | ✗ | NBM-only |
| `thunderstorm_probability` | ✓ ~8 days | ✓ ~7 days | **Not equivalent**: NBM = general tstm; NDFD = severe tstm only |
| `cape` | ✓ ~11 days | ✗ | NBM-only |
| `cloud_cover` | ✓ ~11 days | ✓ ~7 days | |
| `solar_radiation` | ✓ ~11 days | ✗ | NBM-only |
| `visibility` | ✓ days 1–3 | ✓ days 1–3 | Both cut off at ~3 days |
| `cloud_ceiling` | ✓ days 1–3.5 | ✓ days 1–3 | |
| `wet_bulb_globe_temp` | ✗ | ✓ ~7 days | NDFD-only |
| `snowfall` | ✗ | ✓ days 1–3 | NDFD-only (NBM has `total_precipitation` + `precip_type`) |
| `sun_elevation` | ✓ full | ✓ full | Derived (identical formula) on both |

---

## Merge Policy Reference

The blend service applies these rules per variable (see `BLEND_RULES` in `blend_forecast.py`):

| Variable | Strategy | NDFD horizon | NBM horizon |
|---|---|---|---|
| temperature, dewpoint, RH, apparent_temperature | ndfd_preferred | 168 h | 264 h |
| wind_speed, wind_direction, wind_gust, cloud_cover | ndfd_preferred | 168 h | 264 h |
| visibility, cloud_ceiling | ndfd_preferred | 72 h | 76/82 h |
| total_precipitation | ndfd_preferred | 72 h (NDFD QPF only days 1–3) | 264 h |
| precip_type, cape, solar_radiation | nbm_only | — | full |
| thunderstorm_probability | nbm_only | — | 190 h |
| thunderstorm_probability_severe | ndfd_only | 168 h | — |
| wet_bulb_globe_temp | ndfd_only | 168 h | — |
| snowfall | ndfd_only | 72 h | — |
| sun_elevation | derived | — | — |

---

## Notes for Frontend Development

**Grid snapping:** Each API snaps to its own nearest grid point (~2.5 km resolution). The two grids are independent; for the same lat/lon input the returned `latitude`/`longitude` may differ slightly between NBM and NDFD responses.

**Time zone handling:** All times in the API are UTC with a `Z` suffix. Convert to local time for display.

**Null handling:** Variables with coverage cutoffs have `null` in their arrays beyond the cutoff. Treat as "data unavailable" — mask in the UI rather than plotting zero or interpolating across the gap.

**`precip_type` codes (NBM):** Only meaningful when `total_precipitation` > 0. At dry time steps, the value may be 0 or carry forward due to forward-fill. Guard against this in display logic.

**`thunderstorm_probability` semantic difference:** The NBM and NDFD values are not comparable. NBM reports general thunderstorm probability; NDFD reports total severe thunderstorm probability. Do not display them interchangeably without a label.

**Polling / refresh:** NBM updates roughly once per hour; NDFD every 3 hours. A reasonable refresh policy: check `/status` on both services every 10–15 minutes; if `runtime` has changed, re-fetch and store the new snapshot for drift comparison.

---

## Backend Operations Reference

*(This section is for backend maintenance, not frontend development.)*

### Server

| Item | Value |
|---|---|
| Host | `precip.aos.wisc.edu` (Ubuntu, 32 cores, 128 GiB RAM, 10 TB disk) |
| NBM API binding | `127.0.0.1:8001` (Caddy reverse-proxies `/wxapp/` → here) |
| NDFD API binding | `127.0.0.1:8002` (Caddy reverse-proxies `/wxndfd/` → here) |
| Blend API binding | `127.0.0.1:8004` (Caddy reverse-proxies `/blend/` → here) |
| Python venv | `/home/gpetty/WxApp/.venv` |
| NBM data root | `/12TB2/NBM/` (env var `DATA_DIR`) |
| NDFD data root | `/12TB2/NDFD/` (env var `NDFD_DATA_DIR`) |

### Systemd Services

**NBM:**
- **`wxapi.service`**: gunicorn, port 8001, `-w 1`. Restart: `systemctl restart wxapi`.
- **`wxingest.service`** + **`wxingest.timer`**: runs NBM ingest + slab ingest every 3 hours (00/03/06/.../21Z). Posts `/admin/reload` to ports 8001 and 8004.

**NDFD:**
- **`wxndfdapi.service`**: gunicorn, port 8002, `-w 1`. Restart: `systemctl restart wxndfdapi`.
- **`wxndfdingest.service`** + **`wxndfdingest.timer`**: runs NDFD ingest + slab ingest every 3 hours. Posts `/admin/reload` to ports 8002 and 8004.

**Blend:**
- **`wxblendapi.service`**: gunicorn, port 8004, `-w 1`. Reads from both stores; no ingest of its own. Restart: `systemctl restart wxblendapi`.
- No blend ingest timer — blend reloads are triggered by the NBM and NDFD ingest services.

**Health monitor:**
- `wxmonitor@nbm.timer` / `wxmonitor@ndfd.timer` / `wxmonitor@blend.timer` — fire every 20 minutes.
- Fill in healthchecks.io UUIDs in `scripts/wxmonitor.py` (`FILL_IN_*_HC_UUID`).

Check logs: `journalctl -u wxapi.service -f` / `journalctl -u wxndfdapi.service -f` / `journalctl -u wxblendapi.service -f`

### Slab Store Format

Both services use the slab ring buffer architecture (`NBMStore` / `NDFDStore`).

**NBM store:**
- Grid: `(valid_time: ~260, y: 1597, x: 2345)`, ~2.5 km, uncompressed float32
- Retained cycles: 00Z/06Z/12Z/18Z for 7 days + current
- Total size: ~22 GB per cycle

**NDFD store:**
- Grid: `(valid_time: ~65, y: 1377, x: 2145)`, ~2.5 km, uncompressed float32
- Retained cycles: 48 snapshots (~6 days at 3-hourly updates)
- Total size: substantially smaller than NBM (fewer variables, fewer time steps)

**Longitude convention in stores:** 0–360 east-positive. The API converts ±180 input internally.

### Key Config

**NBM (`backend/app/config.py`):**

| Constant | Default | Env var |
|---|---|---|
| `DATA_ROOT` | `./data/nbm` | `DATA_DIR` |
| `DOWNLOAD_WORKERS` | 6 | `DOWNLOAD_WORKERS` |
| `POSTPROCESS_WORKERS` | 8 | `POSTPROCESS_WORKERS` |

**NDFD (`backend/app/config_ndfd.py`):**

| Constant | Default | Env var |
|---|---|---|
| `NDFD_DATA_ROOT` | `./data/ndfd` | `NDFD_DATA_DIR` |
| `NDFD_DOWNLOAD_WORKERS` | 6 | `NDFD_DOWNLOAD_WORKERS` |
| `NDFD_POSTPROCESS_WORKERS` | 8 | `NDFD_POSTPROCESS_WORKERS` |
| `NDFD_SLAB_N_RUNS` | 48 | `NDFD_SLAB_N_RUNS` |

### Performance

- NBM ingestion: ~210s download + ~278s slab ingest per 99-file cycle
- NDFD ingestion: much faster (~24 small files per cycle)
- Query latency: sub-millisecond from page cache on both services (128 GiB server holds both stores in RAM)
