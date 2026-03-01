# ARCHITECTURE: Weather Window Web App

*Last updated: 2026-02-27*

---

## System Overview

```
┌─────────────────────────────────────────────────────────┐
│                    NOAA AWS S3                           │
│          s3://noaa-nbm-grib2-pds/ (NBM CONUS)           │
└───────────────────────┬─────────────────────────────────┘
                        │ hourly download (~15 GB/cycle, full suite)
                        ▼
┌─────────────────────────────────────────────────────────┐
│           Ubuntu Server: precip.aos.wisc.edu             │
│                                                         │
│  ┌──────────────────┐    ┌────────────────────────────┐ │
│  │  Ingestion       │    │  GRIB2 Staging Store       │ │
│  │  (systemd timer) │───▶│  /data/nbm/staging/        │ │
│  │  Herbie + Python │    │  ~15 GB, current download │ │
│  └──────────────────┘    └───────────┬────────────────┘ │
│                                      │ post-process      │
│                          ┌───────────▼────────────────┐ │
│                          │  Post-Processor             │ │
│                          │  cfgrib / xarray            │ │
│                          │  extract variables.yaml set │ │
│                          │  rechunk → Zarr store       │ │
│                          └───────────┬────────────────┘ │
│                                      │ atomic swap       │
│                          ┌───────────▼────────────────┐ │
│                          │  Zarr Store (live)          │ │
│                          │  /data/nbm/zarr/current/   │ │
│                          │  ~20–30 GB uncompressed     │ │
│                          │  dims: (valid_time, y, x)   │ │
│                          │  per variable               │ │
│                          └───────────┬────────────────┘ │
│                                      │ xr.open_zarr()   │
│  ┌───────────────────────────────────▼───────────────┐  │
│  │  FastAPI REST Service                              │  │
│  │  GET /forecast?lat=&lon=&vars=&start=&end=         │  │
│  │  GET /variables                                    │  │
│  │  GET /status                                       │  │
│  └───────────────────────────────────┬───────────────┘  │
└──────────────────────────────────────┼──────────────────┘
                                       │ JSON response
                                       ▼
┌─────────────────────────────────────────────────────────┐
│                  Client (React PWA)                      │
│                                                         │
│  ┌────────────────────┐   ┌───────────────────────────┐ │
│  │  Activity Manager  │   │  Forecast Snapshot Store  │ │
│  │  (criteria editor) │   │  (IndexedDB – drift data) │ │
│  └────────────────────┘   └───────────────────────────┘ │
│                                                         │
│  ┌─────────────────────────────────────────────────────┐│
│  │  Weather Window Timeline (Recharts / D3)             ││
│  │  • Current forecast: colored suitability bars        ││
│  │  • Past snapshots overlaid (forecast drift)          ││
│  └─────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────┘
```

---

## Backend: Data Ingestion

### Data source

**NOAA National Blend of Models (NBM), CONUS domain**

- S3 bucket: `noaa-nbm-grib2-pds` (us-east-1, public, no auth required)
- Path pattern: `blend.YYYYMMDD/HH/core/blend.tHHz.core.fXXX.co.grib2`
- Issued: every hour (00Z–23Z). Each cycle contains ~100 files covering the full forecast horizon.
- Temporal resolution: f001–f036 hourly (step 1); f037–f190 3-hourly (step 3); f196–f262 6-hourly (step 6). Verified 2026-02-27.
- File size: ~150 MB per forecast-hour file (all ~294 GRIB2 records). Total per cycle: ~15 GB.
- Source availability: hourly files on NOMADS and S3; extended-range (f037+) on S3 only. Herbie `priority=["aws", "nomads"]`.

### Ingestion strategy

- A **systemd timer** triggers a Python ingestion script each hour, offset ~10 minutes after the top of the hour to allow NOAA upload to complete.
- The script uses **Herbie** to identify and download the latest available cycle into `/data/nbm/staging/`.
- Download is the full GRIB2 suite for all forecast hours (no message-level subsetting at download time). This simplifies ingestion and means the raw files are available if variable needs change without re-downloading.
- On download completion, the post-processor runs (see next section). Once the Zarr store is written and validated, the staging GRIB2 files are deleted and the Zarr store is swapped live.
- A lock file prevents overlapping ingestion runs.

### Retention policy

Keep only the **current cycle's Zarr store** on disk. Raw GRIB2 staging files are deleted after post-processing. The client retains past forecast snapshots for drift visualization — the backend does not serve historical forecasts.

If a new cycle fails to download or process, the previous Zarr store remains live and continues to serve API requests.

---

## Backend: Variable Registry

A YAML configuration file (`variables.yaml`) drives both the post-processing step (which variables to extract from GRIB2) and the API layer (what clients can request). It is the single source of truth for supported variables and is designed to be easily extended without code changes.

**Structure:**
```yaml
variables:

  # --- Native NBM variables (extracted from GRIB2) ---

  temperature:
    grib_shortName: "2t"
    grib_typeOfLevel: "heightAboveGround"
    grib_level: 2
    units_raw: "K"
    units_out: "F"
    description: "2-meter air temperature"

  wind_speed:
    grib_shortName: "si10"
    grib_typeOfLevel: "heightAboveGround"
    grib_level: 10
    units_raw: "m/s"
    units_out: "mph"
    description: "10-meter wind speed"

  wind_direction:
    grib_shortName: "wdir10"
    grib_typeOfLevel: "heightAboveGround"
    grib_level: 10
    units_raw: "degrees"
    units_out: "degrees"
    description: "10-meter wind direction (meteorological)"

  wind_gust:
    grib_shortName: "gust"
    grib_typeOfLevel: "surface"
    units_raw: "m/s"
    units_out: "mph"
    description: "Wind gust speed"

  relative_humidity:
    grib_shortName: "r2"
    grib_typeOfLevel: "heightAboveGround"
    grib_level: 2
    units_raw: "%"
    units_out: "%"
    description: "2-meter relative humidity"

  precipitation_probability:
    grib_shortName: "pop12"          # verify exact NBM shortName
    grib_typeOfLevel: "surface"
    units_raw: "%"
    units_out: "%"
    description: "12-hour probability of precipitation"

  cloud_cover:
    grib_shortName: "tcc"
    grib_typeOfLevel: "atmosphere"
    units_raw: "%"
    units_out: "%"
    description: "Total cloud cover"

  solar_radiation:
    grib_shortName: "dswrf"
    grib_typeOfLevel: "surface"
    units_raw: "W/m²"
    units_out: "W/m²"
    description: "Downward short-wave radiation flux (12-hr mean)"

  # --- Derived variables (computed post-extraction, not stored in Zarr) ---

  sun_elevation:
    derived: true
    requires: ["lat", "lon", "valid_time"]
    formula: "pysolar"
    units_out: "degrees"
    description: "Solar elevation angle above horizon (purely astronomical)"

  heat_index:
    derived: true
    requires: ["temperature", "relative_humidity"]
    formula: "rothfusz"
    units_out: "F"
    description: "Heat index (NWS formula; valid when temp ≥ 80°F)"

  wind_chill:
    derived: true
    requires: ["temperature", "wind_speed"]
    formula: "noaa_2001"
    units_out: "F"
    description: "Wind chill (valid when temp ≤ 50°F and wind > 3 mph)"
```

Adding a native variable: add an entry here; the post-processor picks it up on the next cycle. Adding a derived variable: add an entry here and implement the formula in `derived.py`.

---

## Backend: Post-Processing (GRIB2 → Zarr)

After each successful GRIB2 download, the post-processor converts the raw files into a single Zarr store optimized for point time series queries.

### Why Zarr

- xarray-native (`xr.open_zarr()`) — clean, idiomatic API
- Coordinate metadata (lat, lon, valid_time) stored alongside data
- Chunking is controllable and critical for query performance (see below)
- Supports uncompressed or fast codecs (LZ4 via Blosc) without third-party tools

### Dataset structure

One xarray variable per weather variable in `variables.yaml`, all in a single Zarr store:

```
/data/nbm/zarr/current/
  ├── .zmetadata
  ├── temperature/        ← array shape: (n_time, n_y, n_x), float32
  ├── wind_speed/
  ├── wind_direction/
  ├── ...
  └── coords/
        valid_time/       ← datetime64 array, length n_time
        latitude/         ← 2D float32 array, shape (n_y, n_x)
        longitude/        ← 2D float32 array, shape (n_y, n_x)
```

NBM CONUS grid: approximately 2345 × 1597 grid points (~2.5 km resolution).
Time steps per cycle: ~100 (36 hourly + ~52 at 3-hr + ~12 at 6-hr).

### Chunking strategy

**Goal:** minimize I/O for a point time series query (all time steps at one lat/lon).

**Chunk shape:** `(n_time, 256, 256)` — all time steps in one chunk per 256×256 spatial tile (~640 km × 640 km).

This means:
- A single-point time series query reads exactly **one chunk per variable** (~100 time steps × 256 × 256 × 4 bytes ≈ 25 MB per chunk).
- Total for a 10-variable query: ~250 MB of I/O. Well under 100ms from SSD or page cache (128 GiB RAM holds the entire store).
- Only ~70 chunks per variable (ceil(1597/256) × ceil(2345/256) = 7 × 10), so writes are fast (~5s for the full store).
- Spatial map queries are also reasonably efficient at this tile size.

### Storage size estimate

Per variable, float32, uncompressed: 100 time steps × 2345 × 1597 × 4 bytes ≈ 1.5 GB.
For ~20 variables: ~30 GB per cycle. Well within the 10 TB budget.

Note: the ~150 MB/file raw GRIB2 size is larger than the ~75 MB initially estimated, but the
extracted Zarr arrays reflect only the decoded float32 grids — the GRIB2 overhead (metadata,
compression, multi-variable packing) does not carry over. The ~30 GB Zarr estimate stands.

With LZ4 compression (Blosc codec, level 1): expect ~50% reduction → ~15 GB, with negligible decompression overhead. **Start uncompressed for simplicity; add LZ4 if disk usage becomes a concern.**

### Post-processing pipeline

```python
# Pseudocode sketch
for var_name, var_cfg in variable_registry.native_variables():
    arrays = []
    for grib2_file in sorted(staging_files):
        ds = xr.open_dataset(grib2_file, engine='cfgrib',
                             backend_kwargs={'filter_by_keys': {
                                 'shortName': var_cfg['grib_shortName'],
                                 'typeOfLevel': var_cfg['grib_typeOfLevel'],
                             }})
        arrays.append(apply_unit_conversion(ds[var_name], var_cfg))
    combined = xr.concat(arrays, dim='valid_time')
    dataset[var_name] = combined.chunk({'valid_time': -1, 'y': 4, 'x': 4})

dataset.to_zarr('/data/nbm/zarr/staging_new/', mode='w')
atomic_swap('/data/nbm/zarr/staging_new/', '/data/nbm/zarr/current/')
```

The atomic swap (rename at the directory level) ensures the API never reads a partially-written store.

---

## Backend: API (FastAPI)

### Hardware reference

| Machine | Role | RAM | Notes |
|---|---|---|---|
| `precip.aos.wisc.edu` (Ubuntu) | Production | 128 GiB | Fast university connection; ~98 GiB headroom after Zarr store |
| MacPro M4 (macOS) | Development | 48 GiB | Zarr store (~30 GB) fits in page cache; ~18 GiB headroom |

**Query performance on both machines:** The full Zarr store (~30 GB for 20 variables) fits in RAM on both the production server and the dev Mac. After the first access following each ingestion cycle, the OS page cache will hold the entire dataset in memory and point queries will be sub-millisecond. No application-level caching layer is needed.

**Optional production optimization:** Explicitly `load()` the Zarr dataset into an in-process xarray Dataset on API startup, pinning it in RAM independently of the page cache. This guarantees fast queries even under memory pressure from other server processes, at the cost of ~30 GB of committed RAM. With 128 GiB available this is low-risk; defer until benchmarking suggests it's needed.

**Local testing note:** On the 48 GiB Mac, avoid explicit `load()` during development — the page cache approach leaves more headroom for other apps. Single-user local testing will not stress the page cache.

### Serving point queries from Zarr

The API opens the Zarr store once at startup with `xr.open_zarr()`. Because the array is chunked with `valid_time=-1`, extracting a full time series at one point is:

```python
ds = xr.open_zarr('/data/nbm/zarr/current/')
lat_idx, lon_idx = find_nearest_grid_point(lat, lon, ds)
timeseries = ds['temperature'].isel(y=lat_idx, x=lon_idx).values  # one chunk read
```

Derived variables (heat index, wind chill, sun elevation) are computed on the fly from the extracted native values after the Zarr read.

### Temporal interpolation to 1-hour resolution

The Zarr store contains data only at NBM valid times: hourly for f001–f036, then every 3 hours, then every 6 hours. The query layer upsamples to a **consistent 1-hour grid** before returning a response, so the client always receives a uniform time axis regardless of how far out the forecast extends.

**Design rule:** The Zarr store is always the unmodified source of truth — raw valid times and NaN where data is absent. Interpolation is performed entirely at query time in `zarr_query.py`, never at ingest time.

**Interpolation method by variable type:**

| Variable type | Variables | Method |
|---|---|---|
| Continuously varying | temperature, dewpoint, RH, apparent_temperature, wind_speed, wind_gust, cloud_cover, solar_radiation, CAPE, visibility, ceiling, thunderstorm_probability, total_precipitation | **Linear interpolation** between adjacent valid times |
| Circular | wind_direction | **Circular interpolation** on the unit circle — convert to sin/cos, interpolate linearly, convert back. Avoids the 350°→10° wrapping error that plain linear interpolation produces. |
| Categorical | precip_type | **Forward-fill (nearest neighbor)** — there is no meaningful interpolation between integer precipitation type codes (rain=1, freezing rain=3, snow=5, sleet=8). Use the most recently valid type. |

`total_precipitation` (QPF01) belongs with the continuously-varying group. At the short-range (f001–f036, hourly) each value is a point estimate where timing precision matters. But by the 3-hourly and 6-hourly segments, forecast timing uncertainty has grown large enough that the NBM's QPF01 value is already expressing a smoothed expected intensity over a broad temporal neighborhood rather than a sharp point-in-time event. Interpolating to intermediate hours is therefore no less accurate than the underlying forecast — it simply makes explicit what the model's temporal uncertainty already implies. The smooth precipitation intensity assumption is a meteorological property of extended-range forecasting, not a data convenience.

**Variables with hard fxx cutoffs** (visibility, ceiling, thunderstorm_probability beyond f076/f082/f190): return NaN beyond the cutoff — do not extrapolate.

**Optional: interpolation flag in response.** The API response can include a boolean `interpolated` per time step so the front end can distinguish native NBM values from filled-in hours. This lets the UI render interpolated steps with subtly different styling if desired (lighter weight, dotted line) without requiring the client to infer it from timestamps.

**Implementation sketch:**
```python
# After extracting point time series from Zarr:
# raw_times: irregular datetime64 array (actual NBM valid times)
# raw_values: float32 array, same length, NaN where not extracted

target_times = pd.date_range(start=raw_times[0], end=raw_times[-1], freq='1h')

# Linear (most variables):
interp_values = np.interp(target_times.astype(np.int64),
                          raw_times.astype(np.int64),
                          raw_values)

# Circular (wind_direction):
sin_vals = np.interp(..., np.sin(np.radians(raw_values)))
cos_vals = np.interp(..., np.cos(np.radians(raw_values)))
interp_dir = np.degrees(np.arctan2(sin_vals, cos_vals)) % 360
```

The Zarr store can be held open in memory by the FastAPI process. After the atomic swap of a new cycle, the API reloads it (triggered by the ingestion script via an internal signal or by detecting a new cycle timestamp in `/status`).

### Endpoints

**`GET /forecast`**
```
Parameters:
  lat       float   required   Latitude (decimal degrees)
  lon       float   required   Longitude (decimal degrees)
  vars      string  required   Comma-separated variable names
  start     string  optional   ISO datetime (defaults to current time)
  end       string  optional   ISO datetime (defaults to +10 days)

Response (JSON):
{
  "cycle": "2026-02-27T12:00:00Z",
  "lat_actual": 43.072,              // nearest grid point lat
  "lon_actual": -89.398,             // nearest grid point lon
  "variables": {
    "temperature": {
      "units": "F",
      "values": [
        {"time": "2026-02-27T13:00:00Z", "value": 34.2},
        ...
      ]
    },
    "wind_speed": { ... },
    "heat_index": { ... }            // derived, computed on the fly
  }
}
```

**`GET /variables`**
Returns the full variable registry with names, units, descriptions, derived flag.

**`GET /status`**
Returns cycle timestamp of current Zarr store, last successful ingestion time, store size on disk.

### Notes

- Response time target: <100ms for all queries (single chunk read per variable from local SSD).
- No authentication in Phase 1.
- CORS enabled for all origins during prototype.

---

## Frontend: React PWA

### Stack

| Concern | Library |
|---|---|
| Framework | React 18 |
| Routing | React Router |
| Styling | Tailwind CSS |
| Charts | Recharts (primary); drop to D3 only if custom viz requires it |
| Local storage | IndexedDB via `idb` wrapper |
| PWA | Vite PWA plugin (service worker, installable) |
| HTTP client | TanStack Query (caching, background refresh) |

### Key views

**1. Activity Manager** — create/edit named activities. Each activity has a list of weather criteria: variable + comparison operator + threshold value(s). Persisted in localStorage.

**2. Location Picker** — address/place search (OpenStreetMap Nominatim, free). Multiple saved locations. Persisted in localStorage.

**3. Weather Window Timeline** — core view:
- X-axis: time (next 10 days)
- Top strip: suitability bar (green = all criteria met, yellow = marginal, red = one or more criteria failed)
- Below: overlaid strips from past forecast snapshots stored in IndexedDB (forecast drift visualization)
- Tap on any time slot: popover with variable values + per-criterion pass/fail status
- Toggle individual criteria on/off

**4. Variable Detail Charts** — standard line charts for individual variables for context.

### Client-side forecast drift

When the client fetches a forecast, it stores the full JSON response in IndexedDB tagged with the fetch timestamp and location. The timeline overlays up to 10 past snapshots. A cleanup routine enforces a retention limit (last 30 snapshots per location) to prevent unbounded storage growth.

---

## Development & Deployment

### Dev environment (macOS)

- Ingest a single NBM cycle for testing (one pass of the ingestion script).
- Zarr store written to local disk; API opens it the same way as production.
- `uvicorn app.main:app --reload` for API.
- `npm run dev` (Vite) for frontend.

### Production (Ubuntu server)

- Ingestion + post-processing: systemd timer, Python venv.
- API: `uvicorn` behind `nginx` reverse proxy, HTTPS via Let's Encrypt.
- Frontend: static Vite build served by `nginx`.

### Directory layout

```
/data/nbm/
  staging/             ← GRIB2 download in progress (deleted after post-processing)
  zarr/
    current/           ← live Zarr store (API reads from here)
    staging_new/       ← new Zarr being written (swapped in atomically)

/srv/weatherwindow/
  backend/
    app/
      main.py
      routers/
      extraction/
        zarr_query.py
        derived.py
      postprocessor/
        grib2_to_zarr.py
      variables.yaml
  frontend/            ← built React PWA (served by nginx)
  venv/
```

---

## Open Questions / Pending Decisions

1. **LZ4 compression on Zarr.** Start uncompressed for simplicity. If ~30 GB/cycle is a concern, add `compressor=Blosc(cname='lz4', clevel=1)` — near-zero decompression overhead, ~50% size reduction. Decide after first production ingestion run.

2. **Zarr reload signal.** After atomic swap of a new Zarr store, the API must reload its open dataset handle. Options: (a) the ingestion script POSTs to an internal `/admin/reload` endpoint; (b) the API polls the cycle timestamp in `/data/nbm/zarr/current/.zattrs` on each request and reloads on change. Option (b) is simpler and avoids shared state.

3. **Frontend chart library.** Start with Recharts. The suitability bar chart (colored segments across a timeline) may require custom D3 rendering — evaluate during Phase 3.

4. **Offline support.** Service worker caching of last forecast. Defer to Phase 2.

5. **GRIB2 shortName verification.** The `grib_shortName` values in `variables.yaml` must be verified against an actual NBM GRIB2 file before the post-processor is written. NBM uses non-standard shortNames for some fields. First development task should be to inspect a real file with `cfgrib.open_datasets()` and inventory available fields.
