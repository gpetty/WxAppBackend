# ROADMAP: Weather Window Web App

*Last updated: 2026-02-28*

---

## Phase 1 — Backend Core (Target: working API on localhost)

### 1.1 Project scaffold ✅
- [x] Initialize Python project structure (`requirements.txt`, venv)
- [x] Set up directory layout as defined in ARCHITECTURE.md
- [x] Install core dependencies: `herbie-data`, `cfgrib`, `xarray`, `fastapi`, `uvicorn`, `pysolar`

### 1.2 Variable registry ✅
- [x] Create `variables.yaml` with initial set of native NBM variables
- [x] Add derived variable definitions (heat index, wind chill, sun elevation)
- [x] Write `VariableRegistry` class to load and validate the YAML

### 1.3 GRIB2 field inventory ✅
- [x] Download one real NBM GRIB2 file (any recent cycle, any forecast hour)
- [x] Open with `cfgrib.open_datasets()` and inventory all available fields
- [x] Verify and correct `grib_shortName` / `grib_typeOfLevel` values in `variables.yaml`
- [x] Cross-reference variables across all three fxx segments (f001, f037, f196)
- [x] Annotate partial-availability variables in `variables.yaml` (`fxx_cutoff`, `fxx_availability`)

### 1.4 Ingestion pipeline ✅
- [x] Write ingestion script: identify latest available NBM cycle via Herbie
- [x] Download full GRIB2 suite for all forecast hours into `/data/nbm/staging/`
- [x] Lock file to prevent overlapping runs
- [x] Test on macOS dev environment — full cycle successfully downloaded (100 files)

### 1.5 Post-processor (GRIB2 → Zarr) ✅
- [x] Write `grib2_to_zarr.py`: iterate forecast-hour files, extract variables from registry
- [x] Concatenate along `valid_time` dimension; apply unit conversions
- [x] Write Zarr store with chunk shape `(n_time, 256, 256)` — uncompressed initially
- [x] Atomic directory swap: write to `zarr/staging_new/`, then rename to `zarr/current/`
- [x] Delete staging GRIB2 files after successful Zarr write
- [x] Full cycle (100 files, M4 Mac): 3h24min extraction (old), 17s Zarr write, 3.41 GB store
- [x] Optimized to file-centric loop (one cfgrib open per file): 12x speedup, ~22s for 3-file test
- [x] Estimated full cycle on M4 with optimization: ~13–15min extraction

### 1.6 Point extraction & derived variables ✅
- [x] Write `zarr_query.py`: open Zarr store, find nearest grid point, slice time range
- [x] Upsample to 1-hour resolution at query time (not ingest time):
  - Linear interpolation for all continuously-varying variables, including `total_precipitation` (QPF01 intensity is a smooth signal at extended range — timing uncertainty is already implicit in the 3/6-hourly output schedule)
  - Circular interpolation for `wind_direction` (sin/cos → atan2; wind_speed interpolated independently so magnitude is fully preserved — 0°@10 mph → 90°@10 mph midpoint = 45°@10 mph, not 7 mph)
  - Forward-fill for `precip_type` (categorical; no meaningful interpolation between type codes)
  - NaN beyond hard fxx cutoffs — no extrapolation (pandas interpolate natural behaviour)
  - Tag each time step with `interpolated: bool` in the response for optional front-end use
- [x] Write `derived.py`: sun_elevation (pysolar). heat_index and wind_chill dropped — `apparent_temperature` (native NBM) covers both roles.
- [x] 23/23 unit tests passing (grid lookup, circular interp, upsampling, NaN cutoff, ffill, full query)

### 1.7 FastAPI service ✅
- [x] `GET /variables` — full variable registry (native + derived, with units)
- [x] `GET /status` — cycle, n_variables, n_time_steps, last_loaded
- [x] `GET /forecast` — compact parallel-array response; unit-suffixed keys (`temperature_F`, `wind_speed_mph`, …); NaN → JSON null; values rounded to meteorologically appropriate precision
- [x] `GET /variables` exposes `response_key_suffix` so clients can predict response keys programmatically
- [x] Input validation with Pydantic (lat/lon bounds, datetime parsing, unknown var names)
- [x] `POST /admin/reload` — internal endpoint for ingestion script to hot-swap store
- [x] 503 middleware guard when Zarr store not yet available
- [x] CORS enabled for all origins (prototype)
- [x] 35/35 API tests passing; 58/58 total across all modules

### 1.8 End-to-end test (localhost) ✅
- [x] Ingest one full NBM cycle locally (2026-02-27T20Z, 100 files)
- [x] Run API server and query several lat/lon points — response effectively instantaneous
- [x] Verify time series values are reasonable — temperature, wind, apparent_temperature confirmed visually
- [x] Benchmark: query latency is effectively instantaneous (Zarr point-slice vs GRIB2 cold read)
- [x] Identified and fixed critical `total_precipitation` bug: 3-hourly/6-hourly GRIB2 files contain
      QPF01 (1-hr), QPF06 (6-hr), and QPF12 (12-hr) messages all under shortName `tp`. cfgrib was
      selecting whichever message appeared first in each file, alternating between QPF01 and QPF06
      every 3 hours and producing a spurious sawtooth oscillation (0.254 ↔ 27–50 mm). Fixed by
      adding `grib_accum_hours: 1` to variables.yaml and opening `total_precipitation` with an
      explicit `stepRange={fxx-1}-{fxx}` filter in the postprocessor.

---

## Phase 2 — Backend Hardening & Server Deployment

### 2.1 Automated ingestion
- [ ] Set up systemd timer on `precip.aos.wisc.edu` to run ingestion hourly
- [ ] Add alerting if ingestion fails (email or log-based)
- [ ] Monitor disk usage; confirm ~15 GB/cycle fits within operational budget

### 2.2 API deployment
- [ ] Deploy FastAPI with `uvicorn` + `nginx` reverse proxy on the Ubuntu server
- [ ] Configure HTTPS (Let's Encrypt)
- [ ] Smoke test API from external client

### 2.3 Performance validation
- [ ] Load test with concurrent requests to assess cache effectiveness
- [ ] Tune SQLite cache (WAL mode, indexes) if needed
- [ ] Profile cold-query latency; decide whether to pre-warm cache on ingestion

---

## Phase 3 — Frontend: Core UI

### 3.1 Project scaffold
- [ ] Initialize React + Vite + Tailwind project
- [ ] Set up PWA plugin (Vite PWA)
- [ ] Configure TanStack Query for API calls
- [ ] Set up `idb` for IndexedDB access (forecast snapshot storage)

### 3.2 Activity Manager
- [ ] Create/edit/delete activities
- [ ] For each activity: add/edit/remove weather criteria (variable + operator + threshold)
- [ ] Persist activities in localStorage

### 3.3 Location Picker
- [ ] Address/place name search → resolve to lat/lon (use OpenStreetMap Nominatim, free)
- [ ] Save multiple named locations
- [ ] Persist locations in localStorage

### 3.4 Forecast fetch & snapshot storage
- [ ] On demand, call `/forecast` for selected location and variables needed by current activity
- [ ] Store response in IndexedDB tagged with fetch timestamp
- [ ] Implement retention policy: keep last 30 snapshots per location, purge older

### 3.5 Weather Window Timeline
- [ ] Render suitability bar (green/yellow/red) for current forecast
- [ ] Overlay past snapshots as secondary bars (forecast drift)
- [ ] Legend showing snapshot ages
- [ ] Tap/click on time → popover with variable values + per-criterion pass/fail
- [ ] Toggle individual criteria on/off

### 3.6 Variable Detail Charts
- [ ] Standard line charts for individual variables (temperature, wind, precip probability, etc.)
- [ ] Reuse the same forecast snapshot data

---

## Phase 4 — Polish & Extended Features

- [ ] Offline support: cache last forecast in service worker for field use
- [ ] Share activity definitions (export/import JSON)
- [ ] Multi-source data (NDFD, GFS) — add abstraction layer, expose source selector to users
- [ ] User accounts & saved preferences (server-side) — deferred from initial scope
- [ ] Native mobile app (iOS/Android) using same backend API

---

## Key Dependencies & Risk Items

| Risk | Mitigation |
|---|---|
| GRIB2 cold-query latency (2–5s) | Eliminated by Zarr post-processing; point queries now <100ms |
| Post-processing time per cycle | Benchmark early; target completion well within 1-hour window |
| Zarr store size (~30 GB uncompressed) | Add LZ4 compression if needed; 10 TB budget is ample |
| Derived variables (heat index, wind chill, sun elevation) | Use well-established formulas; unit test against known values |
| Client-side IndexedDB growth | Enforce snapshot retention limits (30 per location) |
| NBM S3 availability / format changes | Monitor NOAA changelogs; use Herbie which abstracts S3 access |
