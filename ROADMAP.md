# ROADMAP: Weather Window Web App

*Last updated: 2026-03-14*

---

## Phase 1 — NBM Backend Core ✅

### 1.1 Project scaffold ✅
- [x] Initialize Python project structure (`requirements.txt`, venv)
- [x] Set up directory layout as defined in ARCHITECTURE.md
- [x] Install core dependencies: `herbie-data`, `cfgrib`, `xarray`, `fastapi`, `uvicorn`, `pysolar`

### 1.2 Variable registry ✅
- [x] Create `variables.yaml` with initial set of native NBM variables
- [x] Add derived variable definitions (sun elevation)
- [x] Write `VariableRegistry` class to load and validate the YAML

### 1.3 GRIB2 field inventory ✅
- [x] Download one real NBM GRIB2 file and inventory all available fields with `cfgrib`
- [x] Verify and correct `grib_shortName` / `grib_typeOfLevel` values in `variables.yaml`
- [x] Cross-reference variables across all three fxx segments (f001, f037, f196)
- [x] Annotate partial-availability variables (`fxx_cutoff`)

### 1.4 Ingestion pipeline ✅
- [x] Write ingestion script using Herbie; download full GRIB2 suite into staging
- [x] Lock file to prevent overlapping runs
- [x] Switched to slab ring buffer architecture (replaced Zarr store)
- [x] `thin_fxx()` reduces 264 hourly files to ~99 anchor files (3-hourly/6-hourly thinning)

### 1.5 Post-processor (GRIB2 → slab) ✅
- [x] File-centric extraction loop (one cfgrib open per file, all variables extracted in one pass)
- [x] Unit conversions (K→F, m/s→mph, m→miles/feet)
- [x] `NBMStore` / `SlabRingBuffer` / `write_slab` — in-memory ring buffer, ProcessPoolExecutor
- [x] Fixed `total_precipitation` bug: forced `stepRange={fxx-1}-{fxx}` to always extract QPF01

### 1.6 Point extraction & derived variables ✅
- [x] Nearest grid point lookup; 1-hour upsampling at query time
- [x] Linear interpolation (continuous vars), circular interpolation (wind_direction), forward-fill (precip_type)
- [x] `sun_elevation` via pysolar; heat_index and wind_chill dropped (apparent_temperature covers both)
- [x] `interpolated` flag per time step in response
- [x] 23/23 unit tests passing

### 1.7 NBM FastAPI service ✅
- [x] `GET /variables`, `GET /status`, `GET /forecast` with compact parallel-array response
- [x] `age_hours` parameter for forecast drift queries
- [x] `POST /admin/reload` — hot-swap store after ingest
- [x] 503 guard, CORS, input validation
- [x] 58/58 tests passing

### 1.8 End-to-end test ✅
- [x] Full NBM cycle ingest and API query validated on server
- [x] Performance: ingestion ~210s download + ~278s slab ingest; query latency sub-millisecond

---

## Phase 2 — NDFD Backend ✅

### 2.1 NDFD variable registry ✅
- [x] `variables_ndfd.yaml` — 14 native variables + 1 derived (sun_elevation)
- [x] `NDFDVariableRegistry` / `NDFDNativeVariable` dataclasses
- [x] `period_cutoff` field for variables absent from VP.004-007

### 2.2 NDFD ingestion pipeline ✅
- [x] `ndfd_ingest.py` — downloads from `s3://noaa-ndfd-pds/opnl/AR.conus/`
- [x] Two periods: VP.001-003 (hourly, days 1–3), VP.004-007 (3-hourly, days 4–7)
- [x] Per-element bin files (`ds.{element}.bin`); `NDFD_ELEMENTS` dict controls what's downloaded

### 2.3 NDFD post-processor ✅
- [x] `ndfd_slab_ingest.py` — cfgrib extraction from per-element bin files
- [x] `NDFDStore` / slab ring buffer (48 runs retained, ~6 days of drift history)
- [x] `_scan_shortnames()` pre-screening (safety net for missing elements)

### 2.4 NDFD FastAPI service ✅
- [x] `ndfd_main.py` — parallel service on port 8002, path `/wxndfd/`
- [x] Reuses same routers (`forecast`, `variables`, `status`) via duck typing
- [x] `POST /admin/reload` — hot-swap NDFD store after ingest

### 2.5 Server deployment ✅
- [x] Both services live on `precip.aos.wisc.edu`
- [x] Caddy reverse-proxies `/wxapp/` → 8001 (NBM), `/wxndfd/` → 8002 (NDFD)
- [x] systemd timers: NBM hourly, NDFD every 3 hours
- [x] `wxmonitor` watching both services

---

## Phase 3 — Blended API ✅

### 3.1 Blend endpoint design ✅
- [x] Merge rules defined: NDFD-preferred days 1–7, NBM backfill days 8–11
- [x] Incompatible variables served unmerged:
  - `thunderstorm_probability_pct` (NBM general) and `thunderstorm_probability_severe_pct` (NDFD severe) as separate fields
  - NBM-only: `precip_type`, `cape`, `solar_radiation`
  - NDFD-only: `wet_bulb_globe_temp`, `snowfall`
- [x] `source` parallel array per variable (`"ndfd"` / `"nbm"` / `null`) in response

### 3.2 Implementation ✅
- [x] `backend/app/routers/helpers.py` — shared utilities extracted from forecast.py
- [x] `backend/app/routers/blend_models.py` — Pydantic models for blend endpoints
- [x] `backend/app/routers/blend_forecast.py` — merge logic + `BLEND_RULES` registry
- [x] `backend/app/routers/blend_status.py` — reports both NBM and NDFD runtimes
- [x] `backend/app/routers/blend_variables.py` — merged variable catalog
- [x] `backend/app/blend_main.py` — new FastAPI app on port 8004, Caddy path `/blend/`
- [x] `age_hours` drift support: each store independently finds its nearest matching cycle
- [x] Degraded mode: serves available source(s) if one store is down; 503 only if both fail
- [x] NBM ingestion cadence reduced to 3-hourly (synchronized with NDFD at 00/03/06/09/12/15/18/21Z)
- [x] `SLAB_N_RUNS` updated 28 → 56 (7 days × 8 cycles/day)

### 3.3 Health monitor ✅
- [x] `scripts/wxmonitor.py` rewritten — single script, `nbm|ndfd|blend` CLI arg
- [x] `systemd/wxmonitor@.service` — template unit
- [x] Three timer units: `wxmonitor@nbm.timer`, `wxmonitor@ndfd.timer`, `wxmonitor@blend.timer`
- [x] All three healthchecks.io UUIDs to fill in (`FILL_IN_*_HC_UUID` placeholders)

### 3.4 Deployment ← *current*
- [x] Re-init NBM slab store after `SLAB_N_RUNS` change (brief outage required)
- [x] Copy systemd units to `/etc/systemd/system/`, `daemon-reload`, enable/start `wxblendapi.service`
- [x] Copy updated `wxingest.service`, `wxingest.timer` to `/etc/systemd/system/` and reload timer
- [x] Copy updated monitor units and enable three timer instances
- [x] Copy `Caddyfile.tmp` → `/etc/caddy/Caddyfile`, reload Caddy
- [x] Fill in three healthchecks.io UUIDs in `scripts/wxmonitor.py` (all three share one UUID)
- [x] Smoke test: `curl http://127.0.0.1:8004/status` — both sources ready ✓
- [x] Smoke test: forecast endpoint — 257 steps, NDFD days 1–7 / NBM days 8–10, zero nulls ✓

---

## Phase 4 — Frontend: Core UI

### 4.1 Project scaffold
- [ ] Initialize React + Vite + Tailwind project
- [ ] Set up PWA plugin (Vite PWA)
- [ ] Configure TanStack Query for API calls
- [ ] Set up `idb` for IndexedDB access (forecast snapshot storage)

### 4.2 Activity Manager
- [ ] Create/edit/delete activities
- [ ] For each activity: add/edit/remove weather criteria (variable + operator + threshold)
- [ ] Persist activities in localStorage

### 4.3 Location Picker
- [ ] Address/place name search → resolve to lat/lon (OpenStreetMap Nominatim)
- [ ] Save multiple named locations; persist in localStorage

### 4.4 Forecast fetch & snapshot storage
- [ ] Call `/blend/forecast` for selected location and activity variables
- [ ] Store response in IndexedDB tagged with fetch timestamp
- [ ] Retention policy: keep last 30 snapshots per location

### 4.5 Weather Window Timeline
- [ ] Suitability bar (green/yellow/red) for current forecast
- [ ] Overlay past snapshots as secondary bars (forecast drift)
- [ ] Tap/click → popover with variable values + per-criterion pass/fail
- [ ] Toggle criteria on/off; legend showing snapshot ages

### 4.6 Variable Detail Charts
- [ ] Line charts for individual variables
- [ ] Optional: dim interpolated time steps; show `source` provenance on hover

---

## Phase 5 — Polish & Extended Features

- [ ] Offline support: cache last forecast in service worker for field use
- [ ] Share activity definitions (export/import JSON)
- [ ] User accounts & saved preferences (server-side) — deferred from initial scope
- [ ] Native mobile app (iOS/Android) using same backend API
- [ ] GFS as third source (extended range beyond NBM's 11 days)

---

## Key Dependencies & Risk Items

| Risk | Mitigation |
|---|---|
| Blend drift complexity | Both stores retain independent cycle histories; blend endpoint must find a valid `age_hours` match in each — may need to relax to "nearest available" per source |
| `thunderstorm_probability` semantic mismatch | Exposed as two separate named fields; never silently merged |
| NDFD VP.004-007 variable gaps | `period_cutoff` field drives null-filling; same pattern as NBM `fxx_cutoff` |
| Client-side IndexedDB growth | Enforce 30-snapshot retention limit per location |
| NBM S3 availability / format changes | Herbie abstracts S3 access; monitor NOAA changelogs |
