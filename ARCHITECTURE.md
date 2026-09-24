# ARCHITECTURE: Weather Window Web App

*Last updated: 2026-05-14*

---

## System Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│   NOAA S3: noaa-nbm-grib2-pds (NBM CONUS)                           │
│   blend.YYYYMMDD/HH/core/blend.tHHz.core.fXXX.co.grib2             │
│   3-hourly cycles · ~99 files · ~15 GB per cycle                    │
└──────────────────────────┬───────────────────────────────────────────┘
                           │ Herbie + s3fs (6 threads, ~210s)
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│   NOAA S3: noaa-ndfd-pds (NDFD CONUS)                               │
│   opnl/AR.conus/{VP.001-003, VP.004-007}/ds.{element}.bin           │
│   3-hourly cycles · ~24 files · much smaller than NBM               │
└──────────────────────────┬───────────────────────────────────────────┘
                           │ s3fs (anonymous, ~6 threads)
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│   Ubuntu Server: precip.aos.wisc.edu                                 │
│   32 cores · 128 GiB RAM · 10 TB disk                               │
│                                                                      │
│  ┌─────────────────┐     ┌──────────────────────────────────────┐   │
│  │ NBM Ingest      │     │ NBM Slab Ring Buffer                 │   │
│  │ (systemd timer) │────▶│ /12TB2/NBM/slabs/                   │   │
│  │ 3-hourly        │     │ 56 runs · 99 time steps · 15 vars   │   │
│  └─────────────────┘     └──────────────────┬───────────────────┘   │
│                                             │                        │
│  ┌─────────────────┐     ┌──────────────────▼───────────────────┐   │
│  │ NDFD Ingest     │     │ NDFD Slab Ring Buffer                │   │
│  │ (systemd timer) │────▶│ /12TB2/NDFD/slabs/                  │   │
│  │ 3-hourly        │     │ 48 runs · ~65 time steps · 14 vars  │   │
│  └─────────────────┘     └──────────────────┬───────────────────┘   │
│                                             │                        │
│  ┌──────────────────────────────────────────▼───────────────────┐   │
│  │ FastAPI Services (gunicorn -w 1 each)                        │   │
│  │  NBM API  127.0.0.1:8001   /wxapp/   GET /forecast,status   │   │
│  │  NDFD API 127.0.0.1:8002   /wxndfd/  GET /forecast,status   │   │
│  │  Blend    127.0.0.1:8004   /blend/   GET /forecast,status   │   │
│  └──────────────────────────────────────────┬───────────────────┘   │
│                                             │ Caddy reverse proxy   │
└─────────────────────────────────────────────┼──────────────────────┘
                                              │ JSON response
                                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│   Client (React PWA)                                                 │
│                                                                      │
│  ┌───────────────────────┐   ┌────────────────────────────────────┐  │
│  │ Activity Manager      │   │ Forecast Snapshot Store (IndexedDB)│  │
│  │ (criteria editor)     │   │ Forecast drift visualization       │  │
│  └───────────────────────┘   └────────────────────────────────────┘  │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ Weather Window Timeline (Recharts / D3)                         │ │
│  │ • Current forecast: colored suitability bars                    │ │
│  │ • Past snapshots overlaid (forecast drift)                      │ │
│  └─────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Backend: Data Sources

### NBM — National Blend of Models

- S3 bucket: `noaa-nbm-grib2-pds` (us-east-1, public, no auth)
- Path: `blend.YYYYMMDD/HH/core/blend.tHHz.core.fXXX.co.grib2`
- Issued every hour; ingested every 3 hours (00/03/06/09/12/15/18/21Z)
- ~99 files per cycle after thinning (36 hourly + 51 3-hourly + 12 6-hourly)
- Grid: CONUS ~2345 × 1597, ~2.5 km Lambert Conformal
- Horizon: ~260 hours (~11 days)

### NDFD — National Digital Forecast Database

- S3 bucket: `noaa-ndfd-pds` (public, no auth)
- Path: `opnl/AR.conus/{VP.001-003,VP.004-007}/ds.{element}.bin`
- Two periods: VP.001-003 (hourly, days 1–3), VP.004-007 (3-hourly, days 4–7)
- Per-element binary files; ~24 files per cycle
- Grid: CONUS ~2145 × 1377, ~2.5 km Lambert Conformal
- Horizon: ~168 hours (~7 days)

---

## Backend: Slab Ring Buffer Store

Both NBM and NDFD use the same slab ring buffer architecture. Each slab is one forecast-time step for all retained variables across the full spatial grid:

```
shape: (idim, jdim, kvars)
dtype: int16 (scale/offset packed)
order: C (so arr[i, j, :] is contiguous — efficient point queries)
```

Files are fixed-named slots (`slab_000.npy` … `slab_N.npy`) in a ring, with logical ordering tracked in `ring_state.json`. The API reads time series by visiting each slab and reading one `arr[i, j, :]` vector, requiring no full-array loads.

**NBM store:** 56 retained runs × 99 time steps per run (7 days × 8 cycles/day), ~22 GB  
**NDFD store:** 48 retained runs × ~65 time steps per run (~6 days), substantially smaller

After each successful ingest the API hot-swaps its store state via `POST /admin/reload` without restarting gunicorn.

### Incomplete cycles — degrade, don't backfill

**Decision (2026-09-24): when a cycle commits with fewer forecast steps than normal, the missing valid times stay missing. The forecast horizon shortens; the backend does not splice in data from an earlier cycle.**

A cycle can commit truncated when upstream data is incomplete at ingest time. On 2026-09-23 NOAA's NBM S3 mirror lagged ~13 hours; because extended-range files (f038+) are S3-only, the 20Z cycle committed with 40 of 99 slabs and a 2-day horizon instead of 11.

Verified current behaviour — this is clean degradation, not corruption:

- `NBMStore.get_point_timeseries()` resolves one run and walks `fxx_idx` within that slot, so a truncated run simply yields a shorter series. No sentinel values leak into the response.
- Blend responses carry a single contiguous block of trailing nulls where the short NBM tail should have been. NDFD still covers its full 168h, so a truncated NBM costs only the NBM-only tail (days ~7–11).

**Why not fall back to the previous cycle.** It is technically cheap — all 56 runs stay mmap'd, so the older data is already resident, and `_merge_series()` in `blend_forecast.py` already reindexes sources onto a unified valid-time axis with a per-step `source` label. The objection is *forecast drift integrity*, the app's core feature: the backend serves only the current forecast and the client stores snapshots to visualize change. Silently splicing cycle N−1 into cycle N makes the client record a chimera, and the spliced hours then show artificially **zero drift** against the previous snapshot — exactly the signal the app exists to display. A gap is honest; a silent splice is misinformation.

If this is ever revisited, the fallback must be **labeled per time step** (e.g. `source: "nbm@20260923_17"`) so the client can exclude backfilled steps from drift comparison, and it must happen at **read time**, not ingest time — writing borrowed slabs into the ring buffer would make `n_fxx` misreport what was actually fetched and destroy the signal the health monitor's completeness check depends on.

**Frontend implication:** the timeline is variable-length and may end in a run of nulls. Do not hard-code an 11-day axis or assume every snapshot has the same number of steps.

---

## Backend: Variable Registry

`variables.yaml` (NBM) and `variables_ndfd.yaml` (NDFD) are the single sources of truth for both post-processing (which variables to extract) and the API (what clients can request).

**NBM native variables (15):** temperature, dewpoint, relative_humidity, apparent_temperature, wind_speed, wind_direction, wind_gust, total_precipitation, precip_type, thunderstorm_probability, cape, cloud_cover, solar_radiation, visibility, cloud_ceiling

**NDFD native variables (14):** temperature, dewpoint, relative_humidity, apparent_temperature, wind_speed, wind_direction, wind_gust, total_precipitation, thunderstorm_probability (severe), cloud_cover, visibility, cloud_ceiling, wet_bulb_globe_temp, snowfall

**Derived (both):** `sun_elevation` — computed at query time via pysolar from lat/lon + valid_time.

Adding a native variable: add an entry to the relevant YAML; the post-processor picks it up on the next cycle. Adding a derived variable: add an entry (with `derived: true`) and implement the formula in `extraction/derived.py`.

---

## Backend: Blend Strategy

The Blend API (`blend_main.py`, port 8004) reads both stores and merges responses:

- **ndfd_preferred:** NDFD for hours 1–168, NBM fills beyond NDFD horizon  
- **nbm_only:** `precip_type`, `cape`, `solar_radiation`, `thunderstorm_probability` (general)  
- **ndfd_only:** `wet_bulb_globe_temp`, `snowfall`, `thunderstorm_probability_severe`

Every variable array in the blend response has a companion `_source` array (`"ndfd"` / `"nbm"` / `null`). Degraded mode: if one store is down, affected variables return all-null; the other source serves normally.

**Note:** `thunderstorm_probability` (NBM) = general tstm; `thunderstorm_probability_severe` (NDFD) = total severe tstm. They are semantically different — never merged.

---

## Backend: Post-Processing

### File-centric extraction (critical for performance)

Each GRIB2 file is opened **exactly once** with `cfgrib.open_datasets()`, and all applicable variables are extracted in that single pass. The alternative (one open per variable) would be ~15× slower. Do not restructure back to a variable-centric loop.

Uses `ProcessPoolExecutor` (8 workers by default): cfgrib's C extensions hold the GIL, so threading provides no parallelism — processes are required.

### Temporal interpolation

Point queries are upsampled to a uniform 1-hour grid at query time. The store retains only native valid times; interpolation is performed entirely in `zarr_query.py` / `slab_query.py`:

| Variable | Method |
|---|---|
| Continuously varying | Linear (`pd.Series.interpolate(method='time')`) |
| `wind_direction` | Circular (sin/cos → interpolate → atan2) |
| `precip_type` | Forward-fill (categorical) |

`interpolate(method='time')` never extrapolates past the last non-NaN value, so cutoff variables (visibility f076, ceiling f082, thunderstorm f190) naturally return null beyond their range.

---

## Backend: API Services

Three FastAPI services share the same router structure (`/forecast`, `/variables`, `/status`, `/admin/reload`):

| Service | Port | Caddy path | Store |
|---|---|---|---|
| NBM API (`main.py`) | 8001 | `/wxapp/` | NBM slab ring buffer |
| NDFD API (`ndfd_main.py`) | 8002 | `/wxndfd/` | NDFD slab ring buffer |
| Blend API (`blend_main.py`) | 8004 | `/blend/` | Both stores |

**Single gunicorn worker per service** (`-w 1`): `app.state` is process-local. Multi-worker support would require shared memory or a sidecar store; deferred.

**Forecast drift:** the `age_hours` parameter on `/forecast` returns a past retained cycle at least that many hours older than the current run. The frontend uses this to overlay historical forecasts.

**See BACKEND.md** for the full API reference (endpoint parameters, response shapes, variable catalog, merge policy, error codes).

---

## Backend: Ancillary Processes

### Health monitor (`scripts/wxmonitor.py`)

Runs every 20 minutes via `wxmonitor@{nbm,ndfd,blend}.timer`. Pings `GET /status`, POSTs to healthchecks.io (dead-man's switch), and emails on failure. Three checks per service:

1. **Responsiveness** — `/status` answers within the timeout.
2. **Freshness** — the committed cycle is no older than `max_age_h` (6h for all three, matching the 3-hourly ingest cadence).
3. **Completeness** — the committed cycle has at least `min_time_steps` forecast steps: 90 for NBM (full cycle = 99), 50 for NDFD (normal range 59–65). `None` for blend, whose `/status` exposes no step count; its two stores are covered by the NBM and NDFD monitors.

The completeness check exists because freshness alone cannot see a truncated cycle — see *Incomplete cycles* above. Both checks are pure functions (`check_freshness()`, `check_completeness()`) covered by `tests/test_wxmonitor.py`.

Each service pings its **own** healthchecks.io check (`WxApp NBM` / `WxApp NDFD` / `WxApp Blend`). They must never share a UUID: the timers are staggered, so a shared check lets one service's success ping clear another's failure ~20 min later, hiding a single-service outage. `duplicate_hc_uuids()` warns if they collide, and a test asserts they don't.

### METAR archive (`scripts/metar_archive.py`)

Runs hourly at :10 past via `wxmetaringest.timer`. Downloads current METARs from the AWC Aviation Weather Center API for configured stations and appends to JSONL files in `/12TB1/METAR_archive/`. Intended as the "truth" counterpart to the forecast archive (see `backend/app/archive/ARCHIVE.md`).

### Forecast archive (`backend/app/archive/`)

After each NBM or NDFD ingest, the blend API saves a full forecast time series for configured stations to `/12TB1/FCST_series/`. Accumulates training data for a future bias-correction scheme.

---

## Frontend: React PWA

*(Not yet implemented — see ROADMAP.md Phase 4)*

| Concern | Library |
|---|---|
| Framework | React 18 |
| Routing | React Router |
| Styling | Tailwind CSS |
| Charts | Recharts (primary); D3 for custom viz |
| Local storage | IndexedDB via `idb` wrapper |
| PWA | Vite PWA plugin |
| HTTP client | TanStack Query |

### Key views

1. **Activity Manager** — create/edit activities with weather criteria (variable + operator + threshold). Persisted in localStorage.
2. **Location Picker** — address search via OpenStreetMap Nominatim; saved locations in localStorage.
3. **Weather Window Timeline** — suitability bar (green/yellow/red), past snapshot overlays for drift, tap for variable detail popover.
4. **Variable Detail Charts** — standard line charts for individual variables.

### Client-side forecast drift

The frontend stores each fetched forecast in IndexedDB tagged with fetch timestamp and location. The timeline overlays up to 10 past snapshots. Retention limit: last 30 snapshots per location.

---

## Development & Deployment

### Server

| Item | Value |
|---|---|
| Host | `precip.aos.wisc.edu` — Ubuntu, 32 cores, 128 GiB RAM, 10 TB disk |
| Python venv | `/home/gpetty/WxApp/.venv` |
| NBM data root | `/12TB2/NBM/` (env var `DATA_DIR`) |
| NDFD data root | `/12TB2/NDFD/` (env var `NDFD_DATA_DIR`) |
| Forecast archive | `/12TB1/FCST_series/` |
| METAR archive | `/12TB1/METAR_archive/` |
| Reverse proxy | Caddy |

### Repository layout

```
/home/gpetty/WxApp/
├── backend/
│   └── app/
│       ├── config.py               # NBM paths, concurrency, ring buffer params
│       ├── config_ndfd.py          # NDFD paths and params
│       ├── variables.yaml          # NBM variable registry
│       ├── variables_ndfd.yaml     # NDFD variable registry
│       ├── registry.py             # VariableRegistry, NativeVariable, DerivedVariable
│       ├── main.py                 # NBM FastAPI app (port 8001)
│       ├── ndfd_main.py            # NDFD FastAPI app (port 8002)
│       ├── blend_main.py           # Blend FastAPI app (port 8004)
│       ├── archive/
│       │   ├── archiver.py         # forecast archive sweep logic
│       │   ├── config.py           # archive root path
│       │   ├── stations.yaml       # stations to archive
│       │   ├── nbm_variables.yaml  # NBM variables to archive
│       │   └── ndfd_variables.yaml # NDFD variables to archive
│       ├── extraction/
│       │   ├── zarr_query.py       # point query + interpolation
│       │   └── derived.py          # sun_elevation (pysolar)
│       ├── ingest/
│       │   ├── _common.py          # IngestLock, LockError, cycle_tag_in_ring_buffer
│       │   ├── ingest.py           # NBM cycle discovery, download, staging
│       │   ├── ndfd_ingest.py      # NDFD S3 download, staging
│       │   └── __main__.py         # CLI: python -m backend.app.ingest
│       ├── postprocessor/
│       │   ├── grib2_to_zarr.py    # GRIB2 → slab extraction (NBM, file-centric)
│       │   ├── ndfd_slab_ingest.py # GRIB2 → slab extraction (NDFD)
│       │   ├── slab_ingest.py      # NBM slab write coordinator
│       │   ├── conversions.py      # K→F, m/s→mph, m→miles/feet
│       │   └── __main__.py         # CLI: python -m backend.app.postprocessor
│       ├── routers/
│       │   ├── forecast.py         # GET /forecast (NBM + NDFD)
│       │   ├── blend_forecast.py   # GET /blend/forecast, BLEND_RULES
│       │   ├── blend_status.py     # GET /blend/status
│       │   ├── blend_variables.py  # GET /blend/variables
│       │   ├── helpers.py          # shared query utilities
│       │   └── models.py           # Pydantic response models
│       └── store/
│           ├── ring_state.py       # RingState — logical/physical slot mapping
│           ├── writer.py           # SlabWriter — atomic slab writes
│           └── nbm_store.py        # NBMStore — mmap'd point queries
├── scripts/
│   ├── metar_archive.py            # hourly METAR download → JSONL
│   └── wxmonitor.py                # health monitor → healthchecks.io
└── systemd/
    ├── wxapi.service / wxingest.service / wxingest.timer
    ├── wxndfdapi.service / wxndfdingest.service / wxndfdingest.timer
    ├── wxblendapi.service
    ├── wxmetaringest.service / wxmetaringest.timer
    └── wxmonitor@.service / wxmonitor@{nbm,ndfd,blend}.timer
```

---

## Key Implementation Gotchas

**Herbie requires naive UTC datetimes.** Use `datetime.utcnow()` (naive), not `datetime.now(tz=timezone.utc)`. Timezone-aware datetimes cause silent failures.

**Extended-range NBM files (f038+) are S3-only.** Use `priority=["aws", "nomads"]` for all Herbie calls.

**NBM longitude convention: 0–360.** The NBM CONUS grid uses east-positive longitude. Convert user-supplied negative longitudes: `lon_360 = lon + 360 if lon < 0 else lon`. Return `actual_lon` in ±180 in API responses.

**cfgrib shortNames differ from WMO standard:**
- wind speed: `10si` (not `si10`)
- wind gust: `i10fg`
- solar radiation: `sdswrf` (not `dswrf`)
- Filter by `shortName` + `typeOfLevel` only — never by level value. NBM uses non-standard level encoding.

**No dask.** `ds.chunk()` requires dask. Pass chunk sizes via `encoding` in `ds.to_zarr()`. Use `ds.sizes` (not deprecated `ds.dims`).

**pandas Timestamp tz-localize quirk (server's pandas version).** `pd.Timestamp.now("UTC").floor("h")` raises `TypeError`. Instead: construct naive first, then localize: `pd.Timestamp(datetime.utcnow().replace(...)).tz_localize("UTC")`.

**`NativeVariable` is a dataclass** (picklable) — safe for `ProcessPoolExecutor`.

**Single gunicorn worker per service.** `app.state` is process-local. Do not change to `-w N` without adding a shared-memory or sidecar store.

---

## Open Questions / Pending Decisions

1. **healthchecks.io ping URLs are capability secrets, committed inline.** Each service now has its own check (`WxApp NBM` / `WxApp NDFD` / `WxApp Blend`, UUIDs in `scripts/wxmonitor.py`), but anyone holding a ping URL can POST a fake success and silence a real outage. Acceptable for a private repo on a single server. **If this repo is ever published, recreate all three checks and move the new UUIDs into an `EnvironmentFile=` read by the `wxmonitor@.service` unit** — the pattern `DATA_DIR` already uses — since rotating them after exposure means the old UUIDs stay in git history forever.

2. **METAR archive systemd units** (`wxmetaringest.service` / `.timer`). Units exist in `systemd/` but have not yet been installed. Run:
   ```bash
   sudo cp systemd/wxmetaringest.service systemd/wxmetaringest.timer /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now wxmetaringest.timer
   ```

3. **Frontend.** Not yet started. See ROADMAP.md Phase 4.

4. **Zarr compression.** Currently uncompressed float32. The full slab store fits in 128 GiB RAM. Add `Blosc(cname='lz4', clevel=1)` if disk usage becomes a concern.

5. **Multi-worker API.** Currently limited to single gunicorn worker per service. Multi-worker support requires shared memory or a sidecar store; deferred until request volume demands it.

6. **Cross-cycle backfill for incomplete cycles.** Deferred by decision — see *Incomplete cycles — degrade, don't backfill*. Revisit only if the monitor's completeness alerts show truncation is frequent enough to matter, and only under the labeled, read-time constraints described there.

7. **Truncated-commit path in ingest.** The same upstream condition yields two different outcomes depending on timing: if the S3 listing *fails*, the fallback schedule makes every missing file a hard download failure and nothing commits; if the listing *succeeds but is incomplete*, the missing steps are simply "not expected" and a truncated cycle commits silently (`ingest.py`, `download_cycle()`). The health monitor now catches the result, but ingest still creates it. Consider a minimum-horizon guard before commit.
