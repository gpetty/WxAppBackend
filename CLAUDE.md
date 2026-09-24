# PROJECT: Weather Window Web App

This repo is for the development of a prototype web app that provides real-time forecast information to users based on location and selected variables. Unique features of this app include the following:

1. User can define *activities* (e.g., 'picnic', 'boating', 'construction'). To each activity, the user can assign relevant *variables* (e.g., 'temperature', 'wind speed', 'wind chill', 'sun elevation > 10 degrees') along with *boolean tests* for suitability (e.g., 'heat index < 90F', 'wind chill > 50F', 'wind direction between 30 and 270 degrees').
1. App will display a graphical timeline indicating forecast "weather windows" suitable for the currently selected activity.
1. App will also graphically display (on the same time axis) earlier forecasts for the same period, allowing the user to see how the forecast "weather windows" are changing over time with each forecast update. **Note:** forecast history (drift) is a client-side responsibility. The backend always serves only the current forecast; the client retains a local memory of past forecast snapshots to overlay on the timeline.

---

## General Architecture

See ARCHITECTURE.md for full technical design. Summary:

1. A back-end Ubuntu server (`precip.aos.wisc.edu`) downloads NOAA NBM and NDFD GRIB2 forecasts on 3-hourly systemd timers. Each source is post-processed into an in-memory slab ring buffer (one `.npy` file per forecast time step).
1. Three Python/FastAPI services provide REST APIs: NBM (port 8001), NDFD (port 8002), and a Blend API (port 8004) that merges the two sources. Caddy reverse-proxies all three. The Blend API is the primary endpoint for frontend development.
1. A React front-end (mobile-first PWA) manages the local retention of past forecast snapshots and the graphical display of weather windows and forecast drift.

---

## Data Sources

**Primary:** NOAA National Blend of Models (NBM), CONUS domain.
- S3 bucket: `s3://noaa-nbm-grib2-pds/`
- Ingested 3-hourly (00/03/06/09/12/15/18/21Z). Forecast horizon: ~260 hours (~11 days).
- File naming: `blend.tHHz.core.fXXX.co.grib2`
- **Verified fxx schedule** (empirically confirmed against S3):
  - f001–f036: hourly steps, ~150 MB each, available on NOMADS + S3
  - f038–f188: 3-hourly steps (38, 41, …), ~150 MB each, **S3 only**
  - f194–f260: 6-hourly steps (194, 200, …, 260), ~75 MB each, **S3 only**
  - After `thin_fxx()` thinning: ~99 files per cycle (36 + 51 + 12)
- Strategy: 56 retained cycles in slab ring buffer (7 days × 8 cycles/day).

**Secondary:** NOAA National Digital Forecast Database (NDFD), CONUS domain.
- S3 bucket: `s3://noaa-ndfd-pds/`
- Path: `opnl/AR.conus/{VP.001-003,VP.004-007}/ds.{element}.bin`
- Ingested 3-hourly. Forecast horizon: ~168 hours (~7 days).
- 48 retained cycles in slab ring buffer (~6 days).

---

## Technology Decisions

| Layer | Technology | Rationale |
|---|---|---|
| Backend language | Python | Natural fit for GRIB2 processing; rich ecosystem (Herbie, cfgrib, xarray) |
| API framework | FastAPI | Async, auto-docs (OpenAPI), Pydantic validation |
| GRIB2 library (NBM) | Herbie + cfgrib/xarray | Herbie handles S3 access; cfgrib extracts fields |
| GRIB2 library (NDFD) | eccodes + s3fs | Direct S3 access (no Herbie); eccodes reads reference time |
| Data storage | Slab ring buffer (`.npy` files, mmap'd) | Sub-millisecond point queries; no Zarr/HDF5 overhead |
| Scheduler | systemd timers | 3-hourly ingest for both NBM and NDFD |
| Frontend | React (PWA, mobile-first) | Complex stateful UI; rich charting ecosystem (Recharts / D3) |
| Styling | Tailwind CSS | Utility-first; excellent mobile-first support |
| Reverse proxy | Caddy | Simpler config than nginx; auto-HTTPS |

---

## Identified Risks / Potential Roadblocks

1. **GRIB2 extraction performance — mitigated by file-centric loop + ProcessPoolExecutor.** One cfgrib open per file; all applicable variables extracted in one pass (~278s on server for 99 files, 8 workers). Do NOT restructure back to a variable-centric loop.

2. **Derived variables.** `sun_elevation` is computed server-side at query time via pysolar. `heat_index` and `wind_chill` are handled by NBM's native `apparent_temperature` field (pre-blended, better calibration than hand-computed formulas).

3. **Client-side forecast history.** Storing multiple forecast snapshots on a smartphone requires careful use of IndexedDB. Retention policy: last 30 snapshots per location.

4. **Viability / competition.** Research confirms no existing app combines user-defined multi-criteria activity windows + forecast drift visualization. Closest competitors: Weathergraph (timeline viz, no custom criteria), Apollo Weather (custom alerts, no activity windows). **Proceed.**

---

## Current Status

**Backend pipeline fully operational on production server (precip.aos.wisc.edu).** NBM and NDFD ingestion running on 3-hourly systemd timers. Blend API (port 8004) serving merged NBM+NDFD forecasts via Caddy at `/blend/`.

**Forecast archive operational (2026-05):** blend API archives forecast time series for configured stations after each ingest cycle, writing JSONL files to `/12TB1/FCST_series/`. METAR archive script (`scripts/metar_archive.py`) written; systemd units in `systemd/` but not yet installed on server.

**Ingest module refactored (2026-05):** shared `_common.py` helpers (IngestLock, LockError, cycle_tag_in_ring_buffer, dump_manifest) extracted from both NBM and NDFD pipelines. Magic-number constants moved to `config.py`. Both CLIs now have unified `setup_logging()` and full error handling.

**Next task:** Frontend development (React PWA). See ROADMAP.md Phase 4.

---

## Key Implementation Notes & Gotchas

These are non-obvious findings from empirical testing — read before writing any new backend code.

### Herbie: always use naive UTC datetimes
`Herbie()` does not accept timezone-aware datetimes. Use `datetime.utcnow()` (naive), **not** `datetime.now(tz=timezone.utc)`. Passing a tz-aware datetime causes silent failures (cycle not found).

### Extended-range NBM files are S3-only
Files f038+ do not exist on NOMADS — only on S3. All Herbie calls in the ingestion pipeline use `priority=["aws", "nomads"]` to force S3-first lookup.

### fxx schedule: starts at f038, not f037
The 3-hourly segment begins at f038 (not f037 or f039). The 6-hourly segment begins at f194 (not f196). The cycle ends at f260 (not f262). Total: 99 files per cycle (36 + 51 + 12). See `nbm_forecast_hours()` in `ingest.py` for the authoritative implementation.

### Variable availability differs by fxx segment
- `thunderstorm_probability` (tstm): present f001–f190, **absent f194+** (`fxx_cutoff: 190`)
- `visibility` (vis): present f001–f076 (`fxx_cutoff: 76`)
- `cloud_ceiling` (ceil): present f001–f082 (`fxx_cutoff: 82`)
- `precip_probability` (pop12): removed from registry — absent from all standard core files; not usable for quasi-hourly app

### cfgrib field identification
NBM uses non-standard GRIB2 level encoding. Many fields have `typeOfLevel=?` (cfgrib can't parse the level type). Filter by `shortName` + `typeOfLevel` only — do not filter by level value. Key verified shortNames that differ from WMO standard names:
- wind speed: shortName `10si` (not `si10`)
- wind gust: shortName `i10fg`
- solar radiation: shortName `sdswrf`
- Fields with `paramId=0` are genuinely empty placeholder records — ignore them.

### cfgrib performance and extraction strategy
The extractor uses a **file-centric loop**: each file is opened exactly once, and all applicable variables are extracted in that single pass. ~15× faster than the variable-centric alternative.

Do NOT restructure back to a variable-centric loop. The file-centric approach in `extract_variables()` is intentional.

### NBM longitude convention
The NBM CONUS grid uses 0–360 longitude (east-positive). Western-hemisphere longitudes must be converted: -89.4° → 270.6°. The `latitude` and `longitude` coordinate arrays in the slab store use this convention. The API layer handles the conversion from user-supplied negative longitudes.

### Slab store vs Zarr
The data store is a **slab ring buffer** (`.npy` files, one per forecast time step), NOT Zarr. Each slab has shape `(idim, jdim, kvars)`, dtype `int16`, C-order. Point queries read `slab[i, j, :]` — one contiguous read per slab. The API memory-maps all slab files at startup and reuses those mappings.

### No dask
Do NOT use `ds.chunk()` (requires dask). Pass chunk sizes via the `encoding` parameter to `ds.to_zarr()`. Use `ds.sizes` (not deprecated `ds.dims`) for dimension lookups.

### pandas Timestamp tz-localize quirk (server's pandas version)
`pd.Timestamp.now("UTC").floor("h")` raises `TypeError: Cannot localize tz-aware Timestamp`. Instead: construct naive first, then localize: `pd.Timestamp(datetime.utcnow().replace(...)).tz_localize("UTC")`.

### Single gunicorn worker per service
`app.state` is per-process. Do not change to `-w N` without adding a shared-memory or sidecar store mechanism.

### Ingest shared helpers (`_common.py`)
`IngestLock`, `LockError`, `cycle_tag_in_ring_buffer`, `dump_manifest`, `read_manifest`, and `setup_logging` all live in `backend/app/ingest/_common.py`. Both NBM (`ingest.py`) and NDFD (`ndfd_ingest.py`) import from there. `LockError` and `read_manifest` are re-exported from `ingest.py` for `__main__.py` backward compatibility.

---

## Deferred

- User accounts and authentication.
- GFS as third data source (extended range beyond NBM's 11 days).
- Native mobile app (iOS/Android) — PWA first.
- Pipelining downloads → slab extraction mid-cycle (requires redesigning ring-buffer write atomicity in `writer.py`/`ring_state.py`).

---

## Planning Notes

- All architectural decisions should be documented in ARCHITECTURE.md.
- Update this file and ARCHITECTURE.md as decisions are made or revised.
- Before beginning any new code, review ARCHITECTURE.md and this file.
