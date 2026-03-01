# PROJECT: Weather Window Web App

This repo is for the development of a prototype web app that provides real-time forecast information to users based on location and selected variables. Unique features of this app include the following:

1. User can define *activities* (e.g., 'picnic', 'boating', 'construction'). To each activity, the user can assign relevant *variables* (e.g., 'temperature', 'wind speed', 'wind chill', 'sun elevation > 10 degrees') along with *boolean tests* for suitability (e.g., 'heat index < 90F', 'wind chill > 50F', 'wind direction between 30 and 270 degrees').
1. App will display a graphical timeline indicating forecast "weather windows" suitable for the currently selected activity.
1. App will also graphically display (on the same time axis) earlier forecasts for the same period, allowing the user to see how the forecast "weather windows" are changing over time with each forecast update. **Note:** forecast history (drift) is a client-side responsibility. The backend always serves only the current forecast; the client retains a local memory of past forecast snapshots to overlay on the timeline.

---

## General Architecture

See ARCHITECTURE.md for full technical design. Summary:

1. A back-end Ubuntu server (`precip.aos.wisc.edu`) maintains a current repository of CONUS-wide GRIB2 forecast files from the NOAA National Blend of Models (NBM).
1. A Python/FastAPI service provides a REST API that accepts GET requests with lat/lon and a list of requested variables, returning a JSON time series at 1-hour resolution (hours 0–36) blending to 3-hour resolution beyond that.
1. A React front-end (mobile-first PWA) manages the local retention of past forecast snapshots and the graphical display of weather windows and forecast drift.

---

## Data Source

**Primary:** NOAA National Blend of Models (NBM), CONUS domain.
- S3 bucket: `s3://noaa-nbm-grib2-pds/`
- Issued hourly (24 cycles/day). Forecast horizon: 262 hours (~11 days).
- File naming: `blend.tHHz.core.fXXX.co.grib2`
- **Verified fxx schedule** (empirically confirmed against S3):
  - f001–f036: hourly steps, ~150 MB each, available on NOMADS + S3
  - f037–f190: 3-hourly steps (37, 40, 43, …, 190), ~150 MB each, **S3 only**
  - f196–f262: 6-hourly steps (196, 202, 208, …, 262), ~75 MB each, **S3 only**
  - Total: 100 files, ~15 GB per complete forecast cycle
- Strategy: retain only the latest complete forecast cycle on disk. Prior cycles deleted after new cycle is confirmed complete.

**Future (deferred):** NDFD, GFS — user-selectable data source. Architecture should be NBM-only for now; add abstraction layer when needed.

See DATA.md for source documentation and access details.

---

## Technology Decisions

| Layer | Technology | Rationale |
|---|---|---|
| Backend language | Python | Natural fit for GRIB2 processing; rich ecosystem (Herbie, cfgrib, xarray) |
| API framework | FastAPI | Async, auto-docs (OpenAPI), Pydantic validation |
| GRIB2 library | Herbie + cfgrib/xarray | Herbie handles S3 access and message-level subsetting cleanly |
| Data storage | Raw GRIB2 files on disk + Zarr store | Simple; 10 TB disk, 128 GiB RAM; latest cycle only (~15 GB GRIB2, ~30 GB Zarr) |
| Point-query serving | Zarr store memory-mapped or RAM-resident | 128 GiB RAM can hold entire Zarr store; point queries sub-millisecond after warmup |
| Scheduler | systemd timer or cron | Trigger hourly data ingestion |
| Frontend | React (PWA, mobile-first) | Complex stateful UI; rich charting ecosystem (Recharts / D3) |
| Styling | Tailwind CSS | Utility-first; excellent mobile-first support |

---

## Identified Risks / Potential Roadblocks

1. **GRIB2 load latency — mitigated by RAM.** Post-processing GRIB2 → Zarr eliminates per-query GRIB2 loading. With 128 GiB RAM on the production server, the entire Zarr store (~30 GB) can be held in the OS page cache after first access. Point queries will be sub-millisecond once warm.

2. **Derived variables.** Heat index, wind chill, and sun elevation are not raw NBM outputs and must be computed server-side. Sun elevation requires lat/lon + datetime (use `pysolar` or `astropy`). Heat index and wind chill have standard formulas. These must be clearly flagged as derived in the variable registry.

3. **Hourly ingestion window — not a concern.** Server is on a fast university internet connection. ~15 GB/hour (100 files × ~150 MB) should download well within 60 minutes.

4. **Client-side forecast history.** Storing multiple forecast snapshots on a smartphone requires careful use of IndexedDB or a service worker cache. Need to define a retention policy (e.g., keep last 10 snapshots per location) to avoid unbounded growth.

5. **Viability / competition.** Research confirms no existing app combines user-defined multi-criteria activity windows + forecast drift visualization. Closest competitors: Weathergraph (timeline viz, no custom criteria), Apollo Weather (custom alerts, no activity windows). Market interest exists in outdoor/sports communities. **Proceed.**

---

## Current Status

**Phase 1.1–1.5 complete.** The ingestion pipeline and GRIB2 → Zarr post-processor are working end-to-end. Tested with a 3-file subset (one file from each fxx segment): 14 of 15 native variables extracted successfully, Zarr store written in 4.6s with 256×256 spatial chunks. `precip_probability` correctly skipped (only at accumulation-boundary steps, none in the test set). Full 100-file cycle test pending on native Mac hardware.

**Next task: Phase 1.6 — Point extraction & derived variables.** See ROADMAP.md.

Files produced so far:
- `backend/app/variables.yaml` — variable registry (15 native + 3 derived)
- `backend/app/config.py` — central path configuration
- `backend/app/registry.py` — `VariableRegistry` class
- `backend/app/ingest/ingest.py` — core ingestion logic (with `--postprocess` flag)
- `backend/app/ingest/__main__.py` — CLI entry point (`python -m backend.app.ingest`)
- `backend/app/postprocessor/grib2_to_zarr.py` — GRIB2 → Zarr conversion
- `backend/app/postprocessor/conversions.py` — unit conversion functions (K→F, m/s→mph, m→mi)
- `backend/app/postprocessor/__main__.py` — CLI entry point (`python -m backend.app.postprocessor`)
- `scripts/fetch_sample_nbm.py` — download one file for testing
- `scripts/inventory_grib2.py` — inventory all GRIB2 fields in a file
- `requirements.txt`

---

## Key Implementation Notes & Gotchas

These are non-obvious findings from empirical testing — read before writing any new backend code.

### Herbie: always use naive UTC datetimes
`Herbie()` does not accept timezone-aware datetimes. Use `datetime.utcnow()` (naive), **not** `datetime.now(tz=timezone.utc)`. Passing a tz-aware datetime causes silent failures (cycle not found).

### Extended-range files are S3-only
Files f037+ do not exist on NOMADS — only on S3. Herbie's default `priority` tries NOMADS first and fails. All Herbie calls in the ingestion pipeline use `priority=["aws", "nomads"]` to force S3-first lookup.

### fxx schedule: starts at f038, not f037
The 3-hourly segment begins at f038 (not f037 or f039). The 6-hourly segment begins at f194 (not f196). The cycle ends at f260 (not f262). Total: 99 files per cycle (36 + 51 + 12). See `nbm_forecast_hours()` in `ingest.py` for the authoritative implementation. (Note: earlier documentation incorrectly stated f037/f196/f262 — corrected 2026-03-01 by direct S3 listing of the 16Z cycle.)

### Variable availability differs by fxx segment
The 6-hourly segment (f196–f262) has fewer variables than the hourly/3-hourly segments. Verified differences:
- `thunderstorm_probability` (tstm): present f001–f190, **absent f196+** (`fxx_cutoff: 190`)
- `visibility` (vis): present f001–f190, **absent f196+** (`fxx_cutoff: 190`)
- `precip_probability` (pop12): **absent from all standard fxx files**; only appears at accumulation-boundary steps (fxx=006, 012, 018, …). Must be extracted in a separate pass.
- New in f196+ only: `hindex` (native heat index), `tmax` (daily max temp), `sf` (snowfall)

The post-processor must fill NaN for variables beyond their `fxx_cutoff`, and handle `precip_probability` via a separate fxx list.

### cfgrib field identification
NBM uses non-standard GRIB2 level encoding. Many fields have `typeOfLevel=?` (cfgrib can't parse the level type). Filter by `shortName` + `typeOfLevel` only — do not filter by level value. Key verified shortNames that differ from WMO standard names:
- wind speed: shortName `10si` (not `si10`)
- wind gust: shortName `i10fg`
- solar radiation: shortName `sdswrf`
- Fields with `paramId=0` are genuinely empty placeholder records — ignore them.

### Zarr chunk strategy
Target chunk shape `(n_time, 256, 256)` — all time steps for a 256×256 spatial tile. Originally 4×4 was planned, but this produced ~235K chunks per variable (millions of tiny files), making writes extremely slow. 256×256 yields ~70 chunks per variable, writes in ~5s, and point queries still well under 100ms from page cache (~25 MB per chunk read). Do not compress initially; measure size and query latency first.

### NBM longitude convention
The NBM CONUS grid uses 0–360 longitude (east-positive). Western-hemisphere longitudes must be converted: -89.4° → 270.6°. The `latitude` and `longitude` coordinate arrays in the Zarr store are 2D (y, x) and use this convention. The API layer must handle the conversion from user-supplied negative longitudes.

### cfgrib performance and extraction strategy
`cfgrib.open_datasets()` takes ~6–7s per 150 MB file (M4 Mac). The extractor uses a **file-centric loop**: each file is opened exactly once, and all applicable variables are extracted in that single pass. This is ~12–15x faster than the variable-centric alternative (which would open each file once per variable).

Benchmarks (M4 Mac, full 100-file cycle):
- Variable-centric (old): ~3h 24min extraction (1,500 cfgrib opens)
- File-centric (new): ~13–15min extraction (100 cfgrib opens) — estimated from 3-file test

Do NOT restructure back to a variable-centric loop. The file-centric approach in `extract_variables()` is intentional.

### Zarr + xarray: avoid dask dependency
Do NOT use `ds.chunk()` (requires dask). Instead, pass chunk sizes via the `encoding` parameter to `ds.to_zarr()`. Use `ds.sizes` (not deprecated `ds.dims`) for dimension lookups.

---

## Deferred

- User accounts and authentication.
- Multi-source data (NDFD, GFS) with user-selectable source.
- Native mobile app (iOS/Android) — PWA first.

---

## Planning Notes

- All architectural decisions should be documented in ARCHITECTURE.md.
- Update this file and ARCHITECTURE.md as decisions are made or revised.
- Before beginning any new code, review ARCHITECTURE.md and this file.
