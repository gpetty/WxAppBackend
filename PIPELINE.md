# Weather Window — Backend Pipeline Reference

*Reflects the implemented system as of 2026-03-02.*
*See `BACKEND.md` for the API reference (endpoint parameters, response shapes, variable names).*
*See `ARCHITECTURE.md` for design intent and open questions.*

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Directory Layout](#2-directory-layout)
3. [Systemd Services](#3-systemd-services)
4. [End-to-End Data Flow](#4-end-to-end-data-flow)
5. [Ingestion Pipeline](#5-ingestion-pipeline)
6. [Post-Processor (GRIB2 → Zarr)](#6-post-processor-grib2--zarr)
7. [Variable Registry](#7-variable-registry)
8. [FastAPI Service](#8-fastapi-service)
9. [Query Layer](#9-query-layer)
10. [Health Monitor](#10-health-monitor)
11. [Configuration Reference](#11-configuration-reference)
12. [Key Implementation Decisions](#12-key-implementation-decisions)
13. [Measured Performance](#13-measured-performance)
14. [Common Operations](#14-common-operations)

---

## 1. System Overview

```
┌──────────────────────────────────────────────────────────────────┐
│   NOAA S3: noaa-nbm-grib2-pds                                    │
│   blend.YYYYMMDD/HH/core/blend.tHHz.core.fXXX.co.grib2          │
│   Hourly cycles · 99 files · ~15 GB per cycle                    │
└────────────────────────┬─────────────────────────────────────────┘
                         │ Herbie (6 parallel threads)
                         │ ~210s for 99 files
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│   GRIB2 Staging  /12TB2/NBM/staging/{YYYYMMDD_HH}/              │
│   Raw .grib2 files + manifest.json                               │
└────────────────────────┬─────────────────────────────────────────┘
                         │ cfgrib + ProcessPoolExecutor (8 workers)
                         │ ~300s for 99 files
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│   Zarr Store  /12TB2/NBM/zarr/{YYYYMMDD_HH}/                    │
│   xarray Dataset · (valid_time, y, x) · float32                  │
│   15 native variables · ~25–30 GB uncompressed                   │
│   current  →  {YYYYMMDD_HH}  (symlink, updated atomically)       │
└────────────────────────┬─────────────────────────────────────────┘
                         │ xr.open_zarr() — opened once at startup
                         │ sub-millisecond point queries (page cache)
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│   FastAPI   127.0.0.1:8001  (gunicorn -w 1)                      │
│   GET /forecast  GET /variables  GET /status                     │
└────────────────────────┬─────────────────────────────────────────┘
                         │ nginx reverse proxy
                         │ https://precip.aos.wisc.edu/wxapp
                         ▼
                    React PWA client
```

| Item | Value |
|---|---|
| Server | `precip.aos.wisc.edu` — Ubuntu, 32 cores, 128 GiB RAM, 10 TB disk |
| Data root | `/12TB2/NBM` (set via `DATA_DIR` env var in service files) |
| Virtualenv | `/home/gpetty/WxApp/.venv` |
| Repo root | `/home/gpetty/WxApp` |
| Data source | NOAA National Blend of Models (NBM), CONUS domain |
| S3 bucket | `s3://noaa-nbm-grib2-pds/` (public, anonymous, us-east-1) |
| NBM grid | ~2345 × 1597 points, ~2.5 km resolution, LCC projection |
| API port | `127.0.0.1:8001` (nginx proxies from public HTTPS) |

---

## 2. Directory Layout

### Repository

```
/home/gpetty/WxApp/
├── backend/
│   └── app/
│       ├── config.py               # central paths + concurrency settings
│       ├── variables.yaml          # variable registry — single source of truth
│       ├── registry.py             # VariableRegistry, NativeVariable, DerivedVariable
│       ├── main.py                 # FastAPI app, lifespan, admin endpoints
│       ├── routers/
│       │   ├── forecast.py         # GET /forecast
│       │   ├── status.py           # GET /status
│       │   ├── variables.py        # GET /variables
│       │   └── models.py           # Pydantic response models
│       ├── extraction/
│       │   ├── zarr_query.py       # open_zarr_store, query_forecast
│       │   └── derived.py          # sun_elevation computation (pysolar)
│       ├── ingest/
│       │   ├── ingest.py           # cycle discovery, download, lock file
│       │   └── __main__.py         # CLI: python -m backend.app.ingest
│       └── postprocessor/
│           ├── grib2_to_zarr.py    # GRIB2 → Zarr: extraction, assembly, write, swap
│           ├── conversions.py      # unit conversion functions (K→F, m/s→mph, etc.)
│           └── __main__.py         # CLI: python -m backend.app.postprocessor
└── scripts/
    └── wxmonitor.py                # health monitor — pings healthchecks.io
```

### Data root (`/12TB2/NBM/`)

```
/12TB2/NBM/
├── staging/
│   └── {YYYYMMDD_HH}/              # one directory per downloaded cycle
│       ├── manifest.json           # cycle metadata + downloaded file list
│       └── {herbie cache subdirs}/
│           └── blend.tHHz.core.fXXX.co.grib2
├── zarr/
│   ├── {YYYYMMDD_HH}/              # named cycle Zarr store (retained)
│   │   ├── .zmetadata              # consolidated Zarr metadata
│   │   ├── temperature/            # one directory per variable
│   │   ├── wind_speed/
│   │   ├── ...
│   │   ├── valid_time/             # datetime64[ns] coordinate
│   │   ├── latitude/               # 2D float32, shape (y, x)
│   │   └── longitude/              # 2D float32, shape (y, x), values 0–360
│   ├── current -> {YYYYMMDD_HH}    # symlink — always points to live store
│   └── staging_new/                # in-flight write (transient; renamed on completion)
└── ingest.lock                     # PID lock file
```

**Zarr retention:** 00Z, 06Z, 12Z, 18Z stores for the past 7 days are kept; all others
are pruned after each successful post-processing run. The most recent store is always
kept regardless of cycle hour.

**GRIB2 staging:** the most recently completed cycle's staging directory is kept for
debugging; all prior staging directories are deleted after post-processing.

---

## 3. Systemd Services

### `wxapi.service` — FastAPI (persistent)

```ini
[Service]
Type=simple
User=gpetty
WorkingDirectory=/home/gpetty/WxApp
Environment=DATA_DIR=/12TB2/NBM
ExecStart=/home/gpetty/WxApp/.venv/bin/gunicorn backend.app.main:app \
    -k uvicorn.workers.UvicornWorker \
    -w 1 \
    --bind 127.0.0.1:8001 \
    --root-path /wxapp \
    --timeout 30
Restart=always
RestartSec=5
```

**Single worker is required.** The Zarr dataset lives in `app.state`, which is
process-local. A multi-worker setup would require a shared-memory or reload-broadcast
mechanism — deferred.

### `wxingest.service` — Ingestion (oneshot, triggered by timer)

```ini
[Service]
Type=oneshot
User=gpetty
WorkingDirectory=/home/gpetty/WxApp
Environment=DATA_DIR=/12TB2/NBM
Environment=AWS_NO_SIGN_REQUEST=yes
ExecStart=/home/gpetty/WxApp/.venv/bin/python -m backend.app.ingest --postprocess
ExecStartPost=-/usr/bin/curl -s -X POST http://127.0.0.1:8001/admin/reload
StandardOutput=journal
StandardError=journal
```

The `-` prefix on `ExecStartPost` makes the reload curl best-effort — a failure there
does not fail the service unit.

### `wxingest.timer` — Hourly trigger

```ini
[Timer]
OnCalendar=hourly
RandomizedDelaySec=5min
Persistent=true
```

Fires at the top of each hour ± up to 5 min random jitter. `Persistent=true` catches
up a missed run after a reboot.

### `wxmonitor.service` / `wxmonitor.timer` — Health monitor

```ini
[Timer]
OnBootSec=2min
OnUnitActiveSec=20min
Persistent=true
```

Runs `scripts/wxmonitor.py` every 20 minutes. See [§10 Health Monitor](#10-health-monitor).

---

## 4. End-to-End Data Flow

```
[wxingest.timer — fires at top of hour ± 5 min]
        │
        ▼
[wxingest.service]
 python -m backend.app.ingest --postprocess
        │
        ├─ 1. Acquire lock  (/12TB2/NBM/ingest.lock, PID-based)
        │
        ├─ 2. Discover latest available NBM cycle
        │      Walk back from (now - 2h), probe S3 for f001 existence via Herbie
        │      Returns naive UTC datetime (timezone-aware datetimes break Herbie)
        │
        ├─ 3. Check if already staged
        │      If manifest.json exists in staging/{cycle_tag}/, skip download
        │      (unless --force)
        │
        ├─ 4. Download 99 GRIB2 files
        │      Pass 1: S3 directory listing → download available files (6 threads)
        │      Passes 2–6: wait 300s, re-list S3, download newly-posted or failed files
        │      ~210s total on server
        │
        ├─ 5. Write manifest.json
        │
        ├─ 6. Post-process: GRIB2 → Zarr  (~300s)
        │      ├─ Load variables.yaml registry
        │      ├─ Build per-file task list (which vars apply at each fxx)
        │      ├─ ProcessPoolExecutor (8 workers):
        │      │   each worker: cfgrib.open_datasets() → extract vars → unit convert
        │      │   → return {fxx, records{var→(valid_time, arr)}, lat, lon}
        │      ├─ Main process assembles per-variable (valid_time, y, x) DataArrays
        │      ├─ Build xr.Dataset + attach coords + cycle metadata attributes
        │      ├─ Write Zarr to zarr/staging_new/
        │      │   chunk shape: (n_time, 256, 256), uncompressed float32
        │      │   encoding via to_zarr(encoding=...) — no dask
        │      ├─ Atomic swap:
        │      │   staging_new → zarr/{cycle_tag}/  (rename, same filesystem)
        │      │   os.symlink(cycle_tag, tmp) + os.replace(tmp, current)
        │      ├─ Prune old Zarr stores (keep 7d of 00/06/12/18Z)
        │      └─ Prune old GRIB2 staging dirs (keep most recent for debug)
        │
        ├─ 7. Release lock
        │
        └─ 8. ExecStartPost: POST /admin/reload
               API calls _load_state(): re-opens all cycle Zarr stores, updates app.state

[wxmonitor.timer — fires every 20 min]
        │
        └─ GET /status → parse runtime → age > 3h or unreachable → POST hc-ping.com/fail
           Both checks pass → POST hc-ping.com (success, resets dead-man's switch)
```

---

## 5. Ingestion Pipeline

**Module:** `backend/app/ingest/ingest.py`
**CLI:** `python -m backend.app.ingest [--postprocess] [--dry-run] [--force] [--workers N] [--fxx-max N]`

### NBM fxx schedule (empirically verified 2026-03-01, 16Z cycle)

| Segment | fxx range | Step | Approx. size | Sources |
|---|---|---|---|---|
| Hourly | f001–f036 | 1 h | ~150 MB | NOMADS + S3 |
| 3-hourly | f038–f188 | 3 h | ~150 MB | S3 only |
| 6-hourly | f194–f260 | 6 h | ~75 MB | S3 only |
| **Total** | **99 files** | | **~15 GB** | |

`nbm_forecast_hours()` encodes this as a fallback schedule. In normal operation,
`list_available_fxx()` queries the actual S3 directory listing, which is authoritative —
the schedule can vary slightly between cycles.

### Cycle discovery

`find_latest_cycle()` probes S3 starting 2 hours before the current UTC hour and
walks backward up to 24 hours. It checks for `f001` existence via `Herbie.grib`.
Returns a naive UTC `datetime` — Herbie does not accept timezone-aware objects.

### Download strategy

`download_cycle()` uses a multi-pass retry loop:

1. **Pass 1:** List S3 → download all available files in parallel (6 threads via
   `ThreadPoolExecutor`). Each file downloaded by a `Herbie` call with
   `priority=["aws", "nomads"]` — S3-first for consistency; extended-range files
   (f038+) are S3-only.
2. **Passes 2–N:** Wait `retry_delay` seconds (default 300s), re-list S3 (discovers
   newly-posted extended-range files), retry any still-missing files.
3. **Exit:** All S3-listed files downloaded with no failures, or `max_retries`
   exhausted (default 5 passes total).

Raises `RuntimeError` only if a file confirmed present on S3 still fails to download.
Files that never appear on S3 are logged as warnings — the schedule can vary by cycle.

### Lock file

`IngestLock` writes the current PID to `/12TB2/NBM/ingest.lock`. On entry it checks
whether the existing PID is still running; stale locks (dead PID) are removed silently.
Prevents overlapping hourly runs without requiring a daemon.

---

## 6. Post-Processor (GRIB2 → Zarr)

**Module:** `backend/app/postprocessor/grib2_to_zarr.py`
**CLI:** `python -m backend.app.postprocessor <staging_dir> [--workers N] [--keep-staging]`
**Entry point:** `run_postprocessor(staging_dir, cycle_tag, delete_staging, workers)`

### Extraction: file-centric design

Each GRIB2 file is opened **exactly once** with `cfgrib.open_datasets()`, and all
applicable variables are extracted in that single pass. This is ~15× faster than the
variable-centric alternative (one file open per variable × 99 files × 15 variables =
1,485 opens vs 99 opens).

`cfgrib.open_datasets()` splits the file into groups by level type, returning a list
of xarray Datasets. For each variable, `_find_variable_in_datasets()` searches the
groups matching by `cfgrib_var_key`, then validates `grib_shortName`.

**Exception:** `total_precipitation` uses a separate `cfgrib.open_dataset()` call with
an explicit `stepRange` filter (`{fxx-1}-{fxx}`) to always extract the 1-hour QPF01
accumulation. The same GRIB2 shortName (`tp`) appears for 1-h, 6-h, and 12-h
accumulations in the same file; the stepRange filter disambiguates.

### Parallelism

`ProcessPoolExecutor` with 8 workers (default). Each subprocess handles one GRIB2 file
independently and returns a dict of extracted arrays + log messages to the main
process for assembly. `ThreadPoolExecutor` is not used here because cfgrib's C
extensions require GIL bypass.

`NativeVariable` is a `dataclass` (picklable) — safe to pass across process boundaries.

`_extract_file_worker()` is a module-level function (not a method or closure) so it
can be pickled by `ProcessPoolExecutor`.

### Variable applicability (`_should_extract`)

Each variable is only extracted from files where it is expected to be present:

- `fxx_cutoff`: variable absent for `fxx > cutoff`. Files beyond the cutoff are
  skipped for that variable; the resulting time steps are NaN in the assembled array.
- `fxx_availability`: variable only present at specific fxx values (not currently in
  use).
- No constraint: variable expected in every file.

### Dataset assembly

After all workers complete, the main process:
1. Sorts each variable's records by `valid_time`.
2. Stacks into `(n_time, y, x)` numpy arrays.
3. Builds `xr.DataArray` with `dims=["valid_time", "y", "x"]`.
4. Assembles `xr.Dataset` from all variables. xarray aligns variables with different
   `valid_time` extents automatically, filling NaN for absent time steps.
5. Attaches 2D `latitude` and `longitude` coordinate arrays (captured from the first
   successfully parsed GRIB2 file).
6. Adds global attributes: `cycle` (ISO-8601 string), `created` (UTC timestamp).

### Zarr write

```python
chunk_spec = (n_time, 256, 256)   # all time steps, 256×256 spatial tile
encoding   = {var: {"chunks": chunk_spec} for var in ds.data_vars}
ds.to_zarr(zarr_staging, mode="w", encoding=encoding, consolidated=True)
```

Key constraints:
- **Do not use `ds.chunk()`** — requires dask. Pass chunk sizes via `encoding` instead.
- **Do not compress** initially — uncompressed float32. The full store fits in 128 GiB
  RAM. Add Blosc/LZ4 if disk becomes a concern.
- **Use `ds.sizes`**, not deprecated `ds.dims`, for dimension lookups.

Zarr store structure per variable:
```
temperature/
  .zarray        (shape, dtype, chunk shape, compressor)
  0.0.0          (chunk file: time_idx=0, y_tile=0, x_tile=0)
  0.0.1
  ...
  0.6.9          (~70 chunk files total per variable)
```

Chunk count per variable: `ceil(1597/256) × ceil(2345/256) = 7 × 10 = 70`.
Chunk size: `99 × 256 × 256 × 4 bytes ≈ 25 MB`.

### Atomic swap

```
zarr/staging_new/          (just written by write_zarr)
        │
        │  os.rename(staging_new, zarr/20260302_14)   # fast, same filesystem
        ▼
zarr/20260302_14/
        │
        │  os.symlink("20260302_14", zarr/current.new)
        │  os.replace(zarr/current.new, zarr/current)  # atomic on POSIX
        ▼
zarr/current  →  20260302_14
```

`zarr/current` is always either the previous complete store or the new complete store —
never missing, never partial.

### Retention pruning

After the swap, `prune_zarr_stores()` deletes any named store that is:
- More than 7 days old, **or**
- Not a major cycle hour (00Z, 06Z, 12Z, 18Z)

The current store and any in-progress names are always preserved.

`prune_staging_dirs()` deletes all GRIB2 staging directories except the most recently
completed cycle (kept for debugging).

---

## 7. Variable Registry

**File:** `backend/app/variables.yaml` — single source of truth for both the
post-processor (what to extract) and the API (what clients can request).

**Class:** `backend/app/registry.py` — `VariableRegistry` loads and validates the
YAML. Exposes `.native()` → `dict[str, NativeVariable]` and `.derived()` → `dict[str, DerivedVariable]`.

### Native variables (extracted from GRIB2, stored in Zarr)

| Name | cfgrib key | GRIB2 shortName | Raw → Out | fxx_cutoff | Notes |
|---|---|---|---|---|---|
| `temperature` | `t2m` | `2t` | K → °F | — | |
| `dewpoint` | `d2m` | `2d` | K → °F | — | |
| `relative_humidity` | `r2` | `2r` | % → % | — | |
| `apparent_temperature` | `aptmp` | `aptmp` | K → °F | — | NBM blended feels-like |
| `wind_speed` | `si10` | `10si` | m/s → mph | — | Non-standard shortName |
| `wind_direction` | `wdir10` | `10wdir` | degrees → degrees | — | |
| `wind_gust` | `i10fg` | `i10fg` | m/s → mph | — | |
| `total_precipitation` | `tp` | `tp` | kg/m² → mm | — | `grib_accum_hours=1` |
| `precip_type` | `ptype` | `ptype` | code → code | — | GRIB2 table 4.201 |
| `thunderstorm_probability` | `tstm` | `tstm` | % → % | 190 | |
| `cape` | `cape` | `cape` | J/kg → J/kg | — | |
| `cloud_cover` | `tcc` | `tcc` | % → % | — | |
| `solar_radiation` | `sdswrf` | `sdswrf` | W/m² → W/m² | — | Non-standard shortName |
| `visibility` | `vis` | `vis` | m → miles | 76 | |
| `cloud_ceiling` | `ceil` | `ceil` | m → feet | 82 | |

**cfgrib field identification notes:**

- Filter by `shortName` + `typeOfLevel` only — **never by level value**. NBM uses
  non-standard level encoding; cfgrib reports `typeOfLevel=?` for many fields, but
  `shortName` is reliable.
- `wind_speed` shortName is `10si` (not the WMO standard `si10`).
- `solar_radiation` shortName is `sdswrf` (not `dswrf`).
- `wind_gust` key is `i10fg`.
- Fields with `paramId=0` are genuine empty placeholder records — cfgrib returns them;
  ignore them.

**`total_precipitation` special handling:** `grib_accum_hours: 1` triggers an explicit
`stepRange={fxx-1}-{fxx}` filter in `_open_with_accum_filter()`. This forces
extraction of the 1-hour QPF01 accumulation, avoiding accidental pickup of QPF06 or
QPF12 messages with the same shortName.

**`thunderstorm_probability` behavior:** Present at every step through f082; then only
at every-6-hour boundaries through f190 (with missing 3-hourly steps in between);
absent entirely in the 6-hourly segment (f194+). The post-processor extracts what is
present and skips what is absent. `fxx_cutoff: 190` prevents attempting f194+ files.

### Derived variables (computed at query time, not in Zarr)

| Name | Requires | Formula |
|---|---|---|
| `sun_elevation` | lat, lon, valid_time | `pysolar.solar.get_altitude()` |

`heat_index` and `wind_chill` are intentionally absent — `apparent_temperature` is a
native NBM pre-blended feels-like field that covers both with better model calibration
than hand-computed formulas.

**Adding a native variable:** add an entry to `variables.yaml`; the post-processor
picks it up on the next ingest cycle with no code changes.

**Adding a derived variable:** add an entry to `variables.yaml` (with `derived: true`)
and implement the formula in `extraction/derived.py`.

---

## 8. FastAPI Service

**Module:** `backend/app/main.py`

### Startup

The `lifespan` context manager calls `_load_state(app)` once at startup:

1. Instantiate `VariableRegistry` from `variables.yaml`.
2. Discover all retained named cycle stores in `zarr/` (directories matching
   `%Y%m%d_%H`; symlinks excluded).
3. Open each with `xr.open_zarr()` — lazy: reads only chunk metadata, not data.
4. Resolve current cycle tag from `os.readlink(zarr/current)`.
5. Store on `app.state`:

| `app.state` field | Type | Description |
|---|---|---|
| `ds` | `xr.Dataset` | Current cycle dataset (lazy-opened) |
| `stores` | `dict[str, xr.Dataset]` | All retained cycles, keyed by `YYYYMMDD_HH` tag |
| `current_tag` | `str` | e.g. `"20260302_14"` |
| `registry` | `VariableRegistry` | Loaded from `variables.yaml` |
| `zarr_path` | `Path` | Path to `zarr/current` |
| `last_loaded` | `datetime` | UTC timestamp of last `_load_state()` call |

If `zarr/current` does not exist at startup, the API starts in a degraded state:
`/variables` and `/status` respond normally; `/forecast` returns 503 (guarded by
the `require_store` middleware).

### Hot reload

`POST /admin/reload` calls `_load_state(app)` again, replacing all `app.state` fields
in-place. Triggered by `ExecStartPost` in `wxingest.service` after each successful
ingest cycle. Not in the OpenAPI schema; restrict to localhost at the nginx layer.

### Routing

| Path | Handler | Notes |
|---|---|---|
| `GET /forecast` | `routers/forecast.py` | Main data endpoint |
| `GET /variables` | `routers/variables.py` | Variable registry |
| `GET /status` | `routers/status.py` | Cycle info |
| `POST /admin/reload` | `main.py` | Internal; not in schema |
| `GET /review` | `main.py` | Serves `forecast_review.html` (dev tool) |
| `GET /docs` | FastAPI auto | OpenAPI/Swagger |

### CORS

Open to all origins during prototype (`allow_origins=["*"]`). Restrict in production.

---

## 9. Query Layer

**Module:** `backend/app/extraction/zarr_query.py`

### `query_forecast(ds, lat, lon, variables, registry, start, end)`

Full pipeline for one API request:

**Step 1 — Classify variables**

Partition `variables` into native, derived, and unknown. Unknown names are logged and
omitted. Identify extra native variables needed as inputs for derived computation (e.g.
`temperature` and `relative_humidity` if a future `heat_index` derived var is added).

**Step 2 — Find nearest grid point**

```python
lon_360 = lon + 360.0 if lon < 0.0 else lon          # ±180 → 0–360
dist2   = (lat_grid - lat)**2 + (lon_grid - lon_360)**2
y_idx, x_idx = np.unravel_index(np.argmin(dist2), dist2.shape)
```

Uses squared Euclidean distance over the 2D `(y, x)` lat/lon arrays stored in the
Zarr store. Flat-earth error (~0.3% at 50°N) is well under one 2.5 km grid cell.
`actual_lon` returned to the caller is converted back to ±180.

**Step 3 — Extract raw time series**

```python
cols[var] = ds[var].values[:, y_idx, x_idx]   # reads one Zarr chunk per variable
```

Returns a `pd.DataFrame` indexed by the raw Zarr `valid_time` coordinate — irregular
spacing (1 h for f001–f036, then 3 h, then 6 h).

**Step 4 — Upsample to 1-hour grid**

1. Build a uniform hourly `DatetimeIndex` spanning the raw series.
2. Mark rows not in the original Zarr `valid_time` as `interpolated=True`.
3. Reindex the DataFrame (inserts NaN at new rows).
4. Interpolate each column by variable type:

| Variable | Method | Rationale |
|---|---|---|
| Most | `pd.Series.interpolate(method='time')` | Linear in time; no extrapolation past last valid value |
| `wind_direction` | Circular: sin/cos → interpolate → atan2 % 360 | Handles 350°→10° wraparound; midpoint = 0°, not 180° |
| `precip_type` | Forward-fill (`ffill`) | Categorical codes; arithmetic interpolation is meaningless |

`interpolate(method='time')` never extrapolates past the last non-NaN value.
Variables with `fxx_cutoff` (visibility f076, ceiling f082, thunderstorm f190)
therefore have NaN from their last valid time onward — no special-casing required.

**Step 5 — Compute derived variables**

`compute_derived()` in `extraction/derived.py` dispatches to the appropriate formula.
Currently: `sun_elevation` via `pysolar.solar.get_altitude(lat, lon, datetime_utc)`.

**Step 6 — Apply time filter**

Slice `df` to `[start, end]` if provided. Timestamps are normalized to UTC.

**Step 7 — Return**

`(df, actual_lat, actual_lon)` — DataFrame with columns for all requested variables
plus the boolean `interpolated` column.

---

## 10. Health Monitor

**Script:** `scripts/wxmonitor.py`
**Trigger:** `wxmonitor.timer` — every 20 minutes
**Service:** `healthchecks.io` (free tier, period=30 min, grace=10 min)
**UUID:** configured in the script (`HC_UUID`)

### Check logic

1. `GET http://127.0.0.1:8001/status` with 10s timeout.
   - On failure: `POST hc-ping.com/{UUID}/fail` with reason message → exit 1.
2. Parse `runtime` field. Compute `age = now_utc - runtime`.
   - If `age > 3 hours`: `POST hc-ping.com/{UUID}/fail` with reason → exit 1.
3. Both pass: `POST hc-ping.com/{UUID}` (success ping) → exit 0.

All outcomes are logged to the systemd journal via `print(..., flush=True)`.

If no success ping arrives within 40 min (period + grace), healthchecks.io fires its
alert. This also catches the monitor itself crashing or the server going down.

### Staleness threshold: 3 hours

NBM cycles take ~1–2 h to appear on S3 after their nominal issue time, plus ~10 min
for ingest + post-processing. The loaded `runtime` is therefore typically ~2h behind
wall clock in steady state. A 3-hour threshold means an alert fires only when at least
one complete ingest cycle has been missed beyond normal lag.

---

## 11. Configuration Reference

**File:** `backend/app/config.py`

| Constant | Default | Env var | Description |
|---|---|---|---|
| `DATA_ROOT` | `{repo}/data/nbm` | `DATA_DIR` | Root for all GRIB2 + Zarr data |
| `STAGING_DIR` | `{DATA_ROOT}/staging` | — | GRIB2 download staging |
| `ZARR_DIR` | `{DATA_ROOT}/zarr` | — | Zarr store parent directory |
| `LOCK_FILE` | `{DATA_ROOT}/ingest.lock` | — | Ingestion PID lock |
| `NBM_MODEL` | `"nbm"` | — | Herbie model identifier |
| `NBM_PRODUCT` | `"co"` | — | CONUS domain product code |
| `DOWNLOAD_WORKERS` | `6` | `DOWNLOAD_WORKERS` | Parallel download threads |
| `POSTPROCESS_WORKERS` | `8` | `POSTPROCESS_WORKERS` | Parallel cfgrib extraction processes |
| `ZARR_RETAIN_DAYS` | `7` | — | Days of named stores to keep |
| `ZARR_RETAIN_HOURS` | `(0, 6, 12, 18)` | — | Cycle hours eligible for retention |

All paths derive from `REPO_ROOT` (`Path(__file__).resolve().parent.parent.parent`),
making the project relocatable. Override `DATA_DIR` in the systemd service files or
shell to point to a non-default data volume.

---

## 12. Key Implementation Decisions

**File-centric extraction (not variable-centric)**

One `cfgrib.open_datasets()` call per GRIB2 file; all applicable variables extracted
in that pass. The alternative (one file open per variable) would mean 99 × 15 = 1,485
cfgrib opens — measured ~15× slower (~3.4 h vs ~5 min on 8 workers). The file-centric
loop in `extract_variables()` is intentional. Do not restructure it.

**`ProcessPoolExecutor` for post-processing**

cfgrib's C extensions hold the GIL. `ThreadPoolExecutor` would not provide meaningful
parallelism. Each subprocess handles one GRIB2 file and returns numpy arrays to the
main process for assembly.

**Zarr chunk shape `(n_time, 256, 256)`**

All time steps for a spatial tile in one chunk. One chunk read per variable per point
query. The original plan (4×4 tiles) produced ~235K chunks per variable (millions of
tiny files), making writes extremely slow. 256×256 writes in ~5s, queries from page
cache in < 1 ms.

**No dask**

`ds.chunk()` requires dask. Chunk sizes are specified via the `encoding` parameter to
`ds.to_zarr()`. The data stays as in-memory numpy arrays throughout processing. Use
`ds.sizes` (not the deprecated `ds.dims`) for dimension lookups.

**Atomic symlink swap via `os.replace()`**

`os.replace(tmp_link, current_link)` is atomic on POSIX. The API never sees a
partially-written or missing store — `zarr/current` is always a valid, complete store.

**Single gunicorn worker**

`app.state` is per-process. Multiple workers cannot share the in-memory Zarr dataset.
Multi-worker support would require shared memory or a sidecar store; deferred until
request volume demands it.

**Herbie requires naive UTC datetimes**

Use `datetime.utcnow()` (naive UTC), **not** `datetime.now(tz=timezone.utc)` (aware).
Passing a timezone-aware datetime to `Herbie()` causes a silent failure where no cycle
is found. This is a known Herbie API quirk.

**S3-first download (`priority=["aws", "nomads"]`)**

Extended-range files (f038+) are S3-only. Using this priority for all files also avoids
NOMADS rate limits on the hourly segment. All Herbie calls in the ingestion pipeline
use this priority.

**NBM longitude convention**

The NBM CONUS grid uses 0–360 east-positive longitude. The `latitude` and `longitude`
coordinate arrays in the Zarr store use this convention. `_find_nearest_grid_point()`
converts user-supplied negative longitudes to 0–360 before the grid search and
converts `actual_lon` back to ±180 for the API response.

**No Zarr compression (initially)**

Uncompressed float32. At ~25–30 GB per cycle and 128 GiB RAM, the full store fits in
the OS page cache after first access. Add `Blosc(cname='lz4', clevel=1)` compression
if disk usage becomes a concern (~50% reduction, negligible decompression overhead).

---

## 13. Measured Performance

Measured on `precip.aos.wisc.edu` (32 cores, 128 GiB RAM), 2026-03-01.

| Stage | Configuration | Time |
|---|---|---|
| Download 99 GRIB2 files (~15 GB) | 6 threads (Herbie) | ~210s |
| Extract + post-process 99 files | 8 processes (cfgrib) | ~300s |
| Zarr write (15 vars, 99 time steps) | — | ~5s |
| Atomic swap + symlink update | — | < 1s |
| Zarr cold open (xr.open_zarr) | — | < 1s (metadata only) |
| Point query, 1 variable (warm cache) | 1 chunk read ~25 MB | < 1 ms |
| Point query, 10 variables (warm cache) | 10 chunk reads | < 10 ms |

**Total ingest-to-live latency:** ~515s (~8.5 min) from timer trigger to updated
API response.

**Memory footprint:** ~25–30 GB (Zarr page cache) + negligible for open metadata
handles. Well within 128 GiB. No explicit `ds.load()` needed.

---

## 14. Common Operations

**Watch a live ingest run:**
```bash
journalctl -u wxingest.service -f
```

**Check the most recent ingest outcome:**
```bash
journalctl -u wxingest.service -n 80 --no-pager
```

**Force a re-download + re-process of the current cycle:**
```bash
python -m backend.app.ingest --postprocess --force
```

**Run only the post-processor on an already-staged cycle:**
```bash
python -m backend.app.postprocessor /12TB2/NBM/staging/20260302_14
```

**Download only (no post-processing):**
```bash
python -m backend.app.ingest --workers 6
```

**Dry-run (no downloads, no writes):**
```bash
python -m backend.app.ingest --dry-run
```

**Manually reload the API after writing a new Zarr store:**
```bash
curl -s -X POST http://127.0.0.1:8001/admin/reload | python3 -m json.tool
```

**Check the current API status:**
```bash
curl -s http://127.0.0.1:8001/status | python3 -m json.tool
```

**Run the health monitor immediately:**
```bash
sudo systemctl start wxmonitor.service
journalctl -u wxmonitor.service -n 10 --no-pager
```

**List retained Zarr cycle stores:**
```bash
ls -la /12TB2/NBM/zarr/
```

**Inspect the live Zarr store:**
```python
import xarray as xr
ds = xr.open_zarr("/12TB2/NBM/zarr/current", consolidated=True)
print(ds)
print(ds.attrs)
```

**Query a point interactively:**
```python
from pathlib import Path
from backend.app.registry import VariableRegistry
from backend.app.extraction import open_zarr_store, query_forecast
from backend.app.config import ZARR_DIR, VARIABLES_YAML

ds  = open_zarr_store(ZARR_DIR / "current")
reg = VariableRegistry(VARIABLES_YAML)
df, lat, lon = query_forecast(ds, lat=43.07, lon=-89.40,
                              variables=["temperature", "wind_speed"],
                              registry=reg)
print(df.head(10))
```

**Use the browser-based review tool:**
```
http://127.0.0.1:8001/review
```
(Serves `forecast_review.html` from the repo root — a single-page Chart.js tool for
visually inspecting forecast data at any lat/lon.)
