# Forecast Archive

## Purpose

After each successful NBM or NDFD ingest, the blend API automatically saves a
complete forecast time series for a small set of configured stations.  The
accumulated records are intended as training and testing data for a future
empirical bias-correction scheme.  A separate process (not yet implemented)
will collect the corresponding observed ("truth") data.

---

## How It Works

1. The NBM and NDFD ingest services each call `POST /admin/reload` on the
   blend API (port 8004) after completing a successful slab write.
2. `admin_reload` detects which store changed and schedules an archive sweep
   as a background task (response is returned immediately; the sweep runs
   after).
3. Each sweep queries the relevant slab store directly — no HTTP round-trip —
   and appends one JSON record per station per variable to the appropriate
   JSONL file.
4. NBM sweeps fire when the NBM cycle advances (~4×/day at 00, 03, 06, …,
   21 UTC).  NDFD sweeps fire independently when the NDFD cycle advances.

---

## Configuration Files

All configuration lives in `backend/app/archive/`:

| File | Purpose |
|------|---------|
| `stations.yaml` | List of stations to archive (id, lat, lon) |
| `nbm_variables.yaml` | NBM variables to archive |
| `ndfd_variables.yaml` | NDFD variables to archive |
| `config.py` | Archive root path and config file paths |
| `archiver.py` | Sweep logic (not user-editable) |

### `stations.yaml`

```yaml
stations:
  - id: KMSN
    lat: 43.14
    lon: -89.34
    description: Madison, WI
```

- `id` becomes the directory name under the archive root.
- `lat`/`lon` are the requested coordinates; the actual nearest grid point
  may differ slightly (recorded in each archive record as `grid_lat`/`grid_lon`).
- Maximum recommended: 10 stations.  Each additional station adds ~4 files
  per source (one per variable) and ~2 KB per ingest cycle.

### `nbm_variables.yaml` / `ndfd_variables.yaml`

```yaml
variables:
  - temperature
  - dewpoint
  - wind_speed
  - wind_direction
```

Variable names must match those returned by `GET /blend/variables`.  NBM and
NDFD can archive different variable sets since their native variable
availability differs.

---

## Output Data Layout

```
/12TB1/FCST_series/
    <station_id>/
        nbm/
            temperature.jsonl
            dewpoint.jsonl
            wind_speed.jsonl
            wind_direction.jsonl
        ndfd/
            temperature.jsonl
            dewpoint.jsonl
            wind_speed.jsonl
            wind_direction.jsonl
```

Each `.jsonl` file contains one JSON object per line.  Each line is one
complete forecast issued at one cycle time for that station and variable.

---

## Record Format

```json
{
  "archived_at": "2026-05-09T21:14:02Z",
  "source":      "nbm",
  "cycle_tag":   "20260509_20",
  "cycle_time":  "2026-05-09T20:00:00Z",
  "station_id":  "KMSN",
  "req_lat":     43.14,
  "req_lon":     -89.34,
  "grid_lat":    43.1337,
  "grid_lon":    -89.3344,
  "variable":    "temperature",
  "length":      256,
  "times":       ["2026-05-09T21:00:00Z", "2026-05-09T22:00:00Z", "..."],
  "values":      [64.0, 64.0, 63.0, "..."]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `archived_at` | ISO-8601 UTC | Wall-clock time the record was written |
| `source` | string | `"nbm"` or `"ndfd"` |
| `cycle_tag` | string | Unique cycle identifier, e.g. `"20260509_20"` |
| `cycle_time` | ISO-8601 UTC | Model initialisation time |
| `station_id` | string | ICAO identifier from `stations.yaml` |
| `req_lat` / `req_lon` | float | Requested coordinates |
| `grid_lat` / `grid_lon` | float | Actual nearest model grid point |
| `variable` | string | Variable name (matches variable YAML) |
| `length` | int | Number of time steps in this record |
| `times` | list[string] | ISO-8601 UTC timestamps, one per forecast hour |
| `values` | list[float\|null] | Forecast values in output units; `null` where the model has no data |

**Units** are the same as the blend API output (see `GET /blend/variables`):
temperature and dewpoint in °F, wind speed in mph, wind direction in degrees.

**Deduplication:** before appending, the archiver checks whether the last line
in the target file already has the same `cycle_tag`.  If so, the write is
skipped.  This prevents duplicate records when both ingest services fire close
together within the same cycle window.

---

## File Growth

At 4 NBM cycles/day and 4 NDFD cycles/day, each JSONL file grows by
approximately 8 records/day.  At ~2 KB per record, that is ~16 KB/day per
file, or roughly **6 MB/year per station per variable**.  With 6 stations and
4 variables per source, total archive growth is approximately
**300 MB/year** — no rotation needed for the foreseeable future.

---

## Monitoring

Archive activity is logged to the blend API journal:

```bash
journalctl -u wxblendapi | grep Archive
```

Expected output after each ingest:

```
INFO Archive sweep [nbm] cycle=20260509_20  stations=6  vars=['temperature', ...]
INFO Archive sweep [nbm] done — stations ok/failed=6/0  records written=24
```

A non-zero `stations_failed` count indicates a query error for one or more
stations (logged at WARNING level with details); the other stations are
unaffected.

---

## Adding or Removing Stations / Variables

1. Edit `stations.yaml` or the appropriate `*_variables.yaml`.
2. Restart `wxblendapi` so the module-level cache is cleared:
   ```bash
   sudo systemctl restart wxblendapi
   ```
3. No backfill is performed for new stations or variables — accumulation
   starts from the next ingest cycle.

---

## Deferred: Truth Data Collection

A companion process to collect observed values (METARs or other surface
observations) at the same stations is planned but not yet implemented.
The `times` array in each archive record provides the exact valid times
needed to match forecasts against observations.
