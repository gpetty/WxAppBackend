#!/usr/bin/env python3
"""
METAR Archive — fetches one regular hourly METAR per station and appends it
to a monthly JSONL file.

Output layout:
    /12TB1/FCST_series/<station_id>/metar/<YYYY-MM>.jsonl

One JSON record per line.  Fields:
    archived_at   ISO-8601 UTC wall-clock time this record was written
    station_id    ICAO identifier
    report_time   Nominal top-of-the-hour bucket (from API reportTime, :00:00Z)
    obs_time      Actual observation time (from API obsTime unix epoch, e.g. :53Z)
    temp_c        Temperature, °C (null if missing)
    dewp_c        Dewpoint, °C (null if missing)
    wdir          Wind direction, degrees (null if calm/variable/missing)
    wspd_kt       Wind speed, knots (null if missing)
    wgst_kt       Wind gust, knots (null if no gust reported)
    visib         Visibility string as returned by API (e.g. "10+", "1.75")
    altim_hpa     Altimeter setting, hPa (null if missing)
    slp_hpa       Sea-level pressure, hPa (null if missing)
    raw           Full raw METAR string

Usage:
    python3 scripts/metar_archive.py [--dry-run] [--stations KMSN,KDEN,...]

Stations default to those listed in backend/app/archive/stations.yaml.
--dry-run prints records without writing.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Allow direct invocation (`python3 scripts/metar_archive.py`) by ensuring
# the repo root is on sys.path before importing the backend package.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.app.archive.archiver import _append_record, _last_record_field, _load_stations
from backend.app.archive.config import ARCHIVE_ROOT

log = logging.getLogger("metar_archive")

AWC_URL = "https://aviationweather.gov/api/data/metar?ids={ids}&format=json&hours=2"


def _fetch_metars(station_ids: list[str]) -> list[dict]:
    url = AWC_URL.format(ids=",".join(station_ids))
    req = urllib.request.Request(url, headers={"User-Agent": "WxApp-metar-archive/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _iso(unix_ts: int) -> str:
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _opt_float(d: dict, key: str) -> float | None:
    v = d.get(key)
    return round(float(v), 2) if v is not None else None


def run(station_ids: list[str], dry_run: bool = False) -> None:
    now = datetime.now(tz=timezone.utc)
    archived_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    month_tag   = now.strftime("%Y-%m")

    log.info(f"METAR fetch — stations={station_ids}")

    try:
        raw_records = _fetch_metars(station_ids)
    except Exception as exc:
        log.error(f"AWC API fetch failed: {exc}")
        return  # next hour's timer will retry

    # API returns newest-first; first hit per station wins.
    by_station: dict[str, dict] = {}
    for r in raw_records:
        if r.get("metarType") != "METAR":
            continue
        sid = r.get("icaoId")
        if sid not in by_station:
            by_station[sid] = r

    written  = 0
    skipped  = 0
    failed   = 0

    for sid in station_ids:
        obs = by_station.get(sid)
        if obs is None:
            log.warning(f"METAR {sid}: no regular hourly report in API response")
            failed += 1
            continue

        # AWC reportTime is "YYYY-MM-DDTHH:MM:SS.sssZ"; normalise to "...Z" (no millis).
        report_time_raw = obs.get("reportTime", "")
        if report_time_raw:
            parsed = datetime.fromisoformat(report_time_raw.replace("Z", "+00:00"))
            report_time = parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            report_time = ""

        obs_time = _iso(obs["obsTime"]) if obs.get("obsTime") else None

        out_path = ARCHIVE_ROOT / sid / "metar" / f"{month_tag}.jsonl"

        if _last_record_field(out_path, "report_time") == report_time:
            log.debug(f"METAR {sid}: {report_time} already written, skipping")
            skipped += 1
            continue

        record = {
            "archived_at": archived_at,
            "station_id":  sid,
            "report_time": report_time,
            "obs_time":    obs_time,
            "temp_c":      _opt_float(obs, "temp"),
            "dewp_c":      _opt_float(obs, "dewp"),
            "wdir":        obs.get("wdir"),
            "wspd_kt":     obs.get("wspd"),
            "wgst_kt":     obs.get("wgst"),
            "visib":       obs.get("visib"),
            "altim_hpa":   _opt_float(obs, "altim"),
            "slp_hpa":     _opt_float(obs, "slp"),
            "raw":         obs.get("rawOb", ""),
        }

        if dry_run:
            print(json.dumps(record, indent=2))
            written += 1
            continue

        try:
            _append_record(out_path, record)
            log.info(f"METAR {sid}: wrote {report_time} (obs {obs_time})")
            written += 1
        except Exception as exc:
            log.warning(f"METAR {sid}: write failed — {exc}")
            failed += 1

    log.info(f"METAR archive done — written={written}  skipped={skipped}  failed={failed}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Print records without writing")
    parser.add_argument("--stations", help="Comma-separated ICAO IDs (default: all from stations.yaml)")
    args = parser.parse_args()

    if args.stations:
        station_ids = [s.strip().upper() for s in args.stations.split(",")]
    else:
        station_ids = [s["id"] for s in _load_stations()]

    run(station_ids, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
