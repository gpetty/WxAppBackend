#!/usr/bin/env python3
"""
Weather Window backend health monitor.

Checks one service per invocation:
    python scripts/wxmonitor.py nbm
    python scripts/wxmonitor.py ndfd
    python scripts/wxmonitor.py blend

For each service, checks:
  1. The FastAPI /status endpoint responds within timeout.
  2. The forecast cycle is no older than MAX_AGE_H hours.

On success: pings healthchecks.io (HC_UUID) with a success ping.
On failure: pings healthchecks.io at /fail and sends an alert email.

Run via three separate systemd timer instances:
    wxmonitor@nbm.timer   → wxmonitor@nbm.service   → wxmonitor.py nbm
    wxmonitor@ndfd.timer  → wxmonitor@ndfd.service  → wxmonitor.py ndfd
    wxmonitor@blend.timer → wxmonitor@blend.service → wxmonitor.py blend
"""

import json
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

ALERT_EMAIL = "grantwp3@gmail.com"

# ---------------------------------------------------------------------------
# Service configuration
# ---------------------------------------------------------------------------
# Fill in HC_UUID for each service from your healthchecks.io dashboard.
# All three services use the same MAX_AGE_H (6h) after the NBM cadence
# change to 3-hourly (matching NDFD).

SERVICE_CONFIGS: dict[str, dict] = {
    "nbm": {
        "status_url":    "http://127.0.0.1:8001/status",
        "hc_uuid":       "1ffbadd7-5c6b-4217-a709-b272eec6476f",       # ← replace
        "max_age_h":     6.0,
        "runtime_field": "runtime",                    # field in /status response
        "label":         "NBM API",
    },
    "ndfd": {
        "status_url":    "http://127.0.0.1:8002/status",
        "hc_uuid":       "1ffbadd7-5c6b-4217-a709-b272eec6476f",      # ← replace
        "max_age_h":     6.0,
        "runtime_field": "runtime",
        "label":         "NDFD API",
    },
    "blend": {
        "status_url":    "http://127.0.0.1:8004/status",
        "hc_uuid":       "1ffbadd7-5c6b-4217-a709-b272eec6476f",     # ← replace
        "max_age_h":     6.0,
        # Blend /status has separate nbm_runtime and ndfd_runtime.
        # We check the older of the two; either stale means the blend is stale.
        "runtime_field": ["nbm_runtime", "ndfd_runtime"],
        "label":         "Blend API",
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    print(msg, flush=True)


def _email(subject: str, body: str) -> None:
    """Send alert email; silently ignore failures."""
    try:
        subprocess.run(
            ["mail", "-s", f"[precip] {subject}", ALERT_EMAIL],
            input=body, text=True, timeout=30,
        )
    except Exception:
        pass


def _ping(hc_uuid: str, path: str = "", message: str = "") -> None:
    """POST to healthchecks.io; silently ignore network errors."""
    base = f"https://hc-ping.com/{hc_uuid}"
    url  = f"{base}/{path}".rstrip("/") if path else base
    try:
        req = urllib.request.Request(
            url,
            data=message.encode() if message else b"",
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def _parse_runtime(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _oldest_runtime(status: dict, runtime_field) -> datetime:
    """
    Return the oldest (most stale) runtime from the status response.
    runtime_field may be a string (single field) or list of strings (blend).
    """
    if isinstance(runtime_field, str):
        return _parse_runtime(status[runtime_field])

    # Multiple fields — return the oldest non-null value
    runtimes = []
    for field in runtime_field:
        raw = status.get(field)
        if raw:
            runtimes.append(_parse_runtime(raw))
    if not runtimes:
        raise ValueError("No runtime fields present in status response")
    return min(runtimes)   # min = oldest = most stale


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in SERVICE_CONFIGS:
        services = ", ".join(SERVICE_CONFIGS)
        print(f"Usage: wxmonitor.py <{services}>", file=sys.stderr)
        sys.exit(2)

    service = sys.argv[1]
    cfg     = SERVICE_CONFIGS[service]

    label         = cfg["label"]
    status_url    = cfg["status_url"]
    hc_uuid       = cfg["hc_uuid"]
    max_age_h     = cfg["max_age_h"]
    runtime_field = cfg["runtime_field"]

    # --- Check 1: API responsiveness ---
    try:
        with urllib.request.urlopen(status_url, timeout=10) as r:
            status = json.load(r)
    except Exception as exc:
        msg = f"FAIL [{label}]: API not responding at {status_url}: {exc}"
        _log(msg)
        _ping(hc_uuid, "fail", msg)
        _email(f"{label} not responding", msg)
        sys.exit(1)

    # --- Check 2: Cycle freshness ---
    try:
        oldest = _oldest_runtime(status, runtime_field)
    except (KeyError, ValueError) as exc:
        msg = f"FAIL [{label}]: runtime field missing or malformed: {exc}"
        _log(msg)
        _ping(hc_uuid, "fail", msg)
        _email(f"{label} status malformed", msg)
        sys.exit(1)

    age = datetime.now(timezone.utc) - oldest
    if age > timedelta(hours=max_age_h):
        hours = age.total_seconds() / 3600
        msg = (f"FAIL [{label}]: Forecast cycle stale: {hours:.1f}h old "
               f"(limit {max_age_h}h)")
        _log(msg)
        _ping(hc_uuid, "fail", msg)
        _email(f"{label} forecast cycle stale", msg)
        sys.exit(1)

    # --- All good ---
    age_h = age.total_seconds() / 3600
    _log(f"OK [{label}]: cycle age={age_h:.2f}h (limit {max_age_h}h)")
    _ping(hc_uuid)


if __name__ == "__main__":
    main()
