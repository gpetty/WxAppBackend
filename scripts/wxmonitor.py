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
  3. The forecast cycle has at least MIN_TIME_STEPS time steps — a cycle
     can be perfectly fresh yet truncated if upstream data was incomplete
     when it was ingested (see the 2026-09-23 NBM S3 lag incident).

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
from datetime import datetime, timezone

ALERT_EMAIL = "grantwp3@gmail.com"

# ---------------------------------------------------------------------------
# Service configuration
# ---------------------------------------------------------------------------
# Fill in HC_UUID for each service from your healthchecks.io dashboard.
# All three services use the same MAX_AGE_H (6h) after the NBM cadence
# change to 3-hourly (matching NDFD).

#
# min_time_steps: alert if the committed cycle has fewer forecast steps
# than this. A full NBM cycle is 99 slabs; NDFD runs 59–65 depending on
# the cycle. Thresholds are set below the normal range so routine
# variation doesn't alert, but a badly truncated cycle does.
# None = skip the check (blend /status exposes no step count; its two
# stores are covered by the nbm and ndfd monitors).

SERVICE_CONFIGS: dict[str, dict] = {
    "nbm": {
        "status_url":      "http://127.0.0.1:8001/status",
        "hc_uuid":         "1ffbadd7-5c6b-4217-a709-b272eec6476f",
        "max_age_h":       6.0,
        "runtime_field":   "runtime",                  # field in /status response
        "min_time_steps":  90,                         # full cycle = 99
        "label":           "NBM API",
    },
    "ndfd": {
        "status_url":      "http://127.0.0.1:8002/status",
        "hc_uuid":         "1ffbadd7-5c6b-4217-a709-b272eec6476f",
        "max_age_h":       6.0,
        "runtime_field":   "runtime",
        "min_time_steps":  50,                         # normal range 59–65
        "label":           "NDFD API",
    },
    "blend": {
        "status_url":      "http://127.0.0.1:8004/status",
        "hc_uuid":         "1ffbadd7-5c6b-4217-a709-b272eec6476f",
        "max_age_h":       6.0,
        # Blend /status has separate nbm_runtime and ndfd_runtime.
        # We check the older of the two; either stale means the blend is stale.
        "runtime_field":   ["nbm_runtime", "ndfd_runtime"],
        "min_time_steps":  None,                       # no step count in /status
        "label":           "Blend API",
    },
}

STEPS_FIELD = "n_time_steps"


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
# Checks — pure functions returning a failure message, or None if OK
# ---------------------------------------------------------------------------

def check_freshness(
    status: dict,
    runtime_field,
    max_age_h: float,
    now: datetime,
) -> str | None:
    """Fail if the committed cycle is older than *max_age_h* hours."""
    try:
        oldest = _oldest_runtime(status, runtime_field)
    except (KeyError, ValueError) as exc:
        return f"runtime field missing or malformed: {exc}"

    age_h = (now - oldest).total_seconds() / 3600
    if age_h > max_age_h:
        return f"Forecast cycle stale: {age_h:.1f}h old (limit {max_age_h}h)"
    return None


def check_completeness(status: dict, min_time_steps: int | None) -> str | None:
    """
    Fail if the committed cycle has fewer than *min_time_steps* forecast
    steps. A truncated cycle passes the freshness check but silently
    shortens the forecast horizon, so it needs its own check.

    *min_time_steps* of None skips the check entirely.
    """
    if min_time_steps is None:
        return None

    n_steps = status.get(STEPS_FIELD)
    if n_steps is None:
        return f"status response has no {STEPS_FIELD} field"

    if n_steps < min_time_steps:
        return (f"Forecast cycle truncated: {n_steps} time steps "
                f"(expected at least {min_time_steps})")
    return None


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

    label          = cfg["label"]
    status_url     = cfg["status_url"]
    hc_uuid        = cfg["hc_uuid"]
    max_age_h      = cfg["max_age_h"]
    runtime_field  = cfg["runtime_field"]
    min_time_steps = cfg["min_time_steps"]

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

    now = datetime.now(timezone.utc)

    # --- Check 2: Cycle freshness ---
    failure = check_freshness(status, runtime_field, max_age_h, now)
    if failure:
        msg = f"FAIL [{label}]: {failure}"
        _log(msg)
        _ping(hc_uuid, "fail", msg)
        _email(f"{label} forecast cycle stale", msg)
        sys.exit(1)

    # --- Check 3: Cycle completeness ---
    failure = check_completeness(status, min_time_steps)
    if failure:
        msg = f"FAIL [{label}]: {failure}"
        _log(msg)
        _ping(hc_uuid, "fail", msg)
        _email(f"{label} forecast cycle truncated", msg)
        sys.exit(1)

    # --- All good ---
    age_h = (now - _oldest_runtime(status, runtime_field)).total_seconds() / 3600
    steps = status.get(STEPS_FIELD)
    steps_note = f", steps={steps}" if steps is not None else ""
    _log(f"OK [{label}]: cycle age={age_h:.2f}h (limit {max_age_h}h){steps_note}")
    _ping(hc_uuid)


if __name__ == "__main__":
    main()
