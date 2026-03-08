#!/usr/bin/env python3
"""
Weather Window backend health monitor.

Checks that:
  1. The FastAPI service responds on http://127.0.0.1:8001/status
  2. The forecast cycle is less than MAX_AGE_H hours old

Pings healthchecks.io on success; pings /fail with a reason on failure.
Run every 20 minutes via wxmonitor.timer.

Edit HC_UUID to match your healthchecks.io check.
"""

import json
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

ALERT_EMAIL = "grantwp3@gmail.com"

def _log(msg: str) -> None:
    print(msg, flush=True)

def _email(subject: str, body: str) -> None:
    """Send alert email; silently ignore failures so a mail outage doesn't mask the real problem."""
    try:
        subprocess.run(
            ["mail", "-s", f"[precip] {subject}", ALERT_EMAIL],
            input=body, text=True, timeout=30,
        )
    except Exception:
        pass
HC_UUID    = "1ffbadd7-5c6b-4217-a709-b272eec6476f"   # <-- only line to edit
HC_BASE    = f"https://hc-ping.com/{HC_UUID}"
STATUS_URL = "http://127.0.0.1:8001/status"
MAX_AGE_H  = 3.5                         # alert if cycle older than this


def _ping(path: str = "", message: str = "") -> None:
    """POST to healthchecks.io; silently ignore network errors."""
    url = f"{HC_BASE}/{path}".rstrip("/") if path else HC_BASE
    try:
        req = urllib.request.Request(
            url,
            data=message.encode() if message else b"",
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass  # best-effort; don't let a ping failure mask the real failure


def main() -> None:
    # --- Check 1: API responsiveness ---
    try:
        with urllib.request.urlopen(STATUS_URL, timeout=10) as r:
            status = json.load(r)
    except Exception as exc:
        msg = f"FAIL: API not responding: {exc}"
        _log(msg)
        _ping("fail", msg)
        _email("API not responding", msg)
        sys.exit(1)

    # --- Check 2: Cycle freshness ---
    runtime_str = status.get("runtime")
    if not runtime_str:
        msg = "FAIL: runtime field missing from /status response"
        _log(msg)
        _ping("fail", msg)
        _email("API status malformed", msg)
        sys.exit(1)

    runtime = datetime.fromisoformat(runtime_str.replace("Z", "+00:00"))
    age     = datetime.now(timezone.utc) - runtime
    if age > timedelta(hours=MAX_AGE_H):
        hours = age.total_seconds() / 3600
        msg = f"FAIL: Forecast cycle stale: {hours:.1f}h old (limit {MAX_AGE_H}h)"
        _log(msg)
        _ping("fail", msg)
        _email("Forecast cycle stale", msg)
        sys.exit(1)

    # --- All good ---
    _log(f"OK: cycle runtime={runtime_str}, age={age.total_seconds()/3600:.2f}h")
    _ping()


if __name__ == "__main__":
    main()
