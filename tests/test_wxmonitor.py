"""
Tests for the backend health monitor (scripts/wxmonitor.py).

Covers both checks the monitor performs on a /status payload:
  * cycle freshness  — the runtime is no older than max_age_h
  * cycle completeness — the committed cycle has the expected number of
    forecast time steps

The completeness check exists because of the 2026-09-23 incident: NOAA's
NBM S3 mirror lagged ~13h, and one cycle committed with only 40 of 99
slabs (horizon truncated from 11 days to 2). The age-only monitor
reported that cycle as perfectly healthy.

Pure-function tests — no network, no services, no clock dependence.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

# wxmonitor.py lives in scripts/, which is not an importable package.
_SPEC = importlib.util.spec_from_file_location(
    "wxmonitor", Path(__file__).resolve().parent.parent / "scripts" / "wxmonitor.py"
)
wxmonitor = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(wxmonitor)


NOW = datetime(2026, 9, 24, 15, 0, tzinfo=timezone.utc)


def _status(runtime: str = "2026-09-24T14:00:00Z", n_time_steps: int = 99) -> dict:
    return {
        "runtime": runtime,
        "store_path": "/12TB2/NBM/slabs",
        "n_variables": 15,
        "n_time_steps": n_time_steps,
        "last_loaded": "2026-09-24T15:09:51.970284Z",
    }


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------

def test_fresh_cycle_passes():
    assert wxmonitor.check_freshness(_status(), "runtime", 6.0, NOW) is None


def test_stale_cycle_fails_with_age_in_message():
    # The real incident: cycle age reached 19.2h against a 6h limit.
    status = _status(runtime="2026-09-23T20:00:00Z")
    msg = wxmonitor.check_freshness(status, "runtime", 6.0, NOW)
    assert msg is not None
    assert "19.0h" in msg
    assert "6.0h" in msg


def test_blend_uses_oldest_of_two_runtimes():
    status = {
        "nbm_runtime":  "2026-09-23T20:00:00Z",   # stale
        "ndfd_runtime": "2026-09-24T15:00:00Z",   # fresh
    }
    msg = wxmonitor.check_freshness(
        status, ["nbm_runtime", "ndfd_runtime"], 6.0, NOW
    )
    assert msg is not None, "the older of the two runtimes must drive the check"


def test_missing_runtime_field_is_reported():
    msg = wxmonitor.check_freshness({}, "runtime", 6.0, NOW)
    assert msg is not None
    assert "runtime" in msg


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------

def test_full_cycle_passes_completeness():
    assert wxmonitor.check_completeness(_status(n_time_steps=99), 90) is None


def test_truncated_cycle_fails_completeness():
    # The 20260923_20 cycle: 40 of 99 slabs, horizon 11 days -> 2 days,
    # yet fresh enough to pass the age check.
    msg = wxmonitor.check_completeness(_status(n_time_steps=40), 90)
    assert msg is not None
    assert "40" in msg
    assert "90" in msg


def test_completeness_skipped_when_no_minimum_configured():
    # Blend /status carries no n_time_steps field; its stores are covered
    # by the nbm and ndfd monitors instead.
    assert wxmonitor.check_completeness({"nbm_runtime": "x"}, None) is None


def test_missing_step_count_is_reported_when_minimum_configured():
    msg = wxmonitor.check_completeness({"runtime": "x"}, 90)
    assert msg is not None
    assert "n_time_steps" in msg


def test_step_count_at_threshold_passes():
    assert wxmonitor.check_completeness(_status(n_time_steps=90), 90) is None


# ---------------------------------------------------------------------------
# healthchecks.io configuration
# ---------------------------------------------------------------------------

def test_each_service_has_a_distinct_hc_uuid():
    """
    All three services once shared one check UUID. Because the timers are
    staggered, one service's failure ping was cleared by another service's
    success ping ~20 min later, so a single-service outage could hide.
    """
    assert wxmonitor.duplicate_hc_uuids(wxmonitor.SERVICE_CONFIGS) == {}


def test_duplicate_hc_uuids_reports_the_colliding_services():
    configs = {
        "a": {"hc_uuid": "shared"},
        "b": {"hc_uuid": "shared"},
        "c": {"hc_uuid": "unique"},
    }
    assert wxmonitor.duplicate_hc_uuids(configs) == {"shared": ["a", "b"]}


def test_duplicate_hc_uuids_empty_when_all_distinct():
    configs = {"a": {"hc_uuid": "x"}, "b": {"hc_uuid": "y"}}
    assert wxmonitor.duplicate_hc_uuids(configs) == {}
