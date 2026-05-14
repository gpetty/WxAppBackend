"""
Shared helpers for the NBM and NDFD ingest pipelines.

Lock-file management and JSONL manifest read/write are identical across both
sources; keeping them here prevents drift and the same-name-different-class
LockError footgun that arose when each module defined its own.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lock file (simple PID-based; prevents overlapping cron runs)
# ---------------------------------------------------------------------------

class LockError(RuntimeError):
    pass


class IngestLock:
    """Context manager that creates/removes a PID lock file.

    `service_name` only customizes the LockError message; the locking
    behaviour is identical across sources.
    """

    def __init__(self, lock_path: Path, service_name: str = "ingestion") -> None:
        self.path = lock_path
        self.service_name = service_name

    def __enter__(self) -> "IngestLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            pid_str = self.path.read_text().strip()
            stale = False
            try:
                pid = int(pid_str)
                os.kill(pid, 0)   # signal 0: check existence only, no-op if running
            except (ValueError, ProcessLookupError):
                stale = True      # PID is gone
            except PermissionError:
                stale = False     # process exists but owned by another user — treat as live
            if stale:
                log.warning(f"Removing stale lock file (PID {pid_str} no longer running).")
                self.path.unlink()
            else:
                raise LockError(
                    f"Lock file exists ({self.path}). "
                    f"{self.service_name.capitalize()} already running as PID {pid_str}."
                )
        self.path.write_text(str(os.getpid()))
        log.debug(f"Lock acquired: {self.path}")
        return self

    def __exit__(self, *_) -> None:
        try:
            self.path.unlink()
            log.debug(f"Lock released: {self.path}")
        except FileNotFoundError:
            pass


# ---------------------------------------------------------------------------
# CLI logging setup (shared by both ingest CLIs)
# ---------------------------------------------------------------------------

_NOISY_LOGGERS = ("herbie", "urllib3", "requests", "boto3", "botocore", "s3transfer")


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for noisy in _NOISY_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Manifest read/write
# ---------------------------------------------------------------------------

def read_manifest(staging_dir: Path) -> Optional[dict[str, Any]]:
    """Read manifest.json from `staging_dir`. Returns None if not found."""
    path = staging_dir / "manifest.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def dump_manifest(staging_dir: Path, payload: dict[str, Any]) -> Path:
    """Write `payload` as `staging_dir/manifest.json` (pretty-printed)."""
    path = staging_dir / "manifest.json"
    path.write_text(json.dumps(payload, indent=2))
    log.info(f"Manifest written: {path}")
    return path


# ---------------------------------------------------------------------------
# Ring-buffer idempotency check
# ---------------------------------------------------------------------------

def cycle_tag_in_ring_buffer(cycle_tag: str, slab_store_dir: Path) -> bool:
    """Return True if `cycle_tag` is already committed to the slab ring buffer.

    Used by both NBM and NDFD ingest to skip re-downloading a cycle whose
    slabs are already written (e.g. after staging was pruned).
    """
    from ..store.ring_state import RingState
    if not (slab_store_dir / "ring_state.json").exists():
        return False
    state = RingState.load(slab_store_dir)
    return state.run_by_tag(cycle_tag) is not None
