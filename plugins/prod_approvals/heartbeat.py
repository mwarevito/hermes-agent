"""Liveness heartbeat for the prod-approvals fail-closed backstop.

The backstop shell hook (``backstop/gate-prod-writes.sh``) hard-blocks every
production write unless a *fresh* activation marker proves the in-process
middleware gate is alive to offer structured approval. A write-once marker is a
defect: it goes stale after the freshness window even while the plugin is
perfectly healthy, silently disabling the one-tap approval path after a few
minutes of uptime. This module instead:

* refreshes the marker on a timer for as long as the process is alive, and
* exposes the exact freshness check the shell hook mirrors.

Marker format is a single line ``"<unix_ts> <pid>"``; the shell hook reads the
first whitespace token, so the file stays backward/forward compatible.

Multi-process / multi-profile safety: the marker lives under
``HERMES_HOME/prod_approvals`` — one per profile. Multiple gateway processes
sharing a HERMES_HOME each refresh the same marker (last writer wins); as long
as ANY enabled gateway is alive the marker stays fresh. When every such process
dies nobody refreshes, the marker goes stale within the window, and the
backstop blocks (fail closed).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

MARKER_NAME = "plugin_active"
# The backstop treats a marker older than this as "plugin not loaded" → block.
FRESH_WINDOW_SECONDS = 300
# Refresh well within the window so a single missed tick never trips staleness.
REFRESH_INTERVAL_SECONDS = 60


def marker_path(base_dir: Path) -> Path:
    return Path(base_dir) / MARKER_NAME


def write_marker(base_dir: Path, now: Optional[float] = None) -> None:
    """Write/refresh the activation marker (0600) with the current ts + pid."""
    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(base, 0o700)
    except OSError:
        pass
    ts = time.time() if now is None else now
    path = marker_path(base)
    tmp = path.with_suffix(".tmp")
    # Atomic replace so a concurrent reader never sees a half-written marker.
    fd = os.open(str(tmp), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    try:
        os.write(fd, f"{ts:.3f} {os.getpid()}".encode("utf-8"))
    finally:
        os.close(fd)
    os.replace(str(tmp), str(path))
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def read_marker_ts(base_dir: Path) -> Optional[float]:
    """Return the marker's timestamp (first token), or ``None`` if unreadable."""
    try:
        raw = marker_path(base_dir).read_text().strip()
    except Exception:
        return None
    if not raw:
        return None
    try:
        return float(raw.split()[0])
    except (ValueError, IndexError):
        return None


def is_fresh(base_dir: Path, now: Optional[float] = None,
             window: float = FRESH_WINDOW_SECONDS) -> bool:
    """True iff the marker exists and is within *window* seconds of *now*."""
    ts = read_marker_ts(base_dir)
    if ts is None:
        return False
    cur = time.time() if now is None else now
    return (cur - ts) <= window


class Heartbeat:
    """Background refresher: rewrites the marker every *interval* until stopped.

    Uses a daemon thread and an :class:`threading.Event` so ``stop()`` is
    prompt (no interval-length hang) and shutdown never leaks a live thread.
    """

    def __init__(self, base_dir: Path, interval: float = REFRESH_INTERVAL_SECONDS):
        self._base = Path(base_dir)
        self._interval = max(0.001, float(interval))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> "Heartbeat":
        if self._thread is not None and self._thread.is_alive():
            return self
        # Drop an immediate marker so the path is live the instant register()
        # returns, not one interval later.
        try:
            write_marker(self._base)
        except Exception:
            logger.debug("prod-approvals heartbeat: initial marker write failed", exc_info=True)
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="prod-approvals-heartbeat", daemon=True
        )
        self._thread.start()
        return self

    def _run(self) -> None:
        # wait() returns True when the stop event is set → exit promptly.
        while not self._stop.wait(self._interval):
            try:
                write_marker(self._base)
            except Exception:
                logger.debug("prod-approvals heartbeat: refresh failed", exc_info=True)

    def stop(self, join_timeout: float = 2.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=join_timeout)
        self._thread = None

    @property
    def alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()


# Process-global singleton so register() starts exactly one refresher and
# shutdown/atexit can stop it (no daemon leak).
_singleton_lock = threading.Lock()
_singleton: Optional[Heartbeat] = None


def start_singleton(base_dir: Path, interval: float = REFRESH_INTERVAL_SECONDS) -> Heartbeat:
    global _singleton
    with _singleton_lock:
        if _singleton is not None and _singleton.alive:
            return _singleton
        hb = Heartbeat(base_dir, interval=interval)
        hb.start()
        _singleton = hb
    import atexit
    atexit.register(stop_singleton)
    return hb


def stop_singleton() -> None:
    global _singleton
    with _singleton_lock:
        hb = _singleton
        _singleton = None
    if hb is not None:
        hb.stop()
