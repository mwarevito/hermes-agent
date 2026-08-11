"""Tests: one reaper, one criterion, and a kill that reaches the whole worker.

Three defects observed on 2026-08-11 while runs 34/35 of ``t_a6c52bb1`` were
being killed by the runtime cap:

* two detectors decide the same symptom ("this run stopped moving") with two
  different heartbeat rules — a flat 3600s in ``release_stale_claims`` and a
  second flat 3600s constant in ``detect_stale_running``, reachable only after
  a separate 4h gate;
* the reclaim path signals the bare worker pid, so everything the worker
  started (a Claude Code run, a pytest subprocess) survives the reclaim and
  keeps holding the resource the retry needs — the same shape that made the
  2026-08-06 retry time out against a squatted lane slot;
* the pid it signals is read from ``tasks.worker_pid``, while the run's own
  pid lives on ``task_runs.worker_pid``. The two can disagree, and the one
  thing that must never be treated as a worker pid is the claim lock's — that
  is the gateway's (``MacBookPro.mynet:81235`` = all seven bots).
"""

from __future__ import annotations

import os
import signal as _signal_mod
import subprocess
import sys
import time
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    db_path = kb.kanban_db_path(board="default")
    kb._INITIALIZED_PATHS.discard(str(db_path.resolve()))
    kb.init_db()
    return home


@pytest.fixture
def conn(kanban_home):
    with kb.connect() as c:
        yield c


def _expired_running(conn, *, task_pid, run_pid=None, cap=None,
                     elapsed=1800, heartbeat_age=None):
    """A running task whose claim TTL has passed."""
    now = int(time.time())
    tid = kb.create_task(
        conn, title="reaped", assignee="worker", max_runtime_seconds=cap,
    )
    kb.claim_task(conn, tid)
    kb._set_worker_pid(conn, tid, task_pid)
    hb = None if heartbeat_age is None else now - heartbeat_age
    with kb.write_txn(conn):
        conn.execute(
            "UPDATE tasks SET started_at = ?, claim_expires = ?, "
            "last_heartbeat_at = ? WHERE id = ?",
            (now - elapsed, now - 1, hb, tid),
        )
        conn.execute(
            "UPDATE task_runs SET started_at = ?, claim_expires = ?, "
            "last_heartbeat_at = ?, worker_pid = ? "
            "WHERE id = (SELECT current_run_id FROM tasks WHERE id = ?)",
            (now - elapsed, now - 1, hb,
             run_pid if run_pid is not None else task_pid, tid),
        )
    return tid


def _pid_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def test_reclaim_kills_the_whole_worker_group(conn):
    """The worker's children must not outlive the reclaim.

    The worker is spawned with ``start_new_session=True`` so it leads its own
    process group; signalling only its pid leaves the grandchild running,
    unowned, and still holding whatever it took — the shape that left a Claude
    process squatting the launcher's lane slot on 2026-08-06 and guaranteed the
    retry would time out too.
    """
    code = (
        "import subprocess, sys, time\n"
        "p = subprocess.Popen(['sleep', '120'])\n"
        "open(sys.argv[1], 'w').write(str(p.pid))\n"
        "time.sleep(120)\n"
    )
    marker = Path(os.environ["HERMES_HOME"]) / "child.pid"
    worker = subprocess.Popen(
        [sys.executable, "-c", code, str(marker)], start_new_session=True,
    )
    child_pid = None
    try:
        for _ in range(100):
            if marker.exists() and marker.read_text().strip():
                child_pid = int(marker.read_text().strip())
                break
            time.sleep(0.1)
        assert child_pid, "grandchild never reported its pid"

        tid = _expired_running(conn, task_pid=worker.pid)
        assert kb.reclaim_task(conn, tid, reason="operator") is True

        worker.wait(timeout=10)
        for _ in range(40):
            if _pid_gone(child_pid):
                break
            time.sleep(0.1)
        assert _pid_gone(child_pid), (
            f"grandchild {child_pid} survived the reclaim of worker "
            f"{worker.pid} — the reclaim signalled the bare pid, not the group"
        )
    finally:
        for pid in (child_pid, worker.pid):
            if pid:
                try:
                    os.kill(pid, _signal_mod.SIGKILL)
                except OSError:
                    pass
        try:
            worker.wait(timeout=5)
        except Exception:
            pass


def test_termination_targets_the_run_pid_not_the_task_pid(conn, monkeypatch):
    """The kill target comes from ``task_runs.worker_pid`` only."""
    monkeypatch.setattr(kb, "_pid_alive", lambda _p: False)
    signalled: list[tuple[int, int]] = []

    _expired_running(conn, task_pid=991001, run_pid=991002)
    kb.release_stale_claims(
        conn, signal_fn=lambda p, s: signalled.append((int(p), int(s))),
    )

    assert signalled, "nothing was signalled at all"
    assert {p for p, _ in signalled} == {991002}, (
        f"reclaim signalled {signalled!r} — the run's pid is 991002; 991001 is "
        f"the stale task-row copy"
    )


def test_both_reapers_use_the_same_staleness_rule(conn, monkeypatch):
    """``detect_stale_running`` must not keep a second, harsher constant.

    With the budget-derived threshold on, a 3600s card that has been silent for
    1300s is stale. The second detector kept its own flat 3600s gap, so it
    disagreed with the first about the very same run.
    """
    monkeypatch.setenv("HERMES_KANBAN_HEARTBEAT_STALE_FROM_BUDGET", "1")
    monkeypatch.setattr(kb, "_pid_alive", lambda _p: False)

    tid = _expired_running(
        conn, task_pid=991006, cap=3600, elapsed=5 * 3600, heartbeat_age=1300,
    )
    stale = kb.detect_stale_running(
        conn, stale_timeout_seconds=14400, signal_fn=lambda _p, _s: None,
    )
    assert stale == [tid], (
        "the two reapers still answer differently for one run's heartbeat gap"
    )
