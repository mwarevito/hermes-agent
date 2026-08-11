"""Tests: the heartbeat-staleness threshold must come from the run's budget.

2026-08-11, board ``hermes-infra``: runs 34 and 35 of ``t_a6c52bb1`` were both
killed by the runtime cap (3611s and 3661s against a 3600s cap). Run 35's last
heartbeat landed at 17:24:36 and the dispatcher then EXTENDED its claim three
times — 17:40:21, 17:55:44, 18:10:59, every one of them
``claim_extended reason=pid_alive`` — before the cap finally fired at 18:16:20.
52 minutes of a stopped run reported as healthy.

The backstop that exists for exactly this ("pid alive but no observable
progress") is unreachable by arithmetic: it fires above 3600s of heartbeat
silence while the dispatcher auto-caps a card at max_runtime_seconds=3600, so
``enforce_max_runtime`` always wins first. Across the whole recorded history of
the three live boards there is not one ``stale`` event.
"""

from __future__ import annotations

import os
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


def _run35_shape(conn, *, cap=3600, elapsed=3000, heartbeat_age=1500, pid=4242):
    """A running task shaped like run 35: live worker, dead heartbeat, TTL up."""
    now = int(time.time())
    tid = kb.create_task(
        conn, title="run35", assignee="worker", max_runtime_seconds=cap,
    )
    kb.claim_task(conn, tid)
    kb._set_worker_pid(conn, tid, pid)
    with kb.write_txn(conn):
        conn.execute(
            "UPDATE tasks SET started_at = ?, last_heartbeat_at = ?, "
            "claim_expires = ? WHERE id = ?",
            (now - elapsed, now - heartbeat_age, now - 1, tid),
        )
        conn.execute(
            "UPDATE task_runs SET started_at = ?, last_heartbeat_at = ?, "
            "claim_expires = ? "
            "WHERE id = (SELECT current_run_id FROM tasks WHERE id = ?)",
            (now - elapsed, now - heartbeat_age, now - 1, tid),
        )
    return tid


def _live_then_killed(monkeypatch, pid):
    """_pid_alive says the worker is alive until the reaper signals it."""
    state = {"alive": True}
    monkeypatch.setattr(kb, "_pid_alive", lambda p: state["alive"] and p == pid)

    def _signal(_p, _sig):
        state["alive"] = False

    return _signal


def _kinds(conn, tid):
    return [e.kind for e in kb.list_events(conn, tid)]


def test_stale_heartbeat_reclaims_inside_the_budget(conn, monkeypatch):
    """Run 35's exact shape must be reclaimed, not extended.

    25 minutes of heartbeat silence on a 60-minute budget is a stopped run.
    Today the threshold is a flat 3600s, so the claim is extended on
    ``pid_alive`` alone and the card burns the rest of its cap.
    """
    monkeypatch.setenv("HERMES_KANBAN_HEARTBEAT_STALE_FROM_BUDGET", "1")
    tid = _run35_shape(conn)
    signal_fn = _live_then_killed(monkeypatch, 4242)

    assert kb.release_stale_claims(conn, signal_fn=signal_fn) == 1

    kinds = _kinds(conn, tid)
    assert "reclaimed" in kinds
    assert "claim_extended" not in kinds, (
        "a run 25 minutes past its last heartbeat must not have its claim "
        "extended on pid liveness"
    )
    assert kb.get_task(conn, tid).status == "ready"


def test_default_config_still_extends_a_live_worker(conn, monkeypatch):
    """Without the env switch, behaviour is exactly today's.

    The budget-derived threshold ships OFF: lowering it without the activity
    tick that bridges long tool waits into ``last_heartbeat_at`` would reclaim
    runs that are legitimately parked in one process wait (15m04s measured on
    2026-08-11). This pins that the default path is untouched.
    """
    monkeypatch.delenv("HERMES_KANBAN_HEARTBEAT_STALE_FROM_BUDGET", raising=False)
    tid = _run35_shape(conn)
    signal_fn = _live_then_killed(monkeypatch, 4242)

    assert kb.release_stale_claims(conn, signal_fn=signal_fn) == 0

    kinds = _kinds(conn, tid)
    assert "claim_extended" in kinds
    assert "reclaimed" not in kinds
    assert kb.get_task(conn, tid).status == "running"


def test_threshold_is_a_third_of_the_budget(monkeypatch):
    """min(configured, budget // 3) — and strictly below the cap by construction."""
    monkeypatch.setenv("HERMES_KANBAN_HEARTBEAT_STALE_FROM_BUDGET", "1")
    monkeypatch.delenv("HERMES_KANBAN_HEARTBEAT_MAX_STALE_SECONDS", raising=False)

    assert kb.resolve_heartbeat_max_stale_seconds(3600) == 1200
    assert kb.resolve_heartbeat_max_stale_seconds(3600) < 3600
    # A tiny budget must still leave a positive, reachable threshold.
    assert 0 < kb.resolve_heartbeat_max_stale_seconds(10) < 10
    # No budget on the card → the configured flat value, unchanged.
    assert (
        kb.resolve_heartbeat_max_stale_seconds(None)
        == kb.DEFAULT_CLAIM_HEARTBEAT_MAX_STALE_SECONDS
    )


def test_configured_threshold_still_caps_the_derived_one(monkeypatch):
    """The env value is a ceiling, not an override: whichever is smaller wins."""
    monkeypatch.setenv("HERMES_KANBAN_HEARTBEAT_STALE_FROM_BUDGET", "1")
    monkeypatch.setenv("HERMES_KANBAN_HEARTBEAT_MAX_STALE_SECONDS", "300")
    assert kb.resolve_heartbeat_max_stale_seconds(3600) == 300


def test_unreachable_threshold_cannot_be_configured(conn, monkeypatch):
    """A threshold >= the runtime cap is a config that can never fire.

    That is the defect this whole test module exists for, so making it
    expressible again — this time deliberately, by hand — must fail loudly at
    the dispatcher instead of silently disabling the backstop.
    """
    monkeypatch.setenv("HERMES_KANBAN_HEARTBEAT_MAX_STALE_SECONDS", "7200")
    with pytest.raises(ValueError) as excinfo:
        kb.dispatch_once(
            conn,
            spawn_fn=lambda *_a, **_k: None,
            default_max_runtime_seconds=3600,
        )
    assert "7200" in str(excinfo.value)


def test_reachable_configured_threshold_is_accepted(conn, monkeypatch):
    """The same validation must not reject a threshold that CAN fire."""
    monkeypatch.setenv("HERMES_KANBAN_HEARTBEAT_MAX_STALE_SECONDS", "600")
    res = kb.dispatch_once(
        conn,
        spawn_fn=lambda *_a, **_k: None,
        default_max_runtime_seconds=3600,
    )
    assert res is not None


def test_validation_ignores_the_compiled_default(conn, monkeypatch):
    """The shipped default (3600 == cap) is grandfathered, not fatal.

    It is unreachable and that is the bug, but crashing every dispatcher tick
    on a value the operator never chose would turn a silent gap into an outage.
    Only an explicitly configured unreachable threshold raises.
    """
    monkeypatch.delenv("HERMES_KANBAN_HEARTBEAT_MAX_STALE_SECONDS", raising=False)
    res = kb.dispatch_once(
        conn,
        spawn_fn=lambda *_a, **_k: None,
        default_max_runtime_seconds=kb.DEFAULT_CLAIM_HEARTBEAT_MAX_STALE_SECONDS,
    )
    assert res is not None
