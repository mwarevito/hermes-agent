"""Tests: running out of time is not the same thing as failing.

2026-08-11, ``t_a6c52bb1``: runs 34 and 35 both ended on the runtime cap (3611s
and 3661s against 3600s) and both were counted into ``consecutive_failures``.
``DEFAULT_FAILURE_LIMIT`` is 2, so the second shortfall of TIME blocked the card
permanently — even though run 34 finished a diagnosis phase and run 35 got the
focused tests green, both recorded as checkpoints. Worse, the kill and the
respawn happen in the same dispatcher tick, so attempt two starts against the
same wall the moment attempt one dies.

The neighbouring outcome in this file already decided the same question the
other way: ``detect_stale_running`` deliberately does NOT call
``_record_task_failure``, because "the dispatcher noticed no heartbeat" says
nothing about the worker having failed and two long-but-healthy runs would trip
the breaker. A run that produced progress and hit the clock is the same class of
thing.

Tightened the same day after review. "Progress" was first defined as a
checkpoint OR a heartbeat note of >=20 chars, and a backtest over every recorded
run of the three live boards showed the note half re-labels 8 of the 9
historical ``timed_out`` runs — on notes that are about waiting ("Queued behind
one legitimate external Claude review…", "Producer implementation still
running"). That makes not-a-failure the default. Progress is now a checkpoint
whose ``step_key`` is new for the task, which re-labels 4 of the 9, and the
out-of-time budget went 4 -> 3 so an unfittable card burns three hours, not four.
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


@pytest.fixture(autouse=True)
def _dead_pids(monkeypatch):
    """SIGTERM 'works' instantly so the grace poll doesn't cost 5s per test."""
    monkeypatch.setattr(kb, "_pid_alive", lambda _p: False)


def _overrun(conn, tid, *, progress: bool, step_key: str = "focused-tests-green"):
    """Claim the task, optionally record progress, and push it past its cap.

    ``step_key`` is a parameter because progress means a phase the task has
    never finished before: two attempts re-declaring the SAME finished phase are
    the work repeating itself. Runs 34 and 35 checkpointed 'diagnosis-complete'
    and then 'focused-tests-green' — distinct keys — which is what the
    multi-attempt tests below reproduce.
    """
    kb.claim_task(conn, tid)
    kb._set_worker_pid(conn, tid, 991100)
    if progress:
        kb.record_checkpoint(
            conn, tid, step_key=step_key,
            note="narrowed diff kept; focused suite green",
        )
    old = int(time.time()) - 30
    with kb.write_txn(conn):
        conn.execute("UPDATE tasks SET started_at = ? WHERE id = ?", (old, tid))
        conn.execute(
            "UPDATE task_runs SET started_at = ? "
            "WHERE id = (SELECT current_run_id FROM tasks WHERE id = ?)",
            (old, tid),
        )
    return kb.enforce_max_runtime(conn, signal_fn=lambda _p, _s: None)


def test_timeout_after_progress_is_not_a_failure(conn):
    """A run that checkpointed and then hit the clock must not burn a retry."""
    tid = kb.create_task(
        conn, title="long but moving", assignee="worker", max_runtime_seconds=1,
    )
    assert _overrun(conn, tid, progress=True) == [tid]

    task = kb.get_task(conn, tid)
    assert task.consecutive_failures == 0, (
        "a shortfall of time after real progress was counted as a failure"
    )
    assert task.status == "ready"

    run = kb.latest_run(conn, tid)
    assert run.outcome == "out_of_time", (
        f"expected a distinct out-of-time outcome, got {run.outcome!r}"
    )
    # The event kind stays 'timed_out': it is what the gateway's wake
    # notification and every existing watcher subscribe to.
    assert any(e.kind == "timed_out" for e in kb.list_events(conn, tid))


def test_timeout_without_progress_still_counts(conn):
    """No checkpoint, no heartbeat note — nothing moved, so it is a failure.

    Removing the increment outright would give an unbounded carousel: a card
    that can never finish inside its budget would be killed and respawned
    forever.
    """
    tid = kb.create_task(
        conn, title="stuck", assignee="worker", max_runtime_seconds=1,
    )
    assert _overrun(conn, tid, progress=False) == [tid]

    assert kb.get_task(conn, tid).consecutive_failures == 1
    assert kb.latest_run(conn, tid).outcome == "timed_out"


def test_heartbeat_note_is_not_progress(conn):
    """A substantive-looking heartbeat note is still only liveness.

    Backtested 2026-08-11 over every recorded run of the three live boards: the
    ">=20 chars of note" rule re-labels 8 of the 9 historical ``timed_out`` runs
    as out-of-time, on notes that are literally about waiting. The note used
    here is one of them verbatim (llucky-task-ops run 12, ``t_611c3454``).
    """
    tid = kb.create_task(
        conn, title="noted", assignee="worker", max_runtime_seconds=1,
    )
    kb.claim_task(conn, tid)
    kb._set_worker_pid(conn, tid, 991101)
    kb.heartbeat_worker(
        conn, tid,
        note="Producer implementation still running; historical parity "
             "baselines verified",
    )
    old = int(time.time()) - 30
    with kb.write_txn(conn):
        conn.execute("UPDATE tasks SET started_at = ? WHERE id = ?", (old, tid))
        conn.execute(
            "UPDATE task_runs SET started_at = ? "
            "WHERE id = (SELECT current_run_id FROM tasks WHERE id = ?)",
            (old, tid),
        )
    assert kb.enforce_max_runtime(conn, signal_fn=lambda _p, _s: None) == [tid]
    assert kb.get_task(conn, tid).consecutive_failures == 1, (
        "a heartbeat note bought the run a free retry — the detector is "
        "measuring liveness again, only in prose"
    )
    assert kb.latest_run(conn, tid).outcome == "timed_out"


def test_bare_heartbeat_is_not_progress(conn):
    """A note-less heartbeat proves the transport, not the work.

    Run 35 emitted nine of them after its last real note and still produced
    nothing; counting them as progress would make the out-of-time budget
    unbounded in practice.
    """
    tid = kb.create_task(
        conn, title="beating", assignee="worker", max_runtime_seconds=1,
    )
    kb.claim_task(conn, tid)
    kb._set_worker_pid(conn, tid, 991102)
    kb.heartbeat_worker(conn, tid)
    old = int(time.time()) - 30
    with kb.write_txn(conn):
        conn.execute("UPDATE tasks SET started_at = ? WHERE id = ?", (old, tid))
        conn.execute(
            "UPDATE task_runs SET started_at = ? "
            "WHERE id = (SELECT current_run_id FROM tasks WHERE id = ?)",
            (old, tid),
        )
    assert kb.enforce_max_runtime(conn, signal_fn=lambda _p, _s: None) == [tid]
    assert kb.get_task(conn, tid).consecutive_failures == 1


def test_two_timeouts_with_progress_do_not_block_the_card(conn):
    """The exact 2026-08-11 sequence: two shortfalls of time, card still alive.

    ``DEFAULT_FAILURE_LIMIT`` is 2, which is why runs 34 and 35 were enough to
    park ``t_a6c52bb1`` in ``blocked`` for good.
    """
    tid = kb.create_task(
        conn, title="run34+35", assignee="worker", max_runtime_seconds=1,
    )
    assert _overrun(
        conn, tid, progress=True, step_key="diagnosis-complete",
    ) == [tid]
    assert _overrun(
        conn, tid, progress=True, step_key="focused-tests-green",
    ) == [tid]

    task = kb.get_task(conn, tid)
    assert task.status == "ready", (
        "two shortfalls of TIME blocked the card, same as on 2026-08-11"
    )
    assert task.consecutive_failures == 0


def test_out_of_time_streak_is_bounded(conn, monkeypatch):
    """Not a failure still means not forever: the streak has its own limit."""
    monkeypatch.setenv("HERMES_KANBAN_OUT_OF_TIME_LIMIT", "2")
    tid = kb.create_task(
        conn, title="always short", assignee="worker", max_runtime_seconds=1,
    )

    assert _overrun(
        conn, tid, progress=True, step_key="diagnosis-complete",
    ) == [tid]
    assert kb.get_task(conn, tid).status == "ready"

    assert _overrun(
        conn, tid, progress=True, step_key="focused-tests-green",
    ) == [tid]
    task = kb.get_task(conn, tid)
    assert task.status == "blocked", (
        "a card that can never fit its budget must stop being respawned"
    )
    gave_up = [e for e in kb.list_events(conn, tid) if e.kind == "gave_up"]
    assert gave_up, "no gave_up event was emitted"
    assert gave_up[-1].payload.get("trigger_outcome") == "out_of_time", (
        "the card was blocked by the generic failure counter, not by the "
        "out-of-time budget"
    )


def test_respawn_waits_after_a_timeout(conn):
    """Kill and respawn in the same tick guarantees the same wall again."""
    tid = kb.create_task(
        conn, title="cooldown", assignee="worker", max_runtime_seconds=1,
    )
    assert _overrun(conn, tid, progress=True) == [tid]

    assert kb.check_respawn_guard(conn, tid) == "timeout_cooldown"


def test_timeout_cooldown_expires(conn, monkeypatch):
    """The hold is a cooldown, not a block: it lets go on its own."""
    monkeypatch.setenv("HERMES_KANBAN_TIMEOUT_COOLDOWN_SECONDS", "0")
    tid = kb.create_task(
        conn, title="cooldown off", assignee="worker", max_runtime_seconds=1,
    )
    assert _overrun(conn, tid, progress=True) == [tid]

    assert kb.check_respawn_guard(conn, tid) is None


def test_repeating_a_finished_phase_is_not_progress(conn):
    """Re-declaring a phase an earlier attempt finished is not the work moving.

    This is the one way left for a worker to mint unlimited "progress" on a
    timer: one checkpoint per attempt under an unchanged key. DET-E in
    task_liveness_watchdog already refuses to count that, and ``record_checkpoint``
    documents the same rule ("Name the phase you finished, not the one you are
    starting").
    """
    tid = kb.create_task(
        conn, title="same phase twice", assignee="worker",
        max_runtime_seconds=1,
    )
    assert _overrun(
        conn, tid, progress=True, step_key="diagnosis-complete",
    ) == [tid]
    assert kb.get_task(conn, tid).consecutive_failures == 0

    assert _overrun(
        conn, tid, progress=True, step_key="diagnosis-complete",
    ) == [tid]
    assert kb.get_task(conn, tid).consecutive_failures == 1, (
        "the second attempt re-finished the same phase and was still credited "
        "with progress"
    )
    assert kb.latest_run(conn, tid).outcome == "timed_out"


def test_default_out_of_time_budget_is_three_attempts(conn, monkeypatch):
    """The default carousel is bounded at three attempts, not four.

    Four attempts against ``t_a6c52bb1``'s 3600s cap is four hours of wall clock
    and four Claude/Codex runs spent before anybody is told that the budget, not
    the work, is what is wrong.
    """
    monkeypatch.delenv("HERMES_KANBAN_OUT_OF_TIME_LIMIT", raising=False)
    assert kb._resolve_out_of_time_limit() == 3

    tid = kb.create_task(
        conn, title="never fits", assignee="worker", max_runtime_seconds=1,
    )
    for key in ("phase-a", "phase-b"):
        assert _overrun(conn, tid, progress=True, step_key=key) == [tid]
        assert kb.get_task(conn, tid).status == "ready"

    assert _overrun(conn, tid, progress=True, step_key="phase-c") == [tid]
    assert kb.get_task(conn, tid).status == "blocked", (
        "a card that never fits its cap kept being respawned past the budget"
    )
