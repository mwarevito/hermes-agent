"""Batch-4 D1 — the review lane must not inherit the impl-phase auto-cap.

Regression for the Codex-#1 blind spot: FIX C/D5 persists a dispatcher auto-cap
on tasks.max_runtime_seconds; enforce_max_runtime reads that column and requeues
a timed-out run to 'ready', silently downgrading a review to a plain worker
task. claim_review_task clears ONLY the auto-cap; an explicit human cap stays.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    return home


def _review_task_with_cap(conn, cap, *, auto_event_seconds=None):
    tid = kb.create_task(conn, title="reviewable", assignee="worker")
    with kb.write_txn(conn):
        conn.execute(
            "UPDATE tasks SET status='review', claim_lock=NULL, "
            "max_runtime_seconds=? WHERE id=?",
            (cap, tid),
        )
        if auto_event_seconds is not None:
            kb._append_event(
                conn, tid, "max_runtime_defaulted",
                {"seconds": auto_event_seconds,
                 "source": "kanban.default_max_runtime_seconds"},
            )
    return tid


def _task_cap(conn, tid):
    return conn.execute(
        "SELECT max_runtime_seconds FROM tasks WHERE id = ?", (tid,)
    ).fetchone()["max_runtime_seconds"]


def _events(conn, tid, kind):
    return list(conn.execute(
        "SELECT 1 FROM task_events WHERE task_id = ? AND kind = ?", (tid, kind)
    ))


def test_review_claim_clears_auto_cap(kanban_home):
    with kb.connect() as conn:
        tid = _review_task_with_cap(conn, 7200, auto_event_seconds=7200)
        claimed = kb.claim_review_task(conn, tid)
        assert claimed is not None
        run = kb.latest_run(conn, tid)
        run_cap = conn.execute(
            "SELECT max_runtime_seconds FROM task_runs WHERE id = ?", (run.id,)
        ).fetchone()["max_runtime_seconds"]
        cleared = _events(conn, tid, "max_runtime_cap_cleared_for_review")
    assert _task_cap(conn, tid) is None      # enforce_max_runtime reads this
    assert run_cap is None                    # review run does not carry it
    assert len(cleared) == 1


def test_review_claim_keeps_explicit_cap(kanban_home):
    """No max_runtime_defaulted event -> the cap is human-set -> keep it."""
    with kb.connect() as conn:
        tid = _review_task_with_cap(conn, 600)
        assert kb.claim_review_task(conn, tid) is not None
        cleared = _events(conn, tid, "max_runtime_cap_cleared_for_review")
    assert _task_cap(conn, tid) == 600
    assert len(cleared) == 0


def test_review_claim_keeps_human_override_of_auto_cap(kanban_home):
    """An auto event exists (7200) but the current cap (600) differs -> a human
    overrode it -> honour the human cap, do not clear."""
    with kb.connect() as conn:
        tid = _review_task_with_cap(conn, 600, auto_event_seconds=7200)
        assert kb.claim_review_task(conn, tid) is not None
        cleared = _events(conn, tid, "max_runtime_cap_cleared_for_review")
    assert _task_cap(conn, tid) == 600
    assert len(cleared) == 0


def test_review_claim_no_cap_is_noop(kanban_home):
    with kb.connect() as conn:
        tid = _review_task_with_cap(conn, None)
        assert kb.claim_review_task(conn, tid) is not None
    assert _task_cap(conn, tid) is None
