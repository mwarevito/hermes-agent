"""A dependency wait that repeats verbatim is a loop, not a wait.

2026-08-08: five consecutive runs of one card returned an identical dependency
verdict — 7 minutes 35 seconds of pure cycle. A dependency wait always routes
back to ``todo`` (correct: parents may still finish) and nothing counted the
repeat, so the card could spin indefinitely while looking busy.

The escalation is deliberately narrow, and the negative tests are the ones that
matter: a task that waits, progresses, and later waits again for the same reason
is making progress between the two and must NOT be escalated.
"""
import json

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli.kanban_db import _dependency_wait_is_repeating


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "k.db"))
    kb.init_db()
    c = kb.connect()
    yield c
    c.close()


def _event(conn, task_id, kind, payload):
    conn.execute(
        "INSERT INTO task_events (task_id, kind, payload, created_at) VALUES (?,?,?,?)",
        (task_id, kind, json.dumps(payload), 0),
    )
    conn.commit()


def test_no_history_is_not_a_repeat(conn):
    tid = kb.create_task(conn, title="t", assignee="w")
    assert _dependency_wait_is_repeating(conn, tid, "waiting for parent") is False


def test_the_same_reason_twice_is_a_repeat(conn):
    tid = kb.create_task(conn, title="t", assignee="w")
    _event(conn, tid, "dependency_wait", {"reason": "waiting for parent"})
    assert _dependency_wait_is_repeating(conn, tid, "waiting for parent") is True


def test_whitespace_does_not_disguise_a_repeat(conn):
    tid = kb.create_task(conn, title="t", assignee="w")
    _event(conn, tid, "dependency_wait", {"reason": " waiting for parent\n"})
    assert _dependency_wait_is_repeating(conn, tid, "waiting for parent") is True


def test_a_different_reason_is_progress(conn):
    tid = kb.create_task(conn, title="t", assignee="w")
    _event(conn, tid, "dependency_wait", {"reason": "waiting for A"})
    assert _dependency_wait_is_repeating(conn, tid, "waiting for B") is False


def test_only_the_most_recent_wait_is_compared(conn):
    """Waited, moved on, waited again for the old reason — that is progress."""
    tid = kb.create_task(conn, title="t", assignee="w")
    _event(conn, tid, "dependency_wait", {"reason": "waiting for A"})
    _event(conn, tid, "dependency_wait", {"reason": "waiting for B"})
    assert _dependency_wait_is_repeating(conn, tid, "waiting for A") is False


def test_another_task_s_history_is_not_borrowed(conn):
    a = kb.create_task(conn, title="a", assignee="w")
    b = kb.create_task(conn, title="b", assignee="w")
    _event(conn, a, "dependency_wait", {"reason": "same words"})
    assert _dependency_wait_is_repeating(conn, b, "same words") is False


def test_other_event_kinds_are_not_dependency_waits(conn):
    tid = kb.create_task(conn, title="t", assignee="w")
    _event(conn, tid, "blocked", {"reason": "same words"})
    assert _dependency_wait_is_repeating(conn, tid, "same words") is False


def test_a_missing_reason_in_history_is_not_a_repeat(conn):
    tid = kb.create_task(conn, title="t", assignee="w")
    _event(conn, tid, "dependency_wait", {})
    assert _dependency_wait_is_repeating(conn, tid, "waiting") is False


def test_a_none_reason_now_is_never_a_repeat(conn):
    tid = kb.create_task(conn, title="t", assignee="w")
    _event(conn, tid, "dependency_wait", {"reason": "waiting"})
    assert _dependency_wait_is_repeating(conn, tid, None) is False


def test_unparseable_payload_answers_false(conn):
    """An unreadable history must not invent an escalation."""
    tid = kb.create_task(conn, title="t", assignee="w")
    conn.execute(
        "INSERT INTO task_events (task_id, kind, payload, created_at) VALUES (?,?,?,?)",
        (tid, "dependency_wait", "{not json", 0),
    )
    conn.commit()
    assert _dependency_wait_is_repeating(conn, tid, "waiting") is False
