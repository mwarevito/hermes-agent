"""FIX C (batch 3) — dispatcher stamps a worker-scoped default
max_runtime_seconds on claimed tasks that set no explicit limit, so a
runaway worker cannot grind for hours. Mirrors test_kanban_default_assignee.
"""
from __future__ import annotations

import sys
import tempfile

import pytest


@pytest.fixture()
def isolated_kanban_home(monkeypatch):
    test_home = tempfile.mkdtemp(prefix="kanban_default_maxrt_test_")
    monkeypatch.setenv("HERMES_HOME", test_home)
    for mod in list(sys.modules.keys()):
        if mod.startswith("hermes_cli") or mod.startswith("hermes_state") or mod == "hermes_constants":
            del sys.modules[mod]
    from hermes_cli import kanban_db
    yield kanban_db, test_home


def _fake_spawn(*args, **kwargs):
    return 12345


def test_no_default_cap_when_param_omitted(isolated_kanban_home):
    kb, _home = isolated_kanban_home
    with kb.connect_closing() as conn:
        kb.create_board(slug="default", name="Test")
        tid = kb.create_task(conn, title="t1", assignee="default")
    with kb.connect_closing() as conn:
        res = kb.dispatch_once(conn, spawn_fn=_fake_spawn, dry_run=False)
    assert len(res.spawned) == 1
    with kb.connect_closing() as conn:
        row = conn.execute("SELECT max_runtime_seconds FROM tasks WHERE id = ?", (tid,)).fetchone()
    assert row["max_runtime_seconds"] is None


def test_default_cap_stamped_on_claimed_task(isolated_kanban_home):
    kb, _home = isolated_kanban_home
    with kb.connect_closing() as conn:
        kb.create_board(slug="default", name="Test")
        tid = kb.create_task(conn, title="t1", assignee="default")
    with kb.connect_closing() as conn:
        res = kb.dispatch_once(
            conn, spawn_fn=_fake_spawn, dry_run=False,
            default_max_runtime_seconds=2700,
        )
    assert len(res.spawned) == 1
    with kb.connect_closing() as conn:
        row = conn.execute("SELECT max_runtime_seconds FROM tasks WHERE id = ?", (tid,)).fetchone()
        assert row["max_runtime_seconds"] == 2700
        evs = list(conn.execute(
            "SELECT payload FROM task_events WHERE task_id = ? AND kind = 'max_runtime_defaulted'",
            (tid,),
        ))
    assert len(evs) == 1


def test_explicit_cap_not_overwritten(isolated_kanban_home):
    kb, _home = isolated_kanban_home
    with kb.connect_closing() as conn:
        kb.create_board(slug="default", name="Test")
        tid = kb.create_task(conn, title="t1", assignee="default", max_runtime_seconds=600)
    with kb.connect_closing() as conn:
        res = kb.dispatch_once(
            conn, spawn_fn=_fake_spawn, dry_run=False,
            default_max_runtime_seconds=2700,
        )
    assert len(res.spawned) == 1
    with kb.connect_closing() as conn:
        row = conn.execute("SELECT max_runtime_seconds FROM tasks WHERE id = ?", (tid,)).fetchone()
        assert row["max_runtime_seconds"] == 600
        evs = list(conn.execute(
            "SELECT payload FROM task_events WHERE task_id = ? AND kind = 'max_runtime_defaulted'",
            (tid,),
        ))
    assert len(evs) == 0
