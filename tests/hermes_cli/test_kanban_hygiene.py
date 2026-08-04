"""Board hygiene / domain routing (P0).

Covers the pieces added for deterministic creation routing, the fresh
"ordinary" list view vs the history view, blocked-staleness surfacing, and the
safe idempotent retention sweep + cancel/won't-do close:

* ``resolve_creation_board`` precedence — explicit board > worker/env pin >
  profile/source metadata > content heuristic > default. Crucially routing is
  independent of the persisted *current* board and ambiguity falls to
  ``default`` (never the current board).
* ``list_ordinary`` / ``list_stale_blocked`` view semantics.
* ``sweep_done_tasks`` safety + idempotency and ``cancel_task`` archiving.
* Backward compatibility of the CLI, the ``kanban_create`` tool, and the
  dashboard create endpoint (explicit board still wins; worker pin preserved).
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

# Ensure the worktree (not a stale global clone) is first on sys.path.
_WORKTREE = Path(__file__).resolve().parents[2]
if str(_WORKTREE) not in sys.path:
    sys.path.insert(0, str(_WORKTREE))

from hermes_cli import kanban_db as kb


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_home(tmp_path, monkeypatch):
    """Isolated HERMES_HOME with no prior kanban state and no board pin."""
    home = tmp_path / "hermes_home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    for var in (
        "HERMES_KANBAN_DB",
        "HERMES_KANBAN_WORKSPACES_ROOT",
        "HERMES_KANBAN_HOME",
        "HERMES_KANBAN_BOARD",
        "HERMES_KANBAN_TASK",
        "HERMES_PROFILE",
        "HERMES_PROFILE_NAME",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    try:
        import hermes_constants
        hermes_constants._cached_default_hermes_root = None  # type: ignore[attr-defined]
    except Exception:
        pass
    kb._INITIALIZED_PATHS.clear()
    return home


def _seed_routing_boards():
    kb.create_board("llucky-task-ops")
    kb.create_board("hermes-infra")


# ---------------------------------------------------------------------------
# Routing — deterministic and independent of the current board
# ---------------------------------------------------------------------------

def test_explicit_board_wins_over_everything(fresh_home):
    _seed_routing_boards()
    # dentor would route to llucky-task-ops, but an explicit board wins.
    assert kb.resolve_creation_board(
        board="hermes-infra", assignee="dentor", title="clinic outreach",
    ) == "hermes-infra"


def test_worker_env_pin_preserved_over_routing(fresh_home, monkeypatch):
    _seed_routing_boards()
    # A dispatcher-spawned worker inherits its parent board via the env pin;
    # routing must not override it even though "dentor" points elsewhere.
    monkeypatch.setenv("HERMES_KANBAN_BOARD", "hermes-infra")
    assert kb.resolve_creation_board(assignee="dentor") == "hermes-infra"


def test_profile_metadata_routes(fresh_home):
    _seed_routing_boards()
    assert kb.resolve_creation_board(assignee="dentor") == "llucky-task-ops"
    assert kb.resolve_creation_board(created_by="tony") == "hermes-infra"


def test_content_heuristic_fallback(fresh_home):
    _seed_routing_boards()
    # No profile signal → keywords in title/body decide.
    assert kb.resolve_creation_board(
        assignee="someone", title="clinic outreach plan",
    ) == "llucky-task-ops"
    assert kb.resolve_creation_board(
        assignee="someone", title="fix gateway dispatcher wedge",
    ) == "hermes-infra"


def test_ambiguous_metadata_goes_to_default_not_current(fresh_home):
    _seed_routing_boards()
    # Operator is *viewing* llucky-task-ops...
    kb.set_current_board("llucky-task-ops")
    # ...but a card whose signals point at BOTH boards is ambiguous → default,
    # never the persisted current board.
    assert kb.resolve_creation_board(
        assignee="dentor", created_by="tony",
    ) == "default"


def test_no_signal_goes_to_default_not_current(fresh_home):
    _seed_routing_boards()
    kb.set_current_board("hermes-infra")
    # A create with no routable signal must NOT inherit the current board.
    assert kb.resolve_creation_board() == "default"
    # And a routable signal still routes, ignoring the current board entirely.
    assert kb.resolve_creation_board(assignee="dentor") == "llucky-task-ops"


def test_routed_board_absent_falls_back_to_default(fresh_home):
    # Boards not created → routing is a safe no-op (backward compatible).
    kb.connect().close()  # materialise the default board only
    assert kb.resolve_creation_board(assignee="dentor") == "default"


def test_resolution_is_deterministic(fresh_home):
    _seed_routing_boards()
    a = kb.resolve_creation_board(assignee="dentor", title="clinic outreach")
    b = kb.resolve_creation_board(assignee="dentor", title="clinic outreach")
    assert a == b == "llucky-task-ops"


# ---------------------------------------------------------------------------
# Parent location is authoritative — a child lives with its parents
# ---------------------------------------------------------------------------

def _seed_task(board, title):
    with kb.connect(board=board) as conn:
        return kb.create_task(conn, title=title)


def test_parent_on_default_routes_child_to_default(fresh_home):
    _seed_routing_boards()
    # Operator is viewing infra, and the child's profile/content would route to
    # infra — but the parent lives on default, so the child must too.
    kb.set_current_board("hermes-infra")
    pid = _seed_task("default", "parent on default")
    assert kb.resolve_creation_board(
        parents=[pid], created_by="tony", title="fix gateway dispatcher",
    ) == "default"


def test_parent_on_llucky_beats_profile_and_content(fresh_home):
    _seed_routing_boards()
    pid = _seed_task("llucky-task-ops", "parent on llucky")
    # created_by + keywords both point at hermes-infra; parent location wins.
    assert kb.resolve_creation_board(
        parents=[pid], created_by="tony", title="deploy hermes plugin gateway",
    ) == "llucky-task-ops"


def test_parents_split_across_boards_raises(fresh_home):
    _seed_routing_boards()
    p_default = _seed_task("default", "p1")
    p_llucky = _seed_task("llucky-task-ops", "p2")
    with pytest.raises(ValueError, match="multiple boards"):
        kb.resolve_creation_board(parents=[p_default, p_llucky])


def test_unknown_parent_raises(fresh_home):
    _seed_routing_boards()
    with pytest.raises(ValueError, match="unknown parent"):
        kb.resolve_creation_board(parents=["task_does_not_exist"])


def test_explicit_board_wins_even_with_parents(fresh_home):
    _seed_routing_boards()
    pid = _seed_task("llucky-task-ops", "parent on llucky")
    # Explicit board still wins outright at resolution; create_task then applies
    # its own parent validation (which fails on the mismatched board).
    assert kb.resolve_creation_board(board="hermes-infra", parents=[pid]) == "hermes-infra"


def test_env_pin_wins_even_with_parents(fresh_home, monkeypatch):
    _seed_routing_boards()
    pid = _seed_task("llucky-task-ops", "parent on llucky")
    monkeypatch.setenv("HERMES_KANBAN_BOARD", "hermes-infra")
    assert kb.resolve_creation_board(parents=[pid]) == "hermes-infra"


def test_tool_create_with_parent_routes_to_parent_board(fresh_home):
    from tools import kanban_tools as kt

    _seed_routing_boards()
    pid = _seed_task("llucky-task-ops", "parent on llucky")
    # profile/content would route to infra; parent pins the child to llucky.
    out = kt._handle_create({
        "title": "deploy hermes plugin", "assignee": "tony",
        "parents": [pid],
    })
    d = json.loads(out)
    assert d["ok"] is True, d
    tid = d["task_id"]
    with kb.connect(board="llucky-task-ops") as conn:
        child = kb.get_task(conn, tid)
        assert child is not None
        assert pid in {r["parent_id"] for r in conn.execute(
            "SELECT parent_id FROM task_links WHERE child_id = ?", (tid,)
        ).fetchall()}
    with kb.connect(board="hermes-infra") as conn:
        assert kb.get_task(conn, tid) is None


def test_tool_create_explicit_board_mismatch_errors(fresh_home):
    from tools import kanban_tools as kt

    _seed_routing_boards()
    pid = _seed_task("llucky-task-ops", "parent on llucky")
    # Explicit board pins to infra, but the parent lives on llucky → the
    # create's own parent validation rejects it (clean error, no dangling link).
    out = kt._handle_create({
        "title": "x", "assignee": "tony", "board": "hermes-infra",
        "parents": [pid],
    })
    d = json.loads(out)
    assert "error" in d, d
    assert "unknown parent" in d["error"]


# ---------------------------------------------------------------------------
# Views — ordinary vs history, blocked staleness
# ---------------------------------------------------------------------------

def test_ordinary_shows_actionable_and_recent_done(fresh_home):
    now0 = int(time.time())
    with kb.connect_closing() as conn:
        ready = kb.create_task(conn, title="ready one")
        done = kb.create_task(conn, title="done one")
        kb.complete_task(conn, done, result="ok")
        ids = {t.id for t in kb.list_ordinary(conn, now=now0, done_within_days=3)}
    assert ready in ids and done in ids


def test_ordinary_hides_done_older_than_window(fresh_home):
    now0 = int(time.time())
    with kb.connect_closing() as conn:
        ready = kb.create_task(conn, title="ready one")
        done = kb.create_task(conn, title="stale done")
        kb.complete_task(conn, done, result="ok")
        # Ten days later the done card drops out of the ordinary view...
        future = now0 + 10 * 86400
        ordinary = {t.id for t in kb.list_ordinary(conn, now=future, done_within_days=3)}
        # ...but is still present in the history/all view.
        history = {t.id for t in kb.list_tasks(conn, include_archived=True)}
    assert ready in ordinary
    assert done not in ordinary
    assert done in history


def test_ordinary_excludes_archived(fresh_home):
    with kb.connect_closing() as conn:
        keep = kb.create_task(conn, title="keep")
        gone = kb.create_task(conn, title="gone")
        kb.archive_task(conn, gone)
        ordinary = {t.id for t in kb.list_ordinary(conn)}
        history = {t.id for t in kb.list_tasks(conn, include_archived=True)}
    assert keep in ordinary and gone not in ordinary
    assert gone in history


def test_blocked_never_hidden_regardless_of_age(fresh_home):
    now0 = int(time.time())
    with kb.connect_closing() as conn:
        blocked = kb.create_task(conn, title="blocked", initial_status="blocked")
        far_future = now0 + 400 * 86400
        ids = {t.id for t in kb.list_ordinary(conn, now=far_future, done_within_days=3)}
    assert blocked in ids


def test_stale_blocked_surfaces_only_after_threshold(fresh_home):
    now0 = int(time.time())
    with kb.connect_closing() as conn:
        blocked = kb.create_task(conn, title="blocked", initial_status="blocked")
        # Fresh: not stale.
        assert [t.id for t in kb.list_stale_blocked(conn, now=now0, stale_days=7)] == []
        # Ten days later: needs a decision.
        future = now0 + 10 * 86400
        stale = [t.id for t in kb.list_stale_blocked(conn, now=future, stale_days=7)]
    assert stale == [blocked]


def test_stale_blocked_respects_scope_filters(fresh_home):
    now0 = int(time.time())
    future = now0 + 10 * 86400
    with kb.connect_closing() as conn:
        mine = kb.create_task(conn, title="mine", assignee="alice",
                              initial_status="blocked")
        kb.create_task(conn, title="theirs", assignee="bob",
                       initial_status="blocked")
        # Unscoped: both surface.
        allids = {t.id for t in kb.list_stale_blocked(conn, now=future, stale_days=7)}
        # Scoped to alice: only her blocked card, so a `list --mine` footer never
        # lists other people's ids.
        scoped = [t.id for t in kb.list_stale_blocked(
            conn, now=future, stale_days=7, assignee="alice")]
    assert len(allids) == 2
    assert scoped == [mine]


# ---------------------------------------------------------------------------
# Retention sweep + cancel/won't-do
# ---------------------------------------------------------------------------

def test_sweep_archives_old_done_and_is_idempotent(fresh_home):
    now0 = int(time.time())
    with kb.connect_closing() as conn:
        done = kb.create_task(conn, title="old done")
        kb.complete_task(conn, done, result="ok")
        future = now0 + 40 * 86400
        first = kb.sweep_done_tasks(conn, done_within_days=30, now=future)
        second = kb.sweep_done_tasks(conn, done_within_days=30, now=future)
        status = kb.get_task(conn, done).status
    assert first == [done]
    assert second == []          # idempotent — nothing new to archive
    assert status == "archived"


def test_sweep_never_touches_active_or_blocked(fresh_home):
    now0 = int(time.time())
    with kb.connect_closing() as conn:
        ready = kb.create_task(conn, title="ready")
        # A genuine (sticky) operator block — emits a 'blocked' event so it is
        # not auto-recovered. This is the case that must stay visible forever.
        blocked = kb.create_task(conn, title="blocked")
        kb.block_task(conn, blocked, reason="needs a human decision")
        triage = kb.create_task(conn, title="triage", triage=True)
        done = kb.create_task(conn, title="old done")
        kb.complete_task(conn, done, result="ok")
        future = now0 + 40 * 86400
        archived = kb.sweep_done_tasks(conn, done_within_days=30, now=future)
        statuses = {tid: kb.get_task(conn, tid).status
                    for tid in (ready, blocked, triage, done)}
    assert archived == [done]
    assert statuses[ready] == "ready"
    assert statuses[blocked] == "blocked"    # sticky block stays visible forever
    assert statuses[triage] == "triage"
    assert statuses[done] == "archived"


def test_sweep_skips_done_with_null_completed_at(fresh_home):
    now0 = int(time.time())
    with kb.connect_closing() as conn:
        t = kb.create_task(conn, title="legacy done")
        # Simulate a legacy/hand-set done row with no completion timestamp.
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE tasks SET status='done', completed_at=NULL WHERE id=?",
                (t,),
            )
        archived = kb.sweep_done_tasks(conn, done_within_days=1, now=now0 + 999 * 86400)
        assert archived == []
        assert kb.get_task(conn, t).status == "done"


def test_cancel_task_archives_not_done_and_audits(fresh_home):
    with kb.connect_closing() as conn:
        t = kb.create_task(conn, title="wont do")
        assert kb.cancel_task(conn, t, reason="duplicate") is True
        task = kb.get_task(conn, t)
        assert task.status == "archived"
        assert task.completed_at is None          # never counts as done
        kinds = [e.kind for e in kb.list_events(conn, t)]
        assert "cancelled" in kinds
        # Hidden from ordinary, present in history.
        assert t not in {x.id for x in kb.list_ordinary(conn)}
        assert t in {x.id for x in kb.list_tasks(conn, include_archived=True)}
        # Idempotent: a second cancel on an archived task is a no-op.
        assert kb.cancel_task(conn, t) is False


def test_cancel_preserves_comments_and_events(fresh_home):
    with kb.connect_closing() as conn:
        t = kb.create_task(conn, title="has history")
        kb.add_comment(conn, t, author="vito", body="please cancel")
        kb.cancel_task(conn, t, reason="obsolete")
        comments = kb.list_comments(conn, t)
        events = kb.list_events(conn, t)
    assert any(c.body == "please cancel" for c in comments)   # not deleted
    assert len(events) >= 1


# ---------------------------------------------------------------------------
# Backward compatibility — existing list semantics unchanged
# ---------------------------------------------------------------------------

def test_list_tasks_default_still_excludes_archived(fresh_home):
    with kb.connect_closing() as conn:
        ready = kb.create_task(conn, title="ready")
        gone = kb.create_task(conn, title="archived")
        kb.archive_task(conn, gone)
        ids = {t.id for t in kb.list_tasks(conn)}
    assert ready in ids and gone not in ids


# ---------------------------------------------------------------------------
# CLI compatibility (routing + explicit --board pin)
# ---------------------------------------------------------------------------

def test_cli_create_routes_and_ignores_current_board(fresh_home, monkeypatch):
    from hermes_cli import kanban as kc

    _seed_routing_boards()
    kb.set_current_board("hermes-infra")           # operator viewing infra board
    monkeypatch.setenv("HERMES_PROFILE", "operator")  # neutral created_by

    kc.run_slash("create 'clinic outreach' --assignee dentor")

    # Routed to llucky-task-ops by the dentor profile...
    with kb.connect(board="llucky-task-ops") as conn:
        assert any(t.title == "clinic outreach" for t in kb.list_tasks(conn))
    # ...NOT the persisted current board, and NOT default.
    with kb.connect(board="hermes-infra") as conn:
        assert not any(t.title == "clinic outreach" for t in kb.list_tasks(conn))
    with kb.connect() as conn:
        assert not any(t.title == "clinic outreach" for t in kb.list_tasks(conn))


def test_cli_explicit_board_pins_create(fresh_home, monkeypatch):
    from hermes_cli import kanban as kc

    _seed_routing_boards()
    monkeypatch.setenv("HERMES_PROFILE", "operator")

    # dentor would route to llucky-task-ops, but --board pins it to infra.
    kc.run_slash("--board hermes-infra create 'clinic thing' --assignee dentor")

    with kb.connect(board="hermes-infra") as conn:
        assert any(t.title == "clinic thing" for t in kb.list_tasks(conn))
    with kb.connect(board="llucky-task-ops") as conn:
        assert not any(t.title == "clinic thing" for t in kb.list_tasks(conn))


# ---------------------------------------------------------------------------
# Tool compatibility (kanban_create routing + explicit board wins)
# ---------------------------------------------------------------------------

def test_tool_create_routes_by_profile(fresh_home):
    from tools import kanban_tools as kt

    _seed_routing_boards()
    out = kt._handle_create({"title": "clinic outreach", "assignee": "dentor"})
    d = json.loads(out)
    assert d["ok"] is True, d
    tid = d["task_id"]
    with kb.connect(board="llucky-task-ops") as conn:
        assert kb.get_task(conn, tid) is not None
    with kb.connect() as conn:
        assert kb.get_task(conn, tid) is None


def test_tool_explicit_board_still_wins(fresh_home):
    from tools import kanban_tools as kt

    _seed_routing_boards()
    out = kt._handle_create(
        {"title": "x", "assignee": "dentor", "board": "hermes-infra"}
    )
    d = json.loads(out)
    assert d["ok"] is True, d
    tid = d["task_id"]
    with kb.connect(board="hermes-infra") as conn:
        assert kb.get_task(conn, tid) is not None
    with kb.connect(board="llucky-task-ops") as conn:
        assert kb.get_task(conn, tid) is None


# ---------------------------------------------------------------------------
# Dashboard compatibility (create endpoint routes when board omitted)
# ---------------------------------------------------------------------------

def test_dashboard_create_routes_when_board_omitted(fresh_home):
    import importlib.util

    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    plugin_file = _WORKTREE / "plugins" / "kanban" / "dashboard" / "plugin_api.py"
    spec = importlib.util.spec_from_file_location(
        "hermes_dashboard_plugin_kanban_hygiene_test", plugin_file
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)

    _seed_routing_boards()
    app = fastapi.FastAPI()
    app.include_router(mod.router, prefix="/api/plugins/kanban")
    client = TestClient(app)

    r = client.post(
        "/api/plugins/kanban/tasks",
        json={"title": "clinic outreach", "assignee": "dentor"},
    )
    assert r.status_code == 200, r.text
    tid = r.json()["task"]["id"]
    with kb.connect(board="llucky-task-ops") as conn:
        assert kb.get_task(conn, tid) is not None
    with kb.connect() as conn:
        assert kb.get_task(conn, tid) is None
