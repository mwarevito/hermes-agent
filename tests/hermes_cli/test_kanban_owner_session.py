"""Batch-4 R1 — core task-ownership incident-replay regression tests.

The 2026-07-28 incident: a decide_path plan-critic subagent, spawned inside a
kanban worker process, inherited ``HERMES_KANBAN_TASK`` via the process env and
called ``kanban_complete`` — silently closing EVERY heavy task the moment the
critic ran, before the worker finished. R1 pins the owning worker session on
the run row at boot (``stamp_run_owner_session``) and rejects any completion /
block whose caller session_id differs. These tests are the permanent guard.
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


WORKER = "2026_worker_sess_aaa"
CRITIC = "2026_critic_sess_bbb"


def _claimed_run(conn):
    """Create + claim a worker task; return (task_id, run_id)."""
    tid = kb.create_task(conn, title="heavy", assignee="worker")
    claimed = kb.claim_task(conn, tid)
    assert claimed is not None, "task must be claimable (ready)"
    run = kb.latest_run(conn, tid)
    assert run is not None
    return tid, run.id


def _events(conn, tid, kind):
    return list(conn.execute(
        "SELECT payload FROM task_events WHERE task_id = ? AND kind = ?",
        (tid, kind),
    ))


# --------------------------------------------------------------------------
# stamp_run_owner_session — first-writer-wins
# --------------------------------------------------------------------------

def test_owner_column_exists(kanban_home):
    with kb.connect() as conn:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(task_runs)")}
    assert "owner_session_id" in cols


def test_stamp_first_writer_wins(kanban_home):
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)
        # A later subagent trying to steal ownership is a no-op.
        kb.stamp_run_owner_session(conn, rid, CRITIC)
        row = conn.execute(
            "SELECT owner_session_id FROM task_runs WHERE id = ?", (rid,)
        ).fetchone()
    assert row["owner_session_id"] == WORKER


def test_stamp_ignores_blank_session(kanban_home):
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, "")
        row = conn.execute(
            "SELECT owner_session_id FROM task_runs WHERE id = ?", (rid,)
        ).fetchone()
    assert row["owner_session_id"] is None


# --------------------------------------------------------------------------
# complete_task ownership gate
# --------------------------------------------------------------------------

def test_complete_blocked_when_caller_not_owner(kanban_home):
    """THE incident: a critic (different session) must NOT complete the task."""
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)
        ok = kb.complete_task(
            conn, tid, summary="critic tried to close it",
            expected_run_id=rid,
            caller_session_id=CRITIC, require_owner=True,
        )
        task = kb.get_task(conn, tid)
        blocked = _events(conn, tid, "completion_blocked_ownership")
    assert ok is False
    assert task.status == "running"        # task untouched
    assert len(blocked) == 1


def test_complete_allowed_when_caller_is_owner(kanban_home):
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)
        ok = kb.complete_task(
            conn, tid, summary="worker finished",
            expected_run_id=rid,
            caller_session_id=WORKER, require_owner=True,
        )
        task = kb.get_task(conn, tid)
    assert ok is True
    assert task.status == "done"


def test_complete_falls_through_when_owner_unstamped(kanban_home):
    """Fail-open: an unstamped run (legacy / boot-stamp not landed) is not
    gated, so a genuine worker is never falsely refused."""
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        # NOTE: no stamp_run_owner_session here.
        ok = kb.complete_task(
            conn, tid, summary="ok",
            expected_run_id=rid,
            caller_session_id=CRITIC, require_owner=True,
        )
        task = kb.get_task(conn, tid)
    assert ok is True
    assert task.status == "done"


def test_complete_not_gated_without_require_owner(kanban_home):
    """CLI / orchestrator / dashboard path (require_owner=False) is never
    gated even when a different session owns the run."""
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)
        ok = kb.complete_task(
            conn, tid, summary="human closed it",
            expected_run_id=rid,
            caller_session_id="human_cli", require_owner=False,
        )
        task = kb.get_task(conn, tid)
    assert ok is True
    assert task.status == "done"


def test_complete_blank_caller_cannot_prove_ownership(kanban_home):
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)
        ok = kb.complete_task(
            conn, tid, summary="no id",
            expected_run_id=rid,
            caller_session_id="", require_owner=True,
        )
        task = kb.get_task(conn, tid)
    assert ok is False
    assert task.status == "running"


# --------------------------------------------------------------------------
# block_task ownership gate
# --------------------------------------------------------------------------

def test_block_blocked_when_caller_not_owner(kanban_home):
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)
        ok = kb.block_task(
            conn, tid, reason="critic tried to block",
            expected_run_id=rid,
            caller_session_id=CRITIC, require_owner=True,
        )
        task = kb.get_task(conn, tid)
        blocked = _events(conn, tid, "block_blocked_ownership")
    assert ok is False
    assert task.status == "running"
    assert len(blocked) == 1


def test_block_allowed_when_caller_is_owner(kanban_home):
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)
        ok = kb.block_task(
            conn, tid, reason="worker needs input",
            expected_run_id=rid,
            caller_session_id=WORKER, require_owner=True,
        )
        task = kb.get_task(conn, tid)
    assert ok is True
    assert task.status == "blocked"


# --------------------------------------------------------------------------
# owner resolution via current_run_id (expected_run_id omitted)
# --------------------------------------------------------------------------

def test_complete_gate_resolves_via_current_run_id(kanban_home):
    """When expected_run_id is not passed, the gate reads tasks.current_run_id."""
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)
        ok = kb.complete_task(
            conn, tid, summary="critic, no explicit run id",
            caller_session_id=CRITIC, require_owner=True,
        )
        task = kb.get_task(conn, tid)
    assert ok is False
    assert task.status == "running"



# --------------------------------------------------------------------------
# Batch-4 adversarial-review blocker regression: session rotation (compaction)
# --------------------------------------------------------------------------
ROTATED = "2026_worker_rotated_ccc"
ROTATED2 = "2026_worker_rotated_ddd"


def test_owner_carried_across_session_rotation(kanban_home):
    """A worker whose session_id rotates mid-run (context compaction) must still
    complete its OWN task -- carry_run_owner_session re-stamps the run owner to
    the current session so the gate keeps recognising the worker."""
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)     # boot: owner=S0
        kb.carry_run_owner_session(conn, rid, ROTATED)    # compaction S0->S1
        ok = kb.complete_task(
            conn, tid, summary="finished after compaction",
            expected_run_id=rid,
            caller_session_id=ROTATED, require_owner=True,
        )
        task = kb.get_task(conn, tid)
    assert ok is True
    assert task.status == "done"


def test_carry_is_self_healing_across_rotations(kanban_home):
    """Carry is by run_id (Codex P1): a stale/failed earlier carry cannot
    permanently strand ownership -- the NEXT rotation re-asserts the current
    session regardless of the stored value, and that session can complete."""
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)     # owner=S0
        # simulate a rotation whose carry "failed": owner still S0 while the
        # live session is already ROTATED. The next rotation carries to ROTATED2.
        kb.carry_run_owner_session(conn, rid, ROTATED2)   # unconditional by run_id
        owner = conn.execute(
            "SELECT owner_session_id FROM task_runs WHERE id=?", (rid,)
        ).fetchone()["owner_session_id"]
        ok = kb.complete_task(
            conn, tid, summary="done after re-assert", expected_run_id=rid,
            caller_session_id=ROTATED2, require_owner=True,
        )
    assert owner == ROTATED2
    assert ok is True


def test_carry_ignores_blank_new_session(kanban_home):
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)
        kb.carry_run_owner_session(conn, rid, "")
        owner = conn.execute(
            "SELECT owner_session_id FROM task_runs WHERE id=?", (rid,)
        ).fetchone()["owner_session_id"]
    assert owner == WORKER


def test_session_id_reaches_ownership_gate_end_to_end(kanban_home, monkeypatch):
    """Integration guard (Codex P0 refutation): the caller's session_id must
    flow through the FULL real path model_tools.handle_function_call ->
    _dispatch closure -> registry.dispatch -> _handle_complete -> the ownership
    gate. Direct complete_task(...) unit tests cannot catch a dispatch-plumbing
    break (session_id dropped from the closure); this end-to-end test can, and
    would fail loudly if a legitimate worker were ever false-blocked."""
    import model_tools
    with kb.connect() as conn:
        tid, rid = _claimed_run(conn)
        kb.stamp_run_owner_session(conn, rid, WORKER)
    monkeypatch.setenv("HERMES_KANBAN_TASK", tid)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(rid))

    def _complete(sess):
        return model_tools.handle_function_call(
            "kanban_complete", {"task_id": tid, "summary": "x"},
            task_id="", session_id=sess, turn_id="", api_request_id="",
        )

    # A different (critic) session must NOT complete the worker's task...
    _complete(CRITIC)
    with kb.connect() as conn:
        assert kb.get_task(conn, tid).status == "running"
    # ...the owner (worker) session completes -> session_id reached the gate.
    _complete(WORKER)
    with kb.connect() as conn:
        assert kb.get_task(conn, tid).status == "done"
