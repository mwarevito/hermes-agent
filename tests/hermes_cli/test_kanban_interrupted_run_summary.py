"""Tests: a killed run must not throw away what it already did.

2026-08-11: all four ``timed_out`` runs in the ``hermes-infra`` board's history
closed with an EMPTY summary — including runs 34 and 35 of ``t_a6c52bb1``,
which had both written checkpoints ("diagnosis-complete",
"focused-tests-green"). The retry therefore restarted from zero against the
same budget, and the third attempt was blocked by the failure counter before
anyone read what the first two produced.

Two separate holes:

* ``_end_run`` writes ``summary = ?`` unconditionally, so a summary the worker
  had already stored on the open run is overwritten with NULL by the reaper
  that closes it;
* nothing writes a summary on the way out. The worker's SIGTERM path
  (``hermes_cli/cli.py``, the ``HERMES_KANBAN_TASK`` branch that calls
  ``os._exit(0)``) flushes logs and leaves. Doing DB work there without a
  deadman timer is worse than not doing it: a blocked write parks the process
  inside the handler until SIGKILL, losing the summary AND the clean exit.
"""

from __future__ import annotations

import os
import subprocess
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
    monkeypatch.setattr(kb, "_pid_alive", lambda _p: False)


def _git_workspace(root: Path) -> Path:
    ws = root / "ws"
    ws.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "GIT_CONFIG_GLOBAL": str(root / "gitconfig"),
           "GIT_CONFIG_SYSTEM": str(root / "gitconfig-sys")}
    subprocess.run(["git", "init", "-q"], cwd=ws, env=env, check=True)
    (ws / "committed.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=ws, env=env, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t",
         "commit", "-qm", "base"],
        cwd=ws, env=env, check=True,
    )
    (ws / "committed.txt").write_text("changed\n", encoding="utf-8")
    (ws / "new_report.md").write_text("findings\n", encoding="utf-8")
    return ws


def _running_over_budget(conn, tid, *, workspace=None):
    kb.claim_task(conn, tid)
    kb._set_worker_pid(conn, tid, 991200)
    if workspace is not None:
        kb.set_workspace_path(conn, tid, str(workspace))
    old = int(time.time()) - 30
    with kb.write_txn(conn):
        conn.execute("UPDATE tasks SET started_at = ? WHERE id = ?", (old, tid))
        conn.execute(
            "UPDATE task_runs SET started_at = ? "
            "WHERE id = (SELECT current_run_id FROM tasks WHERE id = ?)",
            (old, tid),
        )


def test_reaper_does_not_erase_the_summary_on_the_open_run(conn):
    """What the worker stored before the kill must survive the kill."""
    tid = kb.create_task(
        conn, title="killed", assignee="worker", max_runtime_seconds=1,
    )
    _running_over_budget(conn, tid)
    with kb.write_txn(conn):
        conn.execute(
            "UPDATE task_runs SET summary = ? "
            "WHERE id = (SELECT current_run_id FROM tasks WHERE id = ?)",
            ("DONE: narrowed the diff. NEXT: run the gate suite.", tid),
        )

    assert kb.enforce_max_runtime(conn, signal_fn=lambda _p, _s: None) == [tid]

    run = kb.latest_run(conn, tid)
    assert run.summary and "NEXT: run the gate suite" in run.summary, (
        f"the reaper overwrote the worker's summary with {run.summary!r}"
    )


def test_flush_writes_a_summary_from_the_run_so_far(conn, kanban_home, monkeypatch):
    """The exit path can hand the next attempt what this one finished."""
    tid = kb.create_task(conn, title="interrupted", assignee="worker")
    kb.claim_task(conn, tid)
    kb._set_worker_pid(conn, tid, 991201)
    kb.record_checkpoint(
        conn, tid, step_key="diagnosis-complete",
        note="two physical indexes, three scheduler owners",
    )
    run_id = kb.get_task(conn, tid).current_run_id

    monkeypatch.setenv("HERMES_KANBAN_TASK", tid)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(run_id))
    monkeypatch.setenv("HERMES_KANBAN_DB", str(kb.kanban_db_path(board="default")))

    assert kb.flush_interrupted_run_summary(reason="SIGTERM") is True

    row = conn.execute(
        "SELECT summary FROM task_runs WHERE id = ?", (run_id,),
    ).fetchone()
    assert row["summary"], "the interrupted run still closes with nothing"
    assert "diagnosis-complete" in row["summary"]


def test_flush_fills_artifacts_from_the_workspace(conn, kanban_home, monkeypatch):
    """Everything the run touched is on disk — read it instead of asking."""
    ws = _git_workspace(Path(kanban_home) / "repo")
    tid = kb.create_task(conn, title="dirty", assignee="worker")
    _running_over_budget(conn, tid, workspace=ws)
    run_id = kb.get_task(conn, tid).current_run_id

    monkeypatch.setenv("HERMES_KANBAN_TASK", tid)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(run_id))
    monkeypatch.setenv("HERMES_KANBAN_DB", str(kb.kanban_db_path(board="default")))
    monkeypatch.setenv("HERMES_KANBAN_WORKSPACE", str(ws))

    assert kb.flush_interrupted_run_summary(reason="SIGTERM") is True

    artifacts = []
    for ev in kb.list_events(conn, tid):
        if ev.kind == "checkpoint" and ev.payload:
            artifacts.extend(ev.payload.get("artifacts") or [])
    names = {Path(a).name for a in artifacts}
    assert {"committed.txt", "new_report.md"} <= names, (
        f"git-visible work was not recorded: {sorted(names)}"
    )


def test_flush_gives_up_on_its_deadline(conn, kanban_home, monkeypatch):
    """A write that blocks must not park the worker inside the signal handler.

    Without the deadman the handler waits on the DB while the dispatcher's
    SIGKILL lands 5s later — losing the summary and the clean exit both.
    """
    tid = kb.create_task(conn, title="wedged", assignee="worker")
    kb.claim_task(conn, tid)
    run_id = kb.get_task(conn, tid).current_run_id

    monkeypatch.setenv("HERMES_KANBAN_TASK", tid)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(run_id))
    monkeypatch.setenv("HERMES_KANBAN_DB", str(kb.kanban_db_path(board="default")))
    monkeypatch.setattr(
        kb, "_write_interrupted_run_summary",
        lambda *a, **k: time.sleep(30),
    )

    started = time.monotonic()
    assert kb.flush_interrupted_run_summary(
        reason="SIGTERM", deadline_seconds=1,
    ) is False
    assert time.monotonic() - started < 5, "the deadman never fired"


def test_checkpoint_artifacts_autofill_from_git_status(conn, kanban_home):
    """A checkpoint that names no artifacts still records what changed."""
    ws = _git_workspace(Path(kanban_home) / "repo2")
    tid = kb.create_task(conn, title="cp", assignee="worker")
    kb.claim_task(conn, tid)
    kb.set_workspace_path(conn, tid, str(ws))

    assert kb.record_checkpoint(
        conn, tid, step_key="phase-one", note="did the thing",
        workspace=str(ws),
    ) is True

    cp = [e for e in kb.list_events(conn, tid) if e.kind == "checkpoint"][-1]
    names = {Path(a).name for a in (cp.payload.get("artifacts") or [])}
    assert {"committed.txt", "new_report.md"} <= names, (
        f"checkpoint recorded no artifacts: {sorted(names)}"
    )


def test_explicit_artifacts_are_not_replaced(conn, kanban_home):
    """Autofill is a fallback, never an override of what the worker named."""
    ws = _git_workspace(Path(kanban_home) / "repo3")
    named = ws / "committed.txt"
    tid = kb.create_task(conn, title="cp2", assignee="worker")
    kb.claim_task(conn, tid)

    assert kb.record_checkpoint(
        conn, tid, step_key="phase-two", artifacts=[str(named)],
        workspace=str(ws),
    ) is True

    cp = [e for e in kb.list_events(conn, tid) if e.kind == "checkpoint"][-1]
    assert cp.payload["artifacts"] == [str(named)]
