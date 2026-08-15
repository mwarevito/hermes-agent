"""Kanban run and transcript lifecycle must terminate together."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb
from hermes_state import SessionDB


@pytest.fixture
def kanban_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    return home


def mark_worker_dead(
    conn: sqlite3.Connection,
    task_id: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    pid: int = 9_999_991,
) -> None:
    kb._set_worker_pid(conn, task_id, pid)
    with kb.write_txn(conn):
        conn.execute(
            "UPDATE tasks SET started_at = ? WHERE id = ?",
            (int(time.time()) - 120, task_id),
        )
        conn.execute(
            "UPDATE task_runs SET started_at = ? WHERE task_id = ? "
            "AND ended_at IS NULL",
            (int(time.time()) - 120, task_id),
        )
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)


def test_dispatcher_worker_death_ends_linked_worker_transcript(
    kanban_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A confirmed dead worker must not leave an open transcript session."""
    worker_session = "kanban-worker-session"
    state = SessionDB(kanban_home / "state.db")
    state.create_session(worker_session, source="kanban")
    state.close()

    with kb.connect() as conn:
        task_id = kb.create_task(
            conn,
            title="deterministic expired worker",
        )
        claimed = kb.claim_task(conn, task_id)
        assert claimed is not None
        run = kb.latest_run(conn, task_id)
        assert run is not None
        kb.stamp_run_owner_session(conn, run.id, worker_session)
        mark_worker_dead(conn, task_id, monkeypatch)

        result = kb.dispatch_once(
            conn,
            spawn_fn=lambda *_args: pytest.fail("expired task was respawned"),
            max_spawn=0,
        )
        task = kb.get_task(conn, task_id)
        closed_run = kb.latest_run(conn, task_id)
        transcript_receipt = conn.execute(
            "SELECT transcript_ended_at FROM task_runs WHERE id = ?",
            (run.id,),
        ).fetchone()[0]
        assert kb.finalize_terminal_run_transcripts(conn) == []
        receipt_events = conn.execute(
            "SELECT COUNT(*) FROM task_events "
            "WHERE task_id = ? AND kind = 'transcript_terminalized'",
            (task_id,),
        ).fetchone()[0]

    assert result.crashed == [task_id]
    assert task is not None and task.status == "ready"
    assert closed_run is not None and closed_run.ended_at is not None
    assert transcript_receipt is not None
    assert receipt_events == 1

    with sqlite3.connect(kanban_home / "state.db") as state_conn:
        transcript = state_conn.execute(
            "SELECT ended_at, end_reason FROM sessions WHERE id = ?",
            (worker_session,),
        ).fetchone()
    assert transcript is not None
    assert transcript[0] is not None
    assert transcript[1] == "kanban_run_terminal"


def test_terminal_run_never_ends_a_non_kanban_session(
    kanban_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bad owner link must fail closed instead of ending user history."""
    session_id = "ordinary-cli-session"
    state = SessionDB(kanban_home / "state.db")
    state.create_session(session_id, source="cli")
    state.close()

    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="source safety")
        assert kb.claim_task(conn, task_id) is not None
        run = kb.latest_run(conn, task_id)
        assert run is not None
        kb.stamp_run_owner_session(conn, run.id, session_id)
        mark_worker_dead(conn, task_id, monkeypatch)
        result = kb.dispatch_once(conn, max_spawn=0)
        closed_run = kb.latest_run(conn, task_id)
        transcript_receipt = conn.execute(
            "SELECT transcript_ended_at FROM task_runs WHERE id = ?",
            (run.id,),
        ).fetchone()[0]

    assert result.crashed == [task_id]
    assert closed_run is not None and closed_run.ended_at is not None
    assert transcript_receipt is None
    with sqlite3.connect(kanban_home / "state.db") as state_conn:
        transcript = state_conn.execute(
            "SELECT ended_at, end_reason FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
    assert transcript == (None, None)


def test_expired_compressed_owner_ends_current_transcript_tip(
    kanban_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = "kanban-compression-parent"
    child = "kanban-compression-tip"
    state = SessionDB(kanban_home / "state.db")
    state.create_session(parent, source="kanban")
    state.end_session(parent, "compression")
    state.create_session(child, source="kanban", parent_session_id=parent)
    state.close()

    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="compressed worker")
        assert kb.claim_task(conn, task_id) is not None
        run = kb.latest_run(conn, task_id)
        assert run is not None
        # Simulate a failed best-effort carry after the worker compressed.
        kb.stamp_run_owner_session(conn, run.id, parent)
        mark_worker_dead(conn, task_id, monkeypatch)
        result = kb.dispatch_once(conn, max_spawn=0)
        receipt = conn.execute(
            "SELECT transcript_ended_at FROM task_runs WHERE id = ?",
            (run.id,),
        ).fetchone()[0]

    assert result.crashed == [task_id]
    assert receipt is not None
    with sqlite3.connect(kanban_home / "state.db") as state_conn:
        rows = dict(
            state_conn.execute(
                "SELECT id, end_reason FROM sessions WHERE id IN (?, ?)",
                (parent, child),
            )
        )
    assert rows[parent] == "compression"
    assert rows[child] == "kanban_run_terminal"


def test_named_profile_transcript_uses_exact_profile_database(
    kanban_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_home = kanban_home / "profiles" / "worker"
    profile_home.mkdir(parents=True)
    session_id = "named-profile-kanban-session"
    state = SessionDB(profile_home / "state.db")
    state.create_session(session_id, source="kanban")
    state.close()

    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="profile routing", assignee="worker")
        assert kb.claim_task(conn, task_id) is not None
        run = kb.latest_run(conn, task_id)
        assert run is not None
        kb.stamp_run_owner_session(conn, run.id, session_id)
        mark_worker_dead(conn, task_id, monkeypatch)
        result = kb.dispatch_once(conn, max_spawn=0)

    assert result.crashed == [task_id]
    with sqlite3.connect(profile_home / "state.db") as state_conn:
        ended_at = state_conn.execute(
            "SELECT ended_at FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()[0]
    assert ended_at is not None


def test_unresolvable_named_profile_never_falls_back_to_default(
    kanban_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collision = "profile-collision-session"
    state = SessionDB(kanban_home / "state.db")
    state.create_session(collision, source="kanban")
    state.close()

    with kb.connect() as conn:
        task_id = kb.create_task(
            conn,
            title="missing profile",
            assignee="missing-profile",
        )
        assert kb.claim_task(conn, task_id) is not None
        run = kb.latest_run(conn, task_id)
        assert run is not None
        kb.stamp_run_owner_session(conn, run.id, collision)
        mark_worker_dead(conn, task_id, monkeypatch)
        result = kb.dispatch_once(conn, max_spawn=0)
        receipt = conn.execute(
            "SELECT transcript_ended_at FROM task_runs WHERE id = ?",
            (run.id,),
        ).fetchone()[0]

    assert result.crashed == [task_id]
    assert receipt is None
    with sqlite3.connect(kanban_home / "state.db") as state_conn:
        ended_at = state_conn.execute(
            "SELECT ended_at FROM sessions WHERE id = ?", (collision,)
        ).fetchone()[0]
    assert ended_at is None


@pytest.mark.parametrize(
    "operation",
    ["archive", "cancel", "reclaim", "reclaim_killed"],
)
def test_nonmaintenance_reclaimed_run_does_not_end_live_transcript(
    kanban_home: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    session_id = f"live-{operation}-worker"
    state = SessionDB(kanban_home / "state.db")
    state.create_session(session_id, source="kanban")
    state.close()

    with kb.connect() as conn:
        task_id = kb.create_task(conn, title=f"live {operation}")
        assert kb.claim_task(conn, task_id) is not None
        run = kb.latest_run(conn, task_id)
        assert run is not None
        kb.stamp_run_owner_session(conn, run.id, session_id)
        if operation == "archive":
            assert kb.archive_task(conn, task_id)
        elif operation == "cancel":
            assert kb.cancel_task(conn, task_id)
        else:
            terminated = operation == "reclaim_killed"
            monkeypatch.setattr(
                kb,
                "_terminate_reclaimed_worker",
                lambda *_args, **_kwargs: {
                    "termination_attempted": True,
                    "host_local": True,
                    "terminated": terminated,
                },
            )
            assert kb.reclaim_task(conn, task_id)
        assert kb.finalize_terminal_run_transcripts(conn) == []
        receipt = conn.execute(
            "SELECT transcript_ended_at FROM task_runs WHERE id = ?",
            (run.id,),
        ).fetchone()[0]

    assert receipt is None
    with sqlite3.connect(kanban_home / "state.db") as state_conn:
        ended_at = state_conn.execute(
            "SELECT ended_at FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()[0]
    assert ended_at is None


def test_completion_metadata_cannot_forge_maintenance_eligibility(
    kanban_home: Path,
) -> None:
    session_id = "normal-completion-worker"
    state = SessionDB(kanban_home / "state.db")
    state.create_session(session_id, source="kanban")
    state.close()

    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="ordinary completion")
        assert kb.claim_task(conn, task_id) is not None
        run = kb.latest_run(conn, task_id)
        assert run is not None
        kb.stamp_run_owner_session(conn, run.id, session_id)
        assert kb.complete_task(
            conn,
            task_id,
            summary="done",
            metadata={"transcript_terminalize": True, "terminated": True},
        )
        assert kb.finalize_terminal_run_transcripts(conn) == []
        eligibility = conn.execute(
            "SELECT transcript_finalize_required FROM task_runs WHERE id = ?",
            (run.id,),
        ).fetchone()[0]

    assert eligibility == 0
    with sqlite3.connect(kanban_home / "state.db") as state_conn:
        ended_at = state_conn.execute(
            "SELECT ended_at FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()[0]
    assert ended_at is None


@pytest.mark.parametrize("case", ["pidless", "refused", "nonlocal"])
def test_unverified_expiry_never_ends_transcript(
    kanban_home: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    session_id = f"unverified-{case}-worker"
    state = SessionDB(kanban_home / "state.db")
    state.create_session(session_id, source="kanban")
    state.close()

    with kb.connect() as conn:
        claimer = "remote-host:123" if case == "nonlocal" else None
        task_id = kb.create_task(conn, title=f"unverified {case}")
        assert kb.claim_task(conn, task_id, claimer=claimer) is not None
        run = kb.latest_run(conn, task_id)
        assert run is not None
        kb.stamp_run_owner_session(conn, run.id, session_id)
        if case != "pidless":
            kb._set_worker_pid(conn, task_id, 9_999_881)
            monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
        if case == "refused":
            monkeypatch.setattr(kb, "_signal_refusal_reason", lambda _pid: "test refusal")
        expired = int(time.time()) - 60
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE tasks SET claim_expires = ? WHERE id = ?",
                (expired, task_id),
            )
            conn.execute(
                "UPDATE task_runs SET claim_expires = ? WHERE id = ?",
                (expired, run.id),
            )
        result = kb.dispatch_once(conn, max_spawn=0)
        eligibility = conn.execute(
            "SELECT transcript_finalize_required, transcript_ended_at "
            "FROM task_runs WHERE id = ?",
            (run.id,),
        ).fetchone()

    assert result.reclaimed == 1
    assert tuple(eligibility) == (0, None)
    with sqlite3.connect(kanban_home / "state.db") as state_conn:
        ended_at = state_conn.execute(
            "SELECT ended_at FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()[0]
    assert ended_at is None


def test_timeout_survivor_is_reconciled_only_after_late_exit(
    kanban_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = "late-timeout-worker"
    fake_pid = 9_999_771
    alive = {"value": True}
    state = SessionDB(kanban_home / "state.db")
    state.create_session(session_id, source="kanban")
    state.close()

    with kb.connect() as conn:
        task_id = kb.create_task(
            conn,
            title="late timeout",
            max_runtime_seconds=1,
        )
        assert kb.claim_task(conn, task_id) is not None
        run = kb.latest_run(conn, task_id)
        assert run is not None
        kb.stamp_run_owner_session(conn, run.id, session_id)
        kb._set_worker_pid(conn, task_id, fake_pid)
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE task_runs SET started_at = ? WHERE id = ?",
                (int(time.time()) - 120, run.id),
            )
        monkeypatch.setattr(kb, "_pid_alive", lambda _pid: alive["value"])
        monkeypatch.setattr(kb.time, "sleep", lambda _seconds: None)
        monkeypatch.setattr(kb, "_signal_refusal_reason", lambda _pid: None)
        monkeypatch.setattr(kb, "_snapshot_worker_descendants", lambda *_a, **_k: [])
        monkeypatch.setattr(kb, "_signal_worker_tree", lambda *_a, **_k: True)
        assert kb.enforce_max_runtime(conn, signal_fn=lambda *_args: None) == [task_id]
        assert kb.finalize_terminal_run_transcripts(conn) == []
        pending = conn.execute(
            "SELECT transcript_finalize_required, transcript_finalize_pid, "
            "       transcript_ended_at FROM task_runs WHERE id = ?",
            (run.id,),
        ).fetchone()
        assert tuple(pending) == (1, fake_pid, None)

        alive["value"] = False
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE task_runs SET transcript_finalize_attempted_at = 0 "
                "WHERE id = ?",
                (run.id,),
            )
        assert kb.finalize_terminal_run_transcripts(conn) == [run.id]

    with sqlite3.connect(kanban_home / "state.db") as state_conn:
        reason = state_conn.execute(
            "SELECT end_reason FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()[0]
    assert reason == "kanban_run_terminal"


def test_timeout_refused_pid_never_becomes_eligible(
    kanban_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = "refused-timeout-worker"
    fake_pid = 9_999_721
    alive = {"value": True}
    state = SessionDB(kanban_home / "state.db")
    state.create_session(session_id, source="kanban")
    state.close()

    with kb.connect() as conn:
        task_id = kb.create_task(
            conn,
            title="refused timeout",
            max_runtime_seconds=1,
        )
        assert kb.claim_task(conn, task_id) is not None
        run = kb.latest_run(conn, task_id)
        assert run is not None
        kb.stamp_run_owner_session(conn, run.id, session_id)
        kb._set_worker_pid(conn, task_id, fake_pid)
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE task_runs SET started_at = ? WHERE id = ?",
                (int(time.time()) - 120, run.id),
            )
        monkeypatch.setattr(kb, "_pid_alive", lambda _pid: alive["value"])
        monkeypatch.setattr(kb, "_signal_refusal_reason", lambda _pid: "identity refused")
        assert kb.enforce_max_runtime(conn, signal_fn=lambda *_args: None) == [task_id]
        eligibility = conn.execute(
            "SELECT transcript_finalize_required, transcript_finalize_pid "
            "FROM task_runs WHERE id = ?",
            (run.id,),
        ).fetchone()
        assert tuple(eligibility) == (0, None)
        alive["value"] = False
        assert kb.finalize_terminal_run_transcripts(conn) == []

    with sqlite3.connect(kanban_home / "state.db") as state_conn:
        ended_at = state_conn.execute(
            "SELECT ended_at FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()[0]
    assert ended_at is None


def test_legacy_crash_evidence_is_reconciled_after_schema_upgrade(
    kanban_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_id = "legacy-crashed-worker"
    state = SessionDB(kanban_home / "state.db")
    state.create_session(session_id, source="kanban")
    state.close()

    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="legacy crash")
        assert kb.claim_task(conn, task_id) is not None
        run = kb.latest_run(conn, task_id)
        assert run is not None
        kb.stamp_run_owner_session(conn, run.id, session_id)
        mark_worker_dead(conn, task_id, monkeypatch, pid=9_999_661)
        assert kb.detect_crashed_workers(conn) == [task_id]
        # Model an upgraded row written before the dedicated eligibility
        # columns existed; the old crash metadata + dead pid are independent
        # evidence, while archive/cancel rows have neither.
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE task_runs SET transcript_finalize_required = 0, "
                "transcript_finalize_pid = NULL WHERE id = ?",
                (run.id,),
            )
        assert kb.finalize_terminal_run_transcripts(conn) == [run.id]

    with sqlite3.connect(kanban_home / "state.db") as state_conn:
        reason = state_conn.execute(
            "SELECT end_reason FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()[0]
    assert reason == "kanban_run_terminal"


def test_busy_state_database_waits_once_for_a_profile(
    kanban_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_ids = [f"busy-worker-{index}" for index in range(4)]
    state = SessionDB(kanban_home / "state.db")
    for session_id in session_ids:
        state.create_session(session_id, source="kanban")
    state.close()

    with kb.connect() as conn:
        for index, session_id in enumerate(session_ids):
            task_id = kb.create_task(conn, title=session_id)
            assert kb.claim_task(conn, task_id) is not None
            run = kb.latest_run(conn, task_id)
            assert run is not None
            kb.stamp_run_owner_session(conn, run.id, session_id)
            mark_worker_dead(
                conn,
                task_id,
                monkeypatch,
                pid=9_999_900 + index,
            )
        # Close all board runs without invoking the post-lock reconciler.
        result = kb._dispatch_once_locked(conn, max_spawn=0)
        assert len(result.crashed) == len(session_ids)

        blocker = sqlite3.connect(kanban_home / "state.db")
        blocker.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        try:
            assert kb.finalize_terminal_run_transcripts(conn) == []
        finally:
            blocker.rollback()
            blocker.close()
        elapsed = time.monotonic() - started
        assert elapsed < 1.0

        with kb.write_txn(conn):
            conn.execute(
                "UPDATE task_runs SET transcript_finalize_attempted_at = 0 "
                "WHERE transcript_ended_at IS NULL"
            )
        assert len(kb.finalize_terminal_run_transcripts(conn)) == len(session_ids)


def test_finalize_backlog_advances_past_first_bounded_batch(
    kanban_home: Path,
) -> None:
    session_ids = [f"backlog-worker-{index}" for index in range(40)]
    state = SessionDB(kanban_home / "state.db")
    for session_id in session_ids:
        state.create_session(session_id, source="kanban")
    state.close()

    run_ids = []
    with kb.connect() as conn:
        for session_id in session_ids:
            task_id = kb.create_task(conn, title=session_id)
            assert kb.claim_task(conn, task_id) is not None
            run = kb.latest_run(conn, task_id)
            assert run is not None
            run_ids.append(run.id)
            kb.stamp_run_owner_session(conn, run.id, session_id)
            with kb.write_txn(conn):
                conn.execute(
                    "UPDATE tasks SET status = 'ready' WHERE id = ?",
                    (task_id,),
                )
                kb._end_run(
                    conn,
                    task_id,
                    outcome="crashed",
                    status="crashed",
                    finalize_transcript=True,
                )

        first = kb.finalize_terminal_run_transcripts(conn)
        second = kb.finalize_terminal_run_transcripts(conn)
        third = kb.finalize_terminal_run_transcripts(conn)

    assert len(first) == kb.MAX_TRANSCRIPT_FINALIZATIONS_PER_TICK
    assert len(second) == len(session_ids) - len(first)
    assert third == []
    assert set(first + second) == set(run_ids)


def test_malformed_legacy_prefix_cannot_starve_valid_candidate(
    kanban_home: Path,
) -> None:
    state = SessionDB(kanban_home / "state.db")
    valid_session_id = "valid-after-poisoned-prefix"
    state.create_session(valid_session_id, source="kanban")
    state.close()

    with kb.connect() as conn:
        for index in range(kb.MAX_TRANSCRIPT_FINALIZATIONS_PER_TICK):
            task_id = kb.create_task(conn, title=f"malformed legacy {index}")
            assert kb.claim_task(conn, task_id) is not None
            run = kb.latest_run(conn, task_id)
            assert run is not None
            kb.stamp_run_owner_session(conn, run.id, f"missing-{index}")
            with kb.write_txn(conn):
                conn.execute(
                    "UPDATE tasks SET status = 'ready' WHERE id = ?",
                    (task_id,),
                )
                kb._end_run(
                    conn,
                    task_id,
                    outcome="crashed",
                    status="crashed",
                    metadata="[",
                )

        valid_task_id = kb.create_task(conn, title="valid candidate")
        assert kb.claim_task(conn, valid_task_id) is not None
        valid_run = kb.latest_run(conn, valid_task_id)
        assert valid_run is not None
        kb.stamp_run_owner_session(conn, valid_run.id, valid_session_id)
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE tasks SET status = 'ready' WHERE id = ?",
                (valid_task_id,),
            )
            kb._end_run(
                conn,
                valid_task_id,
                outcome="crashed",
                status="crashed",
                finalize_transcript=True,
            )

        assert kb.finalize_terminal_run_transcripts(conn) == []
        assert kb.finalize_terminal_run_transcripts(conn) == [valid_run.id]
        malformed_attempts = conn.execute(
            "SELECT COUNT(*) FROM task_runs "
            "WHERE metadata = ? AND transcript_finalize_attempted_at IS NOT NULL",
            ('"["',),
        ).fetchone()[0]

    assert malformed_attempts == kb.MAX_TRANSCRIPT_FINALIZATIONS_PER_TICK
    with sqlite3.connect(kanban_home / "state.db") as state_conn:
        ended_at = state_conn.execute(
            "SELECT ended_at FROM sessions WHERE id = ?",
            (valid_session_id,),
        ).fetchone()[0]
    assert ended_at is not None
