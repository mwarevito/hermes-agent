"""Чекпоинт продолжения: воркер не начинает retry с нуля.

Замер 06.08: перезапущенная карточка грузила скиллы 9 и 8 раз и гоняла гейт
7-8 раз, тогда как единственная сработавшая делала то и другое по разу.
Пережить рестарт было нечему — каждый retry начинался с нуля.

⛑ Главный инвариант здесь — НЕ функциональность, а то, что чекпоинт не
подменяет собой хартбит. `checkpoint` имеет `kind != 'heartbeat'`, поэтому
наивно засчитался бы прогрессом и заглушил DET-E — единственный детектор,
построенный под инцидент 06.08 (30 минут зелёных хартбитов и молчаливый
таймаут). Контракт: чекпоинт НЕ трогает last_heartbeat_at, а вотчдог считает
его движением только при СМЕНЕ step_key.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb


@pytest.fixture
def kanban_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


def _running(conn, title="t"):
    tid = kb.create_task(conn, title=title, assignee="worker")
    with kb.write_txn(conn):
        conn.execute("UPDATE tasks SET status='ready' WHERE id=?", (tid,))
    assert kb.claim_task(conn, tid, claimer="worker") is not None
    return tid


def _events(conn, tid, kind):
    return conn.execute(
        "SELECT payload FROM task_events WHERE task_id=? AND kind=? ORDER BY id",
        (tid, kind),
    ).fetchall()


def test_checkpoint_persists_step_and_appends_event(kanban_home):
    with kb.connect_closing() as conn:
        tid = _running(conn)
        assert kb.record_checkpoint(conn, tid, step_key="repo-cloned") is True
        row = conn.execute("SELECT current_step_key FROM tasks WHERE id=?", (tid,)).fetchone()
        assert row["current_step_key"] == "repo-cloned"
        evs = _events(conn, tid, "checkpoint")
        assert len(evs) == 1
        assert json.loads(evs[0]["payload"])["step_key"] == "repo-cloned"


def test_checkpoint_is_not_a_heartbeat(kanban_home):
    """Контракт с DET-E: чекпоинт не продлевает признак жизни."""
    with kb.connect_closing() as conn:
        tid = _running(conn)
        before = conn.execute(
            "SELECT last_heartbeat_at FROM tasks WHERE id=?", (tid,)).fetchone()["last_heartbeat_at"]
        assert kb.record_checkpoint(conn, tid, step_key="tests-green") is True
        after = conn.execute(
            "SELECT last_heartbeat_at FROM tasks WHERE id=?", (tid,)).fetchone()["last_heartbeat_at"]
        assert after == before, "чекпоинт не должен изображать хартбит"
        assert _events(conn, tid, "heartbeat") == []


def test_checkpoint_event_kind_is_stable(kanban_home):
    """Имя вида события — часть контракта с вотчдогом, а не деталь."""
    with kb.connect_closing() as conn:
        tid = _running(conn)
        kb.record_checkpoint(conn, tid, step_key="phase-1")
        kinds = [r["kind"] for r in conn.execute(
            "SELECT kind FROM task_events WHERE task_id=?", (tid,)).fetchall()]
        assert "checkpoint" in kinds


def test_checkpoint_refused_when_not_running(kanban_home):
    with kb.connect_closing() as conn:
        tid = kb.create_task(conn, title="t", assignee="worker")  # остаётся todo
        assert kb.record_checkpoint(conn, tid, step_key="x") is False
        assert _events(conn, tid, "checkpoint") == []


def test_checkpoint_refused_on_run_id_mismatch(kanban_home):
    with kb.connect_closing() as conn:
        tid = _running(conn)
        assert kb.record_checkpoint(
            conn, tid, step_key="x", expected_run_id=999999) is False


def test_empty_step_key_is_refused(kanban_home):
    with kb.connect_closing() as conn:
        tid = _running(conn)
        assert kb.record_checkpoint(conn, tid, step_key="   ") is False
        assert kb.record_checkpoint(conn, tid, step_key="") is False


def test_step_key_is_normalised_and_capped(kanban_home):
    with kb.connect_closing() as conn:
        tid = _running(conn)
        kb.record_checkpoint(conn, tid, step_key="  many   spaces  here ")
        row = conn.execute("SELECT current_step_key FROM tasks WHERE id=?", (tid,)).fetchone()
        assert row["current_step_key"] == "many spaces here"
        kb.record_checkpoint(conn, tid, step_key="z" * 200)
        row = conn.execute("SELECT current_step_key FROM tasks WHERE id=?", (tid,)).fetchone()
        assert len(row["current_step_key"]) == 64


def test_missing_artifacts_are_dropped_not_trusted(kanban_home, tmp_path):
    """Путь, которого нет, ронять обязательно: воркспейс чистится rmtree."""
    real = tmp_path / "proof.txt"
    real.write_text("ok")
    with kb.connect_closing() as conn:
        tid = _running(conn)
        kb.record_checkpoint(
            conn, tid, step_key="built",
            artifacts=[str(real), str(tmp_path / "gone.txt"), "relative/path.txt"])
        payload = json.loads(_events(conn, tid, "checkpoint")[0]["payload"])
        assert payload["artifacts"] == [str(real)], payload["artifacts"]


def test_worker_context_shows_checkpoint_of_a_finished_run(kanban_home, tmp_path):
    art = tmp_path / "artifact.md"
    art.write_text("x")
    with kb.connect_closing() as conn:
        tid = _running(conn)
        kb.record_checkpoint(conn, tid, step_key="repo-cloned", artifacts=[str(art)])
        # завершаем прогон и поднимаем задачу заново — это и есть retry
        assert kb.reclaim_task(conn, tid, reason="retry") is True
        assert kb.claim_task(conn, tid, claimer="worker") is not None
        ctx = kb.build_worker_context(conn, tid)
        assert "## Resume checkpoint" in ctx
        assert "repo-cloned" in ctx
        assert str(art) in ctx


def test_worker_context_hides_own_live_checkpoint(kanban_home):
    """Свой же чекпоинт текущего прогона — не новость для самого себя."""
    with kb.connect_closing() as conn:
        tid = _running(conn)
        kb.record_checkpoint(conn, tid, step_key="phase-1")
        ctx = kb.build_worker_context(conn, tid)
        assert "## Resume checkpoint" not in ctx


def test_worker_context_unchanged_without_checkpoints(kanban_home):
    with kb.connect_closing() as conn:
        tid = _running(conn)
        ctx = kb.build_worker_context(conn, tid)
        assert "Resume checkpoint" not in ctx


def test_worker_context_says_so_when_artifacts_vanished(kanban_home, tmp_path):
    art = tmp_path / "temp.md"
    art.write_text("x")
    with kb.connect_closing() as conn:
        tid = _running(conn)
        kb.record_checkpoint(conn, tid, step_key="built", artifacts=[str(art)])
        assert kb.reclaim_task(conn, tid, reason="retry") is True
        assert kb.claim_task(conn, tid, claimer="worker") is not None
        art.unlink()                      # воркспейс подчистили между прогонами
        ctx = kb.build_worker_context(conn, tid)
        assert "## Resume checkpoint" in ctx
        assert "no longer exist" in ctx or "не существу" in ctx or "unverified" in ctx
