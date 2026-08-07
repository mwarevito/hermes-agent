"""Background-execution reliability: process ownership and delivery addressing.

Both classes of defect were measured on the 2026-08-06 Givi incident and both
were invisible to the existing suites, so they are pinned here.

1. The kanban timeout reaper signalled ``worker_pid`` alone while workers are
   spawned with ``start_new_session=True``. A Claude Code run started by the
   worker outlived it, kept holding the launcher's lane slot, and the task went
   straight back to ``ready`` — so the retry queued behind a corpse and timed
   out too. B blocked -> C timed_out -> C retry timed_out -> B retry timed_out.

2. Five cards on one board in one hour got three different delivery outcomes:
   no subscription, ``platform='tui'`` whose chat_id encoded a real Telegram DM
   topic, and a correct row. Nothing could deliver the tui rows and nothing
   complained — the silence Vito saw.
"""
import os
import signal
import subprocess
import sys
import time

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli.kanban_db import _signal_worker_tree

LIVE_KEY = "agent:main:telegram:dm:405154434:702308"  # the exact key that was lost


def _alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError, OSError):
        return False


def _worker_with_child():
    """A 'worker' that starts a long-lived child, like claude-hermes does."""
    proc = subprocess.Popen(
        [sys.executable, "-c",
         "import subprocess,time\n"
         "c=subprocess.Popen(['sleep','120'])\n"
         "print(c.pid, flush=True)\n"
         "time.sleep(120)\n"],
        stdout=subprocess.PIPE, text=True,
        start_new_session=True,          # exactly what the dispatcher uses
    )
    child = int(proc.stdout.readline().strip())
    time.sleep(0.3)
    return proc, child


# ── process ownership ───────────────────────────────────────────────────────


# These two genuinely deliver signals: the whole point is which processes a
# signal reaches. conftest's live-system guard sanctions exactly this case.
# Everything signalled is spawned by the test itself — the grandchild only looks
# foreign to the guard because start_new_session detaches it, which is the very
# property under test.
@pytest.mark.live_system_guard_bypass
@pytest.mark.skipif(not hasattr(os, "killpg"), reason="POSIX process groups only")
def test_group_kill_takes_the_child_with_the_worker():
    proc, child = _worker_with_child()
    try:
        assert os.getpgid(proc.pid) == proc.pid, "воркер должен вести свою группу"
        assert _alive(child)
        assert _signal_worker_tree(proc.pid, signal.SIGKILL) is True
        proc.wait(timeout=5)
        time.sleep(0.5)
        assert not _alive(child), (
            "ребёнок обязан умереть вместе с воркером — иначе он остаётся "
            "держать полосу Claude и следующая попытка гарантированно встаёт в очередь"
        )
    finally:
        for p in (child,):
            try:
                os.kill(p, signal.SIGKILL)
            except OSError:
                pass


@pytest.mark.live_system_guard_bypass
@pytest.mark.skipif(not hasattr(os, "killpg"), reason="POSIX process groups only")
def test_single_pid_kill_leaves_an_orphan_which_is_why_the_fix_exists():
    """The old behaviour, pinned so nobody 'simplifies' back to it."""
    proc, child = _worker_with_child()
    try:
        os.kill(proc.pid, signal.SIGKILL)      # what the reaper used to do
        proc.wait(timeout=5)
        assert _alive(child), "одиночный kill оставляет сироту — это и был баг"
    finally:
        try:
            os.kill(child, signal.SIGKILL)
        except OSError:
            pass


def test_never_signals_a_group_we_do_not_lead():
    """The safety property. Under an in-gateway dispatcher a worker that is not
    its own group leader shares the GATEWAY's group; killpg there would take the
    supervisor down with the worker."""
    calls = {"kill": [], "killpg": []}
    real = os.getpgid
    try:
        os.getpgid = lambda p: 999                       # somebody else's group
        res = _signal_worker_tree(
            4242, signal.SIGTERM,
            kill=lambda p, s: calls["kill"].append((p, s)),
            killpg=lambda g, s: calls["killpg"].append((g, s)),
        )
        assert calls["killpg"] == [], "нельзя сигналить чужую группу"
        assert calls["kill"] == [(4242, signal.SIGTERM)]
        assert res is False
    finally:
        os.getpgid = real


def test_missing_process_does_not_raise():
    _signal_worker_tree(4242424, signal.SIGTERM)          # must not raise


# ── delivery addressing ─────────────────────────────────────────────────────


class _Env:
    def __init__(self, **kw):
        self.vals = kw

    def __call__(self, name, default=""):
        return self.vals.get(name, default)


def _subscribe(conn, task_id, env=None, kanban_task=None, session_key=None,
               monkeypatch=None):
    import gateway.session_context as sctx
    import tools.kanban_tools as kt
    monkeypatch.setattr(sctx, "get_session_env", env or _Env())
    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    monkeypatch.delenv("HERMES_SESSION_KEY", raising=False)
    if kanban_task:
        monkeypatch.setenv("HERMES_KANBAN_TASK", kanban_task)
    if session_key:
        monkeypatch.setenv("HERMES_SESSION_KEY", session_key)
    return kt._maybe_auto_subscribe(conn, task_id)


def _rows(conn, task_id):
    return conn.execute(
        "SELECT platform, chat_id, thread_id, chat_type, delivery_metadata "
        "FROM kanban_notify_subs WHERE task_id = ?", (task_id,)).fetchall()


def test_session_key_encoding_a_gateway_address_is_not_a_tui_key(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "d1.db"))
    kb.init_db()
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="ребёнок", assignee="default")
        assert _subscribe(conn, tid, env=_Env(HERMES_SESSION_KEY=LIVE_KEY),
                          session_key=LIVE_KEY, monkeypatch=monkeypatch)
        row = _rows(conn, tid)[0]
        assert row["platform"] == "telegram", "адрес был в ключе — его нельзя терять"
        assert row["chat_id"] == "405154434"
        assert row["thread_id"] == "702308"
        assert row["chat_type"] == "dm"
    finally:
        conn.close()


@pytest.mark.parametrize("key", ["herm-desktop-9f3a", "agent:main:tui:local:1:2"])
def test_a_real_tui_key_still_registers_as_tui(tmp_path, monkeypatch, key):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "d2.db"))
    kb.init_db()
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="локальная", assignee="default")
        _subscribe(conn, tid, env=_Env(HERMES_SESSION_KEY=key),
                   session_key=key, monkeypatch=monkeypatch)
        assert _rows(conn, tid)[0]["platform"] == "tui"
    finally:
        conn.close()


def test_worker_inherits_the_address_of_the_card_it_is_running(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "d3.db"))
    kb.init_db()
    conn = kb.connect()
    try:
        parent = kb.create_task(conn, title="родитель", assignee="default")
        kb.add_notify_sub(conn, task_id=parent, platform="telegram",
                          chat_id="405154434", chat_type="dm", thread_id="702308",
                          user_id="405154434", notifier_profile="default")
        child = kb.create_task(conn, title="ребёнок", assignee="default")
        assert _subscribe(conn, child, env=_Env(), kanban_task=parent,
                          monkeypatch=monkeypatch), (
            "воркер не должен создавать карточку без адреса доставки"
        )
        row = _rows(conn, child)[0]
        assert (row["platform"], row["chat_id"], row["thread_id"]) == (
            "telegram", "405154434", "702308")
    finally:
        conn.close()


def test_a_dead_tui_parent_row_is_not_propagated(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "d4.db"))
    kb.init_db()
    conn = kb.connect()
    try:
        parent = kb.create_task(conn, title="родитель-tui", assignee="default")
        kb.add_notify_sub(conn, task_id=parent, platform="tui", chat_id="k",
                          notifier_profile="default")
        child = kb.create_task(conn, title="ребёнок", assignee="default")
        assert not _subscribe(conn, child, env=_Env(), kanban_task=parent,
                              monkeypatch=monkeypatch)
        assert _rows(conn, child) == [], "недоставляемую строку не размножаем"
    finally:
        conn.close()


def test_inherited_subscription_keeps_its_topic_routing(tmp_path, monkeypatch):
    """Inheriting the address but dropping delivery_metadata sends a DM-topic
    result into the root DM — a wrong destination, worse than none."""
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "d5.db"))
    kb.init_db()
    conn = kb.connect()
    try:
        parent = kb.create_task(conn, title="p", assignee="default")
        kb.add_notify_sub(
            conn, task_id=parent, platform="telegram", chat_id="405154434",
            chat_type="dm", thread_id="702308", user_id="405154434",
            notifier_profile="default",
            delivery_metadata={"direct_messages_topic_id": "702308"},
        )
        child = kb.create_task(conn, title="c", assignee="default")
        kb._inherit_notify_subs(conn, child, (parent,))
        row = _rows(conn, child)[0]
        assert row["chat_type"] == "dm"
        assert row["delivery_metadata"] and "702308" in row["delivery_metadata"]
    finally:
        conn.close()


@pytest.mark.parametrize("bad", [
    "", "nonsense", "agent:main", "agent:main:telegram", "agent:main:telegram:dm",
    "agent:main:telegram:dm:", "notagent:main:telegram:dm:1:2",
])
def test_garbage_never_becomes_a_delivery_address(bad):
    import tools.kanban_tools as kt
    assert kt._decode_session_key(bad) is None
